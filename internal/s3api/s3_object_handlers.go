package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/wpnpeiris/nats-s3/internal/client"
	"github.com/wpnpeiris/nats-s3/internal/model"
)

// PrefixEntry represents a common prefix in S3 list results.
type PrefixEntry struct {
	Prefix string `xml:"Prefix"`
}

// ListBucketResult is a minimal representation of S3's ListBucket result.
type ListBucketResult struct {
	IsTruncated    bool          `xml:"IsTruncated"`
	Contents       []s3.Object   `xml:"Contents"`
	Name           string        `xml:"Name"`
	Prefix         string        `xml:"Prefix"`
	Delimiter      string        `xml:"Delimiter,omitempty"`
	MaxKeys        int           `xml:"MaxKeys"`
	CommonPrefixes []PrefixEntry `xml:"CommonPrefixes,omitempty"`
}

// CopyObjectResult is a compact response shape used by some S3 clients
// to acknowledge a successful object write/copy with an ETag.
type CopyObjectResult struct {
	ETag           string    `xml:"ETag"`
	LastModified   time.Time `xml:"LastModified"`
	ChecksumCRC32  string    `xml:"ChecksumCRC32"`
	ChecksumCRC32C string    `xml:"ChecksumCRC32C"`
	ChecksumSHA1   string    `xml:"ChecksumSHA1"`
	ChecksumSHA256 string    `xml:"ChecksumSHA256"`
}

// DeleteRequest represents the S3 DeleteObjects request structure.
type DeleteRequest struct {
	XMLName xml.Name         `xml:"Delete"`
	Objects []ObjectToDelete `xml:"Object"`
	Quiet   bool             `xml:"Quiet"`
}

// DeleteResult represents the S3 DeleteObjects response structure.
type DeleteResult struct {
	XMLName xml.Name        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult"`
	Deleted []DeletedObject `xml:"Deleted,omitempty"`
	Error   []DeleteError   `xml:"Error,omitempty"`
}

// DeleteError represents an error during object deletion.
type DeleteError struct {
	Key       string `xml:"Key"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	VersionId string `xml:"VersionId,omitempty"`
}

// ObjectToDelete represents an object to be deleted.
type ObjectToDelete struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

// DeletedObject represents a successfully deleted object.
type DeletedObject struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

// CopyObject performs a server-side copy of an object from source to destination.
func (s *S3Gateway) CopyObject(w http.ResponseWriter, r *http.Request) {
	destBucket := mux.Vars(r)["bucket"]
	destKey := mux.Vars(r)["key"]

	copySourceHeader := r.Header.Get("x-amz-copy-source")
	sourceBucket, sourceKey, err := parseCopySource(copySourceHeader)
	if err != nil {
		model.WriteErrorResponse(w, r, model.ErrInvalidCopySource)
		return
	}

	log.Printf("CopyObject from %s/%s to %s/%s", sourceBucket, sourceKey, destBucket, destKey)

	// Get source object
	sourceObj, sourceData, err := s.client.GetObject(r.Context(), sourceBucket, sourceKey)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchKey)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	// Determine metadata handling based on x-amz-metadata-directive
	contentType, metadata := determineMetadataForCopy(r, sourceObj)

	// Put object at destination
	destInfo, err := s.client.PutObject(r.Context(), destBucket, destKey, contentType, metadata, sourceData)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	// Return CopyObjectResult XML response
	result := CopyObjectResult{
		ETag:         formatETag(destInfo.Digest),
		LastModified: destInfo.ModTime,
	}

	model.WriteXMLResponse(w, r, http.StatusOK, result)
}

// DeleteObject deletes the specified object and responds with 204 No Content.
func (s *S3Gateway) DeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	err := s.client.DeleteObject(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchKey)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	model.WriteEmptyResponse(w, r, http.StatusNoContent)
}

// DeleteObjects handles batch deletion of multiple objects in a single request.
func (s *S3Gateway) DeleteObjects(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]

	// Parse request body
	var deleteReq DeleteRequest
	if err := xml.NewDecoder(r.Body).Decode(&deleteReq); err != nil {
		model.WriteErrorResponse(w, r, model.ErrMalformedXML)
		return
	}

	// Process each object deletion
	var deleted []DeletedObject
	var deleteErrors []DeleteError

	for _, obj := range deleteReq.Objects {
		err := s.client.DeleteObject(r.Context(), bucket, obj.Key)
		if err != nil {
			// Record error
			code := "InternalError"
			message := err.Error()

			if errors.Is(err, client.ErrBucketNotFound) {
				code = "NoSuchBucket"
				message = "The specified bucket does not exist"
			} else if errors.Is(err, client.ErrObjectNotFound) {
				// S3 considers deleting non-existent object as success
				deleted = append(deleted, DeletedObject{Key: obj.Key})
				continue
			}

			deleteErrors = append(deleteErrors, DeleteError{
				Key:     obj.Key,
				Code:    code,
				Message: message,
			})
		} else {
			// Successfully deleted
			deleted = append(deleted, DeletedObject{Key: obj.Key})
		}
	}

	// Build response
	result := DeleteResult{
		Deleted: deleted,
		Error:   deleteErrors,
	}

	// If Quiet mode, only return errors
	if deleteReq.Quiet {
		result.Deleted = nil
	}

	model.WriteXMLResponse(w, r, http.StatusOK, result)
}

// Download writes object content to the response and sets typical S3 headers
// such as Last-Modified, ETag, Content-Type, and Content-Length.
func (s *S3Gateway) Download(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	// If key is empty, return error
	if key == "" {
		model.WriteErrorResponse(w, r, model.ErrInvalidRequest)
		return
	}

	info, data, err := s.client.GetObject(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchKey)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	// Set common headers
	if info != nil {
		updateLastModifiedHeader(info, w)
		updateETagHeader(info, w)
		updateContentTypeHeaders(info, w)
	}

	// Check for Range header
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		// Parse and handle range request
		start, end, err := parseRangeHeader(rangeHeader, len(data))
		if err != nil {
			// Invalid range - return 416 Range Not Satisfiable
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}

		// Return partial content
		rangeData := data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(rangeData)))
		w.WriteHeader(http.StatusPartialContent)

		_, err = w.Write(rangeData)
		if err != nil {
			log.Printf("Error writing range response body for %s/%s: %s", bucket, key, err)
		}
		return
	}

	// No range header - return full content
	if info != nil {
		updateContentLength(info, w)
	}

	_, err = w.Write(data)
	if err != nil {
		log.Printf("Error writing response body for %s/%s: %s", bucket, key, err)
		return
	}
}

// GetObjectRetention returns object retention configuration (mode and retain-until-date)
func (s *S3Gateway) GetObjectRetention(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	log.Printf("GetObjectRetention: bucket=%s key=%s", bucket, key)

	mode, retainUntilDate, err := s.client.GetObjectRetention(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			// No retention configuration exists
			model.WriteErrorResponse(w, r, model.ErrNoSuchKey)
			return
		}
		log.Printf("Error getting object retention: %v", err)
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	response := model.ObjectRetentionResponse{
		Mode:            mode,
		RetainUntilDate: retainUntilDate,
	}

	model.WriteXMLResponse(w, r, http.StatusOK, response)
}

// HeadObject writes object metadata headers without a response body.
func (s *S3Gateway) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	res, err := s.client.GetObjectInfo(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchKey)
			return
		}

		http.Error(w, "Object not found in the bucket", http.StatusNotFound)
		return
	}

	log.Printf("Head object %s/%s", bucket, key)
	if res != nil {
		updateLastModifiedHeader(res, w)
		updateContentLength(res, w)
		updateETagHeader(res, w)
		updateContentTypeHeaders(res, w)
		updateMetadataHeaders(res, w)
	}
}

// ListObjects returns objects in a bucket as a simple S3-compatible XML list.
func (s *S3Gateway) ListObjects(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	delimiter := r.URL.Query().Get("delimiter")
	prefix := r.URL.Query().Get("prefix")

	log.Println("List Objects in bucket", bucket)

	res, err := s.client.ListObjects(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			model.WriteEmptyResponse(w, r, http.StatusOK)
			return
		}

		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	var contents []s3.Object
	var commonPrefixes []PrefixEntry

	if delimiter != "" {
		// Group objects by common prefix when delimiter is specified
		contents, commonPrefixes = groupObjectsByDelimiter(res, prefix, delimiter)
	} else {
		// No delimiter: return all objects as Contents
		contents = objectsToContents(res, prefix)
	}

	xmlResponse := ListBucketResult{
		IsTruncated:    false,
		Contents:       contents,
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		MaxKeys:        1000,
		CommonPrefixes: commonPrefixes,
	}

	err = xml.NewEncoder(w).Encode(xmlResponse)
	if err != nil {
		log.Printf("Error enconding the response, %s", err)
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}
}

// UpdateObjectRetention sets object retention configuration (mode and retain-until-date)
func (s *S3Gateway) UpdateObjectRetention(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	log.Printf("PutObjectRetention: bucket=%s key=%s", bucket, key)

	// Parse retention configuration from request body
	var retention model.ObjectRetention
	if err := xml.NewDecoder(r.Body).Decode(&retention); err != nil {
		log.Printf("Error decoding retention XML: %v", err)
		model.WriteErrorResponse(w, r, model.ErrMalformedXML)
		return
	}

	// Validate required fields
	if retention.Mode == nil || *retention.Mode == "" {
		log.Printf("Missing retention mode")
		model.WriteErrorResponse(w, r, model.ErrMalformedXML)
		return
	}
	if retention.RetainUntilDate == nil || *retention.RetainUntilDate == "" {
		log.Printf("Missing retain until date")
		model.WriteErrorResponse(w, r, model.ErrMalformedXML)
		return
	}

	// Validate mode is either GOVERNANCE or COMPLIANCE
	mode := *retention.Mode
	if mode != "GOVERNANCE" && mode != "COMPLIANCE" {
		log.Printf("Invalid retention mode: %s", mode)
		model.WriteErrorResponse(w, r, model.ErrMalformedXML)
		return
	}

	// Validate date format (ISO 8601)
	retainUntilDate := *retention.RetainUntilDate
	_, err := time.Parse(time.RFC3339, retainUntilDate)
	if err != nil {
		log.Printf("Invalid retention date format: %s (error: %v)", retainUntilDate, err)
		model.WriteErrorResponse(w, r, model.ErrMalformedXML)
		return
	}

	// Set retention in NATS object metadata
	err = s.client.PutObjectRetention(r.Context(), bucket, key, mode, retainUntilDate)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchKey)
			return
		}
		log.Printf("Error setting object retention: %v", err)
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// Upload stores an object and responds with 200 and an ETag header.
func (s *S3Gateway) Upload(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	// S3-compatible size limit: 5GB for single PUT operation
	const maxSinglePutSize = 5 * 1024 * 1024 * 1024

	// Validate Content-Length to prevent DoS attacks
	if r.ContentLength < 0 {
		model.WriteErrorResponse(w, r, model.ErrMissingFields)
		return
	}
	if r.ContentLength > maxSinglePutSize {
		model.WriteErrorResponse(w, r, model.ErrEntityTooLarge)
		return
	}

	// Use LimitReader as defense-in-depth to ensure we never read more than maxSinglePutSize
	limitedBody := io.LimitReader(r.Body, maxSinglePutSize+1)
	body, err := io.ReadAll(limitedBody)
	if err != nil {
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	// Additional check: if we read more than expected, reject the request
	if int64(len(body)) > maxSinglePutSize {
		model.WriteErrorResponse(w, r, model.ErrEntityTooLarge)
		return
	}

	contentType := extractContentType(r)
	meta := extractMetadata(r)

	log.Println("Upload to", bucket, "with key", key, " with content-type", contentType, " with user-meta", meta)
	res, err := s.client.PutObject(r.Context(), bucket, key, contentType, meta, body)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}
	if res.Digest != "" {
		w.Header().Set("ETag", formatETag(res.Digest))
	}
	model.WriteEmptyResponse(w, r, http.StatusOK)
}

// determineMetadataForCopy determines which metadata to use for the destination object
// based on the x-amz-metadata-directive header (COPY or REPLACE).
// Returns the content type and metadata map to use for the destination.
func determineMetadataForCopy(r *http.Request, sourceObj *jetstream.ObjectInfo) (contentType string, metadata map[string]string) {
	metadataDirective := r.Header.Get("x-amz-metadata-directive")
	if metadataDirective == "" {
		metadataDirective = "COPY" // Default to COPY
	}

	if metadataDirective == "REPLACE" {
		// Use metadata from the request
		contentType = extractContentType(r)
		metadata = extractMetadata(r)
	} else {
		// Copy metadata from source object
		contentType = ""
		if sourceObj.Headers != nil {
			if cts, ok := sourceObj.Headers["Content-Type"]; ok && len(cts) > 0 {
				contentType = cts[0]
			}
		}
		metadata = sourceObj.Metadata
	}

	return contentType, metadata
}

// extractContentType returns request Header value of "Content-Type"
func extractContentType(r *http.Request) string {
	return r.Header.Get("Content-Type")
}

// extractMetadata returns request Header value of "x-amz-meta-"
func extractMetadata(r *http.Request) map[string]string {
	meta := map[string]string{}
	for name, vals := range r.Header {
		ln := strings.ToLower(name)
		// Extract user metadata (x-amz-meta-*)
		if strings.HasPrefix(ln, "x-amz-meta-") {
			meta[ln] = strings.Join(vals, ",")
		}
		// Extract object retention headers if present during PUT
		if ln == "x-amz-object-lock-mode" || ln == "x-amz-object-lock-retain-until-date" {
			meta[ln] = strings.Join(vals, ",")
		}
	}
	return meta
}

// formatETag wraps a digest string in quotes to create an S3-compatible ETag value.
func formatETag(digest string) string {
	return fmt.Sprintf("\"%s\"", digest)
}

// groupObjectsByDelimiter groups objects by common prefix when a delimiter is specified.
func groupObjectsByDelimiter(objects []*jetstream.ObjectInfo, prefix, delimiter string) ([]s3.Object, []PrefixEntry) {
	var contents []s3.Object
	prefixMap := make(map[string]bool) // Track unique prefixes

	for _, obj := range objects {
		key := obj.Name

		// Filter by prefix if specified
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}

		// Remove the prefix to get the remaining path
		remainingPath := key
		if prefix != "" {
			remainingPath = strings.TrimPrefix(key, prefix)
		}

		// Split by delimiter to check if delimiter exists in remaining path
		parts := strings.SplitN(remainingPath, delimiter, 2)

		if len(parts) == 2 {
			// Delimiter found - add to common prefixes
			commonPrefix := prefix + parts[0] + delimiter
			prefixMap[commonPrefix] = true
		} else {
			// No delimiter - add to contents
			etag := ""
			if obj.Digest != "" {
				etag = formatETag(obj.Digest)
			}
			contents = append(contents, s3.Object{
				ETag:         aws.String(etag),
				Key:          aws.String(key),
				LastModified: aws.Time(obj.ModTime),
				Size:         aws.Int64(int64(obj.Size)),
				StorageClass: aws.String(""),
			})
		}
	}

	// Convert prefix map to sorted slice
	var commonPrefixes []PrefixEntry
	for p := range prefixMap {
		commonPrefixes = append(commonPrefixes, PrefixEntry{Prefix: p})
	}

	return contents, commonPrefixes
}

// parseRangeHeader parses an HTTP Range header and returns start and end byte positions.
// Format: "bytes=start-end" or "bytes=start-" or "bytes=-suffix"
// Returns inclusive start and end positions (both zero-indexed).
func parseRangeHeader(rangeHeader string, contentLength int) (start, end int, err error) {
	// Remove "bytes=" prefix
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range header format")
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range specification")
	}

	// Handle different range formats
	if parts[0] == "" {
		// Suffix range: bytes=-500 (last 500 bytes)
		suffix, err := strconv.Atoi(parts[1])
		if err != nil || suffix <= 0 {
			return 0, 0, fmt.Errorf("invalid suffix range")
		}
		start = contentLength - suffix
		if start < 0 {
			start = 0
		}
		end = contentLength - 1
	} else if parts[1] == "" {
		// Open-ended range: bytes=500- (from 500 to end)
		start, err = strconv.Atoi(parts[0])
		if err != nil || start < 0 {
			return 0, 0, fmt.Errorf("invalid start position")
		}
		if start >= contentLength {
			return 0, 0, fmt.Errorf("start position beyond content length")
		}
		end = contentLength - 1
	} else {
		// Normal range: bytes=0-499
		start, err = strconv.Atoi(parts[0])
		if err != nil || start < 0 {
			return 0, 0, fmt.Errorf("invalid start position")
		}
		end, err = strconv.Atoi(parts[1])
		if err != nil || end < start {
			return 0, 0, fmt.Errorf("invalid end position")
		}
		// Clamp end to content length
		if end >= contentLength {
			end = contentLength - 1
		}
		if start >= contentLength {
			return 0, 0, fmt.Errorf("start position beyond content length")
		}
	}

	return start, end, nil
}

// objectsToContents converts NATS ObjectInfo objects to S3 Object format.
// Optionally filters by prefix if specified.
func objectsToContents(objects []*jetstream.ObjectInfo, prefix string) []s3.Object {
	var contents []s3.Object
	for _, obj := range objects {
		// Filter by prefix if specified
		if prefix != "" && !strings.HasPrefix(obj.Name, prefix) {
			continue
		}

		etag := ""
		if obj.Digest != "" {
			etag = formatETag(obj.Digest)
		}
		contents = append(contents, s3.Object{
			ETag:         aws.String(etag),
			Key:          aws.String(obj.Name),
			LastModified: aws.Time(obj.ModTime),
			Size:         aws.Int64(int64(obj.Size)),
			StorageClass: aws.String(""),
		})
	}
	return contents
}

// parseCopySource extracts the source bucket and key from the x-amz-copy-source header.
// The header format can be "/sourcebucket/sourcekey" or "sourcebucket/sourcekey".
// Returns the source bucket, source key, and an error if the format is invalid.
func parseCopySource(copySourceHeader string) (bucket, key string, err error) {
	if copySourceHeader == "" {
		return "", "", errors.New("x-amz-copy-source header is empty")
	}

	// Remove leading slash if present
	copySource := strings.TrimPrefix(copySourceHeader, "/")
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid x-amz-copy-source format: expected bucket/key")
	}

	return parts[0], parts[1], nil
}

// updateContentLength writes 'Content-Length' header in response
func updateContentLength(obj *jetstream.ObjectInfo, w http.ResponseWriter) {
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
}

// updateContentTypeHeaders writes 'Content-Type' header in response
func updateContentTypeHeaders(obj *jetstream.ObjectInfo, w http.ResponseWriter) {
	if obj != nil && obj.Headers != nil {
		if cts, ok := obj.Headers["Content-Type"]; ok && len(cts) > 0 && cts[0] != "" {
			w.Header().Set("Content-Type", cts[0])
		}
	}
}

// updateETagHeader writes 'ETag' header in response
func updateETagHeader(obj *jetstream.ObjectInfo, w http.ResponseWriter) {
	if obj.Digest != "" {
		w.Header().Set("ETag", formatETag(obj.Digest))
	}
}

// updateLastModifiedHeader writes 'Last-Modified' header in response
func updateLastModifiedHeader(obj *jetstream.ObjectInfo, w http.ResponseWriter) {
	w.Header().Set("Last-Modified", obj.ModTime.UTC().Format(time.RFC1123))
}

// updateMetadataHeaders writes metadata headers in response
func updateMetadataHeaders(obj *jetstream.ObjectInfo, w http.ResponseWriter) {
	if obj.Metadata != nil {
		for k, v := range obj.Metadata {
			if k == "" {
				continue
			}
			w.Header().Set(k, v)
		}
	}
}
