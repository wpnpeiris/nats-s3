package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/wpnpeiris/nats-s3/internal/model"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/wpnpeiris/nats-s3/internal/client"
)

const (
	maxUploadsList = 10000 // Max number of uploads in a listUploadsResponse.
	maxPartsList   = 10000 // Max number of parts in a listPartsResponse.

	// S3-compatible size limits
	maxPartSize    = 5 * 1024 * 1024 * 1024 // 5GB per part (S3 multipart limit)
	maxXMLBodySize = 1 * 1024 * 1024        // 1MB for XML request bodies
)

// InitiateMultipartUpload creates a new multipart upload session for the given
// bucket and object key, returning UploadId/Bucket/Key in S3-compatible XML.
func (s *S3Gateway) InitiateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	uploadID := uuid.New().String()
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	err := s.multiPartStore.InitMultipartUpload(r.Context(), bucket, key, uploadID)
	if err != nil {
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	response := model.InitiateMultipartUploadResult{
		CreateMultipartUploadOutput: s3.CreateMultipartUploadOutput{
			Bucket:   aws.String(bucket),
			Key:      objectKey(key),
			UploadId: aws.String(uploadID),
		},
	}
	model.WriteXMLResponse(w, r, http.StatusOK, response)
}

// objectKey normalizes a mux-extracted key for response payloads by trimming
// a possible leading slash. AWS S3 omits a leading '/' in the Key field.
func objectKey(key string) *string {
	if strings.HasPrefix(key, "/") {
		t := (key)[1:]
		return &t
	}
	return &key
}

// UploadPart receives an object part for an existing multipart upload and
// responds with the part's ETag in the response headers. The ETag value is
// quoted to match S3 behavior.
func (s *S3Gateway) UploadPart(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	uploadID := r.URL.Query().Get("uploadId")

	if uploadID == "" {
		model.WriteErrorResponse(w, r, model.ErrNoSuchUpload)
		return
	}

	pnStr := r.URL.Query().Get("partNumber")
	partNum, _ := strconv.Atoi(pnStr)
	if partNum < 1 || partNum > maxUploadsList {
		model.WriteErrorResponse(w, r, model.ErrInvalidPart)
		return
	}

	// Validate Content-Length
	if r.ContentLength < 0 {
		model.WriteErrorResponse(w, r, model.ErrMissingFields)
		return
	}
	if r.ContentLength > maxPartSize {
		model.WriteErrorResponse(w, r, model.ErrEntityTooLarge)
		return
	}

	// Use LimitReader as defense-in-depth to ensure we never read more than maxPartSize
	// Wrap it in a limitedReadCloser to satisfy io.ReadCloser interface
	limitedBody := &limitedReadCloser{
		Reader: io.LimitReader(r.Body, maxPartSize+1),
		Closer: r.Body,
	}

	etag, err := s.multiPartStore.UploadPart(r.Context(), bucket, key, uploadID, partNum, limitedBody)
	if err != nil {
		if errors.Is(err, client.ErrUploadNotFound) || errors.Is(err, client.ErrUploadCompleted) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchUpload)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	model.SetEtag(w, etag)
	model.WriteEmptyResponse(w, r, http.StatusOK)
}

// CompleteMultipartUpload finalizes a multipart upload by parsing the client
// provided part list, delegating composition to the storage client, and
// returning an S3-compatible XML response with the final ETag.
func (s *S3Gateway) CompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	uploadID := r.URL.Query().Get("uploadId")

	if uploadID == "" {
		model.WriteErrorResponse(w, r, model.ErrNoSuchUpload)
		return
	}

	if r.ContentLength < 0 {
		model.WriteErrorResponse(w, r, model.ErrMissingFields)
		return
	}
	if r.ContentLength > maxXMLBodySize {
		model.WriteErrorResponse(w, r, model.ErrEntityTooLarge)
		return
	}

	parts := &model.CompleteMultipartUpload{}
	xmlSize := r.ContentLength
	if xmlSize > maxXMLBodySize {
		xmlSize = maxXMLBodySize
	}
	if err := xmlDecoder(r.Body, parts, xmlSize); err != nil {
		model.WriteErrorResponse(w, r, model.ErrMalformedXML)
		return
	}

	sortedPartNumbers := parsePartNumbers(parts)
	etag, err := s.multiPartStore.CompleteMultipartUpload(r.Context(), bucket, key, uploadID, sortedPartNumbers)
	if err != nil {
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	response := model.CompleteMultipartUploadResult{
		Bucket: aws.String(bucket),
		ETag:   aws.String(etag),
		Key:    objectKey(key),
	}
	model.WriteXMLResponse(w, r, http.StatusOK, response)
}

// AbortMultipartUpload aborts a multipart upload, removing uploaded parts and
// session metadata, and responds with 204 No Content on success.
func (s *S3Gateway) AbortMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	uploadID := r.URL.Query().Get("uploadId")

	if uploadID == "" {
		model.WriteErrorResponse(w, r, model.ErrNoSuchUpload)
		return
	}
	err := s.multiPartStore.AbortMultipartUpload(r.Context(), bucket, key, uploadID)
	if err != nil {
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	model.WriteEmptyResponse(w, r, http.StatusNoContent)
}

// ListParts lists uploaded parts for a multipart upload. Supports pagination via
// part-number-marker and max-parts query parameters.
func (s *S3Gateway) ListParts(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	uploadID := r.URL.Query().Get("uploadId")
	partNumberMarker, _ := strconv.Atoi(r.URL.Query().Get("part-number-marker"))

	if uploadID == "" {
		model.WriteErrorResponse(w, r, model.ErrNoSuchUpload)
		return
	}
	if partNumberMarker < 0 {
		model.WriteErrorResponse(w, r, model.ErrInvalidPartNumberMarker)
		return
	}

	var maxParts int
	if r.URL.Query().Get("max-parts") != "" {
		maxParts, _ = strconv.Atoi(r.URL.Query().Get("max-parts"))
	} else {
		maxParts = maxPartsList
	}
	if maxParts < 0 {
		model.WriteErrorResponse(w, r, model.ErrInvalidMaxParts)
		return
	}

	meta, err := s.multiPartStore.ListParts(r.Context(), bucket, key, uploadID)
	if err != nil {
		if errors.Is(err, client.ErrUploadNotFound) || errors.Is(err, client.ErrUploadCompleted) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchUpload)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}
	response := model.ListPartsResult{
		Bucket:           aws.String(meta.Bucket),
		Key:              aws.String(meta.Key),
		UploadId:         aws.String(uploadID),
		MaxParts:         aws.Int64(int64(maxParts)),
		PartNumberMarker: aws.Int64(int64(partNumberMarker)),
		StorageClass:     aws.String("STANDARD"),
	}

	// Collect and sort part numbers to ensure deterministic ordering.
	partNumbers := make([]int, 0, len(meta.Parts))
	for pn := range meta.Parts {
		partNumbers = append(partNumbers, pn)
	}
	sort.Ints(partNumbers)

	// Apply part-number-marker: start strictly after the marker value.
	start := 0
	if partNumberMarker > 0 {
		for i, pn := range partNumbers {
			if pn > partNumberMarker {
				start = i
				break
			}
			start = len(partNumbers) // if all <= marker, this will skip all
		}
	}

	// Limit by max-parts.
	end := start + maxParts
	if end > len(partNumbers) {
		end = len(partNumbers)
	}

	// Slice and build response parts.
	if start < end {
		for _, pn := range partNumbers[start:end] {
			p := meta.Parts[pn]
			response.Part = append(response.Part, &s3.Part{
				PartNumber: aws.Int64(int64(p.Number)),
				Size:       aws.Int64(int64(p.Size)),
				ETag:       aws.String(p.ETag),
			})
			response.NextPartNumberMarker = aws.Int64(int64(p.Number))
		}
	} else {
		// No parts in this page; keep marker as provided.
		response.NextPartNumberMarker = aws.Int64(int64(partNumberMarker))
	}

	// Indicate if there are more parts beyond this page.
	isTruncated := end < len(partNumbers)
	response.IsTruncated = aws.Bool(isTruncated)

	model.WriteXMLResponse(w, r, http.StatusOK, response)
}

func parsePartNumbers(parts *model.CompleteMultipartUpload) []int {
	if parts == nil || len(parts.Parts) == 0 {
		return nil
	}

	nums := make([]int, 0, len(parts.Parts))
	for _, p := range parts.Parts {
		nums = append(nums, p.PartNumber)
	}
	sort.Ints(nums)
	return nums
}

func xmlDecoder(body io.Reader, v interface{}, size int64) error {
	var reader io.Reader
	if size > 0 {
		reader = io.LimitReader(body, size)
	} else {
		reader = body
	}
	d := xml.NewDecoder(reader)
	d.CharsetReader = func(label string, input io.Reader) (io.Reader, error) {
		return input, nil
	}
	return d.Decode(v)
}

// limitedReadCloser wraps an io.Reader with io.Closer to satisfy io.ReadCloser interface
type limitedReadCloser struct {
	Reader io.Reader
	Closer io.Closer
}

func (lrc *limitedReadCloser) Read(p []byte) (n int, err error) {
	return lrc.Reader.Read(p)
}

func (lrc *limitedReadCloser) Close() error {
	if lrc.Closer != nil {
		return lrc.Closer.Close()
	}
	return nil
}
