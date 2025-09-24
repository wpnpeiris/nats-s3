package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/wpnpeiris/nats-s3/internal/client"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
)

// ListBucketResult is a minimal representation of S3's ListBucket result.
type ListBucketResult struct {
	IsTruncated bool        `xml:"IsTruncated"`
	Contents    []s3.Object `xml:"Contents"`
	Name        string      `xml:"Name"`
	Prefix      string      `xml:"Prefix"`
	MaxKeys     int         `xml:"MaxKeys"`
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

// ListObjects returns objects in a bucket as a simple S3-compatible XML list.
func (s *S3Gateway) ListObjects(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]

	log.Println("List Objects in bucket", bucket)

	res, err := s.client.ListObjects(bucket)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			WriteErrorResponse(w, r, ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			WriteEmptyResponse(w, r, http.StatusOK)
			return
		}

		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	var contents []s3.Object
	for _, obj := range res {
		etag := ""
		if obj.Digest != "" {
			etag = fmt.Sprintf("\"%s\"", obj.Digest)
		}
		contents = append(contents, s3.Object{
			ETag:         aws.String(etag),
			Key:          aws.String(obj.Name),
			LastModified: aws.Time(obj.ModTime),
			Size:         aws.Int64(int64(obj.Size)),
			StorageClass: aws.String(""),
		})
	}

	xmlResponse := ListBucketResult{
		IsTruncated: false,
		Contents:    contents,
		Name:        bucket,
		Prefix:      "",
		MaxKeys:     1000,
	}

	err = xml.NewEncoder(w).Encode(xmlResponse)
	if err != nil {
		log.Printf("Error enconding the response, %s", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
}

// Download writes object content to the response and sets typical S3 headers
// such as Last-Modified, ETag, Content-Type, and Content-Length.
func (s *S3Gateway) Download(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	info, data, err := s.client.GetObject(bucket, key)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			WriteErrorResponse(w, r, ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			WriteErrorResponse(w, r, ErrNoSuchKey)
			return
		}
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	if info != nil {
		w.Header().Set("Last-Modified", info.ModTime.UTC().Format(time.RFC1123))
		if info.Digest != "" {
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", info.Digest))
		}
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(data)
	if err != nil {
		log.Printf("Error writing the response, %s", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
}

// HeadObject writes object metadata headers without a response body.
func (s *S3Gateway) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	res, err := s.client.GetObjectInfo(bucket, key)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			WriteErrorResponse(w, r, ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			WriteErrorResponse(w, r, ErrNoSuchKey)
			return
		}

		http.Error(w, "Object not found in the bucket", http.StatusNotFound)
		return
	}

	log.Printf("Head object %s/%s", bucket, key)
	w.Header().Set("Last-Modified", res.ModTime.UTC().Format(time.RFC1123))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", res.Size))
	if res.Digest != "" {
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", res.Digest))
	}
	if res.Headers != nil {
		if cts, ok := res.Headers["Content-Type"]; ok && len(cts) > 0 && cts[0] != "" {
			w.Header().Set("Content-Type", cts[0])
		}
	}
	if res.Metadata != nil {
		for k, v := range res.Metadata {
			if k == "" {
				continue
			}
			w.Header().Set(k, v)
		}
	}

}

// Upload stores an object and responds with 200 and an ETag header.
func (s *S3Gateway) Upload(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	contentType := extractContentType(r)
	meta := extractMetadata(r)

	log.Println("Upload to", bucket, "with key", key, " with content-type", contentType, " with user-meta", meta)
	res, err := s.client.PutObject(bucket, key, contentType, meta, body)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			WriteErrorResponse(w, r, ErrNoSuchBucket)
			return
		}
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	if res.Digest != "" {
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", res.Digest))
	}
	WriteEmptyResponse(w, r, http.StatusOK)
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
		if strings.HasPrefix(ln, "x-amz-meta-") {
			meta[ln] = strings.Join(vals, ",")
		}
	}

	return meta
}

// DeleteObject deletes the specified object and responds with 204 No Content.
func (s *S3Gateway) DeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	err := s.client.DeleteObject(bucket, key)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			WriteErrorResponse(w, r, ErrNoSuchBucket)
			return
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			WriteErrorResponse(w, r, ErrNoSuchKey)
			return
		}
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	WriteEmptyResponse(w, r, http.StatusNoContent)
}
