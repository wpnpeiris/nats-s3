package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/wpnpeiris/nats-s3/internal/client"
)

const (
	maxUploadsList = 10000 // Max number of uploads in a listUploadsResponse.
	maxPartsList   = 10000 // Max number of parts in a listPartsResponse.
)

// InitiateMultipartUpload creates a new multipart upload session for the given
// bucket and object key, returning UploadId/Bucket/Key in S3-compatible XML.
func (s *S3Gateway) InitiateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	uploadID := uuid.New().String()
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	if bucket == "" || key == "" {
		WriteErrorResponse(w, r, ErrInvalidRequest)
		return
	}

	err := s.client.InitMultipartUpload(bucket, key, uploadID)
	if err != nil {
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	response := InitiateMultipartUploadResult{
		CreateMultipartUploadOutput: s3.CreateMultipartUploadOutput{
			Bucket:   aws.String(bucket),
			Key:      objectKey(key),
			UploadId: aws.String(uploadID),
		},
	}
	WriteXMLResponse(w, r, http.StatusOK, response)
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
	if bucket == "" || key == "" || uploadID == "" {
		WriteErrorResponse(w, r, ErrNoSuchUpload)
		return
	}

	pnStr := r.URL.Query().Get("partNumber")
	partNum, _ := strconv.Atoi(pnStr)
	if partNum < 1 || partNum > maxUploadsList {
		WriteErrorResponse(w, r, ErrInvalidPart)
		return
	}

	etag, err := s.client.UploadPart(bucket, key, uploadID, partNum, r.Body)
	if err != nil {
		if errors.Is(err, client.ErrUploadNotFound) || errors.Is(err, client.ErrUploadCompleted) {
			WriteErrorResponse(w, r, ErrNoSuchUpload)
			return
		}
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	SetEtag(w, etag)
	WriteEmptyResponse(w, r, http.StatusOK)
}

// CompleteMultipartUpload finalizes a multipart upload by parsing the client
// provided part list, delegating composition to the storage client, and
// returning an S3-compatible XML response with the final ETag.
func (s *S3Gateway) CompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	uploadID := r.URL.Query().Get("uploadId")
	if bucket == "" || key == "" || uploadID == "" {
		WriteErrorResponse(w, r, ErrNoSuchUpload)
		return
	}

	parts := &CompleteMultipartUpload{}
	if err := xmlDecoder(r.Body, parts, r.ContentLength); err != nil {
		WriteErrorResponse(w, r, ErrMalformedXML)
		return
	}

	sortedPartNumbers := parsePartNumbers(parts)
	etag, err := s.client.CompleteMultipartUpload(bucket, key, uploadID, sortedPartNumbers)
	if err != nil {
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	response := CompleteMultipartUploadResult{
		Bucket: aws.String(bucket),
		ETag:   aws.String(etag),
		Key:    objectKey(key),
	}
	WriteXMLResponse(w, r, http.StatusOK, response)
}

// AbortMultipartUpload aborts a multipart upload, removing uploaded parts and
// session metadata, and responds with 204 No Content on success.
func (s *S3Gateway) AbortMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	uploadID := r.URL.Query().Get("uploadId")
	if bucket == "" || key == "" || uploadID == "" {
		WriteErrorResponse(w, r, ErrNoSuchUpload)
		return
	}
	err := s.client.AbortMultipartUpload(bucket, key, uploadID)
	if err != nil {
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	WriteEmptyResponse(w, r, http.StatusNoContent)
}

// ListParts lists uploaded parts for a multipart upload. Supports pagination via
// part-number-marker and max-parts query parameters.
func (s *S3Gateway) ListParts(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	uploadID := r.URL.Query().Get("uploadId")
	partNumberMarker, _ := strconv.Atoi(r.URL.Query().Get("part-number-marker"))
	if bucket == "" || key == "" || uploadID == "" {
		WriteErrorResponse(w, r, ErrNoSuchUpload)
		return
	}
	if partNumberMarker < 0 {
		WriteErrorResponse(w, r, ErrInvalidPartNumberMarker)
		return
	}

	var maxParts int
	if r.URL.Query().Get("max-parts") != "" {
		maxParts, _ = strconv.Atoi(r.URL.Query().Get("max-parts"))
	} else {
		maxParts = maxPartsList
	}
	if maxParts < 0 {
		WriteErrorResponse(w, r, ErrInvalidMaxParts)
		return
	}

	meta, err := s.client.ListParts(bucket, key, uploadID)
	if err != nil {
		if errors.Is(err, client.ErrUploadNotFound) || errors.Is(err, client.ErrUploadCompleted) {
			WriteErrorResponse(w, r, ErrNoSuchUpload)
			return
		}
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	response := ListPartsResult{
		Bucket:               aws.String(meta.Bucket),
		Key:                  aws.String(meta.Key),
		UploadId:             aws.String(uploadID),
		MaxParts:             aws.Int64(int64(maxParts)),
		NextPartNumberMarker: aws.Int64(int64(partNumberMarker)),
		StorageClass:         aws.String("STANDARD"),
	}
	for _, part := range meta.Parts {
		response.Part = append(response.Part, &s3.Part{
			PartNumber: aws.Int64(int64(part.Number)),
			Size:       aws.Int64(int64(part.Size)),
			ETag:       aws.String(part.ETag),
		})
		response.NextPartNumberMarker = aws.Int64(int64(part.Number))
	}

	WriteXMLResponse(w, r, http.StatusOK, response)
}

func parsePartNumbers(parts *CompleteMultipartUpload) []int {
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
