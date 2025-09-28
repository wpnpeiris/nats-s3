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

// InitiateMultipartUploadResult wraps the minimal fields returned by S3
// when initiating a multipart upload. It embeds the AWS SDK's response type
// to preserve field names and XML shape.
type InitiateMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	s3.CreateMultipartUploadOutput
}

type CompleteMultipartUpload struct {
	Parts []CompletedPart `xml:"Part"`
}

type CompletedPart struct {
	ETag       string
	PartNumber int
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	Location *string  `xml:"Location,omitempty"`
	Bucket   *string  `xml:"Bucket,omitempty"`
	Key      *string  `xml:"Key,omitempty"`
	ETag     *string  `xml:"ETag,omitempty"`
	// VersionId is NOT included in XML body - it should only be in x-amz-version-id HTTP header

	// Store the VersionId internally for setting HTTP header, but don't marshal to XML
	VersionId *string `xml:"-"`
}

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
	if partNum < 1 || partNum > 10000 {
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
