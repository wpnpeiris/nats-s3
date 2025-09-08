package s3api

import (
	"encoding/xml"
	"errors"
	"github.com/gorilla/mux"
	"github.com/wpnpeiris/nats-s3/internal/client"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// BucketsResult is the XML envelope for ListBuckets responses.
type BucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   *s3.Owner
	Buckets []*s3.Bucket `xml:"Buckets>Bucket"`
}

// CreateBucket handles S3 CreateBucket by creating a JetStream Object Store
// bucket and returning a minimal S3-compatible XML response.
func (s *S3Gateway) CreateBucket(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	os, err := s.client.CreateBucket(bucket)
	if err != nil {
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	buckets := []*s3.Bucket{{
		Name:         aws.String(os.Bucket()),
		CreationDate: aws.Time(time.Now()),
	}}

	response := BucketsResult{
		Buckets: buckets,
	}

	WriteXMLResponse(w, r, http.StatusOK, response)
}

func (s *S3Gateway) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	err := s.client.DeleteBucket(bucket)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			WriteErrorResponse(w, r, ErrNoSuchBucket)
			return
		}
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	WriteEmptyResponse(w, r, http.StatusNoContent)
}

// ListBuckets enumerates existing JetStream Object Store buckets and returns
// a simple S3-compatible XML response.
func (s *S3Gateway) ListBuckets(w http.ResponseWriter, r *http.Request) {
	entries, err := s.client.ListBuckets()
	if err != nil {
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	var buckets []*s3.Bucket

	for entry := range entries {
		buckets = append(buckets, &s3.Bucket{
			Name:         aws.String(entry.Bucket()),
			CreationDate: aws.Time(time.Now())},
		)
	}

	response := BucketsResult{
		Buckets: buckets,
	}

	WriteXMLResponse(w, r, http.StatusOK, response)
}
