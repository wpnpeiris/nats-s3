package s3api

import (
	"encoding/xml"
	"github.com/gorilla/mux"
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
		handleInternalError(err, w)
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
		handleInternalError(err, w)
		return
	}

	WriteEmptyResponse(w, r, http.StatusNoContent)
}

// ListBuckets enumerates existing JetStream Object Store buckets and returns
// a simple S3-compatible XML response.
func (s *S3Gateway) ListBuckets(w http.ResponseWriter, r *http.Request) {
	entries, err := s.client.ListBuckets()
	if err != nil {
		handleInternalError(err, w)
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
