package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/wpnpeiris/nats-s3/internal/client"
	"github.com/wpnpeiris/nats-s3/internal/model"
)

// BucketsResult is the XML envelope for ListBuckets responses.
type BucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   *s3.Owner
	Buckets []*s3.Bucket `xml:"Buckets>Bucket"`
}

// LocationConstraintResponse is the XML response for GetBucketLocation.
type LocationConstraintResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
	Location string   `xml:",chardata"`
}

// CreateBucket handles S3 CreateBucket by creating a JetStream Object Store
// bucket and returning a minimal S3-compatible XML response.
func (s *S3Gateway) CreateBucket(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	os, err := s.client.CreateBucket(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, client.ErrBucketAlreadyExists) {
			model.WriteErrorResponse(w, r, model.ErrBucketAlreadyOwnedByYou)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	buckets := []*s3.Bucket{{
		Name:         aws.String(os.Bucket()),
		CreationDate: aws.Time(time.Now()),
	}}

	response := BucketsResult{
		Buckets: buckets,
	}

	model.WriteXMLResponse(w, r, http.StatusOK, response)
}

// DeleteBucket deletes the specified bucket and responds with 204 No Content.
// Returns NoSuchBucket if the bucket does not exist.
// Returns BucketNotEmpty if the bucket contains objects.
func (s *S3Gateway) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]

	// Check if bucket is empty before attempting deletion
	objects, err := s.client.ListObjects(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		// If no objects found, that's actually fine - bucket is empty
		if !errors.Is(err, client.ErrObjectNotFound) {
			model.WriteErrorResponse(w, r, model.ErrInternalError)
			return
		}
	} else if len(objects) > 0 {
		// Bucket has objects, cannot delete
		model.WriteErrorResponse(w, r, model.ErrBucketNotEmpty)
		return
	}

	// Bucket is empty, proceed with deletion
	err = s.client.DeleteBucket(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	model.WriteEmptyResponse(w, r, http.StatusNoContent)
}

// ListBuckets enumerates existing JetStream Object Store buckets and returns
// a simple S3-compatible XML response.
func (s *S3Gateway) ListBuckets(w http.ResponseWriter, r *http.Request) {
	entries, err := s.client.ListBuckets(r.Context())
	if err != nil {
		model.WriteErrorResponse(w, r, model.ErrInternalError)
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

	model.WriteXMLResponse(w, r, http.StatusOK, response)
}

// HeadBucket checks if a bucket exists and the user has permission to access it.
// Returns 200 OK if bucket exists, 404 if not found.
func (s *S3Gateway) HeadBucket(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]

	err := s.bucketExists(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GetBucketLocation returns the region where the bucket resides.
func (s *S3Gateway) GetBucketLocation(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]

	err := s.bucketExists(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			model.WriteErrorResponse(w, r, model.ErrNoSuchBucket)
			return
		}
		model.WriteErrorResponse(w, r, model.ErrInternalError)
		return
	}

	// Return empty location constraint for default location
	response := LocationConstraintResponse{
		Location: "",
	}

	model.WriteXMLResponse(w, r, http.StatusOK, response)
}

// bucketExists checks if a bucket exists by attempting to list objects.
func (s *S3Gateway) bucketExists(ctx context.Context, bucket string) error {
	_, err := s.client.ListObjects(ctx, bucket)
	if err != nil {
		if errors.Is(err, client.ErrBucketNotFound) {
			return client.ErrBucketNotFound
		}
		if errors.Is(err, client.ErrObjectNotFound) {
			return nil
		}
		return err
	}
	return nil
}
