package s3api

import (
	"github.com/nats-io/nats.go"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/wpnpeiris/nats-s3/internal/client"
)

// S3Gateway provides a scaffold of the Amazon S3 REST S3Gateway (2006-03-01)
// All endpoints are registered and respond with HTTP 501 Not Implemented.
type S3Gateway struct {
	*client.Client
}

func NewS3Gateway(natsServers string, options []nats.Option) *S3Gateway {
	natsClient := client.NewClient("s3-gateway")
	err := natsClient.SetupConnectionToNATS(natsServers, options...)
	if err != nil {
		panic("Failed to connect to NATS")
	}
	return &S3Gateway{
		Client: natsClient,
	}
}

// RegisterRoutes registers the full set of S3 REST S3Gateway endpoints.
// Handlers return StatusNotImplemented (501).
func (s3Gateway *S3Gateway) RegisterRoutes(router *mux.Router) {
	r := router.PathPrefix("/").Subrouter()

	r.Methods(http.MethodOptions).HandlerFunc(s3Gateway.SetOptionHeaders)

	// Service level
	r.Methods(http.MethodGet).Path("/").HandlerFunc(s3Gateway.ListBuckets) // ListBuckets

	// Bucket root
	r.Methods(http.MethodPut).Path("/{bucket}").HandlerFunc(s3Gateway.notImplemented)     // CreateBucket
	r.Methods(http.MethodHead).Path("/{bucket}").HandlerFunc(s3Gateway.notImplemented)    // HeadBucket
	r.Methods(http.MethodGet).Path("/{bucket}").HandlerFunc(s3Gateway.ListObjects)        // ListObjects/ListObjectsV2
	r.Methods(http.MethodDelete).Path("/{bucket}").HandlerFunc(s3Gateway.notImplemented)  // DeleteBucket
	r.Methods(http.MethodOptions).Path("/{bucket}").HandlerFunc(s3Gateway.notImplemented) // CORS preflight
	r.Methods(http.MethodPost).Path("/{bucket}").HandlerFunc(s3Gateway.notImplemented)    // POST object (HTML form upload)

	// Bucket sub-resources (Queries matchers)
	// Common GET/PUT/DELETE controls
	addBucketSubresource(r, http.MethodGet, "acl", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "acl", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "cors", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "cors", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "cors", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "lifecycle", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "lifecycle", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "lifecycle", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "policy", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "policy", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "policy", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "replication", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "replication", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "versioning", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "versioning", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "website", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "website", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "website", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "tagging", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "tagging", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "tagging", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "logging", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "logging", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "notification", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "notification", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "encryption", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "encryption", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "encryption", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "object-lock", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "object-lock", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "ownershipControls", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "ownershipControls", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "ownershipControls", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "accelerate", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "accelerate", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "location", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "uploads", s3Gateway.notImplemented) // List multipart uploads
	addBucketSubresource(r, http.MethodGet, "versions", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "requestPayment", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "requestPayment", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "inventory", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "inventory", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "inventory", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "metrics", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "metrics", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "metrics", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "analytics", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "analytics", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "analytics", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodGet, "intelligent-tiering", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPut, "intelligent-tiering", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "intelligent-tiering", s3Gateway.notImplemented)
	addBucketSubresource(r, http.MethodPost, "delete", s3Gateway.notImplemented) // Multi-object delete

	// Object root
	r.Methods(http.MethodPut).Path("/{bucket}/{key:.*}").HandlerFunc(s3Gateway.Upload)             // PutObject / CopyObject
	r.Methods(http.MethodGet).Path("/{bucket}/{key:.*}").HandlerFunc(s3Gateway.Download)           // GetObject / SelectObjectContent
	r.Methods(http.MethodHead).Path("/{bucket}/{key:.*}").HandlerFunc(s3Gateway.HeadObject)        // HeadObject
	r.Methods(http.MethodDelete).Path("/{bucket}/{key:.*}").HandlerFunc(s3Gateway.notImplemented)  // DeleteObject
	r.Methods(http.MethodOptions).Path("/{bucket}/{key:.*}").HandlerFunc(s3Gateway.notImplemented) // CORS preflight

	// Object sub-resources
	addObjectSubresource(r, http.MethodGet, "acl", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodPut, "acl", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodDelete, "acl", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodGet, "tagging", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodPut, "tagging", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodDelete, "tagging", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodGet, "torrent", s3Gateway.notImplemented) // deprecated
	addObjectSubresource(r, http.MethodPost, "restore", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodGet, "legal-hold", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodPut, "legal-hold", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodGet, "retention", s3Gateway.notImplemented)
	addObjectSubresource(r, http.MethodPut, "retention", s3Gateway.notImplemented)

	// Multipart upload operations on object
	addObjectSubresource(r, http.MethodPost, "uploads", s3Gateway.notImplemented) // Initiate multipart upload
	// Upload part, Get part, Complete, Abort (all matched by uploadId presence)
	r.Methods(http.MethodPut).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s3Gateway.notImplemented)    // UploadPart
	r.Methods(http.MethodGet).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s3Gateway.notImplemented)    // GetObject (by part?) / List parts
	r.Methods(http.MethodPost).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s3Gateway.notImplemented)   // CompleteMultipartUpload
	r.Methods(http.MethodDelete).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s3Gateway.notImplemented) // AbortMultipartUpload
}

func (s3Gateway *S3Gateway) notImplemented(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func addBucketSubresource(r *mux.Router, method, sub string, h http.HandlerFunc) {
	r.Methods(method).Path("/{bucket}").Queries(sub, "").HandlerFunc(h)
}

func addObjectSubresource(r *mux.Router, method, sub string, h http.HandlerFunc) {
	r.Methods(method).Path("/{bucket}/{key:.*}").Queries(sub, "").HandlerFunc(h)
}
