package s3api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/client"
)

// S3Gateway registers S3-compatible HTTP routes (2006-03-01) and delegates
// implemented operations to NATS JetStream-backed object storage.
// Unimplemented endpoints intentionally respond with HTTP 501 Not Implemented.
type S3Gateway struct {
	client *client.NatsObjectClient
}

// NewS3Gateway creates a gateway instance and establishes a connection to
// NATS using the given servers and options.
func NewS3Gateway(natsServers string, options []nats.Option) *S3Gateway {
	natsClient := client.NewClient("s3-gateway")
	err := natsClient.SetupConnectionToNATS(natsServers, options...)
	if err != nil {
		panic("Failed to connect to NATS")
	}
	return &S3Gateway{
		client: &client.NatsObjectClient{
			Client: natsClient,
		},
	}
}

// RegisterRoutes wires the S3 REST API endpoints onto the provided mux router.
func (s *S3Gateway) RegisterRoutes(router *mux.Router) {
	r := router.PathPrefix("/").Subrouter()

	r.Methods(http.MethodOptions).HandlerFunc(s.SetOptionHeaders)

	// Service level
	r.Methods(http.MethodGet).Path("/").HandlerFunc(s.ListBuckets) // ListBuckets

	// Bucket root
	r.Methods(http.MethodPut).Path("/{bucket}").HandlerFunc(s.notImplemented)     // CreateBucket
	r.Methods(http.MethodHead).Path("/{bucket}").HandlerFunc(s.notImplemented)    // HeadBucket
	r.Methods(http.MethodGet).Path("/{bucket}").HandlerFunc(s.ListObjects)        // ListObjects/ListObjectsV2
	r.Methods(http.MethodDelete).Path("/{bucket}").HandlerFunc(s.notImplemented)  // DeleteBucket
	r.Methods(http.MethodOptions).Path("/{bucket}").HandlerFunc(s.notImplemented) // CORS preflight
	r.Methods(http.MethodPost).Path("/{bucket}").HandlerFunc(s.notImplemented)    // POST object (HTML form upload)

	// Bucket sub-resources (Queries matchers)
	// Common GET/PUT/DELETE controls
	addBucketSubresource(r, http.MethodGet, "acl", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "acl", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "cors", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "cors", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "cors", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "lifecycle", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "lifecycle", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "lifecycle", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "policy", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "policy", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "policy", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "replication", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "replication", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "versioning", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "versioning", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "website", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "website", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "website", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "tagging", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "tagging", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "tagging", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "logging", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "logging", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "notification", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "notification", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "encryption", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "encryption", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "encryption", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "object-lock", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "object-lock", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "ownershipControls", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "ownershipControls", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "ownershipControls", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "accelerate", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "accelerate", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "location", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "uploads", s.notImplemented) // List multipart uploads
	addBucketSubresource(r, http.MethodGet, "versions", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "requestPayment", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "requestPayment", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "inventory", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "inventory", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "inventory", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "metrics", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "metrics", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "metrics", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "analytics", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "analytics", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "analytics", s.notImplemented)
	addBucketSubresource(r, http.MethodGet, "intelligent-tiering", s.notImplemented)
	addBucketSubresource(r, http.MethodPut, "intelligent-tiering", s.notImplemented)
	addBucketSubresource(r, http.MethodDelete, "intelligent-tiering", s.notImplemented)
	addBucketSubresource(r, http.MethodPost, "delete", s.notImplemented) // Multi-object delete

	// Object root
	r.Methods(http.MethodPut).Path("/{bucket}/{key:.*}").HandlerFunc(s.Upload)             // PutObject / CopyObject
	r.Methods(http.MethodGet).Path("/{bucket}/{key:.*}").HandlerFunc(s.Download)           // GetObject / SelectObjectContent
	r.Methods(http.MethodHead).Path("/{bucket}/{key:.*}").HandlerFunc(s.HeadObject)        // HeadObject
	r.Methods(http.MethodDelete).Path("/{bucket}/{key:.*}").HandlerFunc(s.DeleteObject)    // DeleteObject
	r.Methods(http.MethodOptions).Path("/{bucket}/{key:.*}").HandlerFunc(s.notImplemented) // CORS preflight

	// Object sub-resources
	addObjectSubresource(r, http.MethodGet, "acl", s.notImplemented)
	addObjectSubresource(r, http.MethodPut, "acl", s.notImplemented)
	addObjectSubresource(r, http.MethodDelete, "acl", s.notImplemented)
	addObjectSubresource(r, http.MethodGet, "tagging", s.notImplemented)
	addObjectSubresource(r, http.MethodPut, "tagging", s.notImplemented)
	addObjectSubresource(r, http.MethodDelete, "tagging", s.notImplemented)
	addObjectSubresource(r, http.MethodGet, "torrent", s.notImplemented) // deprecated
	addObjectSubresource(r, http.MethodPost, "restore", s.notImplemented)
	addObjectSubresource(r, http.MethodGet, "legal-hold", s.notImplemented)
	addObjectSubresource(r, http.MethodPut, "legal-hold", s.notImplemented)
	addObjectSubresource(r, http.MethodGet, "retention", s.notImplemented)
	addObjectSubresource(r, http.MethodPut, "retention", s.notImplemented)

	// Multipart upload operations on object
	addObjectSubresource(r, http.MethodPost, "uploads", s.notImplemented) // Initiate multipart upload
	// Upload part, Get part, Complete, Abort (all matched by uploadId presence)
	r.Methods(http.MethodPut).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.notImplemented)    // UploadPart
	r.Methods(http.MethodGet).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.notImplemented)    // GetObject (by part?) / List parts
	r.Methods(http.MethodPost).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.notImplemented)   // CompleteMultipartUpload
	r.Methods(http.MethodDelete).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.notImplemented) // AbortMultipartUpload
}

func (s *S3Gateway) notImplemented(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

// addBucketSubresource registers a bucket-level subresource path using a
// query-string flag like ?acl= or ?cors= to disambiguate behavior.
func addBucketSubresource(r *mux.Router, method, sub string, h http.HandlerFunc) {
	r.Methods(method).Path("/{bucket}").Queries(sub, "").HandlerFunc(h)
}

// addObjectSubresource registers an object-level subresource path using a
// query-string flag like ?tagging= or ?retention= to disambiguate behavior.
func addObjectSubresource(r *mux.Router, method, sub string, h http.HandlerFunc) {
	r.Methods(method).Path("/{bucket}/{key:.*}").Queries(sub, "").HandlerFunc(h)
}
