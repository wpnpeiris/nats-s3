package s3api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"

	"github.com/wpnpeiris/nats-s3/internal/auth"
	"github.com/wpnpeiris/nats-s3/internal/client"
	"github.com/wpnpeiris/nats-s3/internal/credential"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/metrics"
	"github.com/wpnpeiris/nats-s3/internal/validation"
)

type S3GatewayOptions struct {
	Replicas int
}

// S3Gateway registers S3-compatible HTTP routes (2006-03-01) and delegates
// implemented operations to NATS JetStream-backed object storage.
// Unimplemented endpoints intentionally respond with HTTP 501 Not Implemented.
type S3Gateway struct {
	client         *client.NatsObjectClient
	multiPartStore *client.MultiPartStore
	iam            *auth.IdentityAccessManagement
	started        time.Time
}

// NewS3Gateway creates a gateway instance and establishes a connection to
// NATS using the given servers and connection options. Returns an error if initialization fails.
func NewS3Gateway(ctx context.Context,
	logger log.Logger,
	natsServers string,
	natsOptions []nats.Option,
	credStore credential.Store,
	opts S3GatewayOptions) (*S3Gateway, error) {

	if opts.Replicas < 1 {
		logging.Info(logger, "msg", fmt.Sprintf("Invalid replicas count, defaulting to 1: [%d]", opts.Replicas))
		opts.Replicas = 1
	}

	natsClient := client.NewClient(ctx, "s3-gateway")
	err := natsClient.SetupConnectionToNATS(natsServers, natsOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	oc, err := client.NewNatsObjectClient(ctx, logger, natsClient, client.NatsObjectClientOptions{
		Replicas: opts.Replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize NATS object client: %w", err)
	}

	mps, err := client.NewMultiPartStore(ctx, logger, natsClient, client.MultiPartStoreOptions{
		Replicas: opts.Replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize multipart store: %w", err)
	}

	mc := client.NewMetricCollector(ctx, logger, oc)
	err = metrics.RegisterPrometheusCollector(mc)
	if err != nil {
		logging.Error(logger, "msg", "Error at registering metric collector", "err", err)
	}

	return &S3Gateway{
		client:         oc,
		multiPartStore: mps,
		iam:            auth.NewIdentityAccessManagement(credStore),
		started:        time.Now().UTC(),
	}, nil
}

// RegisterRoutes wires the S3 REST API endpoints onto the provided mux router.
func (s *S3Gateway) RegisterRoutes(router *mux.Router) {
	r := router.PathPrefix("/").Subrouter()

	// Apply validation to all routes
	validator := &validation.RequestValidator{}
	r.Use(validator.Validate)

	// Unauthenticated monitoring endpoints
	r.Methods(http.MethodGet).Path("/healthz").HandlerFunc(s.Healthz)

	r.Methods(http.MethodOptions).HandlerFunc(s.iam.Auth(s.SetOptionHeaders))

	// Service level
	r.Methods(http.MethodGet).Path("/").HandlerFunc(s.iam.Auth(s.ListBuckets)) // ListBuckets

	// Bucket root
	r.Methods(http.MethodPut).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.CreateBucket))        // CreateBucket
	r.Methods(http.MethodPut).Path("/{bucket}/").HandlerFunc(s.iam.Auth(s.CreateBucket))       // CreateBucket (with trailing slash)
	r.Methods(http.MethodHead).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.notImplemented))     // HeadBucket
	r.Methods(http.MethodHead).Path("/{bucket}/").HandlerFunc(s.iam.Auth(s.notImplemented))    // HeadBucket (with trailing slash)
	r.Methods(http.MethodGet).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.ListObjects))         // ListObjects/ListObjectsV2
	r.Methods(http.MethodGet).Path("/{bucket}/").HandlerFunc(s.iam.Auth(s.ListObjects))        // ListObjects/ListObjectsV2 (with trailing slash)
	r.Methods(http.MethodDelete).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.DeleteBucket))     // DeleteBucket
	r.Methods(http.MethodDelete).Path("/{bucket}/").HandlerFunc(s.iam.Auth(s.DeleteBucket))    // DeleteBucket (with trailing slash)
	r.Methods(http.MethodOptions).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.notImplemented))  // CORS preflight
	r.Methods(http.MethodOptions).Path("/{bucket}/").HandlerFunc(s.iam.Auth(s.notImplemented)) // CORS preflight (with trailing slash)
	//r.Methods(http.MethodPost).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.notImplemented))    // POST object (HTML form upload)

	// Bucket sub-resources (Queries matchers)
	// Common GET/PUT/DELETE controls
	addBucketSubresource(r, http.MethodGet, "acl", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "acl", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "cors", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "cors", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "cors", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "lifecycle", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "lifecycle", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "lifecycle", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "policy", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "policy", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "policy", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "replication", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "replication", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "versioning", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "versioning", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "website", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "website", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "website", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "tagging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "tagging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "tagging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "logging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "logging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "notification", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "notification", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "encryption", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "encryption", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "encryption", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "object-lock", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "object-lock", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "ownershipControls", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "ownershipControls", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "ownershipControls", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "accelerate", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "accelerate", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "location", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "uploads", s.iam.Auth(s.notImplemented)) // List multipart uploads
	addBucketSubresource(r, http.MethodGet, "versions", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "requestPayment", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "requestPayment", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "inventory", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "inventory", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "inventory", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "metrics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "metrics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "metrics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "analytics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "analytics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "analytics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodGet, "intelligent-tiering", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPut, "intelligent-tiering", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodDelete, "intelligent-tiering", s.iam.Auth(s.notImplemented))
	addBucketSubresource(r, http.MethodPost, "delete", s.iam.Auth(s.DeleteObjects)) // Multi-object delete

	// Object sub-resources
	addObjectSubresource(r, http.MethodGet, "acl", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodPut, "acl", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodDelete, "acl", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodGet, "tagging", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodPut, "tagging", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodDelete, "tagging", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodGet, "torrent", s.iam.Auth(s.notImplemented)) // deprecated
	addObjectSubresource(r, http.MethodPost, "restore", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodGet, "legal-hold", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodPut, "legal-hold", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodGet, "retention", s.iam.Auth(s.GetObjectRetention))
	addObjectSubresource(r, http.MethodPut, "retention", s.iam.Auth(s.UpdateObjectRetention))

	// Multipart upload operations on object
	addObjectSubresource(r, http.MethodPost, "uploads", s.iam.Auth(s.InitiateMultipartUpload)) // Initiate multipart upload
	// Upload part, Get part, Complete, Abort (all matched by uploadId presence)
	r.Methods(http.MethodPut).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.UploadPart))               // UploadPart
	r.Methods(http.MethodGet).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.ListParts))                // GetObject (by part?) / List parts
	r.Methods(http.MethodPost).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.CompleteMultipartUpload)) // CompleteMultipartUpload
	r.Methods(http.MethodDelete).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.AbortMultipartUpload))  // AbortMultipartUpload

	// Object root
	r.Methods(http.MethodPut).Path("/{bucket}/{key:.*}").HeadersRegexp("x-amz-copy-source", ".+").HandlerFunc(s.iam.Auth(s.CopyObject)) // CopyObject
	r.Methods(http.MethodPut).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.Upload))                                              // PutObject
	r.Methods(http.MethodGet).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.Download))                                            // GetObject / SelectObjectContent
	r.Methods(http.MethodHead).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.HeadObject))                                         // HeadObject
	r.Methods(http.MethodDelete).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.DeleteObject))                                     // DeleteObject
	r.Methods(http.MethodOptions).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.notImplemented))                                  // CORS preflight
}

func (s *S3Gateway) notImplemented(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

// gatewayStats represents a minimal JSON payload for /stats.
type gatewayStats struct {
	UptimeSeconds      int64 `json:"uptime_seconds"`
	NATSConnected      bool  `json:"nats_connected"`
	NATSReconnects     int   `json:"nats_reconnects"`
	ObjectStoreBuckets int   `json:"objectstore_buckets"`
}

// addBucketSubresource registers a bucket-level subresource path using a
// query-string flag like ?acl= or ?cors= to disambiguate behavior.
// Registers both with and without trailing slash.
func addBucketSubresource(r *mux.Router, method, sub string, h http.HandlerFunc) {
	r.Methods(method).Path("/{bucket}").Queries(sub, "").HandlerFunc(h)
	r.Methods(method).Path("/{bucket}/").Queries(sub, "").HandlerFunc(h)
}

// addObjectSubresource registers an object-level subresource path using a
// query-string flag like ?tagging= or ?retention= to disambiguate behavior.
func addObjectSubresource(r *mux.Router, method, sub string, h http.HandlerFunc) {
	r.Methods(method).Path("/{bucket}/{key:.*}").Queries(sub, "").HandlerFunc(h)
}
