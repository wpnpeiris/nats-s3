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
	"github.com/wpnpeiris/nats-s3/internal/interceptor"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/metrics"
)

// S3Gateway registers S3-compatible HTTP routes (2006-03-01) and delegates
// implemented operations to NATS JetStream-backed object storage.
// Unimplemented endpoints intentionally respond with HTTP 501 Not Implemented.
type S3Gateway struct {
	client         *client.NatsObjectClient
	multiPartStore *client.MultiPartStore
	iam            *auth.IdentityAccessManagement
	logger         log.Logger
	started        time.Time
}

// NewS3Gateway creates a gateway instance and establishes a connection to
// NATS using the given servers and connection options. Returns an error if initialization fails.
func NewS3Gateway(logger log.Logger,
	natsServers string,
	replicas int,
	natsOptions []nats.Option,
	credStore credential.Store) (*S3Gateway, error) {

	natsClient := client.NewClient("s3-gateway")
	err := natsClient.SetupConnectionToNATS(natsServers, natsOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	oc, err := client.NewNatsObjectClient(logger,
		natsClient,
		client.NatsObjectClientOptions{
			Replicas: replicas,
		})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize NATS object client: %w", err)
	}

	mps, err := client.NewMultiPartStore(context.Background(), logger, natsClient)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize multipart store: %w", err)
	}

	mc := client.NewMetricCollector(logger, oc)
	err = metrics.RegisterPrometheusCollector(mc)
	if err != nil {
		logging.Error(logger, "msg", "Error at registering metric collector", "err", err)
	}

	return &S3Gateway{
		client:         oc,
		multiPartStore: mps,
		iam:            auth.NewIdentityAccessManagement(credStore),
		logger:         logger,
		started:        time.Now().UTC(),
	}, nil
}

// RegisterRoutes wires the S3 REST API endpoints onto the provided mux router.
func (s *S3Gateway) RegisterRoutes(router *mux.Router) {
	r := router.PathPrefix("/").Subrouter()

	// Apply cancellation and validation to all routes
	cancel := &interceptor.RequestCancellation{}
	r.Use(cancel.CancelIfDone)
	validator := &interceptor.RequestValidator{}
	r.Use(validator.Validate)

	// Unauthenticated monitoring endpoints
	r.Methods(http.MethodGet).Path("/healthz").HandlerFunc(s.Healthz)

	r.Methods(http.MethodOptions).HandlerFunc(s.iam.Auth(s.SetOptionHeaders))

	// Service level
	r.Methods(http.MethodGet).Path("/").HandlerFunc(s.iam.Auth(s.ListBuckets))

	// Routes relative to /{bucket}
	bucket := r.PathPrefix("/{bucket}").Subrouter()

	// 1: Object operations with query parameters
	// These routes have both .Path("/{key:.+}") AND .Queries()

	// Multipart upload operations
	addObjectSubresource(bucket, http.MethodPost, "uploads", s.iam.Auth(s.InitiateMultipartUpload))
	// Route SigV4 streaming-chunked parts to a dedicated handler
	bucket.Methods(http.MethodPut).Path("/{key:.+}").
		Queries("uploadId", "{uploadId}").
		HeadersRegexp("x-amz-content-sha256", "(?i)^STREAMING-AWS4-HMAC-SHA256-PAYLOAD(?:-TRAILER)?$").
		HandlerFunc(s.iam.Auth(s.StreamUploadPart))
	// Default multipart part upload handler (non-streaming)
	bucket.Methods(http.MethodPut).Path("/{key:.+}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.UploadPart))
	bucket.Methods(http.MethodGet).Path("/{key:.+}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.ListParts))
	bucket.Methods(http.MethodPost).Path("/{key:.+}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.CompleteMultipartUpload))
	bucket.Methods(http.MethodDelete).Path("/{key:.+}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.AbortMultipartUpload))

	// Object subresources
	addObjectSubresource(bucket, http.MethodGet, "acl", s.iam.Auth(s.notImplemented))
	addObjectSubresource(bucket, http.MethodPut, "acl", s.iam.Auth(s.notImplemented))
	addObjectSubresource(bucket, http.MethodDelete, "acl", s.iam.Auth(s.notImplemented))
	addObjectSubresource(bucket, http.MethodGet, "tagging", s.iam.Auth(s.notImplemented))
	addObjectSubresource(bucket, http.MethodPut, "tagging", s.iam.Auth(s.notImplemented))
	addObjectSubresource(bucket, http.MethodDelete, "tagging", s.iam.Auth(s.notImplemented))
	addObjectSubresource(bucket, http.MethodGet, "torrent", s.iam.Auth(s.notImplemented)) // deprecated
	addObjectSubresource(bucket, http.MethodPost, "restore", s.iam.Auth(s.notImplemented))
	addObjectSubresource(bucket, http.MethodGet, "legal-hold", s.iam.Auth(s.notImplemented))
	addObjectSubresource(bucket, http.MethodPut, "legal-hold", s.iam.Auth(s.notImplemented))
	addObjectSubresource(bucket, http.MethodGet, "retention", s.iam.Auth(s.GetObjectRetention))
	addObjectSubresource(bucket, http.MethodPut, "retention", s.iam.Auth(s.UpdateObjectRetention))

	// 2: Object operations without query parameters
	// These routes have .Path("/{key:.+}") but NO .Queries()
	bucket.Methods(http.MethodPut).Path("/{key:.+}").HeadersRegexp("x-amz-copy-source", ".+").HandlerFunc(s.iam.Auth(s.CopyObject))
	// Route streaming SigV4 payload uploads to dedicated handler first
	bucket.Methods(http.MethodPut).Path("/{key:.+}").
		HeadersRegexp("x-amz-content-sha256", "(?i)^STREAMING-AWS4-HMAC-SHA256-PAYLOAD(?:-TRAILER)?$").
		HandlerFunc(s.iam.Auth(s.StreamUpload))
	// Default single PUT handler
	bucket.Methods(http.MethodPut).Path("/{key:.+}").HandlerFunc(s.iam.Auth(s.Upload))
	bucket.Methods(http.MethodGet).Path("/{key:.+}").HandlerFunc(s.iam.Auth(s.Download))
	bucket.Methods(http.MethodHead).Path("/{key:.+}").HandlerFunc(s.iam.Auth(s.HeadObject))
	bucket.Methods(http.MethodDelete).Path("/{key:.+}").HandlerFunc(s.iam.Auth(s.DeleteObject))
	bucket.Methods(http.MethodOptions).Path("/{key:.+}").HandlerFunc(s.iam.Auth(s.notImplemented))

	// 3: Bucket operations with query parameters
	// These routes have .Queries() but NO .Path()
	// Must be registered after object routes
	addBucketSubresource(bucket, http.MethodGet, "acl", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "acl", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "cors", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "cors", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "cors", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "lifecycle", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "lifecycle", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "lifecycle", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "policy", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "policy", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "policy", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "replication", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "replication", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "versioning", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "versioning", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "website", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "website", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "website", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "tagging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "tagging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "tagging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "logging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "logging", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "notification", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "notification", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "encryption", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "encryption", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "encryption", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "object-lock", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "object-lock", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "ownershipControls", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "ownershipControls", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "ownershipControls", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "accelerate", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "accelerate", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "location", s.iam.Auth(s.GetBucketLocation))
	addBucketSubresource(bucket, http.MethodGet, "uploads", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "versions", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "requestPayment", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "requestPayment", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "inventory", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "inventory", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "inventory", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "metrics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "metrics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "metrics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "analytics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "analytics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "analytics", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodGet, "intelligent-tiering", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPut, "intelligent-tiering", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodDelete, "intelligent-tiering", s.iam.Auth(s.notImplemented))
	addBucketSubresource(bucket, http.MethodPost, "delete", s.iam.Auth(s.DeleteObjects))

	// 4: Bucket operations without query parameters
	// These routes have neither .Path() nor .Queries()
	// Must be registered last
	bucket.Methods(http.MethodPut).HandlerFunc(s.iam.Auth(s.CreateBucket))
	bucket.Methods(http.MethodHead).HandlerFunc(s.iam.Auth(s.HeadBucket))
	bucket.Methods(http.MethodGet).HandlerFunc(s.iam.Auth(s.ListObjects))
	bucket.Methods(http.MethodDelete).HandlerFunc(s.iam.Auth(s.DeleteBucket))
	bucket.Methods(http.MethodOptions).HandlerFunc(s.iam.Auth(s.notImplemented))

}

func (s *S3Gateway) notImplemented(w http.ResponseWriter, r *http.Request) {
	endpoint := fmt.Sprintf("%s %s", r.Method, r.URL.Path)
	if r.URL.RawQuery != "" {
		endpoint = fmt.Sprintf("%s?%s", endpoint, r.URL.RawQuery)
	}
	logging.Info(s.logger, "msg", "Unimplemented endpoint", "endpoint", endpoint)
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
	r.Methods(method).Queries(sub, "").HandlerFunc(h)
}

// addObjectSubresource registers an object-level subresource path using a
// query-string flag like ?tagging= or ?retention= to disambiguate behavior.
func addObjectSubresource(r *mux.Router, method, sub string, h http.HandlerFunc) {
	r.Methods(method).Path("/{key:.+}").Queries(sub, "").HandlerFunc(h)
}
