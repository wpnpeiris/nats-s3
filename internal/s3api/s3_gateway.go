package s3api

import (
	"errors"
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
	iam    *IdentityAccessManagement
}

// NewS3Gateway creates a gateway instance and establishes a connection to
// NATS using the given servers and credentials.
func NewS3Gateway(natsServers string, natsUser string, natsPassword string) *S3Gateway {
	var natsOptions []nats.Option
	var credential Credential
	if natsUser != "" && natsPassword != "" {
		natsOptions = append(natsOptions, nats.UserInfo(natsUser, natsPassword))
		credential = NewCredential(natsUser, natsPassword)
	}

	natsClient := client.NewClient("s3-gateway")
	err := natsClient.SetupConnectionToNATS(natsServers, natsOptions...)
	if err != nil {
		panic("Failed to connect to NATS")
	}
	mps := createMultipartSessionStore(natsClient)
	return &S3Gateway{
		client: &client.NatsObjectClient{
			Client:         natsClient,
			MultiPartStore: mps,
		},
		iam: NewIdentityAccessManagement(credential),
	}
}

func createMultipartSessionStore(c *client.Client) *client.MultiPartStore {
	nc := c.NATS()
	js, err := nc.JetStream()
	if err != nil {
		panic("Failed to create to multipart session store when nc.JetStream()")
	}

	kv, err := js.KeyValue(client.MultiPartSessionStoreName)
	if err != nil {
		if errors.Is(err, nats.ErrBucketNotFound) {
			kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket: client.MultiPartSessionStoreName,
			})
			if err != nil {
				panic("Failed to create to multipart session store when js.CreateKeyValue()")
			}
		} else {
			panic("Failed to create to multipart session store when js.KeyValue()")
		}
	}

	os, err := js.ObjectStore(client.MultiPartTempStoreName)
	if err != nil && errors.Is(err, nats.ErrStreamNotFound) {
		if errors.Is(err, nats.ErrStreamNotFound) {
			os, err = js.CreateObjectStore(&nats.ObjectStoreConfig{
				Bucket: client.MultiPartTempStoreName,
			})
			if err != nil {
				panic("Failed to create to multipart temp store when js.CreateObjectStore()")
			}
		} else {
			panic("Failed to create to multipart temp store when js.ObjectStore()")
		}
	}

	return &client.MultiPartStore{
		SessionStore:  kv,
		TempPartStore: os,
	}
}

// RegisterRoutes wires the S3 REST API endpoints onto the provided mux router.
func (s *S3Gateway) RegisterRoutes(router *mux.Router) {
	r := router.PathPrefix("/").Subrouter()

	r.Methods(http.MethodOptions).HandlerFunc(s.iam.Auth(s.SetOptionHeaders))

	// Service level
	r.Methods(http.MethodGet).Path("/").HandlerFunc(s.iam.Auth(s.ListBuckets)) // ListBuckets

	// Bucket root
	r.Methods(http.MethodPut).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.CreateBucket))       // CreateBucket
	r.Methods(http.MethodHead).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.notImplemented))    // HeadBucket
	r.Methods(http.MethodGet).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.ListObjects))        // ListObjects/ListObjectsV2
	r.Methods(http.MethodDelete).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.DeleteBucket))    // DeleteBucket
	r.Methods(http.MethodOptions).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.notImplemented)) // CORS preflight
	r.Methods(http.MethodPost).Path("/{bucket}").HandlerFunc(s.iam.Auth(s.notImplemented))    // POST object (HTML form upload)

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
	addBucketSubresource(r, http.MethodPost, "delete", s.iam.Auth(s.notImplemented)) // Multi-object delete

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
	addObjectSubresource(r, http.MethodGet, "retention", s.iam.Auth(s.notImplemented))
	addObjectSubresource(r, http.MethodPut, "retention", s.iam.Auth(s.notImplemented))

	// Multipart upload operations on object
	addObjectSubresource(r, http.MethodPost, "uploads", s.iam.Auth(s.InitiateMultipartUpload)) // Initiate multipart upload
	// Upload part, Get part, Complete, Abort (all matched by uploadId presence)
	r.Methods(http.MethodPut).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.UploadPart))               // UploadPart
	r.Methods(http.MethodGet).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.notImplemented))           // GetObject (by part?) / List parts
	r.Methods(http.MethodPost).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.CompleteMultipartUpload)) // CompleteMultipartUpload
	r.Methods(http.MethodDelete).Path("/{bucket}/{key:.*}").Queries("uploadId", "{uploadId}").HandlerFunc(s.iam.Auth(s.AbortMultipartUpload))  // AbortMultipartUpload

	// Object root
	r.Methods(http.MethodPut).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.Upload))             // PutObject / CopyObject
	r.Methods(http.MethodGet).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.Download))           // GetObject / SelectObjectContent
	r.Methods(http.MethodHead).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.HeadObject))        // HeadObject
	r.Methods(http.MethodDelete).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.DeleteObject))    // DeleteObject
	r.Methods(http.MethodOptions).Path("/{bucket}/{key:.*}").HandlerFunc(s.iam.Auth(s.notImplemented)) // CORS preflight
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
