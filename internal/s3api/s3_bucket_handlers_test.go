package s3api

import (
	"encoding/xml"
	"net/http/httptest"
	"testing"

	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
)

func TestListBuckets(t *testing.T) {
	tests := []struct {
		name            string
		existingBuckets []string
		expectedStatus  int
		expectedBuckets []string
	}{
		{
			name:            "list multiple buckets",
			existingBuckets: []string{"bucket1", "bucket2"},
			expectedStatus:  200,
			expectedBuckets: []string{"bucket1", "bucket2"},
		},
		{
			name:            "list no buckets",
			existingBuckets: []string{},
			expectedStatus:  200,
			expectedBuckets: []string{},
		},
		{
			name:            "list single bucket",
			existingBuckets: []string{"single-bucket"},
			expectedStatus:  200,
			expectedBuckets: []string{"single-bucket"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testutil.StartJSServer(t)
			defer s.Shutdown()

			logger := logging.NewLogger(logging.Config{Level: "debug"})
			gw, err := NewS3Gateway(logger, s.ClientURL(), 1, nil, nil)
			if err != nil {
				t.Fatalf("failed to create S3 gateway: %v", err)
			}

			natsEndpoint := s.Addr().String()
			nc, err := nats.Connect(natsEndpoint)
			nc.SetClosedHandler(func(_ *nats.Conn) {})
			defer nc.Close()

			js, err := nc.JetStream()
			if err != nil {
				t.Fatalf("JetStream failed: %v", err)
			}
			for _, b := range tt.existingBuckets {
				if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: b}); err != nil {
					t.Fatalf("create object store %s failed: %v", b, err)
				}
			}

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			req := httptest.NewRequest("GET", "/", nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Fatalf("unexpected status: got %d, want %d body=%s", rr.Code, tt.expectedStatus, rr.Body.String())
			}

			var parsed struct {
				Names []string `xml:"Buckets>Bucket>Name"`
			}
			if err := xml.Unmarshal(rr.Body.Bytes(), &parsed); err != nil {
				t.Fatalf("unmarshal xml failed: %v\nxml=%s", err, rr.Body.String())
			}

			want := make(map[string]bool)
			for _, b := range tt.expectedBuckets {
				want[b] = false
			}
			for _, n := range parsed.Names {
				if _, ok := want[n]; ok {
					want[n] = true
				}
			}
			for name, found := range want {
				if !found {
					t.Fatalf("expected bucket %q in response", name)
				}
			}
		})
	}
}

func TestCreateBucket(t *testing.T) {
	tests := []struct {
		name           string
		bucketName     string
		prexisting     bool
		expectedStatus int
		shouldExist    bool
	}{
		{
			name:           "create new bucket",
			bucketName:     "created-bucket",
			prexisting:     false,
			expectedStatus: 200,
			shouldExist:    true,
		},
		{
			name:           "create duplicate bucket fails",
			bucketName:     "dup-bucket",
			prexisting:     true,
			expectedStatus: 409,
			shouldExist:    true,
		},
		{
			name:           "create bucket with valid name",
			bucketName:     "valid-bucket-name",
			prexisting:     false,
			expectedStatus: 200,
			shouldExist:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testutil.StartJSServer(t)
			defer s.Shutdown()

			logger := logging.NewLogger(logging.Config{Level: "debug"})
			gw, err := NewS3Gateway(logger, s.ClientURL(), 1, nil, nil)
			if err != nil {
				t.Fatalf("failed to create S3 gateway: %v", err)
			}

			natsEndpoint := s.Addr().String()
			nc, err := nats.Connect(natsEndpoint)
			nc.SetClosedHandler(func(_ *nats.Conn) {})
			defer nc.Close()
			js, err := nc.JetStream()
			if err != nil {
				t.Fatalf("JetStream failed: %v", err)
			}

			if tt.prexisting {
				if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: tt.bucketName}); err != nil {
					t.Fatalf("create preexisting object store %s failed: %v", tt.bucketName, err)
				}
			}

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			req := httptest.NewRequest("PUT", "/"+tt.bucketName, nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Fatalf("unexpected status: got %d, want %d body=%s", rr.Code, tt.expectedStatus, rr.Body.String())
			}

			if tt.shouldExist {
				if _, err := js.ObjectStore(tt.bucketName); err != nil {
					t.Fatalf("expected created object store %q, got error: %v", tt.bucketName, err)
				}
			}
		})
	}
}

func TestCreateBucket_Replicated(t *testing.T) {
	servers := testutil.StartJSServerCluster(t)
	for _, s := range servers {
		defer s.Shutdown()
	}
	s := servers[0]

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(logger, s.ClientURL(), 3, nil, nil)
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	bucket := "created-bucket"
	req := httptest.NewRequest("PUT", "/"+bucket, nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("unexpected status: got %d body=%s", rr.Code, rr.Body.String())
	}

	// Verify bucket exists in NATS by opening ObjectStore
	natsEndpoint := s.Addr().String()
	nc, err := nats.Connect(natsEndpoint)
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	objStore, err := js.ObjectStore(bucket)
	if err != nil {
		t.Fatalf("expected created object store %q, got error: %v", bucket, err)
	}

	objStoreStatus, err := objStore.Status()
	if err != nil {
		t.Fatalf("failed to get object store status: %v", err)
	}

	if objStoreStatus.Replicas() != 3 {
		t.Errorf("Expected 3 replicas, got %d", objStoreStatus.Replicas())
	}
}
