package s3api

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"
)

func TestListBuckets(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Create a couple of buckets so ListBuckets has content.
	natsEndpoint := s.Addr().String()
	nc, err := nats.Connect(natsEndpoint)
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}
	// Avoid production panic handler during test shutdown.
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	for _, b := range []string{"bucket1", "bucket2"} {
		if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: b}); err != nil {
			t.Fatalf("create object store %s failed: %v", b, err)
		}
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	req := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("unexpected status: got %d body=%s", rr.Code, rr.Body.String())
	}

	// Minimal struct to pull out bucket names from the XML
	var parsed struct {
		Names []string `xml:"Buckets>Bucket>Name"`
	}
	if err := xml.Unmarshal(rr.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("unmarshal xml failed: %v\nxml=%s", err, rr.Body.String())
	}

	want := map[string]bool{"bucket1": false, "bucket2": false}
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
}

func TestCreateBucket(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
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
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	if _, err := js.ObjectStore(context.Background(), bucket); err != nil {
		t.Fatalf("expected created object store %q, got error: %v", bucket, err)
	}
}

func TestCreateBucketDuplicateFails(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	bucket := "dup-bucket"

	// First create should succeed
	req1 := httptest.NewRequest("PUT", "/"+bucket, nil)
	rr1 := httptest.NewRecorder()
	r.ServeHTTP(rr1, req1)
	if rr1.Code != 200 {
		t.Fatalf("unexpected status on first create: got %d body=%s", rr1.Code, rr1.Body.String())
	}

	// Second create should fail with conflict
	req2 := httptest.NewRequest("PUT", "/"+bucket, nil)
	rr2 := httptest.NewRecorder()
	r.ServeHTTP(rr2, req2)
	if rr2.Code != 409 {
		t.Fatalf("expected 409 on duplicate create, got %d body=%s", rr2.Code, rr2.Body.String())
	}
}

func TestCreateBucket_Replicated(t *testing.T) {
	servers := testutil.StartJSServerCluster(t)
	for _, s := range servers {
		defer s.Shutdown()
	}
	s := servers[0]

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{
		Replicas: 3,
	})
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
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	objStore, err := js.ObjectStore(context.Background(), bucket)
	if err != nil {
		t.Fatalf("expected created object store %q, got error: %v", bucket, err)
	}

	objStoreStatus, err := objStore.Status(context.Background())
	if err != nil {
		t.Fatalf("failed to get object store status: %v", err)
	}

	if objStoreStatus.Replicas() != 3 {
		t.Errorf("Expected 3 replicas, got %d", objStoreStatus.Replicas())
	}
}
