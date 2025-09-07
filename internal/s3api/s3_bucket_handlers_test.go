package s3api

import (
	"encoding/xml"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats-server/v2/server"
	nservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// helper to start a JetStream-enabled NATS server
func startJSServer(t *testing.T) *server.Server {
	t.Helper()
	opts := nservertest.DefaultTestOptions
	opts.Port = server.RANDOM_PORT
	opts.JetStream = true
	s := nservertest.RunServer(&opts)
	return s
}

func TestListBuckets(t *testing.T) {
	s := startJSServer(t)
	defer s.Shutdown()

	gw := NewS3Gateway(s.ClientURL(), "", "")

	// Create a couple of buckets so ListBuckets has content.
	nc := gw.client.Client.NATS()
	// Avoid production panic handler during test shutdown.
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	for _, b := range []string{"bucket1", "bucket2"} {
		if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: b}); err != nil {
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
	s := startJSServer(t)
	defer s.Shutdown()

	gw := NewS3Gateway(s.ClientURL(), "", "")

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
	nc := gw.client.Client.NATS()
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	if _, err := js.ObjectStore(bucket); err != nil {
		t.Fatalf("expected created object store %q, got error: %v", bucket, err)
	}
}
