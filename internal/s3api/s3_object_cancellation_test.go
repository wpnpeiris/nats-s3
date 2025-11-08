package s3api

import (
	"context"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"
)

// TestUploadCancellation_AbortsWrite verifies that canceling the request context
// during a large PUT causes the backend JetStream put to abort and no object is created.
func TestUploadCancellation_AbortsWrite(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(logger, s.ClientURL(), 1, nil, nil)
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Create Object Store bucket
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	bucket := "cancel-bucket"
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	// Prepare a slow streaming body using an io.Pipe
	pr, pw := io.Pipe()
	total := int64(10 * 1024 * 1024) // 10 MiB body
	chunk := make([]byte, 64*1024)
	var wrote int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer pw.Close()
		for wrote < total {
			// write a chunk and sleep a bit
			n, err := pw.Write(chunk)
			if err != nil {
				return
			}
			wrote += int64(n)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Build request with cancelable context and explicit Content-Length
	req := httptest.NewRequest("PUT", "/"+bucket+"/big-file.bin", pr)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	req.ContentLength = total

	rec := httptest.NewRecorder()

	// Serve in a goroutine and cancel midway
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.ServeHTTP(rec, req)
	}()

	// Allow some data to be written then cancel and close the body reader
	time.Sleep(50 * time.Millisecond)
	cancel()
	_ = pr.CloseWithError(context.Canceled)

	// Wait for handler to finish
	wg.Wait()
	<-done // ensure writer exited

	if rec.Code == 200 || rec.Code == 204 {
		t.Fatalf("expected non-success status on canceled upload, got %d", rec.Code)
	}

	// Verify object was not created
	os, err := js.ObjectStore(bucket)
	if err != nil {
		t.Fatalf("ObjectStore failed: %v", err)
	}
	if _, err := os.GetInfo("big-file.bin"); err == nil {
		t.Fatalf("object unexpectedly exists after canceled upload")
	} else if err != nats.ErrObjectNotFound {
		t.Fatalf("unexpected error when checking object: %v", err)
	}
}
