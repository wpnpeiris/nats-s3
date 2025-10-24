package s3api

import (
	"encoding/xml"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
)

func TestObjectHandlers_CRUD(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(logger, s.ClientURL(), "", "", nil)
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Ensure bucket exists
	natsEndpoint := s.Addr().String()
	nc, err := nats.Connect(natsEndpoint)
	// Avoid panic-on-close during tests
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	bucket := "tobj"
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	key := "dir/sub/file.txt"
	data := "hello-objects"

	// PUT object
	putReq := httptest.NewRequest("PUT", "/"+bucket+"/"+key, strings.NewReader(data))
	putRec := httptest.NewRecorder()
	r.ServeHTTP(putRec, putReq)
	if putRec.Code != 200 {
		t.Fatalf("PUT unexpected status: %d body=%s", putRec.Code, putRec.Body.String())
	}
	if etag := putRec.Header().Get("ETag"); etag == "" {
		t.Fatalf("expected ETag header on PUT response")
	}

	// HEAD object
	headReq := httptest.NewRequest("HEAD", "/"+bucket+"/"+key, nil)
	headRec := httptest.NewRecorder()
	r.ServeHTTP(headRec, headReq)
	if headRec.Code != 200 {
		t.Fatalf("HEAD unexpected status: %d", headRec.Code)
	}
	if cl := headRec.Header().Get("Content-Length"); cl == "" {
		t.Fatalf("HEAD missing Content-Length")
	}
	if lm := headRec.Header().Get("Last-Modified"); lm == "" {
		t.Fatalf("HEAD missing Last-Modified")
	}
	if etag := headRec.Header().Get("ETag"); etag == "" {
		t.Fatalf("HEAD missing ETag")
	}

	// GET object
	getReq := httptest.NewRequest("GET", "/"+bucket+"/"+key, nil)
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != 200 {
		t.Fatalf("GET unexpected status: %d", getRec.Code)
	}
	body, _ := io.ReadAll(getRec.Body)
	if string(body) != data {
		t.Fatalf("GET unexpected body: %q", string(body))
	}

	// LIST objects
	listReq := httptest.NewRequest("GET", "/"+bucket, nil)
	listRec := httptest.NewRecorder()
	r.ServeHTTP(listRec, listReq)
	if listRec.Code != 200 {
		t.Fatalf("LIST unexpected status: %d body=%s", listRec.Code, listRec.Body.String())
	}
	var parsed struct {
		Keys []string `xml:"Contents>Key"`
	}
	if err := xml.Unmarshal(listRec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("unmarshal list xml failed: %v\nxml=%s", err, listRec.Body.String())
	}
	found := false
	for _, k := range parsed.Keys {
		if k == key {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("LIST did not include key %q: %+v", key, parsed.Keys)
	}

	// DELETE object
	delReq := httptest.NewRequest("DELETE", "/"+bucket+"/"+key, nil)
	delRec := httptest.NewRecorder()
	r.ServeHTTP(delRec, delReq)
	if delRec.Code != 204 {
		t.Fatalf("DELETE unexpected status: %d", delRec.Code)
	}
}

func TestCopyObject_Basic(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(logger, s.ClientURL(), "", "", nil)
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Setup NATS connection and create bucket
	natsEndpoint := s.Addr().String()
	nc, err := nats.Connect(natsEndpoint)
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	bucket := "copy-test"
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	// Step 1: Upload source object
	sourceKey := "source/file.txt"
	data := "hello-copy-test"
	putReq := httptest.NewRequest("PUT", "/"+bucket+"/"+sourceKey, strings.NewReader(data))
	putReq.Header.Set("Content-Type", "text/plain")
	putReq.Header.Set("x-amz-meta-custom", "source-value")
	putRec := httptest.NewRecorder()
	r.ServeHTTP(putRec, putReq)
	if putRec.Code != 200 {
		t.Fatalf("PUT source unexpected status: %d body=%s", putRec.Code, putRec.Body.String())
	}
	sourceETag := putRec.Header().Get("ETag")

	// Step 2: Copy object (COPY metadata directive - default)
	destKey := "dest/file-copy.txt"
	copyReq := httptest.NewRequest("PUT", "/"+bucket+"/"+destKey, nil)
	copyReq.Header.Set("x-amz-copy-source", bucket+"/"+sourceKey)
	copyRec := httptest.NewRecorder()
	r.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != 200 {
		t.Fatalf("COPY unexpected status: %d body=%s", copyRec.Code, copyRec.Body.String())
	}

	// Verify CopyObjectResult XML response
	var copyResult CopyObjectResult
	if err := xml.Unmarshal(copyRec.Body.Bytes(), &copyResult); err != nil {
		t.Fatalf("unmarshal copy result failed: %v\nxml=%s", err, copyRec.Body.String())
	}
	if copyResult.ETag == "" {
		t.Fatalf("copy result missing ETag")
	}
	if copyResult.LastModified.IsZero() {
		t.Fatalf("copy result missing LastModified")
	}

	// Step 3: GET destination object and verify content
	getReq := httptest.NewRequest("GET", "/"+bucket+"/"+destKey, nil)
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != 200 {
		t.Fatalf("GET dest unexpected status: %d", getRec.Code)
	}
	body, _ := io.ReadAll(getRec.Body)
	if string(body) != data {
		t.Fatalf("GET dest unexpected body: %q, want %q", string(body), data)
	}

	// Step 4: HEAD destination to verify metadata was copied
	headReq := httptest.NewRequest("HEAD", "/"+bucket+"/"+destKey, nil)
	headRec := httptest.NewRecorder()
	r.ServeHTTP(headRec, headReq)
	if headRec.Code != 200 {
		t.Fatalf("HEAD dest unexpected status: %d", headRec.Code)
	}
	if ct := headRec.Header().Get("Content-Type"); ct != "text/plain" {
		t.Fatalf("HEAD dest Content-Type: %q, want text/plain", ct)
	}
	if meta := headRec.Header().Get("x-amz-meta-custom"); meta != "source-value" {
		t.Fatalf("HEAD dest x-amz-meta-custom: %q, want source-value", meta)
	}

	// Verify source still exists
	getSourceReq := httptest.NewRequest("GET", "/"+bucket+"/"+sourceKey, nil)
	getSourceRec := httptest.NewRecorder()
	r.ServeHTTP(getSourceRec, getSourceReq)
	if getSourceRec.Code != 200 {
		t.Fatalf("GET source after copy unexpected status: %d", getSourceRec.Code)
	}

	t.Logf("Source ETag: %s, Dest ETag: %s", sourceETag, copyResult.ETag)
}

func TestCopyObject_ReplaceMetadata(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(logger, s.ClientURL(), "", "", nil)
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
	bucket := "copy-replace-test"
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	// Upload source with metadata
	sourceKey := "source.txt"
	data := "test-data"
	putReq := httptest.NewRequest("PUT", "/"+bucket+"/"+sourceKey, strings.NewReader(data))
	putReq.Header.Set("x-amz-meta-original", "old-value")
	putRec := httptest.NewRecorder()
	r.ServeHTTP(putRec, putReq)
	if putRec.Code != 200 {
		t.Fatalf("PUT source unexpected status: %d", putRec.Code)
	}

	// Copy with REPLACE directive and new metadata
	destKey := "dest.txt"
	copyReq := httptest.NewRequest("PUT", "/"+bucket+"/"+destKey, nil)
	copyReq.Header.Set("x-amz-copy-source", bucket+"/"+sourceKey)
	copyReq.Header.Set("x-amz-metadata-directive", "REPLACE")
	copyReq.Header.Set("x-amz-meta-new", "new-value")
	copyReq.Header.Set("Content-Type", "application/json")
	copyRec := httptest.NewRecorder()
	r.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != 200 {
		t.Fatalf("COPY with REPLACE unexpected status: %d body=%s", copyRec.Code, copyRec.Body.String())
	}

	// Verify destination has new metadata
	headReq := httptest.NewRequest("HEAD", "/"+bucket+"/"+destKey, nil)
	headRec := httptest.NewRecorder()
	r.ServeHTTP(headRec, headReq)
	if headRec.Code != 200 {
		t.Fatalf("HEAD dest unexpected status: %d", headRec.Code)
	}
	if meta := headRec.Header().Get("x-amz-meta-new"); meta != "new-value" {
		t.Fatalf("HEAD dest x-amz-meta-new: %q, want new-value", meta)
	}
	if meta := headRec.Header().Get("x-amz-meta-original"); meta != "" {
		t.Fatalf("HEAD dest should not have x-amz-meta-original, got: %q", meta)
	}
	if ct := headRec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("HEAD dest Content-Type: %q, want application/json", ct)
	}
}

func TestCopyObject_InvalidSource(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(logger, s.ClientURL(), "", "", nil)
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
	bucket := "copy-error-test"
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	// Attempt copy with invalid source format (missing key)
	copyReq := httptest.NewRequest("PUT", "/"+bucket+"/dest.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "just-bucket-name")
	copyRec := httptest.NewRecorder()
	r.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != 400 {
		t.Fatalf("COPY invalid source expected 400, got: %d", copyRec.Code)
	}

	// Attempt copy with non-existent source
	copyReq2 := httptest.NewRequest("PUT", "/"+bucket+"/dest.txt", nil)
	copyReq2.Header.Set("x-amz-copy-source", bucket+"/non-existent-key")
	copyRec2 := httptest.NewRecorder()
	r.ServeHTTP(copyRec2, copyReq2)
	if copyRec2.Code != 404 {
		t.Fatalf("COPY non-existent source expected 404, got: %d", copyRec2.Code)
	}
}
