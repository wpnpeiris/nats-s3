package s3api

import (
	"context"
	"encoding/xml"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"
)

func TestObjectHandlers_CRUD(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Ensure bucket exists
	natsEndpoint := s.Addr().String()
	nc, err := nats.Connect(natsEndpoint)
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}
	// Avoid panic-on-close during tests
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	bucket := "tobj"
	if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: bucket}); err != nil {
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
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Setup NATS connection and create bucket
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
	bucket := "copy-test"
	if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: bucket}); err != nil {
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
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

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
	bucket := "copy-replace-test"
	if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: bucket}); err != nil {
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
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

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
	bucket := "copy-error-test"
	if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: bucket}); err != nil {
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

func TestListObjects_WithDelimiter(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Setup NATS connection and create bucket
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
	bucket := "test-delimiter"
	if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	// Upload test objects
	testData := map[string]string{
		"dir1/file1": "data1",
		"dir1/file2": "data2",
		"dir2/file1": "data3",
	}

	for key, data := range testData {
		req := httptest.NewRequest("PUT", "/"+bucket+"/"+key, strings.NewReader(data))
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("PUT %s unexpected status: %d", key, rec.Code)
		}
	}

	// List with delimiter
	listReq := httptest.NewRequest("GET", "/"+bucket+"?delimiter=/", nil)
	listRec := httptest.NewRecorder()
	r.ServeHTTP(listRec, listReq)

	if listRec.Code != 200 {
		t.Fatalf("LIST with delimiter unexpected status: %d body=%s", listRec.Code, listRec.Body.String())
	}

	// Parse XML response
	var result ListBucketResult
	body, _ := io.ReadAll(listRec.Body)
	t.Logf("XML Response: %s", string(body))

	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal list xml failed: %v\nxml=%s", err, string(body))
	}

	// Verify Contents is empty (all objects should be grouped)
	if len(result.Contents) != 0 {
		t.Errorf("Expected 0 Contents entries, got %d", len(result.Contents))
	}

	// Verify CommonPrefixes
	if len(result.CommonPrefixes) != 2 {
		t.Fatalf("Expected 2 CommonPrefixes, got %d: %+v", len(result.CommonPrefixes), result.CommonPrefixes)
	}

	// Check for dir1/ and dir2/
	prefixes := make(map[string]bool)
	for _, cp := range result.CommonPrefixes {
		prefixes[cp.Prefix] = true
	}

	if !prefixes["dir1/"] {
		t.Errorf("Expected CommonPrefix 'dir1/', got: %+v", result.CommonPrefixes)
	}
	if !prefixes["dir2/"] {
		t.Errorf("Expected CommonPrefix 'dir2/', got: %+v", result.CommonPrefixes)
	}

	// Verify Delimiter is set
	if result.Delimiter != "/" {
		t.Errorf("Expected Delimiter '/', got '%s'", result.Delimiter)
	}
}

func TestObjectRetention(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Create bucket and object
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

	bucket := "retention-test"
	if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	key := "testfile.txt"
	data := "test data for retention"

	// PUT object first
	putReq := httptest.NewRequest("PUT", "/"+bucket+"/"+key, strings.NewReader(data))
	putRec := httptest.NewRecorder()
	r.ServeHTTP(putRec, putReq)
	if putRec.Code != 200 {
		t.Fatalf("PUT object failed with status %d", putRec.Code)
	}

	// Test 1: PUT retention
	retentionXML := `<Retention>
		<Mode>GOVERNANCE</Mode>
		<RetainUntilDate>2025-12-31T23:59:59Z</RetainUntilDate>
	</Retention>`

	putRetReq := httptest.NewRequest("PUT", "/"+bucket+"/"+key+"?retention", strings.NewReader(retentionXML))
	putRetRec := httptest.NewRecorder()
	r.ServeHTTP(putRetRec, putRetReq)

	if putRetRec.Code != 200 {
		body, _ := io.ReadAll(putRetRec.Body)
		t.Fatalf("PUT retention failed with status %d, body: %s", putRetRec.Code, string(body))
	}

	// Test 2: GET retention
	getRetReq := httptest.NewRequest("GET", "/"+bucket+"/"+key+"?retention", nil)
	getRetRec := httptest.NewRecorder()
	r.ServeHTTP(getRetRec, getRetReq)

	if getRetRec.Code != 200 {
		body, _ := io.ReadAll(getRetRec.Body)
		t.Fatalf("GET retention failed with status %d, body: %s", getRetRec.Code, string(body))
	}

	// Parse response
	var retention struct {
		XMLName         xml.Name `xml:"Retention"`
		Mode            string   `xml:"Mode"`
		RetainUntilDate string   `xml:"RetainUntilDate"`
	}

	if err := xml.NewDecoder(getRetRec.Body).Decode(&retention); err != nil {
		t.Fatalf("Failed to decode retention response: %v", err)
	}

	if retention.Mode != "GOVERNANCE" {
		t.Errorf("Expected Mode 'GOVERNANCE', got '%s'", retention.Mode)
	}

	if retention.RetainUntilDate != "2025-12-31T23:59:59Z" {
		t.Errorf("Expected RetainUntilDate '2025-12-31T23:59:59Z', got '%s'", retention.RetainUntilDate)
	}

	// Test 3: GET retention on object without retention (should return error)
	key2 := "noretention.txt"
	putReq2 := httptest.NewRequest("PUT", "/"+bucket+"/"+key2, strings.NewReader("data"))
	putRec2 := httptest.NewRecorder()
	r.ServeHTTP(putRec2, putReq2)

	getRetReq2 := httptest.NewRequest("GET", "/"+bucket+"/"+key2+"?retention", nil)
	getRetRec2 := httptest.NewRecorder()
	r.ServeHTTP(getRetRec2, getRetReq2)

	if getRetRec2.Code == 200 {
		t.Errorf("Expected error for object without retention, got status 200")
	}
}

func TestObjectRetentionOnUpload(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Create bucket
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

	bucket := "retention-upload-test"
	if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	key := "testfile.txt"
	data := "test data with retention on upload"

	// PUT object WITH retention headers
	putReq := httptest.NewRequest("PUT", "/"+bucket+"/"+key, strings.NewReader(data))
	putReq.Header.Set("x-amz-object-lock-mode", "COMPLIANCE")
	putReq.Header.Set("x-amz-object-lock-retain-until-date", "2026-06-30T12:00:00Z")
	putRec := httptest.NewRecorder()
	r.ServeHTTP(putRec, putReq)

	if putRec.Code != 200 {
		body, _ := io.ReadAll(putRec.Body)
		t.Fatalf("PUT object with retention failed with status %d, body: %s", putRec.Code, string(body))
	}

	// GET retention to verify it was set during upload
	getRetReq := httptest.NewRequest("GET", "/"+bucket+"/"+key+"?retention", nil)
	getRetRec := httptest.NewRecorder()
	r.ServeHTTP(getRetRec, getRetReq)

	if getRetRec.Code != 200 {
		body, _ := io.ReadAll(getRetRec.Body)
		t.Fatalf("GET retention failed with status %d, body: %s", getRetRec.Code, string(body))
	}

	// Parse and verify response
	var retention struct {
		XMLName         xml.Name `xml:"Retention"`
		Mode            string   `xml:"Mode"`
		RetainUntilDate string   `xml:"RetainUntilDate"`
	}

	if err := xml.NewDecoder(getRetRec.Body).Decode(&retention); err != nil {
		t.Fatalf("Failed to decode retention response: %v", err)
	}

	if retention.Mode != "COMPLIANCE" {
		t.Errorf("Expected Mode 'COMPLIANCE', got '%s'", retention.Mode)
	}

	if retention.RetainUntilDate != "2026-06-30T12:00:00Z" {
		t.Errorf("Expected RetainUntilDate '2026-06-30T12:00:00Z', got '%s'", retention.RetainUntilDate)
	}
}

// TestListObjects_WithTrailingSlash tests the bug fix for issue where
// GET /bucket/?list-type=2 was incorrectly trying to download an empty key object
// instead of listing bucket contents
func TestListObjects_WithTrailingSlash(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Setup NATS connection and create bucket
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
	bucket := "test-trailing-slash"
	if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: bucket}); err != nil {
		t.Fatalf("create object store failed: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	// Upload test objects
	testObjects := []string{"file1.txt", "file2.txt", "dir/file3.txt"}
	for _, key := range testObjects {
		req := httptest.NewRequest("PUT", "/"+bucket+"/"+key, strings.NewReader("data"))
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("PUT %s unexpected status: %d", key, rec.Code)
		}
	}

	// Test 1: List with trailing slash and no query params
	t.Run("TrailingSlashNoQuery", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/"+bucket+"/", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != 200 {
			t.Fatalf("GET /%s/ unexpected status: %d, body: %s", bucket, rec.Code, rec.Body.String())
		}

		// Verify it's an XML list response, not object data
		var result ListBucketResult
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Expected XML list response, got error: %v\nBody: %s", err, rec.Body.String())
		}

		if result.Name != bucket {
			t.Errorf("Expected bucket name %q, got %q", bucket, result.Name)
		}

		if len(result.Contents) != 3 {
			t.Errorf("Expected 3 objects, got %d", len(result.Contents))
		}
	})

	// Test 2: List with trailing slash and list-type=2 (ListObjectsV2)
	t.Run("TrailingSlashWithListType2", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/"+bucket+"/?list-type=2", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != 200 {
			t.Fatalf("GET /%s/?list-type=2 unexpected status: %d, body: %s", bucket, rec.Code, rec.Body.String())
		}

		// Verify it's an XML list response
		var result ListBucketResult
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Expected XML list response, got error: %v\nBody: %s", err, rec.Body.String())
		}

		if result.Name != bucket {
			t.Errorf("Expected bucket name %q, got %q", bucket, result.Name)
		}

		if len(result.Contents) != 3 {
			t.Errorf("Expected 3 objects, got %d", len(result.Contents))
		}
	})

	// Test 3: List with trailing slash, list-type=2, and other params
	t.Run("TrailingSlashWithMultipleParams", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/"+bucket+"/?list-type=2&prefix=file", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != 200 {
			t.Fatalf("GET /%s/?list-type=2&prefix=file unexpected status: %d, body: %s", bucket, rec.Code, rec.Body.String())
		}

		// Verify it's an XML list response
		var result ListBucketResult
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Expected XML list response, got error: %v\nBody: %s", err, rec.Body.String())
		}

		if result.Name != bucket {
			t.Errorf("Expected bucket name %q, got %q", bucket, result.Name)
		}

		if result.Prefix != "file" {
			t.Errorf("Expected prefix 'file', got %q", result.Prefix)
		}

		// Should only return file1.txt and file2.txt (prefix "file")
		if len(result.Contents) != 2 {
			t.Errorf("Expected 2 objects with prefix 'file', got %d", len(result.Contents))
		}
	})

	// Test 4: Verify that without trailing slash also works
	t.Run("NoTrailingSlash", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/"+bucket+"?list-type=2", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != 200 {
			t.Fatalf("GET /%s?list-type=2 unexpected status: %d", bucket, rec.Code)
		}

		var result ListBucketResult
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Expected XML list response, got error: %v", err)
		}

		if len(result.Contents) != 3 {
			t.Errorf("Expected 3 objects, got %d", len(result.Contents))
		}
	})
}
