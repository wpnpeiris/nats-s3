package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/wpnpeiris/nats-s3/internal/model"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/streams"
	"github.com/wpnpeiris/nats-s3/internal/testutil"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
)

func TestObjectHandlers_CRUD(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		key    string
		data   string
	}{
		{
			name:   "basic object CRUD operations",
			bucket: "tobj",
			key:    "dir/sub/file.txt",
			data:   "hello-objects",
		},
		{
			name:   "simple key object CRUD",
			bucket: "tobj-simple",
			key:    "file.txt",
			data:   "simple data",
		},
		{
			name:   "nested path object CRUD",
			bucket: "tobj-nested",
			key:    "a/b/c/d/file.txt",
			data:   "deeply nested",
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
			if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: tt.bucket}); err != nil {
				t.Fatalf("create object store failed: %v", err)
			}

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			// PUT object
			putReq := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.key, strings.NewReader(tt.data))
			putRec := httptest.NewRecorder()
			r.ServeHTTP(putRec, putReq)
			if putRec.Code != 200 {
				t.Fatalf("PUT unexpected status: %d body=%s", putRec.Code, putRec.Body.String())
			}
			if etag := putRec.Header().Get("ETag"); etag == "" {
				t.Fatalf("expected ETag header on PUT response")
			}

			// HEAD object
			headReq := httptest.NewRequest("HEAD", "/"+tt.bucket+"/"+tt.key, nil)
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
			getReq := httptest.NewRequest("GET", "/"+tt.bucket+"/"+tt.key, nil)
			getRec := httptest.NewRecorder()
			r.ServeHTTP(getRec, getReq)
			if getRec.Code != 200 {
				t.Fatalf("GET unexpected status: %d", getRec.Code)
			}
			body, _ := io.ReadAll(getRec.Body)
			if string(body) != tt.data {
				t.Fatalf("GET unexpected body: %q", string(body))
			}

			// LIST objects
			listReq := httptest.NewRequest("GET", "/"+tt.bucket, nil)
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
				if k == tt.key {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("LIST did not include key %q: %+v", tt.key, parsed.Keys)
			}

			// DELETE object
			delReq := httptest.NewRequest("DELETE", "/"+tt.bucket+"/"+tt.key, nil)
			delRec := httptest.NewRecorder()
			r.ServeHTTP(delRec, delReq)
			if delRec.Code != 204 {
				t.Fatalf("DELETE unexpected status: %d", delRec.Code)
			}
		})
	}
}

func TestCopyObject(t *testing.T) {
	tests := []struct {
		name              string
		bucket            string
		sourceKey         string
		destKey           string
		data              string
		sourceContentType string
		sourceMetadata    map[string]string
		metadataDirective string
		destContentType   string
		destMetadata      map[string]string
		expectedStatus    int
		verifySourceMeta  bool
		verifyDestMeta    bool
		expectedDestCT    string
		expectedDestMeta  map[string]string
		invalidSource     bool
		nonExistentSource bool
	}{
		{
			name:              "basic copy with metadata",
			bucket:            "copy-test",
			sourceKey:         "source/file.txt",
			destKey:           "dest/file-copy.txt",
			data:              "hello-copy-test",
			sourceContentType: "text/plain",
			sourceMetadata:    map[string]string{"x-amz-meta-custom": "source-value"},
			expectedStatus:    200,
			verifyDestMeta:    true,
			expectedDestCT:    "text/plain",
			expectedDestMeta:  map[string]string{"x-amz-meta-custom": "source-value"},
		},
		{
			name:              "copy with REPLACE directive",
			bucket:            "copy-replace-test",
			sourceKey:         "source.txt",
			destKey:           "dest.txt",
			data:              "test-data",
			sourceMetadata:    map[string]string{"x-amz-meta-original": "old-value"},
			metadataDirective: "REPLACE",
			destContentType:   "application/json",
			destMetadata:      map[string]string{"x-amz-meta-new": "new-value"},
			expectedStatus:    200,
			verifyDestMeta:    true,
			expectedDestCT:    "application/json",
			expectedDestMeta:  map[string]string{"x-amz-meta-new": "new-value"},
		},
		{
			name:           "copy with invalid source format",
			bucket:         "copy-error-test",
			destKey:        "dest.txt",
			invalidSource:  true,
			expectedStatus: 400,
		},
		{
			name:              "copy with non-existent source",
			bucket:            "copy-error-test2",
			sourceKey:         "non-existent-key",
			destKey:           "dest.txt",
			nonExistentSource: true,
			expectedStatus:    404,
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
			if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: tt.bucket}); err != nil {
				t.Fatalf("create object store failed: %v", err)
			}

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			// Upload source object if not testing invalid/non-existent
			if !tt.invalidSource && !tt.nonExistentSource {
				putReq := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.sourceKey, strings.NewReader(tt.data))
				if tt.sourceContentType != "" {
					putReq.Header.Set("Content-Type", tt.sourceContentType)
				}
				for k, v := range tt.sourceMetadata {
					putReq.Header.Set(k, v)
				}
				putRec := httptest.NewRecorder()
				r.ServeHTTP(putRec, putReq)
				if putRec.Code != 200 {
					t.Fatalf("PUT source unexpected status: %d body=%s", putRec.Code, putRec.Body.String())
				}
			}

			// Perform copy
			copyReq := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.destKey, nil)
			if tt.invalidSource {
				copyReq.Header.Set("x-amz-copy-source", "just-bucket-name")
			} else {
				copyReq.Header.Set("x-amz-copy-source", tt.bucket+"/"+tt.sourceKey)
			}
			if tt.metadataDirective != "" {
				copyReq.Header.Set("x-amz-metadata-directive", tt.metadataDirective)
			}
			if tt.destContentType != "" {
				copyReq.Header.Set("Content-Type", tt.destContentType)
			}
			for k, v := range tt.destMetadata {
				copyReq.Header.Set(k, v)
			}
			copyRec := httptest.NewRecorder()
			r.ServeHTTP(copyRec, copyReq)

			if copyRec.Code != tt.expectedStatus {
				t.Fatalf("COPY unexpected status: got %d, want %d body=%s", copyRec.Code, tt.expectedStatus, copyRec.Body.String())
			}

			// If copy succeeded, verify result
			if tt.expectedStatus == 200 {
				var copyResult model.CopyObjectResult
				if err := xml.Unmarshal(copyRec.Body.Bytes(), &copyResult); err != nil {
					t.Fatalf("unmarshal copy result failed: %v\nxml=%s", err, copyRec.Body.String())
				}
				if copyResult.ETag == "" {
					t.Fatalf("copy result missing ETag")
				}
				if copyResult.LastModified.IsZero() {
					t.Fatalf("copy result missing LastModified")
				}

				// Verify destination content
				getReq := httptest.NewRequest("GET", "/"+tt.bucket+"/"+tt.destKey, nil)
				getRec := httptest.NewRecorder()
				r.ServeHTTP(getRec, getReq)
				if getRec.Code != 200 {
					t.Fatalf("GET dest unexpected status: %d", getRec.Code)
				}
				body, _ := io.ReadAll(getRec.Body)
				if string(body) != tt.data {
					t.Fatalf("GET dest unexpected body: %q, want %q", string(body), tt.data)
				}

				// Verify destination metadata
				if tt.verifyDestMeta {
					headReq := httptest.NewRequest("HEAD", "/"+tt.bucket+"/"+tt.destKey, nil)
					headRec := httptest.NewRecorder()
					r.ServeHTTP(headRec, headReq)
					if headRec.Code != 200 {
						t.Fatalf("HEAD dest unexpected status: %d", headRec.Code)
					}
					if ct := headRec.Header().Get("Content-Type"); ct != tt.expectedDestCT {
						t.Fatalf("HEAD dest Content-Type: %q, want %q", ct, tt.expectedDestCT)
					}
					for k, v := range tt.expectedDestMeta {
						if meta := headRec.Header().Get(k); meta != v {
							t.Fatalf("HEAD dest %s: %q, want %q", k, meta, v)
						}
					}
					// Verify old metadata is not present if REPLACE
					if tt.metadataDirective == "REPLACE" {
						for k := range tt.sourceMetadata {
							if _, exists := tt.expectedDestMeta[k]; !exists {
								if meta := headRec.Header().Get(k); meta != "" {
									t.Fatalf("HEAD dest should not have %s, got: %q", k, meta)
								}
							}
						}
					}
				}

				// Verify source still exists
				getSourceReq := httptest.NewRequest("GET", "/"+tt.bucket+"/"+tt.sourceKey, nil)
				getSourceRec := httptest.NewRecorder()
				r.ServeHTTP(getSourceRec, getSourceReq)
				if getSourceRec.Code != 200 {
					t.Fatalf("GET source after copy unexpected status: %d", getSourceRec.Code)
				}
			}
		})
	}
}

func TestListObjects_WithDelimiter(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(logger, s.ClientURL(), 1, nil, nil)
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
	bucket := "test-delimiter"
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: bucket}); err != nil {
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
	var result model.ListBucketResult
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
	gw, err := NewS3Gateway(logger, s.ClientURL(), 1, nil, nil)
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Create bucket and object
	natsEndpoint := s.Addr().String()
	nc, err := nats.Connect(natsEndpoint)
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	bucket := "retention-test"
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: bucket}); err != nil {
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
	gw, err := NewS3Gateway(logger, s.ClientURL(), 1, nil, nil)
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	// Create bucket
	natsEndpoint := s.Addr().String()
	nc, err := nats.Connect(natsEndpoint)
	nc.SetClosedHandler(func(_ *nats.Conn) {})
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	bucket := "retention-upload-test"
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: bucket}); err != nil {
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
	gw, err := NewS3Gateway(logger, s.ClientURL(), 1, nil, nil)
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
	bucket := "test-trailing-slash"
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: bucket}); err != nil {
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
		var result model.ListBucketResult
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
		var result model.ListBucketResult
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
		var result model.ListBucketResult
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

		var result model.ListBucketResult
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Expected XML list response, got error: %v", err)
		}

		if len(result.Contents) != 3 {
			t.Errorf("Expected 3 objects, got %d", len(result.Contents))
		}
	})
}

func TestUploadCancellation(t *testing.T) {
	tests := []struct {
		name            string
		bucket          string
		key             string
		totalSize       int64
		chunkSize       int
		sleepPerChunk   time.Duration
		cancelAfter     time.Duration
		expectedSuccess bool
	}{
		{
			name:            "cancel large upload aborts write",
			bucket:          "cancel-bucket",
			key:             "big-file.bin",
			totalSize:       10 * 1024 * 1024, // 10 MiB
			chunkSize:       64 * 1024,
			sleepPerChunk:   5 * time.Millisecond,
			cancelAfter:     50 * time.Millisecond,
			expectedSuccess: false,
		},
		{
			name:            "cancel very large upload",
			bucket:          "cancel-bucket-large",
			key:             "huge-file.bin",
			totalSize:       20 * 1024 * 1024, // 20 MiB
			chunkSize:       64 * 1024,
			sleepPerChunk:   3 * time.Millisecond,
			cancelAfter:     30 * time.Millisecond,
			expectedSuccess: false,
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
			if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: tt.bucket}); err != nil {
				t.Fatalf("create object store failed: %v", err)
			}

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			// Prepare a slow streaming body using an io.Pipe
			pr, pw := io.Pipe()
			chunk := make([]byte, tt.chunkSize)
			var wrote int64
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer pw.Close()
				for wrote < tt.totalSize {
					n, err := pw.Write(chunk)
					if err != nil {
						return
					}
					wrote += int64(n)
					time.Sleep(tt.sleepPerChunk)
				}
			}()

			// Build request with cancelable context and explicit Content-Length
			req := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.key, pr)
			ctx, cancel := context.WithCancel(req.Context())
			req = req.WithContext(ctx)
			req.ContentLength = tt.totalSize

			rec := httptest.NewRecorder()

			// Serve in a goroutine and cancel midway
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.ServeHTTP(rec, req)
			}()

			// Allow some data to be written then cancel and close the body reader
			time.Sleep(tt.cancelAfter)
			cancel()
			_ = pr.CloseWithError(context.Canceled)

			// Wait for handler to finish
			wg.Wait()
			<-done // ensure writer exited

			if tt.expectedSuccess {
				if rec.Code != 200 && rec.Code != 204 {
					t.Fatalf("expected success, got %d", rec.Code)
				}
			} else {
				if rec.Code == 200 || rec.Code == 204 {
					t.Fatalf("expected non-success status on canceled upload, got %d", rec.Code)
				}
			}

			// Verify object was not created
			os, err := js.ObjectStore(tt.bucket)
			if err != nil {
				t.Fatalf("ObjectStore failed: %v", err)
			}
			if _, err := os.GetInfo(tt.key); err == nil {
				t.Fatalf("object unexpectedly exists after canceled upload")
			} else if err != nats.ErrObjectNotFound {
				t.Fatalf("unexpected error when checking object: %v", err)
			}
		})
	}
}

// writeSigV4Chunk writes one SigV4 chunk frame to w with the given payload size.
func writeSigV4Chunk(w io.Writer, size int) error {
	// Chunk header: hex-size + CRLF, then payload bytes, then CRLF
	if _, err := fmt.Fprintf(w, "%x\r\n", size); err != nil {
		return err
	}
	if size > 0 {
		buf := make([]byte, size)
		if _, err := w.Write(buf); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\r\n"); err != nil {
			return err
		}
	}
	return nil
}

func TestStreamUploadCancellation(t *testing.T) {
	tests := []struct {
		name            string
		bucket          string
		key             string
		chunkSize       int
		numChunks       int
		sleepPerChunk   time.Duration
		cancelAfter     time.Duration
		expectedSuccess bool
	}{
		{
			name:            "cancel SigV4 streaming upload aborts write",
			bucket:          "cancel-sigv4",
			key:             "big-stream.bin",
			chunkSize:       32 * 1024,
			numChunks:       1000,
			sleepPerChunk:   3 * time.Millisecond,
			cancelAfter:     30 * time.Millisecond,
			expectedSuccess: false,
		},
		{
			name:            "cancel SigV4 large streaming upload",
			bucket:          "cancel-sigv4-large",
			key:             "huge-stream.bin",
			chunkSize:       64 * 1024,
			numChunks:       2000,
			sleepPerChunk:   2 * time.Millisecond,
			cancelAfter:     40 * time.Millisecond,
			expectedSuccess: false,
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
			if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: tt.bucket}); err != nil {
				t.Fatalf("create object store failed: %v", err)
			}

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			// Prepare a SigV4 chunked body via pipe
			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				// Write a series of chunks slowly
				for i := 0; i < tt.numChunks; i++ {
					if err := writeSigV4Chunk(pw, tt.chunkSize); err != nil {
						return
					}
					time.Sleep(tt.sleepPerChunk)
				}
				// Final zero-size chunk (not expected to be reached in test)
				_ = writeSigV4Chunk(pw, 0)
				io.WriteString(pw, "\r\n")
			}()

			req := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.key, pr)
			// Route to streaming handler by setting SigV4 streaming header
			req.Header.Set("x-amz-content-sha256", streams.SigV4StreamingPayload)
			ctx, cancel := context.WithCancel(req.Context())
			req = req.WithContext(ctx)

			rec := httptest.NewRecorder()

			done := make(chan struct{})
			go func() {
				defer close(done)
				r.ServeHTTP(rec, req)
			}()

			// Cancel soon after starting
			time.Sleep(tt.cancelAfter)
			cancel()
			_ = pr.CloseWithError(context.Canceled)

			<-done

			if tt.expectedSuccess {
				if rec.Code != 200 && rec.Code != 204 {
					t.Fatalf("expected success, got %d", rec.Code)
				}
			} else {
				if rec.Code == 200 || rec.Code == 204 {
					t.Fatalf("expected non-success status on canceled streaming upload, got %d", rec.Code)
				}
			}

			// Verify object was not created
			os, err := js.ObjectStore(tt.bucket)
			if err != nil {
				t.Fatalf("ObjectStore failed: %v", err)
			}
			if _, err := os.GetInfo(tt.key); err == nil {
				t.Fatalf("object unexpectedly exists after canceled streaming upload")
			} else if err != nats.ErrObjectNotFound {
				t.Fatalf("unexpected error when checking object: %v", err)
			}
		})
	}
}
