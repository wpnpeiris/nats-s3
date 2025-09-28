package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// Minimal shape to parse InitiateMultipartUpload XML response
type initResp struct {
	UploadId string `xml:"UploadId"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
}

func TestInitiateMultipartUpload_SucceedsAndPersistsSession(t *testing.T) {
	s := startJSServer(t)
	defer s.Shutdown()

	gw := NewS3Gateway(s.ClientURL(), "", "")

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	bucket := "mpbucket"
	key := "dir/parted.txt"

	req := httptest.NewRequest("POST", "/"+bucket+"/"+key+"?uploads=", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("unexpected status: %d body=%s", rr.Code, rr.Body.String())
	}

	var parsed initResp
	if err := xml.Unmarshal(rr.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("unmarshal xml failed: %v\nxml=%s", err, rr.Body.String())
	}

	if parsed.Bucket != bucket || parsed.Key != key {
		t.Fatalf("unexpected response bucket/key: %q/%q", parsed.Bucket, parsed.Key)
	}
	if parsed.UploadId == "" {
		t.Fatalf("expected non-empty UploadId")
	}
	if _, err := uuid.Parse(parsed.UploadId); err != nil {
		t.Fatalf("UploadId is not a valid UUID: %v", err)
	}

	// Verify the multipart session was recorded in the KV store.
	kv := gw.client.MultiPartStore.SessionStore
	sessionKey := fmt.Sprintf("multi_parts/%s/%s/%s", bucket, key, parsed.UploadId)
	if _, err := kv.Get(sessionKey); err != nil {
		t.Fatalf("expected session key in KV, got error: %v (key=%q)", err, sessionKey)
	}
}
