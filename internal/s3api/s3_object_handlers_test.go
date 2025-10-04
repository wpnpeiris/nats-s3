package s3api

import (
	"encoding/xml"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
)

func TestObjectHandlers_CRUD(t *testing.T) {
	s := startJSServer(t)
	defer s.Shutdown()

	gw := NewS3Gateway(s.ClientURL(), "", "")

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
