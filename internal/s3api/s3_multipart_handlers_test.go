package s3api

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"

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
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

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
}

func TestListParts_PaginatesDeterministically(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}
	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	bucket := "lpbucket"
	key := "dir/large.txt"

	// Initiate multipart upload
	req := httptest.NewRequest("POST", "/"+bucket+"/"+key+"?uploads=", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("init status=%d body=%s", rr.Code, rr.Body.String())
	}
	var ir initResp
	if err := xml.Unmarshal(rr.Body.Bytes(), &ir); err != nil {
		t.Fatalf("unmarshal init xml failed: %v\nxml=%s", err, rr.Body.String())
	}

	// Upload 7 parts with small payloads
	for i := 1; i <= 7; i++ {
		body := bytes.NewBufferString(fmt.Sprintf("part-%d", i))
		upr := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?uploadId=%s&partNumber=%d", bucket, key, ir.UploadId, i), body)
		upr.Header.Set("Content-Length", fmt.Sprintf("%d", body.Len()))
		uprr := httptest.NewRecorder()
		r.ServeHTTP(uprr, upr)
		if uprr.Code != 200 {
			t.Fatalf("upload part %d failed: status=%d body=%s", i, uprr.Code, uprr.Body.String())
		}
	}

	// Helper to call ListParts and parse minimal fields
	type listResp struct {
		IsTruncated          bool  `xml:"IsTruncated"`
		NextPartNumberMarker int   `xml:"NextPartNumberMarker"`
		PartNumbers          []int `xml:"Part>PartNumber"`
	}

	call := func(marker, max int) listResp {
		url := fmt.Sprintf("/%s/%s?uploadId=%s&part-number-marker=%d&max-parts=%d", bucket, key, ir.UploadId, marker, max)
		lr := httptest.NewRequest("GET", url, nil)
		lrr := httptest.NewRecorder()
		r.ServeHTTP(lrr, lr)
		if lrr.Code != 200 {
			t.Fatalf("list parts failed: status=%d body=%s", lrr.Code, lrr.Body.String())
		}
		var out listResp
		if err := xml.Unmarshal(lrr.Body.Bytes(), &out); err != nil {
			t.Fatalf("unmarshal list xml failed: %v\nxml=%s", err, lrr.Body.String())
		}
		return out
	}

	// Page 1: expect 1,2,3
	p1 := call(0, 3)
	if len(p1.PartNumbers) != 3 || p1.PartNumbers[0] != 1 || p1.PartNumbers[1] != 2 || p1.PartNumbers[2] != 3 {
		t.Fatalf("page1 parts unexpected: %+v", p1.PartNumbers)
	}
	if !p1.IsTruncated || p1.NextPartNumberMarker != 3 {
		t.Fatalf("page1 truncation/marker unexpected: truncated=%v next=%d", p1.IsTruncated, p1.NextPartNumberMarker)
	}

	// Page 2: expect 4,5,6
	p2 := call(p1.NextPartNumberMarker, 3)
	if len(p2.PartNumbers) != 3 || p2.PartNumbers[0] != 4 || p2.PartNumbers[1] != 5 || p2.PartNumbers[2] != 6 {
		t.Fatalf("page2 parts unexpected: %+v", p2.PartNumbers)
	}
	if !p2.IsTruncated || p2.NextPartNumberMarker != 6 {
		t.Fatalf("page2 truncation/marker unexpected: truncated=%v next=%d", p2.IsTruncated, p2.NextPartNumberMarker)
	}

	// Page 3: expect 7
	p3 := call(p2.NextPartNumberMarker, 3)
	if len(p3.PartNumbers) != 1 || p3.PartNumbers[0] != 7 {
		t.Fatalf("page3 parts unexpected: %+v", p3.PartNumbers)
	}
	if p3.IsTruncated || p3.NextPartNumberMarker != 7 {
		t.Fatalf("page3 truncation/marker unexpected: truncated=%v next=%d", p3.IsTruncated, p3.NextPartNumberMarker)
	}
}

func TestListParts_NoParts(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}
	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	bucket := "nopartsbucket"
	key := "empty/object"

	// Initiate but do not upload any parts
	req := httptest.NewRequest("POST", "/"+bucket+"/"+key+"?uploads=", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("init status=%d body=%s", rr.Code, rr.Body.String())
	}
	var ir initResp
	if err := xml.Unmarshal(rr.Body.Bytes(), &ir); err != nil {
		t.Fatalf("unmarshal init xml failed: %v\nxml=%s", err, rr.Body.String())
	}

	// List parts should be empty, not truncated, marker 0
	url := fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, ir.UploadId)
	lr := httptest.NewRequest("GET", url, nil)
	lrr := httptest.NewRecorder()
	r.ServeHTTP(lrr, lr)
	if lrr.Code != 200 {
		t.Fatalf("list parts failed: status=%d body=%s", lrr.Code, lrr.Body.String())
	}
	var out struct {
		IsTruncated          bool  `xml:"IsTruncated"`
		NextPartNumberMarker int   `xml:"NextPartNumberMarker"`
		PartNumbers          []int `xml:"Part>PartNumber"`
	}
	if err := xml.Unmarshal(lrr.Body.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal list xml failed: %v\nxml=%s", err, lrr.Body.String())
	}
	if out.IsTruncated || out.NextPartNumberMarker != 0 || len(out.PartNumbers) != 0 {
		t.Fatalf("unexpected empty list response: %+v", out)
	}
}

func TestListParts_MarkerBeyondLast(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}
	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	bucket := "markerbeyond"
	key := "obj/key"

	// Initiate multipart upload
	req := httptest.NewRequest("POST", "/"+bucket+"/"+key+"?uploads=", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("init status=%d body=%s", rr.Code, rr.Body.String())
	}
	var ir initResp
	if err := xml.Unmarshal(rr.Body.Bytes(), &ir); err != nil {
		t.Fatalf("unmarshal init xml failed: %v\nxml=%s", err, rr.Body.String())
	}

	// Upload 3 parts
	for i := 1; i <= 3; i++ {
		body := bytes.NewBufferString(fmt.Sprintf("p-%d", i))
		upr := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?uploadId=%s&partNumber=%d", bucket, key, ir.UploadId, i), body)
		upr.Header.Set("Content-Length", fmt.Sprintf("%d", body.Len()))
		uprr := httptest.NewRecorder()
		r.ServeHTTP(uprr, upr)
		if uprr.Code != 200 {
			t.Fatalf("upload part %d failed: status=%d body=%s", i, uprr.Code, uprr.Body.String())
		}
	}

	// Use a marker beyond the last part
	url := fmt.Sprintf("/%s/%s?uploadId=%s&part-number-marker=%d&max-parts=%d", bucket, key, ir.UploadId, 10, 2)
	lr := httptest.NewRequest("GET", url, nil)
	lrr := httptest.NewRecorder()
	r.ServeHTTP(lrr, lr)
	if lrr.Code != 200 {
		t.Fatalf("list parts failed: status=%d body=%s", lrr.Code, lrr.Body.String())
	}
	var out struct {
		IsTruncated          bool  `xml:"IsTruncated"`
		NextPartNumberMarker int   `xml:"NextPartNumberMarker"`
		PartNumbers          []int `xml:"Part>PartNumber"`
	}
	if err := xml.Unmarshal(lrr.Body.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal list xml failed: %v\nxml=%s", err, lrr.Body.String())
	}
	if out.IsTruncated || out.NextPartNumberMarker != 10 || len(out.PartNumbers) != 0 {
		t.Fatalf("unexpected marker-beyond response: %+v", out)
	}
}

func TestListParts_NonContiguousParts(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(context.Background(), logger, s.ClientURL(), nil, nil, S3GatewayOptions{})
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}
	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	bucket := "noncontig"
	key := "obj/noncontig"

	// Initiate multipart upload
	req := httptest.NewRequest("POST", "/"+bucket+"/"+key+"?uploads=", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("init status=%d body=%s", rr.Code, rr.Body.String())
	}
	var ir initResp
	if err := xml.Unmarshal(rr.Body.Bytes(), &ir); err != nil {
		t.Fatalf("unmarshal init xml failed: %v\nxml=%s", err, rr.Body.String())
	}

	// Upload parts 1,3,5 only
	for _, i := range []int{1, 3, 5} {
		body := bytes.NewBufferString(fmt.Sprintf("pc-%d", i))
		upr := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?uploadId=%s&partNumber=%d", bucket, key, ir.UploadId, i), body)
		upr.Header.Set("Content-Length", fmt.Sprintf("%d", body.Len()))
		uprr := httptest.NewRecorder()
		r.ServeHTTP(uprr, upr)
		if uprr.Code != 200 {
			t.Fatalf("upload part %d failed: status=%d body=%s", i, uprr.Code, uprr.Body.String())
		}
	}

	// Page 1: expect 1,3 (max 2)
	url := fmt.Sprintf("/%s/%s?uploadId=%s&part-number-marker=0&max-parts=2", bucket, key, ir.UploadId)
	lr := httptest.NewRequest("GET", url, nil)
	lrr := httptest.NewRecorder()
	r.ServeHTTP(lrr, lr)
	if lrr.Code != 200 {
		t.Fatalf("list parts failed: status=%d body=%s", lrr.Code, lrr.Body.String())
	}
	var p1 struct {
		IsTruncated          bool  `xml:"IsTruncated"`
		NextPartNumberMarker int   `xml:"NextPartNumberMarker"`
		PartNumbers          []int `xml:"Part>PartNumber"`
	}
	if err := xml.Unmarshal(lrr.Body.Bytes(), &p1); err != nil {
		t.Fatalf("unmarshal list xml failed: %v\nxml=%s", err, lrr.Body.String())
	}
	if len(p1.PartNumbers) != 2 || p1.PartNumbers[0] != 1 || p1.PartNumbers[1] != 3 || !p1.IsTruncated || p1.NextPartNumberMarker != 3 {
		t.Fatalf("unexpected page1: %+v", p1)
	}

	// Page 2 from marker 3: expect 5
	url = fmt.Sprintf("/%s/%s?uploadId=%s&part-number-marker=%d&max-parts=2", bucket, key, ir.UploadId, p1.NextPartNumberMarker)
	lr = httptest.NewRequest("GET", url, nil)
	lrr = httptest.NewRecorder()
	r.ServeHTTP(lrr, lr)
	if lrr.Code != 200 {
		t.Fatalf("list parts failed: status=%d body=%s", lrr.Code, lrr.Body.String())
	}
	var p2 struct {
		IsTruncated          bool  `xml:"IsTruncated"`
		NextPartNumberMarker int   `xml:"NextPartNumberMarker"`
		PartNumbers          []int `xml:"Part>PartNumber"`
	}
	if err := xml.Unmarshal(lrr.Body.Bytes(), &p2); err != nil {
		t.Fatalf("unmarshal list xml failed: %v\nxml=%s", err, lrr.Body.String())
	}
	if len(p2.PartNumbers) != 1 || p2.PartNumbers[0] != 5 || p2.IsTruncated || p2.NextPartNumberMarker != 5 {
		t.Fatalf("unexpected page2: %+v", p2)
	}
}
