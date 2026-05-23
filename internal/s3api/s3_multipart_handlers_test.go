package s3api

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/wpnpeiris/nats-s3/internal/client"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
)

// Minimal shape to parse InitiateMultipartUpload XML response
type initResp struct {
	UploadId string `xml:"UploadId"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
}

func TestInitiateMultipartUpload_SucceedsAndPersistsSession(t *testing.T) {
	tests := []struct {
		name           string
		bucket         string
		key            string
		expectedStatus int
	}{
		{
			name:           "simple key",
			bucket:         "mpbucket",
			key:            "dir/parted.txt",
			expectedStatus: 200,
		},
		{
			name:           "nested path",
			bucket:         "mpbucket-nested",
			key:            "a/b/c/file.txt",
			expectedStatus: 200,
		},
		{
			name:           "root key",
			bucket:         "mpbucket-root",
			key:            "file.txt",
			expectedStatus: 200,
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

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			req := httptest.NewRequest("POST", "/"+tt.bucket+"/"+tt.key+"?uploads=", nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Fatalf("unexpected status: got %d, want %d body=%s", rr.Code, tt.expectedStatus, rr.Body.String())
			}

			var parsed initResp
			if err := xml.Unmarshal(rr.Body.Bytes(), &parsed); err != nil {
				t.Fatalf("unmarshal xml failed: %v\nxml=%s", err, rr.Body.String())
			}

			if parsed.Bucket != tt.bucket || parsed.Key != tt.key {
				t.Fatalf("unexpected response bucket/key: %q/%q", parsed.Bucket, parsed.Key)
			}
			if parsed.UploadId == "" {
				t.Fatalf("expected non-empty UploadId")
			}
			if _, err := uuid.Parse(parsed.UploadId); err != nil {
				t.Fatalf("UploadId is not a valid UUID: %v", err)
			}
		})
	}
}

func TestListParts(t *testing.T) {
	tests := []struct {
		name          string
		bucket        string
		key           string
		partsToUpload []int
		testPages     []struct {
			marker           int
			maxParts         int
			expectedParts    []int
			expectedTruncate bool
			expectedMarker   int
		}
	}{
		{
			name:          "paginate 7 parts deterministically",
			bucket:        "lpbucket",
			key:           "dir/large.txt",
			partsToUpload: []int{1, 2, 3, 4, 5, 6, 7},
			testPages: []struct {
				marker           int
				maxParts         int
				expectedParts    []int
				expectedTruncate bool
				expectedMarker   int
			}{
				{marker: 0, maxParts: 3, expectedParts: []int{1, 2, 3}, expectedTruncate: true, expectedMarker: 3},
				{marker: 3, maxParts: 3, expectedParts: []int{4, 5, 6}, expectedTruncate: true, expectedMarker: 6},
				{marker: 6, maxParts: 3, expectedParts: []int{7}, expectedTruncate: false, expectedMarker: 7},
			},
		},
		{
			name:          "no parts uploaded",
			bucket:        "nopartsbucket",
			key:           "empty/object",
			partsToUpload: []int{},
			testPages: []struct {
				marker           int
				maxParts         int
				expectedParts    []int
				expectedTruncate bool
				expectedMarker   int
			}{
				{marker: 0, maxParts: 10, expectedParts: []int{}, expectedTruncate: false, expectedMarker: 0},
			},
		},
		{
			name:          "marker beyond last part",
			bucket:        "markerbeyond",
			key:           "obj/key",
			partsToUpload: []int{1, 2, 3},
			testPages: []struct {
				marker           int
				maxParts         int
				expectedParts    []int
				expectedTruncate bool
				expectedMarker   int
			}{
				{marker: 10, maxParts: 2, expectedParts: []int{}, expectedTruncate: false, expectedMarker: 10},
			},
		},
		{
			name:          "non-contiguous parts",
			bucket:        "noncontig",
			key:           "obj/noncontig",
			partsToUpload: []int{1, 3, 5},
			testPages: []struct {
				marker           int
				maxParts         int
				expectedParts    []int
				expectedTruncate bool
				expectedMarker   int
			}{
				{marker: 0, maxParts: 2, expectedParts: []int{1, 3}, expectedTruncate: true, expectedMarker: 3},
				{marker: 3, maxParts: 2, expectedParts: []int{5}, expectedTruncate: false, expectedMarker: 5},
			},
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
			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			// Initiate multipart upload
			req := httptest.NewRequest("POST", "/"+tt.bucket+"/"+tt.key+"?uploads=", nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)
			if rr.Code != 200 {
				t.Fatalf("init status=%d body=%s", rr.Code, rr.Body.String())
			}
			var ir initResp
			if err := xml.Unmarshal(rr.Body.Bytes(), &ir); err != nil {
				t.Fatalf("unmarshal init xml failed: %v\nxml=%s", err, rr.Body.String())
			}

			// Upload parts
			for _, partNum := range tt.partsToUpload {
				body := bytes.NewBufferString(fmt.Sprintf("part-%d", partNum))
				upr := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s?uploadId=%s&partNumber=%d", tt.bucket, tt.key, ir.UploadId, partNum), body)
				upr.Header.Set("Content-Length", fmt.Sprintf("%d", body.Len()))
				uprr := httptest.NewRecorder()
				r.ServeHTTP(uprr, upr)
				if uprr.Code != 200 {
					t.Fatalf("upload part %d failed: status=%d body=%s", partNum, uprr.Code, uprr.Body.String())
				}
			}

			// Test pagination
			type listResp struct {
				IsTruncated          bool  `xml:"IsTruncated"`
				NextPartNumberMarker int   `xml:"NextPartNumberMarker"`
				PartNumbers          []int `xml:"Part>PartNumber"`
			}

			for pageIdx, page := range tt.testPages {
				url := fmt.Sprintf("/%s/%s?uploadId=%s&part-number-marker=%d&max-parts=%d", tt.bucket, tt.key, ir.UploadId, page.marker, page.maxParts)
				lr := httptest.NewRequest("GET", url, nil)
				lrr := httptest.NewRecorder()
				r.ServeHTTP(lrr, lr)
				if lrr.Code != 200 {
					t.Fatalf("list parts page %d failed: status=%d body=%s", pageIdx, lrr.Code, lrr.Body.String())
				}
				var out listResp
				if err := xml.Unmarshal(lrr.Body.Bytes(), &out); err != nil {
					t.Fatalf("unmarshal list xml page %d failed: %v\nxml=%s", pageIdx, err, lrr.Body.String())
				}

				if len(out.PartNumbers) != len(page.expectedParts) {
					t.Errorf("page %d: got %d parts, want %d", pageIdx, len(out.PartNumbers), len(page.expectedParts))
				}
				for i, expected := range page.expectedParts {
					if i < len(out.PartNumbers) && out.PartNumbers[i] != expected {
						t.Errorf("page %d part %d: got %d, want %d", pageIdx, i, out.PartNumbers[i], expected)
					}
				}
				if out.IsTruncated != page.expectedTruncate {
					t.Errorf("page %d: IsTruncated got %v, want %v", pageIdx, out.IsTruncated, page.expectedTruncate)
				}
				if out.NextPartNumberMarker != page.expectedMarker {
					t.Errorf("page %d: NextPartNumberMarker got %d, want %d", pageIdx, out.NextPartNumberMarker, page.expectedMarker)
				}
			}
		})
	}
}

func TestMultipartUploadPartCancellation(t *testing.T) {
	tests := []struct {
		name            string
		bucket          string
		key             string
		partNumber      int
		contentLength   int64
		cancelAfter     time.Duration
		expectedSuccess bool
	}{
		{
			name:            "cancel part upload aborts temp write",
			bucket:          "mp-cancel",
			key:             "file.bin",
			partNumber:      1,
			contentLength:   128 << 20, // 128 MB
			cancelAfter:     30 * time.Millisecond,
			expectedSuccess: false,
		},
		{
			name:            "cancel during large part upload",
			bucket:          "mp-cancel-large",
			key:             "large.bin",
			partNumber:      1,
			contentLength:   256 << 20, // 256 MB
			cancelAfter:     20 * time.Millisecond,
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

			// Create user bucket
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

			// Initiate multipart upload
			initReq := httptest.NewRequest("POST", "/"+tt.bucket+"/"+tt.key+"?uploads=", nil)
			initRec := httptest.NewRecorder()
			r.ServeHTTP(initRec, initReq)
			if initRec.Code != 200 {
				t.Fatalf("initiate unexpected status: %d body=%s", initRec.Code, initRec.Body.String())
			}
			var ir initResp
			if err := xml.Unmarshal(initRec.Body.Bytes(), &ir); err != nil || ir.UploadId == "" {
				t.Fatalf("failed to parse initiate response: %v xml=%s", err, initRec.Body.String())
			}

			// Upload part with cancelable body
			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				buf := make([]byte, 64*1024)
				for i := 0; i < 2000; i++ { // continuously write until canceled
					if _, err := pw.Write(buf); err != nil {
						return
					}
					time.Sleep(2 * time.Millisecond)
				}
			}()

			q := url.Values{}
			q.Set("uploadId", ir.UploadId)
			q.Set("partNumber", fmt.Sprintf("%d", tt.partNumber))
			upReq := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.key+"?"+q.Encode(), pr)
			ctx, cancel := context.WithCancel(upReq.Context())
			upReq = upReq.WithContext(ctx)
			upReq.ContentLength = tt.contentLength
			upRec := httptest.NewRecorder()

			done := make(chan struct{})
			go func() { defer close(done); r.ServeHTTP(upRec, upReq) }()
			time.Sleep(tt.cancelAfter)
			cancel()
			_ = pr.CloseWithError(context.Canceled)
			<-done

			if tt.expectedSuccess {
				if upRec.Code != 200 && upRec.Code != 204 {
					t.Fatalf("expected success, got %d", upRec.Code)
				}
			} else {
				if upRec.Code == 200 || upRec.Code == 204 {
					t.Fatalf("expected non-success on canceled part upload, got %d", upRec.Code)
				}
			}

			// Verify that temp part was not committed
			tempOS, err := js.ObjectStore(client.TempStoreName)
			if err != nil {
				t.Fatalf("temp ObjectStore failed: %v", err)
			}
			partKey := fmt.Sprintf("multi_parts/%s/%s/%s/%06d", tt.bucket, tt.key, ir.UploadId, tt.partNumber)
			if _, err := tempOS.GetInfo(partKey); err == nil {
				t.Fatalf("temp part unexpectedly exists after canceled part upload")
			} else if err != nats.ErrObjectNotFound {
				t.Fatalf("unexpected error when checking temp part: %v", err)
			}
		})
	}
}
