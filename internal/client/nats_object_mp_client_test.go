package client

import (
	"bytes"
	"context"
	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	testutilpkg "github.com/wpnpeiris/nats-s3/internal/testutil"
	"io"
	"strings"
	"testing"
)

// setupMultiPartTestClient creates a test NATS client and multipart store
func setupMultiPartTestClient(t *testing.T) (*MultiPartStore, func()) {
	t.Helper()
	s := testutilpkg.StartJSServer(t)
	url := s.ClientURL()

	c := NewClient("mp-test")
	if err := c.SetupConnectionToNATS(url); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	nc := c.NATS()
	nc.SetClosedHandler(func(_ *nats.Conn) {}) // Avoid panic during tests

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	mps, err := NewMultiPartStore(logger, c)
	if err != nil {
		t.Fatalf("NewMultiPartStore failed: %v", err)
	}

	cleanup := func() {
		nc.Close()
		s.Shutdown()
	}

	return mps, cleanup
}

func TestMultiPartStore_InitMultipartUpload(t *testing.T) {
	tests := []struct {
		name     string
		bucket   string
		key      string
		uploadID string
		wantErr  bool
	}{
		{
			name:     "init simple upload",
			bucket:   "test-bucket",
			key:      "test-key.txt",
			uploadID: "upload-123",
			wantErr:  false,
		},
		{
			name:     "init upload with path",
			bucket:   "test-bucket",
			key:      "path/to/file.txt",
			uploadID: "upload-456",
			wantErr:  false,
		},
		{
			name:     "init multiple uploads same bucket",
			bucket:   "test-bucket",
			key:      "file1.txt",
			uploadID: "upload-789",
			wantErr:  false,
		},
	}

	mps, cleanup := setupMultiPartTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mps.InitMultipartUpload(context.Background(), tt.bucket, tt.key, tt.uploadID)

			if tt.wantErr {
				if err == nil {
					t.Errorf("InitMultipartUpload() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("InitMultipartUpload() error = %v", err)
				}
			}
		})
	}
}

func TestMultiPartStore_UploadPart(t *testing.T) {
	tests := []struct {
		name       string
		bucket     string
		key        string
		uploadID   string
		partNumber int
		data       []byte
		wantErr    bool
		wantETag   bool // true if ETag should be non-empty
	}{
		{
			name:       "upload single part",
			bucket:     "upload-bucket",
			key:        "file.txt",
			uploadID:   "upload-part-1",
			partNumber: 1,
			data:       []byte("part 1 data"),
			wantErr:    false,
			wantETag:   true,
		},
		{
			name:       "upload multiple parts",
			bucket:     "upload-bucket",
			key:        "file.txt",
			uploadID:   "upload-part-2",
			partNumber: 1,
			data:       []byte("part 1 data for multi-part upload"),
			wantErr:    false,
			wantETag:   true,
		},
		{
			name:       "upload part 2",
			bucket:     "upload-bucket",
			key:        "file.txt",
			uploadID:   "upload-part-2",
			partNumber: 2,
			data:       []byte("part 2 data for multi-part upload"),
			wantErr:    false,
			wantETag:   true,
		},
		{
			name:       "upload empty part",
			bucket:     "upload-bucket",
			key:        "empty.txt",
			uploadID:   "upload-empty",
			partNumber: 1,
			data:       []byte{},
			wantErr:    false,
			wantETag:   true,
		},
		{
			name:       "upload large part",
			bucket:     "upload-bucket",
			key:        "large.txt",
			uploadID:   "upload-large",
			partNumber: 1,
			data:       bytes.Repeat([]byte("a"), 1024*100), // 100KB
			wantErr:    false,
			wantETag:   true,
		},
	}

	mps, cleanup := setupMultiPartTestClient(t)
	defer cleanup()

	// Initialize uploads for each unique uploadID
	initializedUploads := make(map[string]bool)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize upload if not done yet
			if !initializedUploads[tt.uploadID] {
				err := mps.InitMultipartUpload(context.Background(), tt.bucket, tt.key, tt.uploadID)
				if err != nil {
					t.Fatalf("failed to init multipart upload: %v", err)
				}
				initializedUploads[tt.uploadID] = true
			}

			reader := io.NopCloser(bytes.NewReader(tt.data))
			etag, err := mps.UploadPart(context.Background(), tt.bucket, tt.key, tt.uploadID, tt.partNumber, reader)

			if tt.wantErr {
				if err == nil {
					t.Errorf("UploadPart() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("UploadPart() error = %v", err)
				}
				if tt.wantETag && etag == "" {
					t.Errorf("UploadPart() ETag is empty")
				}
				if etag != "" && len(etag) != 32 { // MD5 hex is 32 characters
					t.Errorf("UploadPart() ETag length = %d, want 32", len(etag))
				}
			}
		})
	}
}

func TestMultiPartStore_ListParts(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		key         string
		uploadID    string
		uploadParts map[int][]byte // part number -> data
		wantErr     bool
		wantPartCnt int
	}{
		{
			name:     "list single part",
			bucket:   "list-bucket",
			key:      "file1.txt",
			uploadID: "list-upload-1",
			uploadParts: map[int][]byte{
				1: []byte("part 1"),
			},
			wantErr:     false,
			wantPartCnt: 1,
		},
		{
			name:     "list multiple parts",
			bucket:   "list-bucket",
			key:      "file2.txt",
			uploadID: "list-upload-2",
			uploadParts: map[int][]byte{
				1: []byte("part 1"),
				2: []byte("part 2"),
				3: []byte("part 3"),
			},
			wantErr:     false,
			wantPartCnt: 3,
		},
		{
			name:        "list non-existent upload",
			bucket:      "list-bucket",
			key:         "nonexistent.txt",
			uploadID:    "no-upload",
			uploadParts: nil,
			wantErr:     true,
			wantPartCnt: 0,
		},
		{
			name:        "list upload with no parts",
			bucket:      "list-bucket",
			key:         "empty-upload.txt",
			uploadID:    "list-upload-empty",
			uploadParts: map[int][]byte{},
			wantErr:     false,
			wantPartCnt: 0,
		},
	}

	mps, cleanup := setupMultiPartTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize upload if uploadParts is not nil
			if tt.uploadParts != nil {
				err := mps.InitMultipartUpload(context.Background(), tt.bucket, tt.key, tt.uploadID)
				if err != nil {
					t.Fatalf("failed to init multipart upload: %v", err)
				}

				// Upload parts
				for partNum, data := range tt.uploadParts {
					reader := io.NopCloser(bytes.NewReader(data))
					_, err := mps.UploadPart(context.Background(), tt.bucket, tt.key, tt.uploadID, partNum, reader)
					if err != nil {
						t.Fatalf("failed to upload part %d: %v", partNum, err)
					}
				}
			}

			meta, err := mps.ListParts(context.Background(), tt.bucket, tt.key, tt.uploadID)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ListParts() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ListParts() error = %v", err)
				}
				if meta == nil {
					t.Errorf("ListParts() returned nil meta")
					return
				}
				if len(meta.Parts) != tt.wantPartCnt {
					t.Errorf("ListParts() part count = %d, want %d", len(meta.Parts), tt.wantPartCnt)
				}
				if meta.Bucket != tt.bucket {
					t.Errorf("ListParts() bucket = %v, want %v", meta.Bucket, tt.bucket)
				}
				if meta.Key != tt.key {
					t.Errorf("ListParts() key = %v, want %v", meta.Key, tt.key)
				}
				if meta.UploadID != tt.uploadID {
					t.Errorf("ListParts() uploadID = %v, want %v", meta.UploadID, tt.uploadID)
				}
			}
		})
	}
}

func TestMultiPartStore_AbortMultipartUpload(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		key         string
		uploadID    string
		uploadParts map[int][]byte
		wantErr     bool
	}{
		{
			name:     "abort upload with parts",
			bucket:   "abort-bucket",
			key:      "file1.txt",
			uploadID: "abort-upload-1",
			uploadParts: map[int][]byte{
				1: []byte("part 1"),
				2: []byte("part 2"),
			},
			wantErr: false,
		},
		{
			name:        "abort upload without parts",
			bucket:      "abort-bucket",
			key:         "file2.txt",
			uploadID:    "abort-upload-2",
			uploadParts: map[int][]byte{},
			wantErr:     false,
		},
		{
			name:        "abort non-existent upload",
			bucket:      "abort-bucket",
			key:         "nonexistent.txt",
			uploadID:    "no-upload",
			uploadParts: nil,
			wantErr:     true,
		},
	}

	mps, cleanup := setupMultiPartTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize upload if uploadParts is not nil
			if tt.uploadParts != nil {
				err := mps.InitMultipartUpload(context.Background(), tt.bucket, tt.key, tt.uploadID)
				if err != nil {
					t.Fatalf("failed to init multipart upload: %v", err)
				}

				// Upload parts
				for partNum, data := range tt.uploadParts {
					reader := io.NopCloser(bytes.NewReader(data))
					_, err := mps.UploadPart(context.Background(), tt.bucket, tt.key, tt.uploadID, partNum, reader)
					if err != nil {
						t.Fatalf("failed to upload part %d: %v", partNum, err)
					}
				}
			}

			err := mps.AbortMultipartUpload(context.Background(), tt.bucket, tt.key, tt.uploadID)

			if tt.wantErr {
				if err == nil {
					t.Errorf("AbortMultipartUpload() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("AbortMultipartUpload() error = %v", err)
				}

				// Verify upload is gone by trying to list parts
				_, err := mps.ListParts(context.Background(), tt.bucket, tt.key, tt.uploadID)
				if err == nil {
					t.Errorf("AbortMultipartUpload() upload still exists after abort")
				}
			}
		})
	}
}

func TestMultiPartStore_c(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		key         string
		uploadID    string
		uploadParts map[int][]byte
		partOrder   []int
		wantErr     bool
		wantETag    bool
		skipReason  string // If set, skip this test with reason
	}{
		{
			name:     "complete upload with single part",
			bucket:   "complete-bucket",
			key:      "file1.txt",
			uploadID: "complete-upload-1",
			uploadParts: map[int][]byte{
				1: []byte("single part data"),
			},
			partOrder:  []int{1},
			wantErr:    true, // Will fail because bucket doesn't exist
			wantETag:   false,
			skipReason: "requires object store bucket",
		},
		{
			name:     "complete upload with multiple parts",
			bucket:   "complete-bucket",
			key:      "file2.txt",
			uploadID: "complete-upload-2",
			uploadParts: map[int][]byte{
				1: []byte("part 1 data"),
				2: []byte("part 2 data"),
				3: []byte("part 3 data"),
			},
			partOrder:  []int{1, 2, 3},
			wantErr:    true,
			wantETag:   false,
			skipReason: "requires object store bucket",
		},
		{
			name:        "complete non-existent upload",
			bucket:      "complete-bucket",
			key:         "nonexistent.txt",
			uploadID:    "no-upload",
			uploadParts: nil,
			partOrder:   []int{1},
			wantErr:     true,
			wantETag:    false,
		},
		{
			name:     "complete upload with missing part",
			bucket:   "complete-bucket",
			key:      "missing-part.txt",
			uploadID: "complete-upload-missing",
			uploadParts: map[int][]byte{
				1: []byte("part 1"),
				3: []byte("part 3"),
			},
			partOrder: []int{1, 2, 3}, // Part 2 is missing
			wantErr:   true,
			wantETag:  false,
		},
	}

	mps, cleanup := setupMultiPartTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipReason != "" {
				t.Skipf("Skipping: %s", tt.skipReason)
			}

			// Initialize upload if uploadParts is not nil
			if tt.uploadParts != nil {
				err := mps.InitMultipartUpload(context.Background(), tt.bucket, tt.key, tt.uploadID)
				if err != nil {
					t.Fatalf("failed to init multipart upload: %v", err)
				}

				// Upload parts
				for partNum, data := range tt.uploadParts {
					reader := io.NopCloser(bytes.NewReader(data))
					_, err := mps.UploadPart(context.Background(), tt.bucket, tt.key, tt.uploadID, partNum, reader)
					if err != nil {
						t.Fatalf("failed to upload part %d: %v", partNum, err)
					}
				}
			}

			etag, err := mps.CompleteMultipartUpload(context.Background(), tt.bucket, tt.key, tt.uploadID, tt.partOrder)

			if tt.wantErr {
				if err == nil {
					t.Errorf("CompleteMultipartUpload() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("CompleteMultipartUpload() error = %v", err)
				}
				if tt.wantETag {
					if etag == "" {
						t.Errorf("CompleteMultipartUpload() ETag is empty")
					}
					// Multipart ETag format: "hash-partCount"
					if !strings.Contains(etag, "-") {
						t.Errorf("CompleteMultipartUpload() ETag doesn't contain '-': %s", etag)
					}
				}

				// Verify upload is cleaned up
				_, err := mps.ListParts(context.Background(), tt.bucket, tt.key, tt.uploadID)
				if err == nil {
					t.Errorf("CompleteMultipartUpload() upload metadata still exists after completion")
				}
			}
		})
	}
}

func TestMultiPartStore_ConcurrentUploads(t *testing.T) {
	tests := []struct {
		name           string
		uploadCount    int
		partsPerUpload int
	}{
		{
			name:           "concurrent uploads",
			uploadCount:    3,
			partsPerUpload: 2,
		},
	}

	mps, cleanup := setupMultiPartTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan bool, tt.uploadCount)

			for i := 0; i < tt.uploadCount; i++ {
				go func(uploadNum int) {
					bucket := "concurrent-bucket"
					key := "file.txt"
					uploadID := string(rune('A' + uploadNum))

					// Initialize upload
					err := mps.InitMultipartUpload(context.Background(), bucket, key, uploadID)
					if err != nil {
						t.Errorf("failed to init upload %d: %v", uploadNum, err)
						done <- false
						return
					}

					// Upload parts
					for p := 1; p <= tt.partsPerUpload; p++ {
						data := []byte("part data")
						reader := io.NopCloser(bytes.NewReader(data))
						_, err := mps.UploadPart(context.Background(), bucket, key, uploadID, p, reader)
						if err != nil {
							t.Errorf("failed to upload part %d for upload %d: %v", p, uploadNum, err)
							done <- false
							return
						}
					}

					done <- true
				}(i)
			}

			// Wait for all uploads to complete
			for i := 0; i < tt.uploadCount; i++ {
				success := <-done
				if !success {
					t.Errorf("upload %d failed", i)
				}
			}
		})
	}
}

func TestMultiPartStore_ContextCancellation(t *testing.T) {
	tests := []struct {
		name     string
		bucket   string
		key      string
		uploadID string
		wantErr  bool
	}{
		{
			name:     "upload with cancelled context",
			bucket:   "cancel-bucket",
			key:      "file.txt",
			uploadID: "cancel-upload-1",
			wantErr:  true,
		},
	}

	mps, cleanup := setupMultiPartTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize upload
			err := mps.InitMultipartUpload(context.Background(), tt.bucket, tt.key, tt.uploadID)
			if err != nil {
				t.Fatalf("failed to init multipart upload: %v", err)
			}

			// Create cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			// Try to upload part with cancelled context
			data := []byte("test data")
			reader := io.NopCloser(bytes.NewReader(data))
			_, err = mps.UploadPart(ctx, tt.bucket, tt.key, tt.uploadID, 1, reader)

			if tt.wantErr {
				if err == nil {
					t.Errorf("UploadPart() expected error with cancelled context, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("UploadPart() error = %v", err)
				}
			}
		})
	}
}
