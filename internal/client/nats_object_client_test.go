package client

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"
)

// setupTestClient creates a test NATS client and object client
func setupTestClient(t *testing.T) (*NatsObjectClient, func()) {
	t.Helper()
	s := testutil.StartJSServer(t)
	url := s.ClientURL()

	c := NewClient("object-test")
	if err := c.SetupConnectionToNATS(url); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	nc := c.NATS()
	nc.SetClosedHandler(func(_ *nats.Conn) {}) // Avoid panic during tests

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	oc, err := NewNatsObjectClient(logger, c, NatsObjectClientOptions{Replicas: 1})
	if err != nil {
		t.Fatalf("NewNatsObjectClient failed: %v", err)
	}

	cleanup := func() {
		nc.Close()
		s.Shutdown()
	}

	return oc, cleanup
}

func TestNatsObjectClient_CreateBucket(t *testing.T) {
	tests := []struct {
		name       string
		bucketName string
		wantErr    bool
	}{
		{
			name:       "create valid bucket",
			bucketName: "valid-bucket",
			wantErr:    false,
		},
		{
			name:       "create bucket with underscore",
			bucketName: "test_bucket",
			wantErr:    false,
		},
		{
			name:       "create duplicate bucket",
			bucketName: "duplicate-bucket",
			wantErr:    true,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pre-create bucket for duplicate test
			if tt.name == "create duplicate bucket" {
				if _, err := oc.CreateBucket(context.Background(), tt.bucketName); err != nil {
					t.Fatalf("failed to pre-create bucket: %v", err)
				}
			}

			status, err := oc.CreateBucket(context.Background(), tt.bucketName)

			if tt.wantErr {
				if err == nil {
					t.Errorf("CreateBucket() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("CreateBucket() error = %v", err)
				return
			}

			if status.Bucket() != tt.bucketName {
				t.Errorf("CreateBucket() bucket name = %v, want %v", status.Bucket(), tt.bucketName)
			}
		})
	}
}

func TestNatsObjectClient_DeleteBucket(t *testing.T) {
	tests := []struct {
		name       string
		bucketName string
		preCreate  bool
		wantErr    bool
	}{
		{
			name:       "delete existing bucket",
			bucketName: "delete-bucket",
			preCreate:  true,
			wantErr:    false,
		},
		{
			name:       "delete non-existent bucket",
			bucketName: "non-existent",
			preCreate:  false,
			wantErr:    true,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preCreate {
				if _, err := oc.CreateBucket(context.Background(), tt.bucketName); err != nil {
					t.Fatalf("failed to create bucket: %v", err)
				}
			}

			err := oc.DeleteBucket(context.Background(), tt.bucketName)

			if tt.wantErr {
				if err == nil {
					t.Errorf("DeleteBucket() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("DeleteBucket() error = %v", err)
				}
			}
		})
	}
}

func TestNatsObjectClient_PutObjectStream(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		key         string
		contentType string
		metadata    map[string]string
		data        []byte
		wantErr     bool
	}{
		{
			name:        "put simple object",
			bucket:      "test-put",
			key:         "file.txt",
			contentType: "text/plain",
			metadata:    map[string]string{"author": "test"},
			data:        []byte("hello world"),
			wantErr:     false,
		},
		{
			name:        "put object with path",
			bucket:      "test-put",
			key:         "path/to/file.txt",
			contentType: "text/plain",
			metadata:    nil,
			data:        []byte("nested object"),
			wantErr:     false,
		},
		{
			name:        "put empty object",
			bucket:      "test-put",
			key:         "empty.txt",
			contentType: "text/plain",
			metadata:    nil,
			data:        []byte{},
			wantErr:     false,
		},
		{
			name:        "put binary object",
			bucket:      "test-put",
			key:         "binary.dat",
			contentType: "application/octet-stream",
			metadata:    nil,
			data:        []byte{0x00, 0x01, 0x02, 0xFF},
			wantErr:     false,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	// Create bucket once
	if _, err := oc.CreateBucket(context.Background(), "test-put"); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := oc.PutObjectStream(
				context.Background(),
				tt.bucket,
				tt.key,
				tt.contentType,
				tt.metadata,
				bytes.NewReader(tt.data),
			)

			if tt.wantErr {
				if err == nil {
					t.Errorf("PutObjectStream() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("PutObjectStream() error = %v", err)
				return
			}

			if info.Name != tt.key {
				t.Errorf("PutObjectStream() key = %v, want %v", info.Name, tt.key)
			}

			if info.Size != uint64(len(tt.data)) {
				t.Errorf("PutObjectStream() size = %v, want %v", info.Size, len(tt.data))
			}

			if tt.contentType != "" && info.Headers.Get("Content-Type") != tt.contentType {
				t.Errorf("PutObjectStream() content-type = %v, want %v", info.Headers.Get("Content-Type"), tt.contentType)
			}

			for k, v := range tt.metadata {
				if info.Metadata[k] != v {
					t.Errorf("PutObjectStream() metadata[%s] = %v, want %v", k, info.Metadata[k], v)
				}
			}
		})
	}
}

func TestNatsObjectClient_GetObjectInfo(t *testing.T) {
	tests := []struct {
		name      string
		bucket    string
		key       string
		preCreate bool
		wantErr   bool
	}{
		{
			name:      "get existing object info",
			bucket:    "test-info",
			key:       "exists.txt",
			preCreate: true,
			wantErr:   false,
		},
		{
			name:      "get non-existent object info",
			bucket:    "test-info",
			key:       "notfound.txt",
			preCreate: false,
			wantErr:   true,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	// Create bucket once
	if _, err := oc.CreateBucket(context.Background(), "test-info"); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preCreate {
				data := []byte("test data")
				if _, err := oc.PutObjectStream(
					context.Background(),
					tt.bucket,
					tt.key,
					"text/plain",
					nil,
					bytes.NewReader(data),
				); err != nil {
					t.Fatalf("failed to create object: %v", err)
				}
			}

			info, err := oc.GetObjectInfo(context.Background(), tt.bucket, tt.key)

			if tt.wantErr {
				if err == nil {
					t.Errorf("GetObjectInfo() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("GetObjectInfo() error = %v", err)
				}
				if info == nil {
					t.Errorf("GetObjectInfo() returned nil info")
				} else if info.Name != tt.key {
					t.Errorf("GetObjectInfo() key = %v, want %v", info.Name, tt.key)
				}
			}
		})
	}
}

func TestNatsObjectClient_GetObject(t *testing.T) {
	tests := []struct {
		name     string
		bucket   string
		key      string
		putData  []byte
		wantErr  bool
		wantData []byte
	}{
		{
			name:     "get text object",
			bucket:   "test-get",
			key:      "text.txt",
			putData:  []byte("hello world"),
			wantErr:  false,
			wantData: []byte("hello world"),
		},
		{
			name:     "get binary object",
			bucket:   "test-get",
			key:      "binary.dat",
			putData:  []byte{0x00, 0xFF, 0xAA, 0x55},
			wantErr:  false,
			wantData: []byte{0x00, 0xFF, 0xAA, 0x55},
		},
		{
			name:     "get large object",
			bucket:   "test-get",
			key:      "large.txt",
			putData:  bytes.Repeat([]byte("a"), 1024*10), // 10KB
			wantErr:  false,
			wantData: bytes.Repeat([]byte("a"), 1024*10),
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	// Create bucket once
	if _, err := oc.CreateBucket(context.Background(), "test-get"); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Put object first
			if _, err := oc.PutObjectStream(
				context.Background(),
				tt.bucket,
				tt.key,
				"application/octet-stream",
				nil,
				bytes.NewReader(tt.putData),
			); err != nil {
				t.Fatalf("failed to put object: %v", err)
			}

			info, data, err := oc.GetObject(context.Background(), tt.bucket, tt.key)

			if tt.wantErr {
				if err == nil {
					t.Errorf("GetObject() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("GetObject() error = %v", err)
				return
			}

			if info == nil {
				t.Errorf("GetObject() returned nil info")
				return
			}

			if !bytes.Equal(data, tt.wantData) {
				t.Errorf("GetObject() data mismatch, got %d bytes, want %d bytes", len(data), len(tt.wantData))
			}

			if info.Name != tt.key {
				t.Errorf("GetObject() key = %v, want %v", info.Name, tt.key)
			}
		})
	}
}

func TestNatsObjectClient_DeleteObject(t *testing.T) {
	tests := []struct {
		name      string
		bucket    string
		key       string
		preCreate bool
		wantErr   bool
	}{
		{
			name:      "delete existing object",
			bucket:    "test-delete",
			key:       "delete-me.txt",
			preCreate: true,
			wantErr:   false,
		},
		{
			name:      "delete non-existent object",
			bucket:    "test-delete",
			key:       "not-exist.txt",
			preCreate: false,
			wantErr:   true,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	// Create bucket once
	if _, err := oc.CreateBucket(context.Background(), "test-delete"); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preCreate {
				if _, err := oc.PutObjectStream(
					context.Background(),
					tt.bucket,
					tt.key,
					"text/plain",
					nil,
					bytes.NewReader([]byte("data")),
				); err != nil {
					t.Fatalf("failed to create object: %v", err)
				}
			}

			err := oc.DeleteObject(context.Background(), tt.bucket, tt.key)

			if tt.wantErr {
				if err == nil {
					t.Errorf("DeleteObject() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("DeleteObject() error = %v", err)
				}

				// Verify deletion
				if _, err := oc.GetObjectInfo(context.Background(), tt.bucket, tt.key); err == nil {
					t.Errorf("DeleteObject() object still exists after deletion")
				}
			}
		})
	}
}

func TestNatsObjectClient_ListBuckets(t *testing.T) {
	tests := []struct {
		name            string
		createBuckets   []string
		expectContains  []string
		wantErr         bool
		timeoutDuration time.Duration
	}{
		{
			name:            "list single bucket",
			createBuckets:   []string{"bucket1"},
			expectContains:  []string{"bucket1"},
			wantErr:         false,
			timeoutDuration: 2 * time.Second,
		},
		{
			name:            "list multiple buckets",
			createBuckets:   []string{"alpha", "beta", "gamma"},
			expectContains:  []string{"alpha", "beta", "gamma"},
			wantErr:         false,
			timeoutDuration: 2 * time.Second,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create buckets
			for _, bucket := range tt.createBuckets {
				if _, err := oc.CreateBucket(context.Background(), bucket); err != nil {
					t.Fatalf("failed to create bucket %s: %v", bucket, err)
				}
			}

			ch, err := oc.ListBuckets(context.Background())

			if tt.wantErr {
				if err == nil {
					t.Errorf("ListBuckets() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("ListBuckets() error = %v", err)
				return
			}

			found := make(map[string]bool)
			timeout := time.After(tt.timeoutDuration)

		loop:
			for {
				select {
				case status, ok := <-ch:
					if !ok {
						break loop
					}
					found[status.Bucket()] = true
				case <-timeout:
					t.Fatalf("timeout waiting for ListBuckets")
				}
			}

			for _, expected := range tt.expectContains {
				if !found[expected] {
					t.Errorf("ListBuckets() missing expected bucket: %s", expected)
				}
			}
		})
	}
}

func TestNatsObjectClient_ListObjects(t *testing.T) {
	tests := []struct {
		name           string
		bucket         string
		createObjects  map[string][]byte // key -> data
		expectContains []string
		wantErr        bool
	}{
		{
			name:   "list single object",
			bucket: "list-bucket1",
			createObjects: map[string][]byte{
				"file1.txt": []byte("data1"),
			},
			expectContains: []string{"file1.txt"},
			wantErr:        false,
		},
		{
			name:   "list multiple objects",
			bucket: "list-bucket2",
			createObjects: map[string][]byte{
				"a.txt":          []byte("a"),
				"b.txt":          []byte("b"),
				"dir/nested.txt": []byte("nested"),
			},
			expectContains: []string{"a.txt", "b.txt", "dir/nested.txt"},
			wantErr:        false,
		},
		{
			name:           "list empty bucket",
			bucket:         "empty-bucket",
			createObjects:  map[string][]byte{},
			expectContains: []string{},
			wantErr:        true, // ListObjects returns error for empty buckets
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create bucket
			if _, err := oc.CreateBucket(context.Background(), tt.bucket); err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}

			// Create objects
			for key, data := range tt.createObjects {
				if _, err := oc.PutObjectStream(
					context.Background(),
					tt.bucket,
					key,
					"text/plain",
					nil,
					bytes.NewReader(data),
				); err != nil {
					t.Fatalf("failed to create object %s: %v", key, err)
				}
			}

			objects, err := oc.ListObjects(context.Background(), tt.bucket)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ListObjects() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("ListObjects() error = %v", err)
				return
			}

			found := make(map[string]bool)
			for _, obj := range objects {
				found[obj.Name] = true
			}

			if len(tt.expectContains) != len(found) {
				t.Errorf("ListObjects() count = %d, want %d", len(found), len(tt.expectContains))
			}

			for _, expected := range tt.expectContains {
				if !found[expected] {
					t.Errorf("ListObjects() missing expected object: %s", expected)
				}
			}
		})
	}
}

func TestNatsObjectClient_GetObjectRetention(t *testing.T) {
	tests := []struct {
		name            string
		bucket          string
		key             string
		setRetention    bool
		retentionMode   string
		retainUntilDate string
		wantMode        string
		wantRetainDate  string
		wantErr         bool
		wantErrType     error
	}{
		{
			name:            "get retention with GOVERNANCE mode",
			bucket:          "retention-bucket1",
			key:             "file1.txt",
			setRetention:    true,
			retentionMode:   "GOVERNANCE",
			retainUntilDate: "2030-01-01T00:00:00Z",
			wantMode:        "GOVERNANCE",
			wantRetainDate:  "2030-01-01T00:00:00Z",
			wantErr:         false,
		},
		{
			name:            "get retention with COMPLIANCE mode",
			bucket:          "retention-bucket2",
			key:             "file2.txt",
			setRetention:    true,
			retentionMode:   "COMPLIANCE",
			retainUntilDate: "2035-12-31T23:59:59Z",
			wantMode:        "COMPLIANCE",
			wantRetainDate:  "2035-12-31T23:59:59Z",
			wantErr:         false,
		},
		{
			name:         "get retention without setting retention",
			bucket:       "retention-bucket3",
			key:          "file3.txt",
			setRetention: false,
			wantMode:     "",
			wantErr:      true,
			wantErrType:  ErrObjectNotFound,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create bucket
			if _, err := oc.CreateBucket(context.Background(), tt.bucket); err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}

			// Create object
			if _, err := oc.PutObjectStream(
				context.Background(),
				tt.bucket,
				tt.key,
				"text/plain",
				nil,
				bytes.NewReader([]byte("data")),
			); err != nil {
				t.Fatalf("failed to create object: %v", err)
			}

			// Set retention if needed
			if tt.setRetention {
				if err := oc.PutObjectRetention(
					context.Background(),
					tt.bucket,
					tt.key,
					tt.retentionMode,
					tt.retainUntilDate,
				); err != nil {
					t.Fatalf("failed to set retention: %v", err)
				}
			}

			mode, retainDate, err := oc.GetObjectRetention(context.Background(), tt.bucket, tt.key)

			if tt.wantErr {
				if err == nil {
					t.Errorf("GetObjectRetention() expected error, got nil")
				}
				if tt.wantErrType != nil && err != tt.wantErrType {
					t.Errorf("GetObjectRetention() error = %v, want %v", err, tt.wantErrType)
				}
				return
			}

			if err != nil {
				t.Errorf("GetObjectRetention() error = %v", err)
				return
			}

			if mode != tt.wantMode {
				t.Errorf("GetObjectRetention() mode = %v, want %v", mode, tt.wantMode)
			}

			if tt.setRetention && retainDate != tt.wantRetainDate {
				t.Errorf("GetObjectRetention() retainDate = %v, want %v", retainDate, tt.wantRetainDate)
			}
		})
	}
}

func TestNatsObjectClient_PutObjectRetention(t *testing.T) {
	tests := []struct {
		name            string
		bucket          string
		key             string
		retentionMode   string
		retainUntilDate string
		wantErr         bool
	}{
		{
			name:            "set GOVERNANCE retention",
			bucket:          "put-retention1",
			key:             "file1.txt",
			retentionMode:   "GOVERNANCE",
			retainUntilDate: "2030-01-01T00:00:00Z",
			wantErr:         false,
		},
		{
			name:            "set COMPLIANCE retention",
			bucket:          "put-retention2",
			key:             "file2.txt",
			retentionMode:   "COMPLIANCE",
			retainUntilDate: "2030-01-01T00:00:00Z",
			wantErr:         false,
		},
		{
			name:            "update existing retention",
			bucket:          "put-retention3",
			key:             "file3.txt",
			retentionMode:   "GOVERNANCE",
			retainUntilDate: "2040-01-01T00:00:00Z",
			wantErr:         false,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create bucket
			if _, err := oc.CreateBucket(context.Background(), tt.bucket); err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}

			// Create object
			if _, err := oc.PutObjectStream(
				context.Background(),
				tt.bucket,
				tt.key,
				"text/plain",
				nil,
				bytes.NewReader([]byte("data")),
			); err != nil {
				t.Fatalf("failed to create object: %v", err)
			}

			err := oc.PutObjectRetention(
				context.Background(),
				tt.bucket,
				tt.key,
				tt.retentionMode,
				tt.retainUntilDate,
			)

			if tt.wantErr {
				if err == nil {
					t.Errorf("PutObjectRetention() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("PutObjectRetention() error = %v", err)
				return
			}

			// Verify retention was set
			mode, retainDate, err := oc.GetObjectRetention(context.Background(), tt.bucket, tt.key)
			if err != nil {
				t.Errorf("failed to verify retention: %v", err)
				return
			}

			if mode != tt.retentionMode {
				t.Errorf("PutObjectRetention() mode = %v, want %v", mode, tt.retentionMode)
			}

			if retainDate != tt.retainUntilDate {
				t.Errorf("PutObjectRetention() retainDate = %v, want %v", retainDate, tt.retainUntilDate)
			}
		})
	}
}

func TestNatsObjectClient_PutObjectTags(t *testing.T) {
	tests := []struct {
		name    string
		bucket  string
		key     string
		tags    map[string]string
		wantErr bool
	}{
		{
			name:   "put single tag",
			bucket: "tag-bucket1",
			key:    "file1.txt",
			tags: map[string]string{
				"Environment": "production",
			},
			wantErr: false,
		},
		{
			name:   "put multiple tags",
			bucket: "tag-bucket2",
			key:    "file2.txt",
			tags: map[string]string{
				"Environment": "test",
				"Owner":       "team-a",
				"Project":     "demo",
			},
			wantErr: false,
		},
		{
			name:    "put empty tags",
			bucket:  "tag-bucket3",
			key:     "file3.txt",
			tags:    map[string]string{},
			wantErr: false,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create bucket
			if _, err := oc.CreateBucket(context.Background(), tt.bucket); err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}

			// Create object
			if _, err := oc.PutObjectStream(
				context.Background(),
				tt.bucket,
				tt.key,
				"text/plain",
				nil,
				bytes.NewReader([]byte("data")),
			); err != nil {
				t.Fatalf("failed to create object: %v", err)
			}

			err := oc.PutObjectTags(context.Background(), tt.bucket, tt.key, tt.tags)

			if tt.wantErr {
				if err == nil {
					t.Errorf("PutObjectTags() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("PutObjectTags() error = %v", err)
				return
			}

			// Verify tags were set by getting object info
			info, err := oc.GetObjectInfo(context.Background(), tt.bucket, tt.key)
			if err != nil {
				t.Errorf("failed to get object info: %v", err)
				return
			}

			for k, v := range tt.tags {
				// Tags are stored directly with their key name (not with x-amz-tag- prefix in PutObjectTags)
				if info.Metadata[k] != v {
					t.Errorf("PutObjectTags() tag %s = %v, want %v", k, info.Metadata[k], v)
				}
			}
		})
	}
}

func TestNatsObjectClient_DeleteObjectTags(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		key         string
		initialTags map[string]string
		wantErr     bool
	}{
		{
			name:   "delete tags from tagged object",
			bucket: "del-tag-bucket1",
			key:    "file1.txt",
			initialTags: map[string]string{
				"Environment": "dev",
				"Owner":       "test",
			},
			wantErr: false,
		},
		{
			name:        "delete tags from untagged object",
			bucket:      "del-tag-bucket2",
			key:         "file2.txt",
			initialTags: nil,
			wantErr:     false,
		},
	}

	oc, cleanup := setupTestClient(t)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create bucket
			if _, err := oc.CreateBucket(context.Background(), tt.bucket); err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}

			// Create object
			if _, err := oc.PutObjectStream(
				context.Background(),
				tt.bucket,
				tt.key,
				"text/plain",
				nil,
				bytes.NewReader([]byte("data")),
			); err != nil {
				t.Fatalf("failed to create object: %v", err)
			}

			// Set initial tags if provided
			if tt.initialTags != nil {
				if err := oc.PutObjectTags(context.Background(), tt.bucket, tt.key, tt.initialTags); err != nil {
					t.Fatalf("failed to set initial tags: %v", err)
				}
			}

			err := oc.DeleteObjectTags(context.Background(), tt.bucket, tt.key)

			if tt.wantErr {
				if err == nil {
					t.Errorf("DeleteObjectTags() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("DeleteObjectTags() error = %v", err)
				return
			}

			// Verify tags were deleted
			info, err := oc.GetObjectInfo(context.Background(), tt.bucket, tt.key)
			if err != nil {
				t.Errorf("failed to get object info: %v", err)
				return
			}

			// Check that no x-amz-tag- prefixed keys exist
			for k := range info.Metadata {
				if strings.HasPrefix(k, "x-amz-tag-") {
					t.Errorf("DeleteObjectTags() tag still exists: %s", k)
				}
			}
		})
	}
}
