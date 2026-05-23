package s3api

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/model"
	"github.com/wpnpeiris/nats-s3/internal/testutil"
)

func TestObjectTagging(t *testing.T) {
	tests := []struct {
		name          string
		bucket        string
		key           string
		content       string
		taggingXML    string
		expectedTags  []model.Tag
		expectedCount int
	}{
		{
			name:       "complete tagging lifecycle",
			bucket:     "test-tagging",
			key:        "test-object.txt",
			content:    "test content",
			taggingXML: `<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>Environment</Key><Value>Production</Value></Tag><Tag><Key>Project</Key><Value>Test</Value></Tag></TagSet></Tagging>`,
			expectedTags: []model.Tag{
				{Key: "Environment", Value: "Production"},
				{Key: "Project", Value: "Test"},
			},
			expectedCount: 2,
		},
		{
			name:       "single tag",
			bucket:     "test-single-tag",
			key:        "single.txt",
			content:    "data",
			taggingXML: `<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>Type</Key><Value>Document</Value></Tag></TagSet></Tagging>`,
			expectedTags: []model.Tag{
				{Key: "Type", Value: "Document"},
			},
			expectedCount: 1,
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

			_, err = gw.client.CreateBucket(context.Background(), tt.bucket)
			if err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			// 1. PUT object
			putReq := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.key, strings.NewReader(tt.content))
			putRec := httptest.NewRecorder()
			r.ServeHTTP(putRec, putReq)
			if putRec.Code != 200 {
				t.Fatalf("PUT object failed: %d, body=%s", putRec.Code, putRec.Body.String())
			}

			// 2. GET tagging (should be empty)
			getTagReq := httptest.NewRequest("GET", "/"+tt.bucket+"/"+tt.key+"?tagging", nil)
			getTagRec := httptest.NewRecorder()
			r.ServeHTTP(getTagRec, getTagReq)
			if getTagRec.Code != 200 {
				t.Fatalf("GET tagging failed: %d, body=%s", getTagRec.Code, getTagRec.Body.String())
			}

			var emptyTagging model.Tagging
			if err := xml.Unmarshal(getTagRec.Body.Bytes(), &emptyTagging); err != nil {
				t.Fatalf("failed to unmarshal empty tagging response: %v", err)
			}
			if len(emptyTagging.TagSet.Tags) != 0 {
				t.Fatalf("expected empty tags, got %d", len(emptyTagging.TagSet.Tags))
			}

			// 3. PUT tagging
			putTagReq := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.key+"?tagging", strings.NewReader(tt.taggingXML))
			putTagRec := httptest.NewRecorder()
			r.ServeHTTP(putTagRec, putTagReq)
			if putTagRec.Code != 200 {
				t.Fatalf("PUT tagging failed: %d, body=%s", putTagRec.Code, putTagRec.Body.String())
			}

			// 4. GET tagging (should return expected tags)
			getTagReq2 := httptest.NewRequest("GET", "/"+tt.bucket+"/"+tt.key+"?tagging", nil)
			getTagRec2 := httptest.NewRecorder()
			r.ServeHTTP(getTagRec2, getTagReq2)
			if getTagRec2.Code != 200 {
				t.Fatalf("GET tagging failed: %d, body=%s", getTagRec2.Code, getTagRec2.Body.String())
			}

			var tagging model.Tagging
			if err := xml.Unmarshal(getTagRec2.Body.Bytes(), &tagging); err != nil {
				t.Fatalf("failed to unmarshal tagging response: %v", err)
			}
			if len(tagging.TagSet.Tags) != tt.expectedCount {
				t.Fatalf("expected %d tags, got %d", tt.expectedCount, len(tagging.TagSet.Tags))
			}

			// Verify tag values
			for i, tag := range tagging.TagSet.Tags {
				if tag.Key != tt.expectedTags[i].Key || tag.Value != tt.expectedTags[i].Value {
					t.Errorf("tag %d: got {%s:%s}, want {%s:%s}",
						i, tag.Key, tag.Value, tt.expectedTags[i].Key, tt.expectedTags[i].Value)
				}
			}

			// 5. DELETE tagging
			delTagReq := httptest.NewRequest("DELETE", "/"+tt.bucket+"/"+tt.key+"?tagging", nil)
			delTagRec := httptest.NewRecorder()
			r.ServeHTTP(delTagRec, delTagReq)
			if delTagRec.Code != 204 {
				t.Fatalf("DELETE tagging failed: %d, body=%s", delTagRec.Code, delTagRec.Body.String())
			}

			// 6. GET tagging (should be empty again)
			getTagReq3 := httptest.NewRequest("GET", "/"+tt.bucket+"/"+tt.key+"?tagging", nil)
			getTagRec3 := httptest.NewRecorder()
			r.ServeHTTP(getTagRec3, getTagReq3)
			if getTagRec3.Code != 200 {
				t.Fatalf("GET tagging after delete failed: %d", getTagRec3.Code)
			}

			var finalTagging model.Tagging
			if err := xml.Unmarshal(getTagRec3.Body.Bytes(), &finalTagging); err != nil {
				t.Fatalf("failed to unmarshal final tagging response: %v", err)
			}
			if len(finalTagging.TagSet.Tags) != 0 {
				t.Fatalf("expected empty tags after delete, got %d", len(finalTagging.TagSet.Tags))
			}
		})
	}
}

func TestTagValidation(t *testing.T) {
	tests := []struct {
		name    string
		tags    []model.Tag
		wantErr bool
	}{
		{
			name: "valid tags",
			tags: []model.Tag{
				{Key: "Environment", Value: "Production"},
				{Key: "Project", Value: "Test"},
			},
			wantErr: false,
		},
		{
			name:    "too many tags",
			tags:    make([]model.Tag, 11), // 11 tags > 10 max
			wantErr: true,
		},
		{
			name:    "empty key",
			tags:    []model.Tag{{Key: "", Value: "test"}},
			wantErr: true,
		},
		{
			name:    "key too long",
			tags:    []model.Tag{{Key: strings.Repeat("a", 129), Value: "test"}},
			wantErr: true,
		},
		{
			name:    "value too long",
			tags:    []model.Tag{{Key: "test", Value: strings.Repeat("a", 257)}},
			wantErr: true,
		},
		{
			name: "duplicate keys",
			tags: []model.Tag{
				{Key: "Environment", Value: "Prod"},
				{Key: "Environment", Value: "Dev"},
			},
			wantErr: true,
		},
		{
			name: "empty value allowed",
			tags: []model.Tag{
				{Key: "EmptyValue", Value: ""},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize tags for "too many tags" test
			if tt.name == "too many tags" {
				for i := range tt.tags {
					tt.tags[i] = model.Tag{Key: "key" + string(rune(i)), Value: "value"}
				}
			}

			err := validateTags(tt.tags)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTags() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseTaggingHeader(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		want    int // number of tags
		wantErr bool
	}{
		{
			name:    "simple tags",
			header:  "Environment=Production&Project=Test",
			want:    2,
			wantErr: false,
		},
		{
			name:    "url encoded",
			header:  "Name=My%20Project&Owner=team%2Dbackend",
			want:    2,
			wantErr: false,
		},
		{
			name:    "empty header",
			header:  "",
			want:    0,
			wantErr: false,
		},
		{
			name:    "single tag",
			header:  "Environment=Production",
			want:    1,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tags, err := parseTaggingHeader(tt.header)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTaggingHeader() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(tags) != tt.want {
				t.Errorf("parseTaggingHeader() got %d tags, want %d", len(tags), tt.want)
			}
		})
	}
}

func TestGetObjectTaggingNotFound(t *testing.T) {
	tests := []struct {
		name           string
		bucket         string
		key            string
		createBucket   bool
		createObject   bool
		expectedStatus int
	}{
		{
			name:           "object not found",
			bucket:         "test-bucket",
			key:            "nonexistent.txt",
			createBucket:   true,
			createObject:   false,
			expectedStatus: 404,
		},
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			key:            "file.txt",
			createBucket:   false,
			createObject:   false,
			expectedStatus: 404,
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

			if tt.createBucket {
				_, err = gw.client.CreateBucket(context.Background(), tt.bucket)
				if err != nil {
					t.Fatalf("failed to create bucket: %v", err)
				}
			}

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			getTagReq := httptest.NewRequest("GET", "/"+tt.bucket+"/"+tt.key+"?tagging", nil)
			getTagRec := httptest.NewRecorder()
			r.ServeHTTP(getTagRec, getTagReq)

			if getTagRec.Code != tt.expectedStatus {
				t.Errorf("expected %d, got %d", tt.expectedStatus, getTagRec.Code)
			}
		})
	}
}

func TestPutObjectWithTagging(t *testing.T) {
	tests := []struct {
		name         string
		bucket       string
		key          string
		content      string
		taggingHdr   string
		expectedTags map[string]string
		expectedCnt  int
	}{
		{
			name:       "put with two tags",
			bucket:     "test-put-tagging",
			key:        "tagged-object.txt",
			content:    "test data",
			taggingHdr: "Environment=Production&Owner=team-a",
			expectedTags: map[string]string{
				"Environment": "Production",
				"Owner":       "team-a",
			},
			expectedCnt: 2,
		},
		{
			name:       "put with single tag",
			bucket:     "test-put-single",
			key:        "single-tag.txt",
			content:    "data",
			taggingHdr: "Type=Document",
			expectedTags: map[string]string{
				"Type": "Document",
			},
			expectedCnt: 1,
		},
		{
			name:       "put with url encoded tags",
			bucket:     "test-put-encoded",
			key:        "encoded.txt",
			content:    "data",
			taggingHdr: "Name=My%20Project&Team=backend%2Ddev",
			expectedTags: map[string]string{
				"Name": "My Project",
				"Team": "backend-dev",
			},
			expectedCnt: 2,
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

			_, err = gw.client.CreateBucket(context.Background(), tt.bucket)
			if err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}

			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			// PUT object with x-amz-tagging header
			putReq := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.key, strings.NewReader(tt.content))
			putReq.Header.Set("x-amz-tagging", tt.taggingHdr)
			putRec := httptest.NewRecorder()
			r.ServeHTTP(putRec, putReq)
			if putRec.Code != 200 {
				t.Fatalf("PUT with tagging failed: %d, body=%s", putRec.Code, putRec.Body.String())
			}

			// GET tagging to verify
			getTagReq := httptest.NewRequest("GET", "/"+tt.bucket+"/"+tt.key+"?tagging", nil)
			getTagRec := httptest.NewRecorder()
			r.ServeHTTP(getTagRec, getTagReq)
			if getTagRec.Code != 200 {
				t.Fatalf("GET tagging failed: %d, body=%s", getTagRec.Code, getTagRec.Body.String())
			}

			var tagging model.Tagging
			if err := xml.Unmarshal(getTagRec.Body.Bytes(), &tagging); err != nil {
				t.Fatalf("failed to unmarshal tagging: %v", err)
			}

			if len(tagging.TagSet.Tags) != tt.expectedCnt {
				t.Fatalf("expected %d tags from PUT, got %d", tt.expectedCnt, len(tagging.TagSet.Tags))
			}

			// Verify tag values
			tagMap := make(map[string]string)
			for _, tag := range tagging.TagSet.Tags {
				tagMap[tag.Key] = tag.Value
			}

			for key, expectedVal := range tt.expectedTags {
				if tagMap[key] != expectedVal {
					t.Errorf("expected %s=%s, got %s", key, expectedVal, tagMap[key])
				}
			}
		})
	}
}

func TestCopyObjectWithTaggingDirective(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw, err := NewS3Gateway(logger, s.ClientURL(), 1, nil, nil)
	if err != nil {
		t.Fatalf("failed to create S3 gateway: %v", err)
	}

	bucket := "test-copy-tagging"
	sourceKey := "source.txt"
	destKey := "dest.txt"

	// Create bucket
	_, err = gw.client.CreateBucket(context.Background(), bucket)
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	// PUT source object with tags
	putReq := httptest.NewRequest("PUT", "/"+bucket+"/"+sourceKey, strings.NewReader("source data"))
	putReq.Header.Set("x-amz-tagging", "Source=Original&Version=1")
	putRec := httptest.NewRecorder()
	r.ServeHTTP(putRec, putReq)
	if putRec.Code != 200 {
		t.Fatalf("PUT source failed: %d", putRec.Code)
	}

	// Test 1: Copy with default (COPY) directive - should preserve tags
	t.Run("CopyTags", func(t *testing.T) {
		copyReq := httptest.NewRequest("PUT", "/"+bucket+"/"+destKey, nil)
		copyReq.Header.Set("x-amz-copy-source", "/"+bucket+"/"+sourceKey)
		copyRec := httptest.NewRecorder()
		r.ServeHTTP(copyRec, copyReq)
		if copyRec.Code != 200 {
			t.Fatalf("COPY failed: %d, body=%s", copyRec.Code, copyRec.Body.String())
		}

		// Verify tags were copied
		getTagReq := httptest.NewRequest("GET", "/"+bucket+"/"+destKey+"?tagging", nil)
		getTagRec := httptest.NewRecorder()
		r.ServeHTTP(getTagRec, getTagReq)

		var tagging model.Tagging
		xml.Unmarshal(getTagRec.Body.Bytes(), &tagging)

		if len(tagging.TagSet.Tags) != 2 {
			t.Errorf("expected 2 copied tags, got %d", len(tagging.TagSet.Tags))
		}
	})

	// Test 2: Copy with REPLACE directive - should use new tags
	t.Run("ReplaceTags", func(t *testing.T) {
		destKey2 := "dest2.txt"
		copyReq := httptest.NewRequest("PUT", "/"+bucket+"/"+destKey2, nil)
		copyReq.Header.Set("x-amz-copy-source", "/"+bucket+"/"+sourceKey)
		copyReq.Header.Set("x-amz-tagging-directive", "REPLACE")
		copyReq.Header.Set("x-amz-tagging", "Destination=Copy&NewTag=Value")
		copyRec := httptest.NewRecorder()
		r.ServeHTTP(copyRec, copyReq)
		if copyRec.Code != 200 {
			t.Fatalf("COPY with REPLACE failed: %d, body=%s", copyRec.Code, copyRec.Body.String())
		}

		// Verify new tags
		getTagReq := httptest.NewRequest("GET", "/"+bucket+"/"+destKey2+"?tagging", nil)
		getTagRec := httptest.NewRecorder()
		r.ServeHTTP(getTagRec, getTagReq)

		var tagging model.Tagging
		xml.Unmarshal(getTagRec.Body.Bytes(), &tagging)

		if len(tagging.TagSet.Tags) != 2 {
			t.Errorf("expected 2 new tags, got %d", len(tagging.TagSet.Tags))
		}

		tagMap := make(map[string]string)
		for _, tag := range tagging.TagSet.Tags {
			tagMap[tag.Key] = tag.Value
		}

		if tagMap["Destination"] != "Copy" {
			t.Errorf("expected Destination=Copy, got %s", tagMap["Destination"])
		}
		if tagMap["NewTag"] != "Value" {
			t.Errorf("expected NewTag=Value, got %s", tagMap["NewTag"])
		}

		// Verify old tags are not present
		if _, exists := tagMap["Source"]; exists {
			t.Error("old tag 'Source' should not exist after REPLACE")
		}
	})
}

// Dummy test to ensure server type compatibility
func dummyServerTest() *server.Server {
	return nil
}
