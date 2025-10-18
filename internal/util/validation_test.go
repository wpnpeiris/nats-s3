package util

import (
	"strings"
	"testing"
)

func TestValidateBucketName(t *testing.T) {
	tests := []struct {
		name      string
		bucket    string
		wantError bool
	}{
		// Valid bucket names
		{"valid simple", "mybucket", false},
		{"valid with dash", "my-bucket", false},
		{"valid with dot", "my.bucket", false},
		{"valid with numbers", "bucket123", false},
		{"valid complex", "my-bucket.name-123", false},
		{"valid minimum length", "abc", false},
		{"valid maximum length", strings.Repeat("a", 63), false},

		// Invalid bucket names
		{"empty", "", true},
		{"too short", "ab", true},
		{"too long", strings.Repeat("a", 64), true},
		{"uppercase", "MyBucket", true},
		{"starts with dash", "-mybucket", true},
		{"ends with dash", "mybucket-", true},
		{"starts with dot", ".mybucket", true},
		{"ends with dot", "mybucket.", true},
		{"double dot", "my..bucket", true},
		{"path traversal", "my../bucket", true},
		{"IP address format", "192.168.1.1", true},
		{"contains underscore", "my_bucket", true},
		{"contains space", "my bucket", true},
		{"contains slash", "my/bucket", true},
		{"null byte", "bucket\x00name", true},
		{"control character", "bucket\nname", true},
		{"tab character", "bucket\tname", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBucketName(tt.bucket)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateBucketName(%q) error = %v, wantError %v", tt.bucket, err, tt.wantError)
			}
		})
	}
}

func TestValidateObjectKey(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		wantError bool
	}{
		// Valid object keys
		{"valid simple", "myfile.txt", false},
		{"valid with path", "path/to/file.txt", false},
		{"valid with dash", "my-file.txt", false},
		{"valid with underscore", "my_file.txt", false},
		{"valid with space", "my file.txt", false},
		{"valid with dots", "my.file.name.txt", false},
		{"valid with tab", "file\tname.txt", false},
		{"valid unicode", "文件.txt", false},
		{"valid maximum length", strings.Repeat("a", 1024), false},
		{"valid deep path", "a/b/c/d/e/f/g/h/i/j/k/file.txt", false},

		// Invalid object keys
		{"empty", "", true},
		{"too long", strings.Repeat("a", 1025), true},
		{"path traversal", "../etc/passwd", true},
		{"path traversal middle", "path/../file.txt", true},
		{"path traversal end", "path/..", true},
		{"double dot alone", "..", true},
		{"null byte", "file\x00name.txt", true},
		{"control character newline", "file\nname.txt", true},
		{"control character carriage return", "file\rname.txt", true},
		{"bell character", "file\x07name.txt", true},
		{"delete character", "file\x7fname.txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateObjectKey(tt.key)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateObjectKey(%q) error = %v, wantError %v", tt.key, err, tt.wantError)
			}
		})
	}
}

func BenchmarkValidateBucketName(b *testing.B) {
	bucket := "my-valid-bucket-name-123"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateBucketName(bucket)
	}
}

func BenchmarkValidateObjectKey(b *testing.B) {
	key := "path/to/my/object/file.txt"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateObjectKey(key)
	}
}
