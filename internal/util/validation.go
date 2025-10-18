package util

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	// MaxBucketNameLength is the maximum allowed length for S3 bucket names
	MaxBucketNameLength = 63
	// MinBucketNameLength is the minimum allowed length for S3 bucket names
	MinBucketNameLength = 3
	// MaxObjectKeyLength is the maximum allowed length for S3 object keys
	MaxObjectKeyLength = 1024
)

// S3 bucket name rules per https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
var bucketNameRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`)

// ValidateBucketName validates an S3 bucket name according to AWS S3 naming rules
func ValidateBucketName(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}

	if len(bucket) < MinBucketNameLength {
		return fmt.Errorf("bucket name too short (min %d characters)", MinBucketNameLength)
	}

	if len(bucket) > MaxBucketNameLength {
		return fmt.Errorf("bucket name too long (max %d characters)", MaxBucketNameLength)
	}

	// Check for control characters or null bytes
	for i, r := range bucket {
		if r < 32 || r == 127 {
			return fmt.Errorf("bucket name contains control character at position %d", i)
		}
	}

	// Check for path traversal sequences
	if strings.Contains(bucket, "..") {
		return fmt.Errorf("bucket name contains path traversal sequence")
	}

	// Validate against S3 naming rules
	if !bucketNameRegex.MatchString(bucket) {
		return fmt.Errorf("bucket name does not conform to S3 naming rules (must be lowercase alphanumeric with dots and hyphens, starting and ending with alphanumeric)")
	}

	// Reject IP address format (simple check for patterns like x.x.x.x)
	if regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`).MatchString(bucket) {
		return fmt.Errorf("bucket name must not be formatted as an IP address")
	}

	return nil
}

// ValidateObjectKey validates an S3 object key
func ValidateObjectKey(key string) error {
	if key == "" {
		return fmt.Errorf("object key cannot be empty")
	}

	if len(key) > MaxObjectKeyLength {
		return fmt.Errorf("object key too long (max %d characters)", MaxObjectKeyLength)
	}

	// Check for control characters (except tab which is sometimes used)
	for i, r := range key {
		if (r < 32 && r != '\t') || r == 127 {
			return fmt.Errorf("object key contains invalid control character at position %d", i)
		}
	}

	// Check for path traversal sequences
	// This catches "../" but also standalone ".." which could be problematic
	if strings.Contains(key, "..") {
		return fmt.Errorf("object key contains path traversal sequence")
	}

	return nil
}
