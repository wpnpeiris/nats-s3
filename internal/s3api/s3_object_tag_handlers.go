package s3api

import (
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/gorilla/mux"
	"github.com/wpnpeiris/nats-s3/internal/model"
)

// Object tagging constants
const (
	tagMetadataPrefix = "x-amz-tag-"
	maxTagsPerObject  = 10
	maxTagKeyLength   = 128
	maxTagValueLength = 256
)

// GetObjectTagging retrieves tags associated with an object.
func (s *S3Gateway) GetObjectTagging(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	log.Printf("GetObjectTagging: bucket=%s key=%s", bucket, key)

	// Get object info to retrieve metadata
	info, err := s.client.GetObjectInfo(r.Context(), bucket, key)
	if s.handleObjectError(w, r, err) {
		return
	}

	// Extract tags from metadata (prefix: x-amz-tag-)
	tags := extractTagsFromMetadata(info.Metadata)

	// Build response
	response := model.Tagging{
		TagSet: model.TagSet{
			Tags: tags,
		},
	}

	model.WriteXMLResponse(w, r, http.StatusOK, response)
}

// PutObjectTagging sets or replaces tags on an existing object.
func (s *S3Gateway) PutObjectTagging(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	log.Printf("PutObjectTagging: bucket=%s key=%s", bucket, key)

	// Parse tagging XML from request body
	var tagging model.Tagging
	if err := xml.NewDecoder(r.Body).Decode(&tagging); err != nil {
		log.Printf("Error decoding tagging XML: %v", err)
		model.WriteErrorResponse(w, r, model.ErrMalformedXML)
		return
	}

	// Validate tags
	if err := validateTags(tagging.TagSet.Tags); err != nil {
		log.Printf("Tag validation failed: %v", err)
		model.WriteErrorResponse(w, r, model.ErrInvalidTag)
		return
	}

	// Convert tags to metadata format
	tagMetadata := tagsToMetadata(tagging.TagSet.Tags)

	// Update object tags in NATS
	err := s.client.PutObjectTags(r.Context(), bucket, key, tagMetadata)
	if s.handleObjectError(w, r, err) {
		return
	}

	model.WriteEmptyResponse(w, r, http.StatusOK)
}

// DeleteObjectTagging removes all tags from an object.
func (s *S3Gateway) DeleteObjectTagging(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	log.Printf("DeleteObjectTagging: bucket=%s key=%s", bucket, key)

	// Delete all tags from object
	err := s.client.DeleteObjectTags(r.Context(), bucket, key)
	if s.handleObjectError(w, r, err) {
		return
	}

	model.WriteEmptyResponse(w, r, http.StatusNoContent)
}

// extractTagsFromMetadata converts NATS metadata to S3 Tag array.
// Filters metadata keys with "x-amz-tag-" prefix.
func extractTagsFromMetadata(metadata map[string]string) []model.Tag {
	var tags []model.Tag

	if metadata == nil {
		return tags
	}

	for key, value := range metadata {
		if strings.HasPrefix(key, tagMetadataPrefix) {
			tagKey := strings.TrimPrefix(key, tagMetadataPrefix)
			tags = append(tags, model.Tag{
				Key:   tagKey,
				Value: value,
			})
		}
	}

	// Sort tags by key for consistent output
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})

	return tags
}

// tagsToMetadata converts S3 Tag array to NATS metadata format.
func tagsToMetadata(tags []model.Tag) map[string]string {
	metadata := make(map[string]string)

	for _, tag := range tags {
		metaKey := tagMetadataPrefix + tag.Key
		metadata[metaKey] = tag.Value
	}

	return metadata
}

// validateTags validates tag constraints per AWS S3 specification.
func validateTags(tags []model.Tag) error {
	// Check max number of tags
	if len(tags) > maxTagsPerObject {
		return fmt.Errorf("tag count exceeds maximum of %d", maxTagsPerObject)
	}

	// Check for duplicate keys (case-sensitive)
	seen := make(map[string]bool)

	for _, tag := range tags {
		// Validate key length
		if len(tag.Key) == 0 {
			return fmt.Errorf("tag key cannot be empty")
		}
		if len(tag.Key) > maxTagKeyLength {
			return fmt.Errorf("tag key exceeds maximum length of %d: %s", maxTagKeyLength, tag.Key)
		}

		// Validate value length
		if len(tag.Value) > maxTagValueLength {
			return fmt.Errorf("tag value exceeds maximum length of %d for key: %s", maxTagValueLength, tag.Key)
		}

		// Check for duplicates
		if seen[tag.Key] {
			return fmt.Errorf("duplicate tag key: %s", tag.Key)
		}
		seen[tag.Key] = true
	}

	return nil
}

// parseTaggingHeader parses the x-amz-tagging header format.
// Format: key1=value1&key2=value2 (URL-encoded query string style)
func parseTaggingHeader(header string) ([]model.Tag, error) {
	if header == "" {
		return nil, nil
	}

	// URL decode the header value
	decoded, err := url.QueryUnescape(header)
	if err != nil {
		return nil, fmt.Errorf("invalid URL encoding in x-amz-tagging header: %w", err)
	}

	// Parse as query string
	values, err := url.ParseQuery(decoded)
	if err != nil {
		return nil, fmt.Errorf("invalid format in x-amz-tagging header: %w", err)
	}

	var tags []model.Tag
	for key, vals := range values {
		if len(vals) > 0 {
			tags = append(tags, model.Tag{
				Key:   key,
				Value: vals[0], // Use first value if multiple
			})
		}
	}

	return tags, nil
}
