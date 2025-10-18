package credential

import (
	"errors"
	"fmt"
	"strings"
)

// Common errors for credential validation and retrieval
var (
	ErrAccessKeyNotFound      = errors.New("access key not found")
	ErrInvalidAccessKeyLength = errors.New("access key must be at least 3 characters")
	ErrInvalidSecretKeyLength = errors.New("secret key must be at least 8 characters")
	ErrContainsReservedChars  = errors.New("access key contains reserved characters '=' or ','")
	ErrEmptyCredentials       = errors.New("access key and secret key cannot be empty")
)

const (
	// Minimum length for access key (following MinIO conventions)
	accessKeyMinLen = 3

	// Minimum length for secret key (following MinIO conventions)
	secretKeyMinLen = 8

	// Reserved characters that cannot be used in access keys
	reservedChars = "=,"
)

// Entry represents a single AWS-style credential pair.
type Entry struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

// Validate checks if the credential entry is valid.
func (e *Entry) Validate() error {
	if e.AccessKey == "" && e.SecretKey == "" {
		return ErrEmptyCredentials
	}

	if len(e.AccessKey) < accessKeyMinLen {
		return fmt.Errorf("%w: got %d characters", ErrInvalidAccessKeyLength, len(e.AccessKey))
	}

	if len(e.SecretKey) < secretKeyMinLen {
		return fmt.Errorf("%w: got %d characters", ErrInvalidSecretKeyLength, len(e.SecretKey))
	}

	if strings.ContainsAny(e.AccessKey, reservedChars) {
		return ErrContainsReservedChars
	}

	return nil
}

// Store defines the interface for credential storage and retrieval.
// Implementations can use different backends (file, memory, database, etc.)
type Store interface {
	// Get retrieves the secret key for the given access key.
	// Returns the secret key and true if found, empty string and false otherwise.
	Get(accessKey string) (secretKey string, found bool)

	// GetName returns a descriptive name of the store implementation.
	GetName() string
}
