package credential

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// CredentialsFile represents the JSON structure of the credentials file.
type CredentialsFile struct {
	Credentials []Entry `json:"credentials"`
}

// StaticFileStore implements Store by loading credentials from a JSON file.
// Credentials are loaded once at initialization and stored in memory.
// This implementation is read-only and thread-safe.
type StaticFileStore struct {
	mu          sync.RWMutex
	credentials map[string]string // accessKey -> secretKey
	filePath    string
}

// NewStaticFileStore creates a new StaticFileStore and loads credentials from the specified file.
// The file should be in JSON format with a "credentials" array containing access/secret key pairs.
//
// Example JSON format:
//
//	{
//	  "credentials": [
//	    {
//	      "accessKey": "AKIAIOSFODNN7EXAMPLE",
//	      "secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
//	    }
//	  ]
//	}
func NewStaticFileStore(filePath string) (*StaticFileStore, error) {
	if filePath == "" {
		return nil, fmt.Errorf("credentials file path cannot be empty")
	}

	store := &StaticFileStore{
		credentials: make(map[string]string),
		filePath:    filePath,
	}

	if err := store.load(); err != nil {
		return nil, fmt.Errorf("failed to load credentials from %s: %w", filePath, err)
	}

	return store, nil
}

// load reads and parses the credentials file.
func (s *StaticFileStore) load() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var credFile CredentialsFile
	if err := json.Unmarshal(data, &credFile); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	if len(credFile.Credentials) == 0 {
		return fmt.Errorf("no credentials found in file")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate and store each credential
	for i, entry := range credFile.Credentials {
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("invalid credential at index %d: %w", i, err)
		}

		// Check for duplicate access keys
		if _, exists := s.credentials[entry.AccessKey]; exists {
			return fmt.Errorf("duplicate access key at index %d: %s", i, entry.AccessKey)
		}

		s.credentials[entry.AccessKey] = entry.SecretKey
	}

	return nil
}

// Get retrieves the secret key for the given access key.
// This method is thread-safe.
func (s *StaticFileStore) Get(accessKey string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	secretKey, found := s.credentials[accessKey]
	return secretKey, found
}

// GetName returns the name of this store implementation.
func (s *StaticFileStore) GetName() string {
	return "static_file"
}

// Count returns the number of credentials loaded.
// This is useful for testing and monitoring.
func (s *StaticFileStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.credentials)
}
