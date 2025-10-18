package s3api

import "github.com/wpnpeiris/nats-s3/internal/credential"

// mockCredentialStore is a simple in-memory credential store for testing
type mockCredentialStore struct {
	creds map[string]string
}

func (m *mockCredentialStore) Get(accessKey string) (string, bool) {
	secret, found := m.creds[accessKey]
	return secret, found
}

func (m *mockCredentialStore) GetName() string {
	return "mock"
}

// newMockCredentialStore returns a mock credential store for testing.
// Returns nil to disable authentication for unauthenticated tests.
func newMockCredentialStore() credential.Store {
	// Return nil to disable authentication in tests
	return nil
}
