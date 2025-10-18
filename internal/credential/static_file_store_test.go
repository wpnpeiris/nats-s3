package credential

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStaticFileStore_Load(t *testing.T) {
	tests := []struct {
		name           string
		fileContent    string
		wantError      bool
		wantCount      int
		checkAccessKey string
		expectFound    bool
	}{
		{
			name: "valid single credential",
			fileContent: `{
				"credentials": [
					{
						"accessKey": "AKIAIOSFODNN7EXAMPLE",
						"secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
					}
				]
			}`,
			wantError:      false,
			wantCount:      1,
			checkAccessKey: "AKIAIOSFODNN7EXAMPLE",
			expectFound:    true,
		},
		{
			name: "valid multiple credentials",
			fileContent: `{
				"credentials": [
					{
						"accessKey": "AKIAIOSFODNN7EXAMPLE",
						"secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
					},
					{
						"accessKey": "AKIAI44QH8DHBEXAMPLE",
						"secretKey": "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY"
					}
				]
			}`,
			wantError:      false,
			wantCount:      2,
			checkAccessKey: "AKIAI44QH8DHBEXAMPLE",
			expectFound:    true,
		},
		{
			name: "empty credentials array",
			fileContent: `{
				"credentials": []
			}`,
			wantError: true,
		},
		{
			name:        "invalid JSON",
			fileContent: `{invalid json}`,
			wantError:   true,
		},
		{
			name: "duplicate access key",
			fileContent: `{
				"credentials": [
					{
						"accessKey": "AKIAIOSFODNN7EXAMPLE",
						"secretKey": "secret1"
					},
					{
						"accessKey": "AKIAIOSFODNN7EXAMPLE",
						"secretKey": "secret2"
					}
				]
			}`,
			wantError: true,
		},
		{
			name: "invalid access key too short",
			fileContent: `{
				"credentials": [
					{
						"accessKey": "AB",
						"secretKey": "validSecretKey123"
					}
				]
			}`,
			wantError: true,
		},
		{
			name: "invalid secret key too short",
			fileContent: `{
				"credentials": [
					{
						"accessKey": "validAccessKey",
						"secretKey": "short"
					}
				]
			}`,
			wantError: true,
		},
		{
			name: "access key with reserved characters",
			fileContent: `{
				"credentials": [
					{
						"accessKey": "ACCESS=KEY",
						"secretKey": "validSecretKey123"
					}
				]
			}`,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary file
			tmpFile := filepath.Join(t.TempDir(), "credentials.json")
			if err := os.WriteFile(tmpFile, []byte(tt.fileContent), 0600); err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}

			// Load credentials
			store, err := NewStaticFileStore(tmpFile)
			if (err != nil) != tt.wantError {
				t.Errorf("NewStaticFileStore() error = %v, wantError %v", err, tt.wantError)
				return
			}

			if err != nil {
				return // Expected error, test passed
			}

			// Verify count
			if store.Count() != tt.wantCount {
				t.Errorf("Count() = %d, want %d", store.Count(), tt.wantCount)
			}

			// Verify access key lookup if specified
			if tt.checkAccessKey != "" {
				_, found := store.Get(tt.checkAccessKey)
				if found != tt.expectFound {
					t.Errorf("Get(%s) found = %v, want %v", tt.checkAccessKey, found, tt.expectFound)
				}
			}
		})
	}
}

func TestStaticFileStore_Get(t *testing.T) {
	// Create a temporary credentials file
	tmpFile := filepath.Join(t.TempDir(), "credentials.json")
	fileContent := `{
		"credentials": [
			{
				"accessKey": "AKIAIOSFODNN7EXAMPLE",
				"secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
			},
			{
				"accessKey": "AKIAI44QH8DHBEXAMPLE",
				"secretKey": "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY"
			}
		]
	}`
	if err := os.WriteFile(tmpFile, []byte(fileContent), 0600); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	store, err := NewStaticFileStore(tmpFile)
	if err != nil {
		t.Fatalf("NewStaticFileStore() failed: %v", err)
	}

	tests := []struct {
		name       string
		accessKey  string
		wantSecret string
		wantFound  bool
	}{
		{
			name:       "existing access key 1",
			accessKey:  "AKIAIOSFODNN7EXAMPLE",
			wantSecret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			wantFound:  true,
		},
		{
			name:       "existing access key 2",
			accessKey:  "AKIAI44QH8DHBEXAMPLE",
			wantSecret: "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY",
			wantFound:  true,
		},
		{
			name:       "non-existing access key",
			accessKey:  "NONEXISTENT",
			wantSecret: "",
			wantFound:  false,
		},
		{
			name:       "empty access key",
			accessKey:  "",
			wantSecret: "",
			wantFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret, found := store.Get(tt.accessKey)
			if found != tt.wantFound {
				t.Errorf("Get() found = %v, want %v", found, tt.wantFound)
			}
			if secret != tt.wantSecret {
				t.Errorf("Get() secret = %v, want %v", secret, tt.wantSecret)
			}
		})
	}
}

func TestStaticFileStore_GetName(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "credentials.json")
	fileContent := `{
		"credentials": [
			{
				"accessKey": "AKIAIOSFODNN7EXAMPLE",
				"secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
			}
		]
	}`
	if err := os.WriteFile(tmpFile, []byte(fileContent), 0600); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	store, err := NewStaticFileStore(tmpFile)
	if err != nil {
		t.Fatalf("NewStaticFileStore() failed: %v", err)
	}

	if name := store.GetName(); name != "static_file" {
		t.Errorf("GetName() = %v, want %v", name, "static_file")
	}
}

func TestStaticFileStore_EmptyPath(t *testing.T) {
	_, err := NewStaticFileStore("")
	if err == nil {
		t.Error("NewStaticFileStore with empty path should return error")
	}
}

func TestStaticFileStore_NonExistentFile(t *testing.T) {
	_, err := NewStaticFileStore("/nonexistent/path/credentials.json")
	if err == nil {
		t.Error("NewStaticFileStore with non-existent file should return error")
	}
}
