package credential

import (
	"testing"
)

func TestEntry_Validate(t *testing.T) {
	tests := []struct {
		name      string
		entry     Entry
		wantError bool
		errorType error
	}{
		{
			name: "valid credential",
			entry: Entry{
				AccessKey: "AKIAIOSFODNN7EXAMPLE",
				SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			wantError: false,
		},
		{
			name: "minimum length credentials",
			entry: Entry{
				AccessKey: "ABC",
				SecretKey: "12345678",
			},
			wantError: false,
		},
		{
			name: "empty credentials",
			entry: Entry{
				AccessKey: "",
				SecretKey: "",
			},
			wantError: true,
			errorType: ErrEmptyCredentials,
		},
		{
			name: "access key too short",
			entry: Entry{
				AccessKey: "AB",
				SecretKey: "validSecretKey123",
			},
			wantError: true,
			errorType: ErrInvalidAccessKeyLength,
		},
		{
			name: "secret key too short",
			entry: Entry{
				AccessKey: "validAccessKey",
				SecretKey: "short",
			},
			wantError: true,
			errorType: ErrInvalidSecretKeyLength,
		},
		{
			name: "access key contains equals sign",
			entry: Entry{
				AccessKey: "ACCESS=KEY",
				SecretKey: "validSecretKey123",
			},
			wantError: true,
			errorType: ErrContainsReservedChars,
		},
		{
			name: "access key contains comma",
			entry: Entry{
				AccessKey: "ACCESS,KEY",
				SecretKey: "validSecretKey123",
			},
			wantError: true,
			errorType: ErrContainsReservedChars,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.entry.Validate()
			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tt.wantError)
				return
			}
		})
	}
}
