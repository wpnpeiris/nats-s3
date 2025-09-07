package s3api

import (
	"net/http"
	"strings"
)

// Credential holds credentials to verify in authentication
// This should be further extend to different authentication mechanism NATS supports.
type Credential struct {
	accessKey string
	secretKey string
}

func NewCredential(accessKey string, secretKey string) Credential {
	return Credential{
		accessKey: accessKey,
		secretKey: secretKey,
	}
}

type IdentityAccessManagement struct {
	credential Credential
}

func NewIdentityAccessManagement(credential Credential) *IdentityAccessManagement {
	return &IdentityAccessManagement{
		credential: credential,
	}
}

func (iam *IdentityAccessManagement) Auth(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" || !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
			WriteErrorResponse(w, r, ErrAccessDenied)
			return
		}

		// Extract Credential=ACCESS/...
		// Example: AWS4-HMAC-SHA256 Credential=AKID/20250101/us-east-1/s3/aws4_request, SignedHeaders=..., Signature=...
		var accessKey string
		parts := strings.Split(auth[len("AWS4-HMAC-SHA256 "):], ",")
		for _, part := range parts {
			kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(kv) != 2 {
				continue
			}
			if kv[0] == "Credential" {
				credParts := strings.Split(kv[1], "/")
				if len(credParts) >= 1 {
					accessKey = credParts[0]
				}
				break
			}
		}

		if accessKey == "" {
			WriteErrorResponse(w, r, ErrInvalidAccessKeyID)
			return
		}

		f(w, r)
		return
	}
}
