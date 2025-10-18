package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/wpnpeiris/nats-s3/internal/model"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type AuthError struct {
	code model.ErrorCode
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("Auth error code: %d", e.code)
}

type AuthHeaderParameters struct {
	algo          string
	accessKey     string
	scopeDate     string
	scopeRegion   string
	scopeService  string
	signedHeaders string
	signature     string
	requestTime   string
	hashedPayload string
}

// Credential holds credentials to verify in authentication
// This should be further extend to different authentication mechanism NATS supports.
// Credential holds a single AWS-style access/secret pair used to validate
// SigV4 requests. The same pair maps to NATS username/password.
type Credential struct {
	accessKey string
	secretKey string
}

// NewCredential constructs a Credential with the given access and secret keys.
func NewCredential(accessKey string, secretKey string) Credential {
	return Credential{
		accessKey: accessKey,
		secretKey: secretKey,
	}
}

// IdentityAccessManagement verifies AWS SigV4 and authorizes requests using
// a configured Credential.
type IdentityAccessManagement struct {
	credential Credential
}

// NewIdentityAccessManagement returns an IAM verifier bound to one credential.
func NewIdentityAccessManagement(credential Credential) *IdentityAccessManagement {
	return &IdentityAccessManagement{
		credential: credential,
	}
}

func (iam *IdentityAccessManagement) isDisabled() bool {
	return iam.credential == Credential{}
}

// Auth verifies AWS SigV4 (header-based and presigned URL) against the
// configured credential. On success, calls the wrapped handler; otherwise
// returns an S3-style XML error response.
func (iam *IdentityAccessManagement) Auth(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if iam.isDisabled() {
			f(w, r)
			return
		}

		// Support both header-based SigV4 and presigned URL SigV4.
		hp, authErr := extractAuthHeaderParameters(r)
		if authErr != nil {
			model.WriteErrorResponse(w, r, authErr.code)
			return
		}

		authErr = validateAuthHeaderParameters(iam.credential.accessKey, hp)
		if authErr != nil {
			model.WriteErrorResponse(w, r, authErr.code)
			return
		}

		canURI := buildCanonicalURI(r)
		canQuery := buildCanonicalQueryString(r)
		canHeaders := buildCanonicalHeaders(hp.signedHeaders, r)

		if hp.hashedPayload == "" {
			hp.hashedPayload = "UNSIGNED-PAYLOAD"
		}

		canReq := strings.Join([]string{
			r.Method,
			canURI,
			canQuery,
			canHeaders,
			strings.ToLower(hp.signedHeaders),
			hp.hashedPayload,
		}, "\n")
		hash := sha256.Sum256([]byte(canReq))
		canReqHashHex := hex.EncodeToString(hash[:])

		// Build StringToSign
		scope := strings.Join([]string{hp.scopeDate, hp.scopeRegion, hp.scopeService, "aws4_request"}, "/")
		sts := strings.Join([]string{
			"AWS4-HMAC-SHA256",
			hp.requestTime,
			scope,
			canReqHashHex,
		}, "\n")

		// Derive signing key and compute signature
		kDate := hmacSHA256([]byte("AWS4"+iam.credential.secretKey), hp.scopeDate)
		kRegion := hmacSHA256(kDate, hp.scopeRegion)
		kService := hmacSHA256(kRegion, hp.scopeService)
		kSigning := hmacSHA256(kService, "aws4_request")
		sigBytes := hmacSHA256(kSigning, sts)
		calcSig := hex.EncodeToString(sigBytes)

		// Constant-time comparison to prevent timing attacks.
		// Both signatures are already lowercase (calcSig from hex.EncodeToString,
		// hp.signature normalized during extraction).
		if !hmac.Equal([]byte(hp.signature), []byte(calcSig)) {
			model.WriteErrorResponse(w, r, model.ErrSignatureDoesNotMatch)
			return
		}

		f(w, r)
		return
	}
}

// extractAuthHeaderParameters extracts parameters from either the Authorization header or X-Amz-* query params.
func extractAuthHeaderParameters(r *http.Request) (*AuthHeaderParameters, *AuthError) {
	headerParams := &AuthHeaderParameters{}
	headerParams.hashedPayload = r.Header.Get("x-amz-content-sha256")
	qs := r.URL.Query()

	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		headerParams.algo = "AWS4-HMAC-SHA256"
		// Parse comma-separated k=v parts
		parts := strings.Split(auth[len("AWS4-HMAC-SHA256 "):], ",")
		for _, part := range parts {
			kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(kv) != 2 {
				continue
			}
			key := kv[0]
			val := strings.Trim(kv[1], " ")
			if key == "Credential" {
				credParts := strings.Split(val, "/")
				if len(credParts) >= 5 {
					headerParams.accessKey = credParts[0]
					headerParams.scopeDate = credParts[1]
					headerParams.scopeRegion = credParts[2]
					headerParams.scopeService = credParts[3]
				}
			} else if key == "SignedHeaders" {
				headerParams.signedHeaders = val
			} else if key == "Signature" {
				// Normalize to lowercase immediately to avoid timing attacks during comparison
				headerParams.signature = strings.ToLower(val)
			}
		}
		headerParams.requestTime = r.Header.Get("x-amz-date")
		if headerParams.requestTime == "" {
			// fallback to Date header is not supported for SigV4 here
			return nil, &AuthError{model.ErrMissingDateHeader}
		}
		if headerParams.hashedPayload == "" {
			return nil, &AuthError{model.ErrInvalidDigest}
		}
	} else if qs.Get("X-Amz-Algorithm") == "AWS4-HMAC-SHA256" {
		headerParams.algo = "AWS4-HMAC-SHA256"
		headerParams.requestTime = qs.Get("X-Amz-Date")
		headerParams.signedHeaders = qs.Get("X-Amz-SignedHeaders")
		// Normalize to lowercase immediately to avoid timing attacks during comparison
		headerParams.signature = strings.ToLower(qs.Get("X-Amz-Signature"))
		cred := qs.Get("X-Amz-Credential")
		credParts := strings.Split(cred, "/")
		if len(credParts) >= 5 {
			headerParams.accessKey = credParts[0]
			headerParams.scopeDate = credParts[1]
			headerParams.scopeRegion = credParts[2]
			headerParams.scopeService = credParts[3]
		}
		// For presigned GET, payload is usually UNSIGNED-PAYLOAD
		if headerParams.hashedPayload == "" {
			headerParams.hashedPayload = qs.Get("X-Amz-Content-Sha256")
		}
		if headerParams.hashedPayload == "" {
			headerParams.hashedPayload = "UNSIGNED-PAYLOAD"
		}
		// Expires validation
		if expStr := qs.Get("X-Amz-Expires"); expStr != "" {
			// Parse requestTime then compare now <= reqTime + exp seconds
			if tReq, err := time.Parse("20060102T150405Z", headerParams.requestTime); err == nil {
				// Accept small clock skew of 5 minutes
				maxT := tReq.Add(time.Duration(parseIntDefault(expStr, 0)) * time.Second).Add(5 * time.Minute)
				if time.Now().UTC().After(maxT) {
					return nil, &AuthError{model.ErrExpiredPresignRequest}
				}
			} else {
				return nil, &AuthError{model.ErrMalformedPresignedDate}
			}
		}
	} else {
		return nil, &AuthError{model.ErrAccessDenied}
	}

	return headerParams, nil
}

func validateAuthHeaderParameters(accessKey string, hp *AuthHeaderParameters) *AuthError {
	if hp.algo != "AWS4-HMAC-SHA256" || hp.accessKey == "" || hp.signedHeaders == "" || hp.signature == "" || hp.requestTime == "" {
		return &AuthError{model.ErrSignatureDoesNotMatch}
	}

	// Verify access key matches configured credential
	if hp.accessKey != accessKey {
		return &AuthError{model.ErrInvalidAccessKeyID}
	}

	// Minimal validation of scope
	if hp.scopeDate == "" || hp.scopeRegion == "" || hp.scopeService != "s3" {
		return &AuthError{model.ErrCredMalformed}
	}

	// Validate time skew (+/- 5 minutes)
	tReq, err := time.Parse("20060102T150405Z", hp.requestTime)
	if err != nil {
		return &AuthError{model.ErrMalformedPresignedDate}
	}
	if d := time.Since(tReq.UTC()); d > 5*time.Minute || d < -5*time.Minute {
		return &AuthError{model.ErrRequestNotReadyYet}
	}

	return nil
}

func buildCanonicalURI(r *http.Request) string {
	canURI := r.URL.EscapedPath()
	if canURI == "" {
		canURI = "/"
	}
	return canURI
}

// buildCanonicalQueryString build canonical query params, for presign include all, otherwise include existing sorted
func buildCanonicalQueryString(r *http.Request) string {
	var canQuery string
	{
		// Copy query and for header-based remove the X-Amz-* if any exist but typically none
		qp := url.Values{}
		for k, vs := range r.URL.Query() {
			for _, v := range vs {
				qp.Add(k, v)
			}
		}
		// AWS requires sorting by key and then by value
		keys := make([]string, 0, len(qp))
		for k := range qp {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			vals := qp[k]
			sort.Strings(vals)
			for _, v := range vals {
				parts = append(parts, awsURLEncode(k, true)+"="+awsURLEncode(v, true))
			}
		}
		canQuery = strings.Join(parts, "&")
	}

	return canQuery
}

func buildCanonicalHeaders(signedHeaders string, r *http.Request) string {
	sh := strings.Split(signedHeaders, ";")
	sort.Strings(sh)
	var canonicalHeaders strings.Builder
	for _, h := range sh {
		name := strings.ToLower(strings.TrimSpace(h))
		val := strings.TrimSpace(r.Header.Get(name))
		if name == "host" && val == "" {
			val = r.Host
		}
		val = collapseSpaces(val)
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(val)
		canonicalHeaders.WriteString("\n")
	}

	return canonicalHeaders.String()
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func collapseSpaces(s string) string {
	// Replace consecutive spaces and tabs with a single space
	var b strings.Builder
	lastSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
		} else {
			b.WriteRune(r)
			lastSpace = false
		}
	}
	return strings.TrimSpace(b.String())
}

// awsURLEncode encodes a string per AWS SigV4 rules (RFC3986),
// optionally encoding '/' when encodeSlash is true.
func awsURLEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		isUnreserved := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~'
		if isUnreserved || (!encodeSlash && c == '/') {
			b.WriteByte(c)
		} else {
			// percent-encode
			b.WriteString("%")
			b.WriteString(strings.ToUpper(hex.EncodeToString([]byte{c})))
		}
	}
	return b.String()
}

func parseIntDefault(s string, def int) int {
	var n int
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return def
		}
		n = n*10 + int(s[i]-'0')
	}
	return n
}
