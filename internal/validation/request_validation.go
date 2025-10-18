package validation

import (
	"github.com/gorilla/mux"
	"github.com/wpnpeiris/nats-s3/internal/model"
	"github.com/wpnpeiris/nats-s3/internal/util"
	"log"
	"net/http"
)

// RequestValidator validates bucket names and object keys from route parameters
// before passing the request to the handler.
type RequestValidator struct{}

// Validate is a middleware that extracts and validates bucket and key parameters
// from the request path. It validates bucket names against S3 naming rules and object
// keys against security constraints (path traversal, control characters, etc.).
func (v *RequestValidator) Validate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		// Validate bucket name if present in route
		if bucket, ok := vars["bucket"]; ok && bucket != "" {
			if err := util.ValidateBucketName(bucket); err != nil {
				log.Printf("Invalid bucket name: %s, error: %v", bucket, err)
				model.WriteErrorResponse(w, r, model.ErrInvalidBucketName)
				return
			}
		}

		// Validate object key if present in route
		if key, ok := vars["key"]; ok && key != "" {
			if err := util.ValidateObjectKey(key); err != nil {
				log.Printf("Invalid object key: %s, error: %v", key, err)
				model.WriteErrorResponse(w, r, model.ErrInvalidRequest)
				return
			}
		}

		// Validation passed, proceed to handler
		next.ServeHTTP(w, r)
	})
}
