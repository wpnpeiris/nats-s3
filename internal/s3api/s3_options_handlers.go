package s3api

import (
	"net/http"
)

// SetOptionHeaders responds to CORS preflight and generic OPTIONS requests with
// permissive headers to simplify development and testing.
func (s *S3Gateway) SetOptionHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "*")
	w.Header().Set("Access-Control-Allow-Methods", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	WriteEmptyResponse(w, r, http.StatusOK)
}
