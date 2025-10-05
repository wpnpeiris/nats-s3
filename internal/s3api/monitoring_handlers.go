package s3api

import (
	"github.com/wpnpeiris/nats-s3/internal/model"
	"net/http"
)

// Healthz returns 200 OK when the gateway has an active connection to NATS.
// Returns 503 Service Unavailable when disconnected.
func (s *S3Gateway) Healthz(w http.ResponseWriter, r *http.Request) {
	if s.client.IsConnected() {
		model.WriteEmptyResponse(w, r, http.StatusOK)
		return
	}
	model.WriteEmptyResponse(w, r, http.StatusServiceUnavailable)
}
