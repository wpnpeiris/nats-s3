package interceptor

import (
	"net/http"
)

// RequestCancellation short-circuits requests whose contexts are already
// canceled (e.g., client disconnected) before invoking the handler. This avoids
// kicking off backend work for requests that cannot complete.
type RequestCancellation struct{}

func (m *RequestCancellation) CancelIfDone(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			// Client canceled or server timed out: stop early.
			return
		default:
		}
		next.ServeHTTP(w, r)
	})
}
