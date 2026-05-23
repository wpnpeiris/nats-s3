package s3api

import (
	"net/http/httptest"
	"testing"

	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"

	"github.com/gorilla/mux"
)

func TestHealthz_OK(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectedStatus int
		description    string
	}{
		{
			name:           "healthz returns 200 when connected",
			path:           "/healthz",
			expectedStatus: 200,
			description:    "health check should return 200 OK when NATS is connected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testutil.StartJSServer(t)
			defer s.Shutdown()

			logger := logging.NewLogger(logging.Config{Level: "debug"})
			gw, err := NewS3Gateway(logger, s.ClientURL(), 1, nil, nil)
			if err != nil {
				t.Fatalf("failed to create S3 gateway: %v", err)
			}
			r := mux.NewRouter()
			gw.RegisterRoutes(r)

			req := httptest.NewRequest("GET", tt.path, nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Fatalf("%s: expected %d from %s, got %d", tt.description, tt.expectedStatus, tt.path, rr.Code)
			}
		})
	}
}
