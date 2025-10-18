package s3api

import (
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/testutil"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

func TestHealthz_OK(t *testing.T) {
	s := testutil.StartJSServer(t)
	defer s.Shutdown()

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	gw := NewS3Gateway(logger, s.ClientURL(), "", "")
	r := mux.NewRouter()
	gw.RegisterRoutes(r)

	req := httptest.NewRequest("GET", "/healthz", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("expected 200 from /healthz, got %d", rr.Code)
	}
}
