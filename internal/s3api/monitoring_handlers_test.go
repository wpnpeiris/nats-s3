package s3api

import (
    "encoding/json"
    "net/http/httptest"
    "strings"
    "testing"

    "github.com/gorilla/mux"
)

func TestHealthz_OK(t *testing.T) {
    s := startJSServer(t)
    defer s.Shutdown()

    gw := NewS3Gateway(s.ClientURL(), "", "")
    r := mux.NewRouter()
    gw.RegisterRoutes(r)

    req := httptest.NewRequest("GET", "/healthz", nil)
    rr := httptest.NewRecorder()
    r.ServeHTTP(rr, req)

    if rr.Code != 200 {
        t.Fatalf("expected 200 from /healthz, got %d", rr.Code)
    }
}

func TestMetrics_ExposesBasicMetrics(t *testing.T) {
    s := startJSServer(t)
    defer s.Shutdown()

    gw := NewS3Gateway(s.ClientURL(), "", "")
    r := mux.NewRouter()
    gw.RegisterRoutes(r)

    req := httptest.NewRequest("GET", "/metrics", nil)
    rr := httptest.NewRecorder()
    r.ServeHTTP(rr, req)

    if rr.Code != 200 {
        t.Fatalf("expected 200 from /metrics, got %d", rr.Code)
    }
    body := rr.Body.String()
    if !strings.Contains(body, "nats_connection_status") || !strings.Contains(body, "gateway_uptime_seconds") {
        t.Fatalf("/metrics missing expected metrics, body=%s", body)
    }
}

func TestStats_JSONPayload(t *testing.T) {
    s := startJSServer(t)
    defer s.Shutdown()

    gw := NewS3Gateway(s.ClientURL(), "", "")
    r := mux.NewRouter()
    gw.RegisterRoutes(r)

    req := httptest.NewRequest("GET", "/stats", nil)
    rr := httptest.NewRecorder()
    r.ServeHTTP(rr, req)

    if rr.Code != 200 {
        t.Fatalf("expected 200 from /stats, got %d", rr.Code)
    }
    var got struct {
        UptimeSeconds       int64 `json:"uptime_seconds"`
        NATSConnected       bool  `json:"nats_connected"`
        NATSReconnects      int   `json:"nats_reconnects"`
        ObjectStoreBuckets  int   `json:"objectstore_buckets"`
    }
    if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
        t.Fatalf("/stats invalid JSON: %v body=%s", err, rr.Body.String())
    }
    // Basic sanity checks
    if !got.NATSConnected {
        t.Fatalf("expected nats_connected=true in /stats")
    }
    if got.ObjectStoreBuckets < 0 {
        t.Fatalf("unexpected bucket count: %d", got.ObjectStoreBuckets)
    }
}

