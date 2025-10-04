package s3api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"
)

// Healthz returns 200 OK when the gateway has an active connection to NATS.
// Returns 503 Service Unavailable when disconnected.
func (s *S3Gateway) Healthz(w http.ResponseWriter, r *http.Request) {
	if s.client.IsConnected() {
		WriteEmptyResponse(w, r, http.StatusOK)
		return
	}
	WriteEmptyResponse(w, r, http.StatusServiceUnavailable)
}

// Metrics exposes a tiny Prometheus text-format metrics set for basic monitoring.
func (s *S3Gateway) Metrics(w http.ResponseWriter, _ *http.Request) {
	_, reconnects := s.countReconnects()
	buckets := s.countBuckets()

	uptime := time.Since(s.started).Seconds()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	// Minimal exposition without labels
	_, _ = w.Write([]byte(
		"# HELP nats_reconnects_total Number of reconnects\n" +
			"# TYPE nats_reconnects_total counter\n" +
			fmtFloatMetric("nats_reconnects_total", float64(reconnects)) +
			"# HELP gateway_uptime_seconds Gateway uptime in seconds\n" +
			"# TYPE gateway_uptime_seconds counter\n" +
			fmtFloatMetric("gateway_uptime_seconds", uptime) +
			"# HELP objectstore_buckets_total Total number of object store buckets\n" +
			"# TYPE objectstore_buckets_total gauge\n" +
			fmtFloatMetric("objectstore_buckets_total", float64(buckets)),
	))
}

// Stats returns a small JSON with uptime, NATS status, reconnect count, and bucket count.
func (s *S3Gateway) Stats(w http.ResponseWriter, r *http.Request) {
	connected, reconnects := s.countReconnects()
	buckets := s.countBuckets()

	payload := gatewayStats{
		UptimeSeconds:      int64(time.Since(s.started).Seconds()),
		NATSConnected:      connected,
		NATSReconnects:     reconnects,
		ObjectStoreBuckets: buckets,
	}

	b, _ := json.Marshal(payload)
	WriteResponse(w, r, http.StatusOK, b, mimeNone)
}

// fmtFloatMetric renders a single-sample metric line with a float value.
func fmtFloatMetric(name string, val float64) string {
	return name + " " + strconv.FormatFloat(val, 'f', -1, 64) + "\n"
}

// countBuckets return number of buckets
func (s *S3Gateway) countBuckets() int {
	buckets := 0
	ch, err := s.client.ListBuckets()
	if err != nil {
		log.Printf("Error at ListBuckets when s.client.ListBuckets(): %v\n", err)
	} else {
		for range ch {
			buckets++
		}
	}
	return buckets
}

// countConnects returns number reconnects
func (s *S3Gateway) countReconnects() (bool, int) {
	connected := false
	reconnects := 0
	if s.client.IsConnected() {
		connected = true
		st := s.client.Stats()
		reconnects = int(st.Reconnects)
	}
	return connected, reconnects
}
