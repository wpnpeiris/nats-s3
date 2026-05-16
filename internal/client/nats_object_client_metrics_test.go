package client

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	testutilpkg "github.com/wpnpeiris/nats-s3/internal/testutil"
)

// setupMetricsTestClient creates a test NATS client, object client, and metric collector
func setupMetricsTestClient(t *testing.T) (*NatsObjectClient, *MetricCollector, func()) {
	t.Helper()
	s := testutilpkg.StartJSServer(t)
	url := s.ClientURL()

	c := NewClient("metrics-test")
	if err := c.SetupConnectionToNATS(url); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	nc := c.NATS()
	nc.SetClosedHandler(func(_ *nats.Conn) {}) // Avoid panic during tests

	logger := logging.NewLogger(logging.Config{Level: "debug"})
	oc, err := NewNatsObjectClient(logger, c, NatsObjectClientOptions{Replicas: 1})
	if err != nil {
		t.Fatalf("NewNatsObjectClient failed: %v", err)
	}

	mc := NewMetricCollector(logger, oc)

	cleanup := func() {
		nc.Close()
		s.Shutdown()
	}

	return oc, mc, cleanup
}

func TestNewMetricCollector(t *testing.T) {
	tests := []struct {
		name string
		want bool // true if collector should be non-nil
	}{
		{
			name: "create metric collector",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oc, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			if tt.want {
				if mc == nil {
					t.Errorf("NewMetricCollector() returned nil")
				}
				if mc.client != oc {
					t.Errorf("NewMetricCollector() client not set correctly")
				}
				if mc.totalBucketsDesc == nil {
					t.Errorf("NewMetricCollector() totalBucketsDesc is nil")
				}
				if mc.connectStateDesc == nil {
					t.Errorf("NewMetricCollector() connectStateDesc is nil")
				}
				if mc.totalReconnectDesc == nil {
					t.Errorf("NewMetricCollector() totalReconnectDesc is nil")
				}
			}
		})
	}
}

func TestMetricCollector_Describe(t *testing.T) {
	tests := []struct {
		name          string
		wantDescCount int // Number of metric descriptions expected
	}{
		{
			name:          "describe metrics",
			wantDescCount: 1, // Only totalBucketsDesc is sent in Describe
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			ch := make(chan *prometheus.Desc, 10)
			go func() {
				mc.Describe(ch)
				close(ch)
			}()

			count := 0
			for range ch {
				count++
			}

			if count != tt.wantDescCount {
				t.Errorf("Describe() sent %d descriptions, want %d", count, tt.wantDescCount)
			}
		})
	}
}

func TestMetricCollector_countBuckets(t *testing.T) {
	tests := []struct {
		name          string
		createBuckets []string
		wantCount     float64
	}{
		{
			name:          "no buckets",
			createBuckets: []string{},
			wantCount:     0.0,
		},
		{
			name:          "single bucket",
			createBuckets: []string{"bucket1"},
			wantCount:     1.0,
		},
		{
			name:          "multiple buckets",
			createBuckets: []string{"bucket1", "bucket2", "bucket3"},
			wantCount:     3.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oc, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			// Create buckets
			for _, bucket := range tt.createBuckets {
				if _, err := oc.CreateBucket(context.Background(), bucket); err != nil {
					t.Fatalf("failed to create bucket %s: %v", bucket, err)
				}
			}

			count := mc.countBuckets()

			if count != tt.wantCount {
				t.Errorf("countBuckets() = %v, want %v", count, tt.wantCount)
			}
		})
	}
}

func TestMetricCollector_clientStats(t *testing.T) {
	tests := []struct {
		name            string
		closeConnection bool
		wantConnected   int
		checkReconnects bool
	}{
		{
			name:            "connected client",
			closeConnection: false,
			wantConnected:   1,
			checkReconnects: false,
		},
		{
			name:            "disconnected client",
			closeConnection: true,
			wantConnected:   0,
			checkReconnects: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oc, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			if tt.closeConnection {
				oc.client.NATS().Close()
			}

			connected, stats := mc.clientStats()

			if connected != tt.wantConnected {
				t.Errorf("clientStats() connected = %v, want %v", connected, tt.wantConnected)
			}

			if tt.checkReconnects && stats.Reconnects < 0 {
				t.Errorf("clientStats() reconnects should be non-negative, got %v", stats.Reconnects)
			}
		})
	}
}

func TestMetricCollector_Collect(t *testing.T) {
	tests := []struct {
		name          string
		createBuckets []string
		wantMetrics   int // Number of metrics collected
	}{
		{
			name:          "collect with no buckets",
			createBuckets: []string{},
			wantMetrics:   3, // totalBuckets, connectState, totalReconnects
		},
		{
			name:          "collect with buckets",
			createBuckets: []string{"test-bucket1", "test-bucket2"},
			wantMetrics:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oc, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			// Create buckets
			for _, bucket := range tt.createBuckets {
				if _, err := oc.CreateBucket(context.Background(), bucket); err != nil {
					t.Fatalf("failed to create bucket %s: %v", bucket, err)
				}
			}

			ch := make(chan prometheus.Metric, 10)
			go func() {
				mc.Collect(ch)
				close(ch)
			}()

			count := 0
			for range ch {
				count++
			}

			if count != tt.wantMetrics {
				t.Errorf("Collect() sent %d metrics, want %d", count, tt.wantMetrics)
			}
		})
	}
}

func TestMetricCollector_CollectMetricValues(t *testing.T) {
	tests := []struct {
		name          string
		createBuckets []string
		metricName    string
		wantValue     float64
		checkValue    bool
	}{
		{
			name:          "bucket count metric with no buckets",
			createBuckets: []string{},
			metricName:    "nats_objectstore_buckets_total",
			wantValue:     0.0,
			checkValue:    true,
		},
		{
			name:          "bucket count metric with buckets",
			createBuckets: []string{"bucket1", "bucket2"},
			metricName:    "nats_objectstore_buckets_total",
			wantValue:     2.0,
			checkValue:    true,
		},
		{
			name:          "connection state metric",
			createBuckets: []string{},
			metricName:    "nats_client_state",
			wantValue:     1.0, // Connected
			checkValue:    true,
		},
		{
			name:          "reconnects metric",
			createBuckets: []string{},
			metricName:    "nats_client_reconnects_total",
			wantValue:     0.0,
			checkValue:    false, // Don't check exact value, just existence
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oc, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			// Create buckets
			for _, bucket := range tt.createBuckets {
				if _, err := oc.CreateBucket(context.Background(), bucket); err != nil {
					t.Fatalf("failed to create bucket %s: %v", bucket, err)
				}
			}

			// Register collector temporarily
			registry := prometheus.NewRegistry()
			registry.MustRegister(mc)

			// Collect metrics and check specific metric value
			metricFamily, err := registry.Gather()
			if err != nil {
				t.Fatalf("failed to gather metrics: %v", err)
			}

			found := false
			for _, mf := range metricFamily {
				if mf.GetName() == tt.metricName {
					found = true
					if tt.checkValue {
						if len(mf.GetMetric()) == 0 {
							t.Errorf("metric %s has no values", tt.metricName)
							continue
						}

						var value float64
						if mf.GetMetric()[0].GetGauge() != nil {
							value = mf.GetMetric()[0].GetGauge().GetValue()
						} else if mf.GetMetric()[0].GetCounter() != nil {
							value = mf.GetMetric()[0].GetCounter().GetValue()
						}

						if value != tt.wantValue {
							t.Errorf("metric %s = %v, want %v", tt.metricName, value, tt.wantValue)
						}
					}
					break
				}
			}

			if !found {
				t.Errorf("metric %s not found in collected metrics", tt.metricName)
			}
		})
	}
}

func TestMetricCollector_PrometheusIntegration(t *testing.T) {
	tests := []struct {
		name          string
		createBuckets []string
		expectError   bool
	}{
		{
			name:          "register collector successfully",
			createBuckets: []string{"bucket1"},
			expectError:   false,
		},
		{
			name:          "register collector with no buckets",
			createBuckets: []string{},
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oc, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			// Create buckets
			for _, bucket := range tt.createBuckets {
				if _, err := oc.CreateBucket(context.Background(), bucket); err != nil {
					t.Fatalf("failed to create bucket %s: %v", bucket, err)
				}
			}

			// Create a new registry
			registry := prometheus.NewRegistry()
			err := registry.Register(mc)

			if tt.expectError {
				if err == nil {
					t.Errorf("Register() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Register() error = %v", err)
				}

				// Verify metrics can be gathered
				_, err := registry.Gather()
				if err != nil {
					t.Errorf("Gather() error = %v", err)
				}
			}
		})
	}
}

func TestMetricCollector_MetricDescriptions(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		wantHelp   string
	}{
		{
			name:       "buckets total metric description",
			metricName: "nats_objectstore_buckets_total",
			wantHelp:   "The total number of buckets.",
		},
		{
			name:       "client state metric description",
			metricName: "nats_client_state",
			wantHelp:   "The state of connection to NATS.",
		},
		{
			name:       "reconnects total metric description",
			metricName: "nats_client_reconnects_total",
			wantHelp:   "The total number of reconnects.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			registry := prometheus.NewRegistry()
			registry.MustRegister(mc)

			metricFamily, err := registry.Gather()
			if err != nil {
				t.Fatalf("failed to gather metrics: %v", err)
			}

			found := false
			for _, mf := range metricFamily {
				if mf.GetName() == tt.metricName {
					found = true
					if mf.GetHelp() != tt.wantHelp {
						t.Errorf("metric %s help = %v, want %v", tt.metricName, mf.GetHelp(), tt.wantHelp)
					}
					break
				}
			}

			if !found {
				t.Errorf("metric %s not found", tt.metricName)
			}
		})
	}
}

func TestMetricCollector_ConcurrentCollect(t *testing.T) {
	tests := []struct {
		name          string
		createBuckets []string
		goroutines    int
	}{
		{
			name:          "concurrent collect calls",
			createBuckets: []string{"bucket1", "bucket2"},
			goroutines:    5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oc, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			// Create buckets
			for _, bucket := range tt.createBuckets {
				if _, err := oc.CreateBucket(context.Background(), bucket); err != nil {
					t.Fatalf("failed to create bucket %s: %v", bucket, err)
				}
			}

			// Run multiple Collect calls concurrently
			done := make(chan bool, tt.goroutines)
			for i := 0; i < tt.goroutines; i++ {
				go func() {
					ch := make(chan prometheus.Metric, 10)
					go func() {
						mc.Collect(ch)
						close(ch)
					}()

					count := 0
					for range ch {
						count++
					}

					if count != 3 { // Should always collect 3 metrics
						t.Errorf("Collect() sent %d metrics, want 3", count)
					}
					done <- true
				}()
			}

			// Wait for all goroutines to complete
			for i := 0; i < tt.goroutines; i++ {
				<-done
			}
		})
	}
}

func TestMetricCollector_MetricTypes(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		wantType   string // "gauge" or "counter"
	}{
		{
			name:       "buckets total is gauge",
			metricName: "nats_objectstore_buckets_total",
			wantType:   "gauge",
		},
		{
			name:       "client state is gauge",
			metricName: "nats_client_state",
			wantType:   "gauge",
		},
		{
			name:       "reconnects total is counter",
			metricName: "nats_client_reconnects_total",
			wantType:   "counter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			registry := prometheus.NewRegistry()
			registry.MustRegister(mc)

			metricFamily, err := registry.Gather()
			if err != nil {
				t.Fatalf("failed to gather metrics: %v", err)
			}

			found := false
			for _, mf := range metricFamily {
				if mf.GetName() == tt.metricName {
					found = true
					metricType := mf.GetType().String()
					expectedType := tt.wantType

					// Normalize type names
					if expectedType == "gauge" && metricType != "GAUGE" {
						t.Errorf("metric %s type = %v, want GAUGE", tt.metricName, metricType)
					}
					if expectedType == "counter" && metricType != "COUNTER" {
						t.Errorf("metric %s type = %v, want COUNTER", tt.metricName, metricType)
					}
					break
				}
			}

			if !found {
				t.Errorf("metric %s not found", tt.metricName)
			}
		})
	}
}

func TestMetricCollector_CountBucketsError(t *testing.T) {
	tests := []struct {
		name               string
		closeConnection    bool
		expectNonZeroCount bool
	}{
		{
			name:               "count buckets with closed connection",
			closeConnection:    true,
			expectNonZeroCount: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oc, mc, cleanup := setupMetricsTestClient(t)
			defer cleanup()

			// Create a bucket first
			if _, err := oc.CreateBucket(context.Background(), "test-bucket"); err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}

			if tt.closeConnection {
				oc.client.NATS().Close()
			}

			count := mc.countBuckets()

			if tt.expectNonZeroCount && count == 0.0 {
				t.Errorf("countBuckets() = 0, expected non-zero")
			}
			if !tt.expectNonZeroCount && count != 0.0 {
				t.Errorf("countBuckets() = %v, expected 0", count)
			}
		})
	}
}
