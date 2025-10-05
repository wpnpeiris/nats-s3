package metrics

import (
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// path is the HTTP route where Prometheus metrics are exposed.
	path = "/metrics"
	// registry is the private Prometheus registry used by the gateway server.
	registry = prometheus.NewRegistry()
	// runtimeMetrics collects Go runtime stats (GC, goroutines, mem, etc.).
	runtimeMetrics = collectors.NewGoCollector()
	// processMetrics collects process-level stats (CPU, RSS, fds, etc.).
	processMetrics = collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})
)

func init() {
	registry.MustRegister(runtimeMetrics)
	registry.MustRegister(processMetrics)
}

// RegisterMetricEndpoint mounts the Prometheus handler for private registry at the standard metrics path
// on the provided router.
func RegisterMetricEndpoint(router *mux.Router) {
	r := router.PathPrefix("/").Subrouter()
	r.Handle(path, promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		},
	))
}

// RegisterPrometheusCollector registers a custom collector with registry.
func RegisterPrometheusCollector(c prometheus.Collector) error {
	return registry.Register(c)
}
