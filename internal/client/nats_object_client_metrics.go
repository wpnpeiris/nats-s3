package client

import (
	"context"

	"github.com/go-kit/log"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/wpnpeiris/nats-s3/internal/logging"
)

const (
	namespace            = "nats"
	clientSubsystem      = "client"
	objectStoreSubsystem = "objectstore"
)

type MetricCollector struct {
	ctx    context.Context
	logger log.Logger
	client *NatsObjectClient

	// Total buckets metrics
	totalBucketsDesc *prometheus.Desc

	// Current connection state to NATS metrics
	connectStateDesc *prometheus.Desc

	// Total reconnect metrics to NATS
	totalReconnectDesc *prometheus.Desc
}

func NewMetricCollector(ctx context.Context, logger log.Logger, client *NatsObjectClient) *MetricCollector {
	return &MetricCollector{
		ctx:    ctx,
		logger: logger,
		client: client,

		totalBucketsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, objectStoreSubsystem, "buckets_total"),
			"The total number of buckets.",
			nil,
			nil,
		),
		connectStateDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, clientSubsystem, "state"),
			"The state of connection to NATS.",
			nil,
			nil,
		),

		totalReconnectDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, clientSubsystem, "reconnects_total"),
			"The total number of reconnects.",
			nil,
			nil,
		),
	}
}

// Describe implements the prometheus.Collector interface.
func (c MetricCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.totalBucketsDesc
}

// Collect implements the prometheus.Collector interface.
func (c MetricCollector) Collect(ch chan<- prometheus.Metric) {
	connected, stats := c.clientStats()

	ch <- prometheus.MustNewConstMetric(
		c.totalBucketsDesc,
		prometheus.GaugeValue,
		c.countBuckets(),
	)
	ch <- prometheus.MustNewConstMetric(
		c.connectStateDesc,
		prometheus.GaugeValue,
		float64(connected),
	)
	ch <- prometheus.MustNewConstMetric(
		c.totalReconnectDesc,
		prometheus.CounterValue,
		float64(stats.Reconnects),
	)
}

// countBuckets return number of buckets
func (c MetricCollector) countBuckets() float64 {
	buckets := 0.0
	ch, err := c.client.ListBuckets(c.ctx)
	if err != nil {
		logging.Warn(c.logger, "msg", "Error at listing buckets for metrics", "err", err)
	} else {
		for range ch {
			buckets++
		}
	}
	return buckets
}

// clientStats return nats client's statistics
func (c MetricCollector) clientStats() (int, nats.Statistics) {
	if c.client.IsConnected() {
		return 1, c.client.Stats()
	}
	return 0, nats.Statistics{}
}
