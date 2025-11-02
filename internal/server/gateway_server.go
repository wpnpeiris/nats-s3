package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"

	"github.com/wpnpeiris/nats-s3/internal/credential"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/metrics"
	"github.com/wpnpeiris/nats-s3/internal/s3api"
)

// Config holds HTTP server configuration options.
type Config struct {
	Endpoint          string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ReadHeaderTimeout time.Duration
}

type GatewayServerOptions struct {
	NATSReplicas int
}

type GatewayServer struct {
	logger    log.Logger
	s3Gateway *s3api.S3Gateway
}

// NewGatewayServer constructs a server that exposes the S3 API backed by NATS.
// Returns an error if initialization fails (e.g., cannot connect to NATS).
func NewGatewayServer(ctx context.Context,
	logger log.Logger,
	natsServers string,
	natsOptions []nats.Option,
	credStore credential.Store,
	opts GatewayServerOptions) (*GatewayServer, error) {
	if opts.NATSReplicas < 1 {
		logging.Info(logger, "msg", fmt.Sprintf("Invalid NATS replicas count, defaulting to 1: [%d]", opts.NATSReplicas))
		opts.NATSReplicas = 1
	}

	s3Gateway, err := s3api.NewS3Gateway(ctx, logger, natsServers, natsOptions, credStore, s3api.S3GatewayOptions{
		Replicas: opts.NATSReplicas,
	})
	if err != nil {
		return nil, err
	}
	return &GatewayServer{logger, s3Gateway}, nil
}

// Start starts the HTTP server with the provided configuration and blocks until it exits.
func (s *GatewayServer) Start(cfg Config) error {
	router := mux.NewRouter()

	metrics.RegisterMetricEndpoint(router)
	s.s3Gateway.RegisterRoutes(router)

	srv := &http.Server{
		Addr:    cfg.Endpoint,
		Handler: router,
		// ReadTimeout covers the time from connection accept to request body read completion.
		// For S3-compatible operations, we need to support large uploads (up to 5GB single PUT).
		ReadTimeout: cfg.ReadTimeout,
		// WriteTimeout covers the time from request header read to response write completion.
		// For S3 downloads of large objects (multi-GB), we need generous timeout.
		WriteTimeout: cfg.WriteTimeout,
		// IdleTimeout is the maximum time to wait for the next request when keep-alives are enabled.
		IdleTimeout: cfg.IdleTimeout,
		// ReadHeaderTimeout prevents slowloris attacks by limiting time to read request headers.
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		// MaxHeaderBytes limits the size of request headers to prevent memory exhaustion.
		MaxHeaderBytes: 1 << 20, // 1 MB
	}

	logging.Info(s.logger, "msg", fmt.Sprintf("Listening for HTTP requests on %v (read:%v write:%v idle:%v header:%v)",
		cfg.Endpoint, cfg.ReadTimeout, cfg.WriteTimeout, cfg.IdleTimeout, cfg.ReadHeaderTimeout))
	return srv.ListenAndServe()
}
