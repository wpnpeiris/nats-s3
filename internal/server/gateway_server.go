package server

import (
	"fmt"
	"github.com/go-kit/log"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/metrics"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/wpnpeiris/nats-s3/internal/s3api"
)

type GatewayServer struct {
	logger    log.Logger
	s3Gateway *s3api.S3Gateway
}

// NewGatewayServer constructs a server that exposes the S3 API backed by NATS.
// Returns an error if initialization fails (e.g., cannot connect to NATS).
func NewGatewayServer(logger log.Logger, natsServers string, natsUser string, natsPassword string) (*GatewayServer, error) {
	s3Gateway, err := s3api.NewS3Gateway(logger, natsServers, natsUser, natsPassword)
	if err != nil {
		return nil, err
	}
	return &GatewayServer{logger, s3Gateway}, nil
}

// Start starts the HTTP server and blocks until it exits.
func (s *GatewayServer) Start(endpoint string) error {
	router := mux.NewRouter()

	metrics.RegisterMetricEndpoint(router)
	s.s3Gateway.RegisterRoutes(router)

	srv := &http.Server{
		Addr:           endpoint,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	logging.Info(s.logger, "msg", fmt.Sprintf("Listening for HTTP requests on %v", endpoint))
	return srv.ListenAndServe()
}
