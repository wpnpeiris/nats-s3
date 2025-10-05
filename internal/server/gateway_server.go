package server

import (
	"fmt"
	"github.com/go-kit/log"
	"github.com/wpnpeiris/nats-s3/internal/logging"
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
func NewGatewayServer(logger log.Logger, natsServers string, natsUser string, natsPassword string) (gatewayServer *GatewayServer) {
	s3Gateway := s3api.NewS3Gateway(logger, natsServers, natsUser, natsPassword)
	return &GatewayServer{logger, s3Gateway}
}

// ListenAndServe starts the HTTP server and blocks until it exits.
func (s *GatewayServer) ListenAndServe(endpoint string) error {
	router := mux.NewRouter()

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
