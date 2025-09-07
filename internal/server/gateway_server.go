package server

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/wpnpeiris/nats-s3/internal/s3api"
)

type GatewayServer struct {
	s3Gateway *s3api.S3Gateway
}

// NewGatewayServer constructs a server that exposes the S3 API backed by NATS.
func NewGatewayServer(natsServers string, natsUser string, natsPassword string) (gatewayServer *GatewayServer) {
	s3Gateway := s3api.NewS3Gateway(natsServers, natsUser, natsPassword)
	return &GatewayServer{s3Gateway}
}

// ListenAndServe starts the HTTP server and blocks until it exits.
func (server *GatewayServer) ListenAndServe(endpoint string) error {
	router := mux.NewRouter()

	server.s3Gateway.RegisterRoutes(router)

	srv := &http.Server{
		Addr:           endpoint,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Printf("Listening for HTTP requests on %v", endpoint)
	return srv.ListenAndServe()
}
