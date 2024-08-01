package server

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/s3"
)

type GatewayServer struct {
	s3Gateway *s3.S3Gateway
}

func NewGatewayServer(natsServers string, options []nats.Option) (gatewayServer *GatewayServer) {
	s3Gateway := s3.NewS3Gateway(natsServers, options)
	return &GatewayServer{s3Gateway}
}

func (server *GatewayServer) ListenAndServe(endpoint string) error {
	router := mux.NewRouter()

	server.s3Gateway.RegisterS3Routes(router)

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
