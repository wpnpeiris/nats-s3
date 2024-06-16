package cmd

import (
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-gateway/internal/s3"
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

	l, err := net.Listen("tcp", endpoint)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:           endpoint,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go srv.Serve(l)

	return nil
}
