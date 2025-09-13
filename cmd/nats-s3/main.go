package main

import (
	"flag"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/server"
)

// Version is set at build time via -ldflags.
var Version string

func main() {
	// Version is injected via -ldflags "-X main.Version=..." at build time.
	// Defaults to "dev" if not set.
	if Version == "" {
		Version = "dev"
	}
	var (
		serverListen string
		natsServers  string
		natsUser     string
		natsPassword string
	)
	flag.Usage = func() {
		log.Printf("Usage: nats-s3 [options...]\n\n")
		flag.PrintDefaults()
	}

	flag.StringVar(&serverListen, "listen", "0.0.0.0:5222", "Network host:port to listen on")
	flag.StringVar(&natsServers, "natsServers", nats.DefaultURL, "List of NATS Servers to connect")
	flag.StringVar(&natsUser, "natsUser", "", "Nats server user name")
	flag.StringVar(&natsPassword, "natsPassword", "", "Nats server password")
	flag.Parse()

	log.Printf("Starting NATS S3 server... version=%s", Version)

	gateway := server.NewGatewayServer(natsServers, natsUser, natsPassword)
	err := gateway.ListenAndServe(serverListen)
	if err != nil {
		log.Fatal(err)
	}
}
