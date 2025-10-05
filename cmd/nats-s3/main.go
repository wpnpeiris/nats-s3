package main

import (
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/logging"
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
		logFormat    string
		logLevel     string
	)
	flag.Usage = func() {
		fmt.Printf("Usage: nats-s3 [options...]\n\n")
		flag.PrintDefaults()
	}

	flag.StringVar(&serverListen, "listen", "0.0.0.0:5222", "Network host:port to listen on")
	flag.StringVar(&natsServers, "natsServers", nats.DefaultURL, "List of NATS Servers to connect")
	flag.StringVar(&natsUser, "natsUser", "", "Nats server user name")
	flag.StringVar(&natsPassword, "natsPassword", "", "Nats server password")
	flag.StringVar(&logFormat, "log.format", "logfmt", "log output format: logfmt or json")
	flag.StringVar(&logLevel, "log.level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	logger := logging.NewLogger(logging.Config{
		Format: logFormat,
		Level:  logLevel,
	})

	logging.Info(logger, "mgs", fmt.Sprintf("Starting NATS S3 server... version=%s", Version))
	gateway := server.NewGatewayServer(logger, natsServers, natsUser, natsPassword)
	err := gateway.ListenAndServe(serverListen)
	if err != nil {
		logging.Error(logger, "msg", "Failure starting NATS S3 server", "err", err)
	}
}
