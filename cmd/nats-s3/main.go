package main

import (
	"flag"
	"fmt"
	"os"
	"time"

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
		serverListen      string
		natsServers       string
		natsUser          string
		natsPassword      string
		logFormat         string
		logLevel          string
		readTimeout       time.Duration
		writeTimeout      time.Duration
		idleTimeout       time.Duration
		readHeaderTimeout time.Duration
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
	flag.DurationVar(&readTimeout, "http.read-timeout", 15*time.Minute, "HTTP server read timeout (for large uploads)")
	flag.DurationVar(&writeTimeout, "http.write-timeout", 15*time.Minute, "HTTP server write timeout (for large downloads)")
	flag.DurationVar(&idleTimeout, "http.idle-timeout", 120*time.Second, "HTTP server idle timeout")
	flag.DurationVar(&readHeaderTimeout, "http.read-header-timeout", 30*time.Second, "HTTP server read header timeout (slowloris protection)")
	flag.Parse()

	logger := logging.NewLogger(logging.Config{
		Format: logFormat,
		Level:  logLevel,
	})

	logging.Info(logger, "mgs", fmt.Sprintf("Starting NATS S3 server... version=%s", Version))
	gateway, err := server.NewGatewayServer(logger, natsServers, natsUser, natsPassword)
	if err != nil {
		logging.Error(logger, "msg", "Failed to initialize NATS S3 server", "err", err)
		os.Exit(1)
	}

	serverConfig := server.Config{
		Endpoint:          serverListen,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	err = gateway.Start(serverConfig)
	if err != nil {
		logging.Error(logger, "msg", "Failure starting NATS S3 server", "err", err)
		os.Exit(1)
	}
}
