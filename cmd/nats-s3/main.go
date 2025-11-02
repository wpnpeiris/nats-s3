package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/credential"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/server"
)

// Version is set at build time via -ldflags.
var Version string

func main() {
	// Create root context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		natsToken         string
		natsNKeyFile      string
		natsCredsFile     string
		natsReplicas      int
		credentialsFile   string
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
	flag.StringVar(&natsUser, "natsUser", "", "NATS server username (basic auth)")
	flag.StringVar(&natsPassword, "natsPassword", "", "NATS server password (basic auth)")
	flag.StringVar(&natsToken, "natsToken", "", "NATS server token (token auth)")
	flag.StringVar(&natsNKeyFile, "natsNKeyFile", "", "NATS server NKey seed file path (nkey auth)")
	flag.StringVar(&natsCredsFile, "natsCredsFile", "", "NATS server credentials file path (JWT auth)")
	flag.IntVar(&natsReplicas, "natsReplicas", 1, "Number of NATS replicas for each jetstream element")
	flag.StringVar(&credentialsFile, "s3.credentials", "", "Path to S3 credentials file (JSON format)")
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

	// Initialize credential store
	if credentialsFile == "" {
		logging.Error(logger, "msg", "Credentials file is required", "flag", "-s3.credentials")
		os.Exit(1)
	}

	credStore, err := credential.NewStaticFileStore(credentialsFile)
	if err != nil {
		logging.Error(logger, "msg", "Failed to load credentials file", "file", credentialsFile, "err", err)
		os.Exit(1)
	}
	logging.Info(logger, "msg", "Loaded S3 credentials", "count", credStore.Count(), "store", credStore.GetName(), "file", credentialsFile)

	// Build NATS connection options based on auth type (priority order)
	var natsOptions []nats.Option

	if natsUser != "" && natsPassword != "" {
		logging.Info(logger, "msg", "Using NATS username/password authentication")
		natsOptions = append(natsOptions, nats.UserInfo(natsUser, natsPassword))
	} else if natsToken != "" {
		logging.Info(logger, "msg", "Using NATS token authentication")
		natsOptions = append(natsOptions, nats.Token(natsToken))
	} else if natsNKeyFile != "" {
		logging.Info(logger, "msg", "Using NATS NKey authentication", "file", natsNKeyFile)
		opt, err := nats.NkeyOptionFromSeed(natsNKeyFile)
		if err != nil {
			logging.Error(logger, "msg", "Failed to load NKey file", "file", natsNKeyFile, "err", err)
			os.Exit(1)
		}
		natsOptions = append(natsOptions, opt)
	} else if natsCredsFile != "" {
		logging.Info(logger, "msg", "Using NATS JWT/Credentials authentication", "file", natsCredsFile)
		natsOptions = append(natsOptions, nats.UserCredentials(natsCredsFile))
	} else {
		logging.Info(logger, "msg", "Using NATS anonymous connection (no authentication)")
	}

	gateway, err := server.NewGatewayServer(ctx, logger, natsServers, natsOptions, credStore, server.GatewayServerOptions{
		NATSReplicas: natsReplicas,
	})
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
