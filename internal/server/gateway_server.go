package server

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/credential"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"github.com/wpnpeiris/nats-s3/internal/metrics"
	"github.com/wpnpeiris/nats-s3/internal/s3api"
)

type GatewayServerOptions struct {
	NATSReplicas int
}

// Config holds HTTP server configuration options.
type Config struct {
	Endpoint          string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ReadHeaderTimeout time.Duration
}

type GatewayServer struct {
	logger    log.Logger
	config    Config
	s3Gateway *s3api.S3Gateway
}

// LogAndExit logs an error message to stderr and exits with status code 1.
func LogAndExit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

// NewGatewayServer constructs a server that exposes the S3 API backed by NATS.
// Returns an error if initialization fails (e.g., cannot connect to NATS).
func NewGatewayServer(opts *Options) (*GatewayServer, error) {
	logger := logging.NewLogger(logging.Config{
		Format: opts.LogFormat,
		Level:  opts.LogLevel,
	})

	natsOptions := loadNatsOptions(logger, opts)
	credStore := initializeCredentialStore(logger, opts)
	s3Gateway, err := s3api.NewS3Gateway(logger,
		opts.NatsServers,
		opts.Replicas,
		natsOptions,
		credStore)
	if err != nil {
		return nil, err
	}

	config := Config{
		Endpoint:          opts.ServerListen,
		ReadTimeout:       opts.ReadTimeout,
		WriteTimeout:      opts.WriteTimeout,
		IdleTimeout:       opts.IdleTimeout,
		ReadHeaderTimeout: opts.ReadHeaderTimeout,
	}
	return &GatewayServer{logger, config, s3Gateway}, nil
}

// initializeCredentialStore loads the S3 credentials from the configured file path.
// It exits with status code 1 if the credentials file is missing or invalid.
func initializeCredentialStore(logger log.Logger, opts *Options) *credential.StaticFileStore {
	credentialsFile := opts.CredentialsFile
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
	return credStore
}

// loadNatsOptions builds NATS connection options based on the configured authentication type.
// It exits with status code 1 if NKey file loading fails.
func loadNatsOptions(logger log.Logger, opts *Options) []nats.Option {
	var natsOptions []nats.Option

	if opts.User != "" && opts.Password != "" {
		logging.Info(logger, "msg", "Using NATS username/password authentication")
		natsOptions = append(natsOptions, nats.UserInfo(opts.User, opts.Password))
	} else if opts.Token != "" {
		logging.Info(logger, "msg", "Using NATS token authentication")
		natsOptions = append(natsOptions, nats.Token(opts.Token))
	} else if opts.NkeyFile != "" {
		logging.Info(logger, "msg", "Using NATS NKey authentication", "file", opts.NkeyFile)
		opt, err := nats.NkeyOptionFromSeed(opts.NkeyFile)
		if err != nil {
			logging.Error(logger, "msg", "Failed to load NKey file", "file", opts.NkeyFile, "err", err)
			os.Exit(1)
		}
		natsOptions = append(natsOptions, opt)
	} else if opts.CredsFile != "" {
		logging.Info(logger, "msg", "Using NATS JWT/Credentials authentication", "file", opts.CredsFile)
		natsOptions = append(natsOptions, nats.UserCredentials(opts.CredsFile))
	} else {
		logging.Info(logger, "msg", "Using NATS anonymous connection (no authentication)")
	}
	return natsOptions
}

// Start starts the HTTP server with the provided configuration and blocks until it exits.
func (s *GatewayServer) Start() error {
	logging.Info(s.logger, "msg", fmt.Sprintf("Starting NATS S3 server..."))
	router := mux.NewRouter().SkipClean(true)

	metrics.RegisterMetricEndpoint(router)
	s.s3Gateway.RegisterRoutes(router)

	srv := &http.Server{
		Addr:    s.config.Endpoint,
		Handler: router,
		// ReadTimeout covers the time from connection accept to request body read completion.
		// For S3-compatible operations, we need to support large uploads (up to 5GB single PUT).
		ReadTimeout: s.config.ReadTimeout,
		// WriteTimeout covers the time from request header read to response write completion.
		// For S3 downloads of large objects (multi-GB), we need generous timeout.
		WriteTimeout: s.config.WriteTimeout,
		// IdleTimeout is the maximum time to wait for the next request when keep-alives are enabled.
		IdleTimeout: s.config.IdleTimeout,
		// ReadHeaderTimeout prevents slowloris attacks by limiting time to read request headers.
		ReadHeaderTimeout: s.config.ReadHeaderTimeout,
		// MaxHeaderBytes limits the size of request headers to prevent memory exhaustion.
		MaxHeaderBytes: 1 << 20, // 1 MB
	}

	logging.Info(s.logger, "msg", fmt.Sprintf("Listening for HTTP requests on %s", s.config.Endpoint))
	return srv.ListenAndServe()
}
