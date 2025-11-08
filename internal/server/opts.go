package server

import (
	"flag"
	"github.com/nats-io/nats.go"
	"time"
)

// Options holds all configuration options for the NATS S3 server.
type Options struct {
	ServerListen      string
	NatsServers       string
	User              string
	Password          string
	Token             string
	NkeyFile          string
	CredsFile         string
	Replicas          int
	CredentialsFile   string
	LogFormat         string
	LogLevel          string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ReadHeaderTimeout time.Duration
}

// ConfigureOptions parses command-line arguments and returns an Options struct.
// It handles -h/--help and -v/--version flags by calling the provided callbacks.
// Returns nil options and nil error when help or version flags are used.
func ConfigureOptions(fs *flag.FlagSet, args []string, printVersion, printHelp func()) (*Options, error) {
	opts := &Options{}
	var (
		showVersion bool
		showHelp    bool
	)
	fs.BoolVar(&showVersion, "v", false, "Print version information.")
	fs.BoolVar(&showVersion, "version", false, "Print version information.")
	fs.BoolVar(&showHelp, "h", false, "Print usage.")
	fs.BoolVar(&showHelp, "help", false, "Print usage.")

	fs.StringVar(&opts.ServerListen, "listen", "0.0.0.0:5222", "Network host:port to listen on")
	fs.StringVar(&opts.NatsServers, "natsServers", nats.DefaultURL, "List of NATS Servers to connect")
	fs.StringVar(&opts.User, "natsUser", "", "NATS server username (basic auth)")
	fs.StringVar(&opts.Password, "natsPassword", "", "NATS server password (basic auth)")
	fs.StringVar(&opts.Token, "natsToken", "", "NATS server token (token auth)")
	fs.StringVar(&opts.NkeyFile, "natsNKeyFile", "", "NATS server NKey seed file path (nkey auth)")
	fs.StringVar(&opts.CredsFile, "natsCredsFile", "", "NATS server credentials file path (JWT auth)")
	fs.IntVar(&opts.Replicas, "replicas", 1, "Number of replicas for each jetstream element")
	fs.StringVar(&opts.CredentialsFile, "s3.credentials", "", "Path to S3 credentials file (JSON format)")
	fs.StringVar(&opts.LogFormat, "log.format", "logfmt", "log output format: logfmt or json")
	fs.StringVar(&opts.LogLevel, "log.level", "info", "log level: debug, info, warn, error")
	fs.DurationVar(&opts.ReadTimeout, "http.read-timeout", 15*time.Minute, "HTTP server read timeout (for large uploads)")
	fs.DurationVar(&opts.WriteTimeout, "http.write-timeout", 15*time.Minute, "HTTP server write timeout (for large downloads)")
	fs.DurationVar(&opts.IdleTimeout, "http.idle-timeout", 120*time.Second, "HTTP server idle timeout")
	fs.DurationVar(&opts.ReadHeaderTimeout, "http.read-header-timeout", 30*time.Second, "HTTP server read header timeout (slowloris protection)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if showVersion {
		printVersion()
		return nil, nil
	}

	if showHelp {
		printHelp()
		return nil, nil
	}

	return opts, nil
}
