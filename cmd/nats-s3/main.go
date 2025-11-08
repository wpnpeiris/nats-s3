package main

import (
	"flag"
	"fmt"
	"github.com/wpnpeiris/nats-s3/internal/server"
	"os"
)

var usageStr = `
Usage: nats-s3 [options]

Server Options:
    --listen <host:port>             HTTP bind address for NATS S3 (default: 0.0.0.0:5222)

NATS Connection Options:
    --natsServers <url>              Comma-separated NATS server URLs (default: nats://127.0.0.1:4222)

NATS Object Store Options:
    --replicas <N>               Number of replicas for each JetStream element (default: 1)

NATS Authentication Options (choose one):
    --user <user>                NATS server username (basic auth)
    --password <pass>            NATS server password (basic auth)
    --token <token>              NATS server token (token-based auth)
    --nkeyFile <path>            NATS server NKey seed file path (NKey auth)
    --credsFile <path>           NATS server credentials file path (JWT auth)

S3 Options:
    --s3.credentials <path>          Path to S3 credentials file (JSON format, required)

Logging Options:
    --log.format <format>            Log output format: logfmt or json (default: logfmt)
    --log.level <level>              Log level: debug, info, warn, error (default: info)

HTTP Server Timeout Options:
    --http.read-timeout <duration>   HTTP server read timeout (default: 15m)
    --http.write-timeout <duration>  HTTP server write timeout (default: 15m)
    --http.idle-timeout <duration>   HTTP server idle timeout (default: 120s)
    --http.read-header-timeout <dur> HTTP server read header timeout (default: 30s)

Common Options:
    -h, --help                       Show this message
    -v, --version                    Show version

Examples:
    # Start with basic configuration
    nats-s3 --natsServers nats://127.0.0.1:4222 --s3.credentials credentials.json

    # Start with NATS authentication
    nats-s3 --natsServers nats://127.0.0.1:4222 \
            --user admin --password secret \
            --s3.credentials credentials.json

    # Start with custom listen address and debug logging
    nats-s3 --listen 0.0.0.0:8080 \
            --natsServers nats://127.0.0.1:4222 \
            --s3.credentials credentials.json \
            --log.level debug
`

// Version is set at build time via -ldflags.
var Version string

// printVersionAndExit will print our version and exit.
func printVersionAndExit() {
	fmt.Printf("nats-s3: v%s\n", Version)
	os.Exit(0)
}

// usage will print out the flag options of nats-s3.
func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func main() {
	fs := flag.NewFlagSet("nats-s3", flag.ExitOnError)
	fs.Usage = usage
	opts, err := server.ConfigureOptions(fs, os.Args[1:], printVersionAndExit, fs.Usage)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	gateway, err := server.NewGatewayServer(opts)
	if err != nil {
		server.LogAndExit(err.Error())
	}

	err = gateway.Start()
	if err != nil {
		server.LogAndExit(err.Error())
	}
}
