package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/server"
)

func main() {
	var (
		serverListen string
		natsServers  string
		natsUser     string
		natsPassword string
	)
	flag.Usage = func() {
		fmt.Printf("Usage: nats-s3 [options...]\n\n")
		flag.PrintDefaults()
	}

	flag.StringVar(&serverListen, "listen", "0.0.0.0:5222", "Network host:port to listen on")
	flag.StringVar(&natsServers, "natsServers", nats.DefaultURL, "List of NATS Servers to connect")
	flag.StringVar(&natsUser, "natsUser", "", "Nats server user name")
	flag.StringVar(&natsPassword, "natsPassword", "", "Nats server password")
	flag.Parse()

	log.Printf("Starting NATS S3 server...")

	var natsOptions []nats.Option
	if natsUser != "" && natsPassword != "" {
		natsOptions = append(natsOptions, nats.UserInfo(natsUser, natsPassword))
	}

	gateway := server.NewGatewayServer(natsServers, natsOptions)
	err := gateway.ListenAndServe(serverListen)
	if err != nil {
		log.Fatal(err)
	}
}
