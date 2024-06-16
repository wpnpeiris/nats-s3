package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"

	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-gateway/cmd"
)

func main() {
	var (
		serverListen string
		natsServers  string
		natsUser     string
		natsPassword string
	)
	flag.Usage = func() {
		fmt.Printf("Usage: nats-gateway [options...]\n\n")
		flag.PrintDefaults()
	}

	flag.StringVar(&serverListen, "listen", "0.0.0.0:5222", "Network host:port to listen on")
	flag.StringVar(&natsServers, "natsServers", nats.DefaultURL, "List of NATS Servers to connect")
	flag.StringVar(&natsUser, "natsUser", "", "Nats server user name")
	flag.StringVar(&natsPassword, "natsPassword", "", "Nats server password")
	flag.Parse()

	log.Printf("Starting NATS Gateway...")

	var natsOptions []nats.Option
	if natsUser != "" && natsPassword != "" {
		natsOptions = append(natsOptions, nats.UserInfo(natsUser, natsPassword))
	}

	gateway := cmd.NewGatewayServer(natsServers, natsOptions)
	err := gateway.ListenAndServe(serverListen)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening for HTTP requests on %v", serverListen)
	runtime.Goexit()
}
