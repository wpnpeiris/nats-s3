package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"

	"github.com/wpnpeiris/nats-gateway/gateway/s3"

	"github.com/nats-io/nats.go"
)

func main() {
	var (
		serverListen string
		natsServers  string
	)
	flag.Usage = func() {
		fmt.Printf("Usage: nats-gateway [options...]\n\n")
		flag.PrintDefaults()
	}

	flag.StringVar(&serverListen, "listen", "0.0.0.0:5222", "Network host:port to listen on")
	flag.StringVar(&natsServers, "nats", nats.DefaultURL, "List of NATS Servers to connect")
	flag.Parse()

	log.Printf("Starting NATS Gateway...")

	s3 := s3.NewS3Server(natsServers)

	err := s3.ListenAndServe(serverListen)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening for HTTP requests on %v", serverListen)
	runtime.Goexit()
}
