package main

import (
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"runtime"
	"s3-gateway/s3"
)

func main() {
	var (
		serverListen string
		natsServers  string
	)
	flag.Usage = func() {
		fmt.Printf("Usage: s3-gateway [options...]\n\n")
		flag.PrintDefaults()
	}

	flag.StringVar(&serverListen, "listen", "0.0.0.0:5222", "Network host:port to listen on")
	flag.StringVar(&natsServers, "nats", nats.DefaultURL, "List of NATS Servers to connect")
	flag.Parse()

	log.Printf("Starting NATS S3 Gateway...")

	comp := s3.NewComponent("s3-gateway")

	err := comp.SetupConnectionToNATS(natsServers)
	if err != nil {
		log.Fatal(err)
	}

	s := s3.Server{
		Component: comp,
	}

	err = s.ListenAndServe(serverListen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening for HTTP requests on %v", serverListen)
	runtime.Goexit()
}
