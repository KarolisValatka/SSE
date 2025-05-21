package main

import (
	"errors"
	"github.com/KarolisValatka/Aardvark/pkg/broker"
	"log"
	"net/http"
	"time"
)

const defaultClientTimeout = 30 * time.Second

func main() {

	infoCenterBroker := broker.NewBrokerWithOptions(
		broker.WithTimeout(defaultClientTimeout),
		broker.WithBufferSize(10),
		broker.WithMaxTopicSize(50),
		broker.WithMaxMsgSize(1024*1024),
		broker.WithRetryInterval(10),
	)

	mux := http.NewServeMux()

	mux.Handle("/infocenter/", infoCenterBroker)

	port := "8080"

	log.Printf("Starting SSE server on port :%s", port)
	log.Printf("Endpoints:")
	log.Printf("  POST /infocenter/{topic}  - Send a message to a topic.")
	log.Printf("  GET  /infocenter/{topic}  - Subscribe to messages from a topic (SSE).")
	log.Printf("Client inactivity timeout is set to %v.", defaultClientTimeout)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	if err := server.ListenAndServe(); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Http server error: %v", err)
		} else {
			log.Println("Http server shutdown gracefully")
		}
	}
}
