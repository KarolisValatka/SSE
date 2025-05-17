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

	infoCenterBroker := broker.NewBroker(defaultClientTimeout)

	mux := http.NewServeMux()

	mux.Handle("/infocenter/", infoCenterBroker)

	port := "8080"
	log.Printf("Starting SSE server on port :%s", port)
	log.Printf("Endpoints:")
	log.Printf("  POST /infocenter/{topic}  - Send a message to a topic.")
	log.Printf("  GET  /infocenter/{topic}  - Subscribe to messages from a topic (SSE).")
	log.Printf("Client inactivity timeout is set to %v.", defaultClientTimeout)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: defaultClientTimeout + 10*time.Second,
		IdleTimeout:  120 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Http server error: %v", err)
		} else {
			log.Println("Http server shutdown gracefully")
		}
	}
}
