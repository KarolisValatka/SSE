package main

import (
	"fmt"
	"net/http"
	"time"
)

func main() {

	http.HandleFunc("/events", sseHandler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

func sseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// This for local only
	w.Header().Set("Access-Control-Allow-Origin", "*")

	memT := time.NewTicker(time.Second)
	defer memT.Stop()

	clientGone := r.Context().Done()

	rc := http.NewResponseController(w)
	for {
		select {
		case <-memT.C:
		case <-clientGone:
			fmt.Printf("client died")
			err := rc.Flush()
			if err != nil {
				fmt.Print("I died flushing")
			}
		}
	}
}
