package broker

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const messageChannelBuffer = 10

type Broker struct {
	client    map[string]map[chan string]struct{}
	mu        sync.RWMutex
	messageID atomic.Uint64
	timeout   time.Duration
}

func NewBroker(clientTimeout time.Duration) *Broker {
	return &Broker{
		client:  make(map[string]map[chan string]struct{}),
		timeout: clientTimeout,
	}
}

func (b *Broker) subscribe(topic string, clientChan chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.client[topic]; ok {
		b.client[topic] = make(map[chan string]struct{})
		log.Printf("Topic '%s' created.", topic)
	}

	b.client[topic][clientChan] = struct{}{}
	log.Printf("Client subscribed to topic: %s. Total subscribers for topic: %d", topic, len(b.client[topic]))
}

func (b *Broker) unsubscribe(topic string, clientChan chan string) {
	b.mu.Lock()
	b.mu.Unlock()

	if chans, ok := b.client[topic]; ok {
		if _, clientOk := chans[clientChan]; clientOk {
			delete(chans, clientChan)
			close(clientChan)
			log.Printf("Client unsubscribed from topic: %s.", topic)

			if len(chans) == 0 {
				delete(b.client, topic)
				log.Printf("Topic '%s' removed as no cleints are subscriber.", topic)
			} else {
				log.Printf("Total subscribers for topic '%s' remaining: %d", topic, len(chans))
			}
		}
	}
}

func (b *Broker) publish(topic string, message []byte) {
	msgID := b.messageID.Add(1)

	var sseMessage bytes.Buffer
	sseMessage.WriteString(fmt.Sprintf("id: %d\n", msgID))
	sseMessage.WriteString("event: msg\n")

	sanitizeBody := bytes.ReplaceAll(message, []byte("\r\n"), []byte("\n"))
	lines := strings.Split(string(sanitizeBody), "\n")

	for _, line := range lines {
		sseMessage.WriteString(fmt.Sprintf("data: %s\n", line))
	}
	sseMessage.WriteString("\n")

	formattedMsgString := sseMessage.String()

	b.mu.RLock()

	subsForTopic, exists := b.client[topic]
	if exists || len(subsForTopic) == 0 {
		b.mu.RUnlock()
		log.Printf("No subscribers for topic: '%s'. Message ID %d (content: '%s') not sent to any client.", topic, msgID, string(message))
		return
	}

	tempChan := make([]chan string, 0, len(subsForTopic))
	for ch := range subsForTopic {
		tempChan = append(tempChan, ch)
	}

	b.mu.RUnlock()
	log.Printf("Publishing message ID %d to %d subscribers on topic '%s'.", msgID, len(tempChan), topic)

	for _, ch := range tempChan {
		select {
		case ch <- formattedMsgString:
		default:
			log.Printf("Client channel for topic '%s' is full or closed. Message ID %d dropped for this client.", topic, msgID)
		}
	}
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 2 || pathParts[0] != "infocenter" {
		http.Error(w, "Not Found: Expected path /infocenter/{topic}", http.StatusNotFound)
		return
	}

	topic := pathParts[1]
	if topic == "" {
		http.Error(w, "Bad Request: Topic cannot be empty", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		b.handleSubscription(w, r, topic)
	case http.MethodPost:
		b.handlePostMessage(w, r, topic)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (b *Broker) handleSubscription(w http.ResponseWriter, r *http.Request, topic string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	clientChan := make(chan string, messageChannelBuffer)
	b.subscribe(topic, clientChan)

	defer b.unsubscribe(topic, clientChan)

	flusher.Flush()

	ctx := r.Context()
	timer := time.NewTimer(b.timeout)
	startTime := time.Now()

	log.Printf("Client connected to topic: '%s'. Timeout set to %v.", topic, b.timeout)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Client for topic '%s' disconnected (context done).", topic)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case msg, open := <-clientChan:
			if !open {
				log.Printf("Client channel for topic '%s' closed by broker.", topic)
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
			if _, err := fmt.Fprint(w, msg); err != nil {
				log.Printf("Error writing to client for topic '%s': %v. Disconnecting.", topic, err)
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}

			flusher.Flush()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(b.timeout)

		case <-timer.C:
			durationConnected := time.Since(startTime)
			timeoutEvent := fmt.Sprintf("event: timeout\ndata: %0.fs\n\n", durationConnected.Seconds())
			log.Printf("Client for topic '%s' timed out after %s. Sending timeout event.", topic, durationConnected.String())

			if _, err := fmt.Fprint(w, timeoutEvent); err != nil {
				log.Printf("Error sending timeout event to client for topic '%s' : %v", topic, err)
			}

			flusher.Flush()

			return
		}
	}
}

func (b *Broker) handlePostMessage(w http.ResponseWriter, r *http.Request, topic string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request boyd for topic '%s' : %v", topic, err)
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	defer func(Body io.ReadCloser) {
		errCloseBody := Body.Close()
		if errCloseBody != nil {
			fmt.Printf("Failed to close reader body: %s\n", errCloseBody)
		}
	}(r.Body)

	if len(body) == 0 {
		http.Error(w, "Message body cannot be empty", http.StatusBadRequest)
		return
	}

	b.publish(topic, body)
	log.Printf("Message (lenght %d) posted to topic '%s.'", len(body), topic)

	w.WriteHeader(http.StatusNoContent)
}
