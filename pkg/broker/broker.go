// Package broker is an in memory publish and subscribe broker over server-sent events.
// Each topic keeps a set of subscriber channels; a published message is formatted once
// as an SSE event and offered to every subscriber, and a subscriber whose buffer is full
// misses it rather than slowing the others.
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

	"github.com/KarolisValatka/Aardvark/internal/utils"
)

// maxLongLength is how much of a message the logs show.
const maxLongLength = 50

// Broker holds the subscribers per topic, the message counter and the limits. It is safe
// for concurrent use.
type Broker struct {
	client           map[string]map[chan string]struct{}
	mu               sync.RWMutex
	messageID        atomic.Uint64
	timeout          time.Duration
	maxMsgSize       int64
	buffSize         int
	maxTopicNameSize int
	retryInterval    time.Duration
}

// Options changes one setting of a new Broker.
type Options func(broker *Broker)

// WithBufferSize sets how many messages a slow subscriber may fall behind.
func WithBufferSize(size int) Options {
	return func(b *Broker) {
		b.buffSize = size
	}
}

// WithTimeout sets how long a quiet subscriber stays connected. It multiplies the value
// by time.Second, so it expects a plain number of seconds, not a Duration.
func WithTimeout(duration time.Duration) Options {
	return func(b *Broker) {
		b.timeout = duration * time.Second
	}
}

// WithMaxTopicSize sets the longest topic name.
func WithMaxTopicSize(size int) Options {
	return func(b *Broker) {
		b.maxTopicNameSize = size
	}
}

// WithMaxMsgSize sets the largest message in bytes.
func WithMaxMsgSize(size int64) Options {
	return func(b *Broker) {
		b.maxMsgSize = size
	}
}

// WithRetryInterval sets the reconnect hint sent to clients.
func WithRetryInterval(duration time.Duration) Options {
	return func(b *Broker) {
		b.retryInterval = duration
	}
}

// NewBrokerWithOptions is a broker with the default settings changed by the options.
func NewBrokerWithOptions(opts ...Options) *Broker {
	cfg := utils.NewDefaultConfig()
	broker := &Broker{
		client:           make(map[string]map[chan string]struct{}),
		timeout:          cfg.ClientTimeout,
		buffSize:         cfg.MessageBufferSize,
		maxTopicNameSize: cfg.MaxTopicNameLength,
		maxMsgSize:       cfg.MaxMessageSizeBytes,
		retryInterval:    cfg.RetryInterval,
	}

	for _, opt := range opts {
		opt(broker)
	}

	return broker
}

// NewBrokerFromEnv is a broker with settings from the environment.
func NewBrokerFromEnv() *Broker {
	cfg := utils.NewConfigFromEnv()

	return &Broker{
		client:           make(map[string]map[chan string]struct{}),
		timeout:          cfg.ClientTimeout,
		buffSize:         cfg.MessageBufferSize,
		maxTopicNameSize: cfg.MaxTopicNameLength,
		maxMsgSize:       cfg.MaxMessageSizeBytes,
		retryInterval:    cfg.RetryInterval,
	}
}

// subscribe adds a client channel to a topic, creating the topic.
func (b *Broker) subscribe(topic string, clientChan chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.client[topic]; !ok {
		b.client[topic] = make(map[chan string]struct{})
		log.Printf("Topic '%s' created", topic)
	}

	b.client[topic][clientChan] = struct{}{}
	log.Printf("Client subscribed to topic: %s. Total subscribers for topic: %d", topic, len(b.client[topic]))
}

// unsubscribe removes and closes a client channel, dropping the topic when it was the
// last one.
func (b *Broker) unsubscribe(topic string, clientChan chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if chans, ok := b.client[topic]; ok {
		if _, clientOk := chans[clientChan]; clientOk {
			delete(chans, clientChan)
			close(clientChan)
			log.Printf("Client unsubscribed from topic: %s", topic)

			if len(chans) == 0 {
				delete(b.client, topic)
				log.Printf("Topic '%s' removed as no cleints are subscribed", topic)
			} else {
				log.Printf("Total subscribers for topic '%s' remaining: %d", topic, len(chans))
			}
		}
	}
}

// publish formats a message as an SSE event with the next id and offers it to every
// subscriber without waiting. It returns the id, or 0 when the message is too big.
func (b *Broker) publish(topic string, message []byte) uint64 {
	if int64(len(message)) > b.maxMsgSize {
		log.Printf("Message size %d exceeds maximum allowed size %d", len(message), b.maxMsgSize)
		return 0
	}

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
	if !exists || len(subsForTopic) == 0 {
		b.mu.RUnlock()
		log.Printf("No subscribers for topic: '%s'. Message ID %d (content: '%s') not sent to any client", topic, msgID, truncateMessage(string(message)))
		return msgID
	}

	tempChan := make([]chan string, 0, len(subsForTopic))
	for ch := range subsForTopic {
		tempChan = append(tempChan, ch)
	}

	b.mu.RUnlock()
	log.Printf("Publishing message ID %d to %d subscribers on topic '%s'", msgID, len(tempChan), topic)

	for _, ch := range tempChan {
		select {
		case ch <- formattedMsgString:
		default:
			log.Printf("Client channel for topic '%s' is full or closed. Message ID %d dropped for this client", topic, msgID)
		}
	}

	return msgID
}

// ServeHTTP routes /infocenter/{topic}: GET subscribes, POST publishes.
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

	if len(topic) > b.maxTopicNameSize {
		http.Error(w, fmt.Sprintf("Bad Request: Topic name too long (max %d characters)", b.maxTopicNameSize), http.StatusBadRequest)
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

// handleSubscription streams a topic's events until the client leaves or stays quiet for
// the timeout, which ends with a "timeout" event. Each message restarts the timer.
func (b *Broker) handleSubscription(w http.ResponseWriter, r *http.Request, topic string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	lastEventID := r.Header.Get("Last-Event-ID")
	if lastEventID != "" {
		log.Printf("Client reconnected with Last-Event-ID: %s", lastEventID)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	fmt.Fprintf(w, "retry: %d\n\n", b.retryInterval.Milliseconds())
	flusher.Flush()

	clientChan := make(chan string, b.buffSize)
	b.subscribe(topic, clientChan)

	defer b.unsubscribe(topic, clientChan)

	flusher.Flush()

	ctx := r.Context()
	timer := time.NewTimer(b.timeout)
	defer timer.Stop()

	startTime := time.Now()

	log.Printf("Client connected to topic: '%s'. Timeout set to %v", topic, b.timeout)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Client for topic '%s' disconnected (context done)", topic)
			return
		case msg, open := <-clientChan:
			if !open {
				log.Printf("Client channel for topic '%s' closed by broker", topic)
				return
			}

			if _, err := fmt.Fprint(w, msg); err != nil {
				log.Printf("Error writing to client for topic '%s': %v. Disconnecting", topic, err)
				return
			}

			flusher.Flush()

			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(b.timeout)

		case <-timer.C:
			durationConnected := time.Since(startTime)
			timeoutEvent := fmt.Sprintf("event: timeout\ndata: %0.fs\n\n", durationConnected.Seconds())
			log.Printf("Client for topic '%s' timed out after %s. Sending timeout event", topic, durationConnected.String())

			if _, err := fmt.Fprint(w, timeoutEvent); err != nil {
				log.Printf("Error sending timeout event to client for topic '%s' : %v", topic, err)
			}

			flusher.Flush()
			return
		}
	}
}

// handlePostMessage publishes the request body to the topic and answers 204 with no
// body; an empty body is refused.
func (b *Broker) handlePostMessage(w http.ResponseWriter, r *http.Request, topic string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, b.maxMsgSize))
	if err != nil {
		log.Printf("Error reading request body for topic '%s' : %v", topic, err)
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

	msgID := b.publish(topic, body)
	log.Printf("Message (lenght %d) posted to topic '%s' with ID %d", len(body), topic, msgID)

	w.WriteHeader(http.StatusNoContent)
}

// truncateMessage cuts a message for the logs.
func truncateMessage(msg string) string {
	if len(msg) <= maxLongLength {
		return msg
	}
	return msg[:maxLongLength] + "..."
}
