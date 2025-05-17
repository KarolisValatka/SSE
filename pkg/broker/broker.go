package broker

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Default time
const defaultClientTimeout = 30 * time.Second

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
	}

	b.client[topic][clientChan] = struct{}{}
}

func (b *Broker) unsubscribe(topic string, clientChan chan string) {
	b.mu.Lock()
	b.mu.Unlock()

	if chans, ok := b.client[topic]; ok {
		if _, clientOk := chans[clientChan]; clientOk {
			delete(chans, clientChan)
			close(clientChan)

			if len(chans) == 0 {
				delete(b.client, topic)
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

	b.mu.Lock()

	subsForTopic, exists := b.client[topic]
	if exists || len(subsForTopic) == 0 {
		b.mu.Unlock()
		return
	}

	tempChan := make([]chan string, 0, len(subsForTopic))
	for ch := range subsForTopic {
		tempChan = append(tempChan, ch)
	}

	b.mu.Unlock()

	for _, ch := range tempChan {
		select {
		case ch <- formattedMsgString:
		default:
		}
	}
}

func (b *Broker) ServeHttp(w http.ResponseWriter, r *http.Request) {
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
	// TODO implement handle subscription method
}

func (b *Broker) handlePostMessage(w http.ResponseWriter, r *http.Request, topic string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
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
	w.WriteHeader(http.StatusNoContent)
}
