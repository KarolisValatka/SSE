package broker

import (
	"net/http"
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
	// TODO implement unsubscribe
}

func (b *Broker) publish(topic string, message []byte) {
	// TODO implement publish
}

func (b *Broker) ServeHttp(w http.ResponseWriter, r *http.Request) {
	// TODO implement serve for http server
}

func (b *Broker) handleSubscription(w http.ResponseWriter, r *http.Request, topic string) {
	// TODO implement handle subscription method
}

func (b *Broker) handlePostMessage(w http.ResponseWriter, r *http.Request, topic string) {
	// TODO implement when post message happens
}
