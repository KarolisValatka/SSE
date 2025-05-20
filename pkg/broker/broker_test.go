package broker

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNewBrokerWithOptions(t *testing.T) {
	tests := []struct {
		name     string
		options  []Options
		expected *Broker
	}{
		{
			name:    "default options",
			options: []Options{},
			expected: &Broker{
				client:           make(map[string]map[chan string]struct{}),
				timeout:          30 * time.Second,
				buffSize:         10,
				maxTopicNameSize: 100,
				maxMsgSize:       1024 * 1024,
			},
		},
		{
			name: "custom options",
			options: []Options{
				WithTimeout(60),
				WithBufferSize(20),
				WithMaxTopicSize(100),
				WithMaxMsgSize(2048),
			},
			expected: &Broker{
				client:           make(map[string]map[chan string]struct{}),
				timeout:          60 * time.Second,
				buffSize:         20,
				maxTopicNameSize: 100,
				maxMsgSize:       2048,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker := NewBrokerWithOptions(tt.options...)
			if broker.timeout != tt.expected.timeout {
				t.Errorf("timeout = %v, want %v", broker.timeout, tt.expected.timeout)
			}
			if broker.buffSize != tt.expected.buffSize {
				t.Errorf("buffSize = %v, want %v", broker.buffSize, tt.expected.buffSize)
			}
			if broker.maxTopicNameSize != tt.expected.maxTopicNameSize {
				t.Errorf("maxTopicNameSize = %v, want %v", broker.maxTopicNameSize, tt.expected.maxTopicNameSize)
			}
			if broker.maxMsgSize != tt.expected.maxMsgSize {
				t.Errorf("maxMsgSize = %v, want %v", broker.maxMsgSize, tt.expected.maxMsgSize)
			}
		})
	}
}

func TestBrokerPublishSubscribe(t *testing.T) {
	broker := NewBrokerWithOptions(
		WithTimeout(1),
		WithBufferSize(2),
	)

	t.Run("subscribe and receive message", func(t *testing.T) {
		topic := "test-topic"
		clientChan := make(chan string, 2)

		broker.subscribe(topic, clientChan)

		message := []byte("test message")
		broker.publish(topic, message)

		select {
		case received := <-clientChan:
			if !strings.Contains(received, "test message") {
				t.Errorf("received message does not contain expected content: %s", received)
			}
			if !strings.Contains(received, "id: 1") {
				t.Errorf("received message does not contain expected message ID: %s", received)
			}
		case <-time.After(time.Second):
			t.Error("timeout waiting for message")
		}
	})

	t.Run("unsubscribe and topic cleanup", func(t *testing.T) {
		topic := "test-topic"
		clientChan := make(chan string, 2)

		broker.subscribe(topic, clientChan)
		broker.unsubscribe(topic, clientChan)

		if _, ok := broker.client[topic]; ok {
			t.Error("topic should be removed after unsubscribe")
		}
	})
}

func TestBrokerHTTPHandlers(t *testing.T) {
	broker := NewBrokerWithOptions(
		WithTimeout(1),
		WithBufferSize(2),
	)

	tests := []struct {
		name           string
		method         string
		path           string
		body           string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "invalid path",
			method:         "GET",
			path:           "/invalid/",
			expectedStatus: http.StatusNotFound,
			expectedBody:   "Not Found: Expected path /infocenter/{topic}",
		},
		{
			name:           "empty topic",
			method:         "GET",
			path:           "/infocenter/",
			expectedStatus: http.StatusNotFound,
			expectedBody:   "Not Found: Expected path /infocenter/{topic}",
		},
		{
			name:           "valid subscription",
			method:         "GET",
			path:           "/infocenter/test-topic",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "valid publish",
			method:         "POST",
			path:           "/infocenter/test-topic",
			body:           "test message",
			expectedStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewBufferString(tt.body))
			w := httptest.NewRecorder()

			broker.ServeHTTP(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			if tt.expectedBody != "" && !strings.Contains(w.Body.String(), tt.expectedBody) {
				t.Errorf("expected body to contain %q, got %q", tt.expectedBody, w.Body.String())
			}
		})
	}
}

func TestBrokerConcurrentOperations(t *testing.T) {
	broker := NewBrokerWithOptions(
		WithTimeout(1),
		WithBufferSize(10),
	)

	t.Run("multiple subscribers and concurrent messages", func(t *testing.T) {
		topic := "test-topic"
		numSubscribers := 5
		subscribers := make([]chan string, numSubscribers)

		for i := 0; i < numSubscribers; i++ {
			subscribers[i] = make(chan string, 10)
			broker.subscribe(topic, subscribers[i])
		}

		numMessages := 10
		done := make(chan bool)

		for i := 0; i < numMessages; i++ {
			go func(msgNum int) {
				message := []byte("test message " + string(rune(msgNum+'0')))
				broker.publish(topic, message)
				done <- true
			}(i)
		}

		for i := 0; i < numMessages; i++ {
			<-done
		}

		for i := 0; i < numSubscribers; i++ {
			receivedCount := 0
			timeout := time.After(time.Second)

			for {
				select {
				case msg := <-subscribers[i]:
					receivedCount++
					if !strings.Contains(msg, "test message") {
						t.Errorf("subscriber %d received unexpected message: %s", i, msg)
					}
				case <-timeout:
					break
				}

				if receivedCount == numMessages {
					break
				}
			}

			if receivedCount != numMessages {
				t.Errorf("subscriber %d received %d messages, expected %d", i, receivedCount, numMessages)
			}

			broker.unsubscribe(topic, subscribers[i])
		}
	})
}

func TestBrokerMessageSizeLimit(t *testing.T) {
	broker := NewBrokerWithOptions(
		WithTimeout(1),
		WithMaxMsgSize(10),
	)

	t.Run("message size handling", func(t *testing.T) {
		topic := "test-topic"
		clientChan := make(chan string, 1)
		broker.subscribe(topic, clientChan)

		smallMessage := []byte("small")
		broker.publish(topic, smallMessage)

		select {
		case msg := <-clientChan:
			if !strings.Contains(msg, "small") {
				t.Error("did not receive small message")
			}
		case <-time.After(time.Second):
			t.Error("timeout waiting for small message")
		}

		largeMessage := []byte("this message is too large")
		broker.publish(topic, largeMessage)

		select {
		case msg := <-clientChan:
			if !strings.Contains(msg, "this message is too large") {
				t.Error("did not receive large message")
			}
		case <-time.After(time.Second):
			t.Error("timeout waiting for large message")
		}
	})
}

func TestBrokerTopicNameSizeLimit(t *testing.T) {
	broker := NewBrokerWithOptions(
		WithTimeout(1),
		WithMaxTopicSize(5),
	)

	t.Run("topic name size validation", func(t *testing.T) {
		validTopic := "test"
		clientChan := make(chan string, 1)
		broker.subscribe(validTopic, clientChan)

		invalidTopic := "too-long-topic-name"
		req := httptest.NewRequest("GET", "/infocenter/"+invalidTopic, nil)
		w := httptest.NewRecorder()

		broker.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
		}

		expectedError := "Topic name too long"
		if !strings.Contains(w.Body.String(), expectedError) {
			t.Errorf("expected error message to contain %q, got %q", expectedError, w.Body.String())
		}
	})
}

func TestBrokerLastEventID(t *testing.T) {
	broker := NewBrokerWithOptions()
	topic := "test-topic"
	lastEventID := "123"

	req := httptest.NewRequest("GET", "/subscribe/"+topic, nil)
	req.Header.Set("Last-Event-ID", lastEventID)

	w := httptest.NewRecorder()

	go broker.handleSubscription(w, req, topic)

	time.Sleep(100 * time.Millisecond)

	response := w.Body.String()
	if !strings.Contains(response, "retry:") {
		t.Error("Expected response to contain retry field")
	}
}

func TestBrokerRetryField(t *testing.T) {
	broker := NewBrokerWithOptions()
	topic := "test-topic"

	req := httptest.NewRequest("GET", "/subscribe/"+topic, nil)
	w := httptest.NewRecorder()

	go broker.handleSubscription(w, req, topic)

	time.Sleep(100 * time.Millisecond)

	response := w.Body.String()
	if !strings.Contains(response, "retry:") {
		t.Error("Expected response to contain retry field")
	}
	
	retryMatch := regexp.MustCompile(`retry: (\d+)`).FindStringSubmatch(response)
	if len(retryMatch) != 2 {
		t.Error("Expected to find retry value in response")
	}
	retryValue, err := strconv.Atoi(retryMatch[1])
	if err != nil {
		t.Errorf("Failed to parse retry value: %v", err)
	}
	if retryValue <= 0 {
		t.Error("Expected retry value to be positive")
	}
}
