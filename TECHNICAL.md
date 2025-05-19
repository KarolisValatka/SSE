# Aardvark - Technical Documentation

This document provides a detailed, line-by-line explanation of the Aardvark SSE broker implementation.

## Core Data Structures

### Broker Structure (`pkg/broker/broker.go`)

```go
type Broker struct {
    client           map[string]map[chan string]struct{} // Topic -> Subscribers mapping
    mu               sync.RWMutex                        // Thread safety for client map
    messageID        atomic.Uint64                       // Thread-safe message counter
    timeout          time.Duration                       // Client connection timeout
    maxMsgSize       int64                              // Maximum allowed message size
    buffSize         int                                // Channel buffer size
    maxTopicNameSize int                                // Maximum topic name length
    retryInterval    time.Duration                      // SSE retry interval
}
```

#### Field Explanations:
- `client`: Two-level map structure
  - First level: topic name → subscriber map
  - Second level: channel → empty struct (set-like behavior)
- `mu`: Read-Write mutex for concurrent access
  - Read lock: Used during message publishing (multiple readers)
  - Write lock: Used for subscriber management (single writer)
- `messageID`: Atomic counter for unique message IDs
  - Thread-safe increment operation
  - No possibility of duplicate IDs
- `timeout`: Duration for client connection timeout
  - Default: 30 seconds
  - Configurable via environment variable
- `maxMsgSize`: Maximum allowed message size in bytes
  - Default: 1MB
  - Prevents memory exhaustion
- `buffSize`: Channel buffer size for each subscriber
  - Default: 10 messages
  - Prevents blocking on slow clients
- `maxTopicNameSize`: Maximum allowed topic name length
  - Default: 100 characters
  - Prevents resource exhaustion
- `retryInterval`: SSE retry interval
  - Default: 3 seconds
  - Configurable via environment variable

## Configuration Management (`internal/utils/config.go`)

### Config Structure
```go
type Config struct {
    ClientTimeout       time.Duration // How long to keep idle connections
    MessageBufferSize   int          // Channel buffer size
    MaxTopicNameLength  int          // Maximum topic name length
    MaxMessageSizeBytes int64        // Maximum message size
    RetryInterval       time.Duration // SSE retry interval
}
```

### Default Configuration
```go
func NewDefaultConfig() *Config {
    return &Config{
        ClientTimeout:       30 * time.Second,
        MessageBufferSize:   10,
        MaxTopicNameLength:  100,
        MaxMessageSizeBytes: 1024 * 1024, // 1MB
        RetryInterval:       3 * time.Second,
    }
}
```

### Environment Variable Processing
```go
func NewConfigFromEnv() *Config {
    config := NewDefaultConfig()
    
    // Process each environment variable
    if timeout := os.Getenv("CLIENT_TIMEOUT_IN_SECONDS"); timeout != "" {
        if parsed, err := time.ParseDuration(timeout); err == nil && parsed.Seconds() > 0.0 {
            config.ClientTimeout = parsed
        }
    }
    // ... similar processing for other variables
}
```

## Core Broker Operations

### 1. Subscription Management

#### Subscribe Method
```go
func (b *Broker) subscribe(topic string, clientChan chan string) {
    b.mu.Lock()         // Acquire write lock
    defer b.mu.Unlock() // Ensure lock is released

    // Create topic if it doesn't exist
    if _, ok := b.client[topic]; !ok {
        b.client[topic] = make(map[chan string]struct{})
        log.Printf("Topic '%s' created", topic)
    }

    // Add subscriber
    b.client[topic][clientChan] = struct{}{}
    log.Printf("Client subscribed to topic: %s. Total subscribers: %d", 
        topic, len(b.client[topic]))
}
```

#### Unsubscribe Method
```go
func (b *Broker) unsubscribe(topic string, clientChan chan string) {
    b.mu.Lock()
    defer b.mu.Unlock()

    if chans, ok := b.client[topic]; ok {
        if _, clientOk := chans[clientChan]; clientOk {
            delete(chans, clientChan)    // Remove subscriber
            close(clientChan)            // Close channel
            log.Printf("Client unsubscribed from topic: %s", topic)

            // Clean up empty topics
            if len(chans) == 0 {
                delete(b.client, topic)
                log.Printf("Topic '%s' removed", topic)
            }
        }
    }
}
```

### 2. Message Publishing

#### Publish Method
```go
func (b *Broker) publish(topic string, message []byte) uint64 {
    // 1. Size validation
    if int64(len(message)) > b.maxMsgSize {
        log.Printf("Message size %d exceeds maximum %d", 
            len(message), b.maxMsgSize)
        return 0
    }

    // 2. Generate message ID
    msgID := b.messageID.Add(1)

    // 3. Format SSE message
    var sseMessage bytes.Buffer
    sseMessage.WriteString(fmt.Sprintf("id: %d\n", msgID))
    sseMessage.WriteString("event: msg\n")

    // 4. Process message content
    sanitizeBody := bytes.ReplaceAll(message, []byte("\r\n"), []byte("\n"))
    lines := strings.Split(string(sanitizeBody), "\n")
    for _, line := range lines {
        sseMessage.WriteString(fmt.Sprintf("data: %s\n", line))
    }
    sseMessage.WriteString("\n")

    // 5. Get subscribers (with read lock)
    b.mu.RLock()
    subsForTopic, exists := b.client[topic]
    if !exists || len(subsForTopic) == 0 {
        b.mu.RUnlock()
        return msgID
    }

    // 6. Prepare subscriber list
    tempChan := make([]chan string, 0, len(subsForTopic))
    for ch := range subsForTopic {
        tempChan = append(tempChan, ch)
    }
    b.mu.RUnlock()

    // 7. Deliver message
    for _, ch := range tempChan {
        select {
        case ch <- sseMessage.String():
        default:
            log.Printf("Channel full/closed for topic '%s'", topic)
        }
    }

    return msgID
}
```

### 3. HTTP Request Handling

#### ServeHTTP Method
```go
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // 1. Path validation
    pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
    if len(pathParts) < 2 || pathParts[0] != "infocenter" {
        http.Error(w, "Not Found: Expected path /infocenter/{topic}", 
            http.StatusNotFound)
        return
    }

    // 2. Topic validation
    topic := pathParts[1]
    if topic == "" {
        http.Error(w, "Bad Request: Topic cannot be empty", 
            http.StatusBadRequest)
        return
    }
    if len(topic) > b.maxTopicNameSize {
        http.Error(w, fmt.Sprintf("Bad Request: Topic name too long (max %d)", 
            b.maxTopicNameSize), http.StatusBadRequest)
        return
    }

    // 3. Method routing
    switch r.Method {
    case http.MethodGet:
        b.handleSubscription(w, r, topic)
    case http.MethodPost:
        b.handlePostMessage(w, r, topic)
    default:
        http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
    }
}
```

#### Subscription Handler
```go
func (b *Broker) handleSubscription(w http.ResponseWriter, r *http.Request, topic string) {
    // 1. Flusher check
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "Streaming unsupported", 
            http.StatusInternalServerError)
        return
    }

    // 2. Last-Event-ID handling
    lastEventID := r.Header.Get("Last-Event-ID")
    if lastEventID != "" {
        log.Printf("Client reconnected with Last-Event-ID: %s", lastEventID)
    }

    // 3. SSE headers
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")

    // 4. Retry interval
    fmt.Fprintf(w, "retry: %d\n\n", b.retryInterval.Milliseconds())
    flusher.Flush()

    // 5. Setup subscription
    clientChan := make(chan string, b.buffSize)
    b.subscribe(topic, clientChan)
    defer b.unsubscribe(topic, clientChan)

    // 6. Message forwarding loop
    ctx := r.Context()
    timer := time.NewTimer(b.timeout)
    defer timer.Stop()

    startTime := time.Now()
    for {
        select {
        case <-ctx.Done():
            return
        case msg, open := <-clientChan:
            if !open {
                return
            }
            if _, err := fmt.Fprint(w, msg); err != nil {
                return
            }
            flusher.Flush()
            timer.Reset(b.timeout)
        case <-timer.C:
            durationConnected := time.Since(startTime)
            timeoutEvent := fmt.Sprintf("event: timeout\ndata: %0.fs\n\n", 
                durationConnected.Seconds())
            fmt.Fprint(w, timeoutEvent)
            flusher.Flush()
            return
        }
    }
}
```

#### Message Publishing Handler
```go
func (b *Broker) handlePostMessage(w http.ResponseWriter, r *http.Request, topic string) {
    // 1. Read body with size limit
    body, err := io.ReadAll(io.LimitReader(r.Body, b.maxMsgSize))
    if err != nil {
        http.Error(w, "Error reading request body", 
            http.StatusInternalServerError)
        return
    }
    defer r.Body.Close()

    // 2. Empty message check
    if len(body) == 0 {
        http.Error(w, "Message body cannot be empty", 
            http.StatusBadRequest)
        return
    }

    // 3. Publish message
    msgID := b.publish(topic, body)
    log.Printf("Message (length %d) posted to topic '%s' with ID %d", 
        len(body), topic, msgID)

    // 4. Response
    w.WriteHeader(http.StatusNoContent)
}
```

## Concurrency Patterns

### 1. Mutex Usage
- Read-Write mutex for subscriber management
- Read lock during message publishing
- Write lock during subscription changes
- Prevents race conditions in subscriber map

### 2. Channel Operations
- Non-blocking sends to prevent deadlocks
- Buffered channels for message queuing
- Channel cleanup on unsubscribe
- Timeout handling with select

### 3. Atomic Operations
- Message ID generation
- Thread-safe counter increments
- No possibility of duplicate IDs

## Error Handling

### 1. Input Validation
- Topic name length
- Message size
- HTTP method
- Path format
- Empty messages

### 2. Connection Management
- Timeout handling
- Client disconnection
- Channel full/closed states
- Write errors

### 3. Resource Cleanup
- Channel closing
- Topic removal
- Timer stopping
- Body closing

## Performance Optimizations

### 1. Memory Management
- Buffered channels
- Message size limits
- Topic name limits
- Efficient string handling

### 2. Concurrency
- Read-Write mutex
- Non-blocking operations
- Efficient subscriber list copying
- Atomic operations

### 3. Network
- Keep-alive connections
- Proper SSE headers
- Efficient message formatting
- Timeout handling 