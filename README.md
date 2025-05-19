# Aardvark - Server-Sent Events (SSE) Broker

Aardvark is a high-performance, concurrent Server-Sent Events (SSE) broker written in Go. It provides real-time message delivery through HTTP endpoints, supporting topic-based message routing and concurrent message handling.

## System Architecture

### Project Structure
```
Aardvark/
├── cmd/
│   └── main.go         # Application entry point and HTTP server setup
├── internal/
│   └── utils/
│       └── config.go   # Configuration management and environment variables
└── pkg/
    └── broker/
        ├── broker.go   # Core broker implementation
        └── broker_test.go # Comprehensive test suite
```

### Core Components

1. **Broker (`pkg/broker/broker.go`)**
   - Central component managing message routing and client connections
   - Thread-safe subscriber management
   - Message publishing and delivery
   - SSE protocol implementation

2. **Configuration (`internal/utils/config.go`)**
   - System-wide configuration management
   - Environment variable support
   - Default values and validation

3. **HTTP Server (`cmd/main.go`)**
   - HTTP endpoint setup
   - Request routing
   - Server configuration

## Detailed Request Flow

### 1. Subscription Request Flow

When a client connects to subscribe to a topic:

```
Client Request → HTTP Server → Broker → Subscription Handler
```

#### Step-by-Step Process:

1. **Initial Request**
   ```http
   GET /infocenter/my-topic HTTP/1.1
   Host: localhost:8080
   Connection: keep-alive
   ```

2. **Path Validation**
   - Checks if path starts with `/infocenter/`
   - Validates topic name presence
   - Returns 404 if path is invalid

3. **Topic Name Validation**
   - Checks topic name length against `maxTopicNameSize` (default: 100)
   - Returns 400 if topic name is too long
   - Validates topic name is not empty

4. **SSE Connection Setup**
   - Sets required SSE headers:
     ```http
     Content-Type: text/event-stream
     Cache-Control: no-cache
     Connection: keep-alive
     ```
   - Configures retry interval
   - Creates client message channel

5. **Subscription Registration**
   - Creates topic if it doesn't exist
   - Adds client channel to topic subscribers
   - Logs subscription details

6. **Message Forwarding Loop**
   - Continuously monitors for:
     - New messages
     - Client disconnection
     - Timeout events
   - Forwards messages to client
   - Handles connection cleanup

### 2. Publishing Request Flow

When a client publishes a message:

```
Client Request → HTTP Server → Broker → Message Publishing → Subscriber Delivery
```

#### Step-by-Step Process:

1. **Initial Request**
   ```http
   POST /infocenter/my-topic HTTP/1.1
   Host: localhost:8080
   Content-Type: text/plain
   Content-Length: 13

   Hello, World!
   ```

2. **Request Validation**
   - Validates HTTP method (must be POST)
   - Checks path format
   - Validates topic name

3. **Message Processing**
   - Reads request body with size limit
   - Validates message size against `maxMsgSize`
   - Generates unique message ID

4. **SSE Message Formatting**
   ```
   id: <message_id>
   event: msg
   data: <message_content>
   ```

5. **Message Delivery**
   - Acquires read lock for thread safety
   - Checks for topic subscribers
   - Forwards message to all subscribers
   - Handles full channels and closed connections

## Configuration Details

### Environment Variables

1. **CLIENT_TIMEOUT_IN_SECONDS**
   - Default: 30
   - Purpose: How long to keep idle connections alive
   - Impact: Affects server resource usage

2. **MESSAGE_BUFFER_SIZE**
   - Default: 10
   - Purpose: Channel buffer size for each subscriber
   - Impact: Memory usage and message delivery reliability

3. **MAX_TOPIC_NAME_LENGTH**
   - Default: 100
   - Purpose: Maximum allowed topic name length
   - Impact: Prevents resource exhaustion

4. **MAX_MESSAGE_SIZE_BYTES**
   - Default: 1MB (1024 * 1024)
   - Purpose: Maximum allowed message size
   - Impact: Memory usage and DoS protection

5. **RETRY_INTERVAL_IN_SECONDS**
   - Default: 3
   - Purpose: SSE retry interval in milliseconds
   - Impact: Reconnection behavior

## Concurrency Model

### Thread Safety

1. **Subscriber Management**
   - Uses `sync.RWMutex` for thread-safe operations
   - Read lock for message publishing
   - Write lock for subscriber modifications

2. **Message ID Generation**
   - Uses `atomic.Uint64` for thread-safe increments
   - Ensures unique message IDs across concurrent publishes

3. **Message Delivery**
   - Non-blocking channel operations
   - Handles full channels gracefully
   - Cleans up closed connections

## Error Handling

### Common Scenarios

1. **Invalid Requests**
   - 404: Invalid path format
   - 400: Invalid topic name
   - 405: Invalid HTTP method

2. **Resource Limits**
   - Message size exceeded
   - Topic name too long
   - Channel buffer full

3. **Connection Issues**
   - Client disconnection
   - Timeout events
   - Write errors

## Testing

### Test Coverage

1. **Unit Tests**
   - Broker initialization
   - Message publishing
   - Subscription management
   - HTTP handlers

2. **Integration Tests**
   - Concurrent operations
   - Message delivery
   - Error handling
   - SSE compliance

3. **Edge Cases**
   - Large messages
   - Long topic names
   - Connection timeouts
   - Reconnection handling

## Usage Examples

### Subscribing to a Topic

```bash
# Basic subscription
curl -N http://localhost:8080/infocenter/my-topic

# With Last-Event-ID
curl -N -H "Last-Event-ID: 123" http://localhost:8080/infocenter/my-topic
```

### Publishing Messages

```bash
# Simple message
curl -X POST http://localhost:8080/infocenter/my-topic -d "Hello, World!"

# JSON message
curl -X POST http://localhost:8080/infocenter/my-topic \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello, World!"}'
```

## Performance Considerations

1. **Memory Usage**
   - Channel buffers per subscriber
   - Message size limits
   - Topic name length limits

2. **Connection Management**
   - Timeout handling
   - Resource cleanup
   - Connection limits

3. **Message Delivery**
   - Non-blocking operations
   - Buffer management
   - Error recovery

## Security Considerations

1. **Input Validation**
   - Topic name sanitization
   - Message size limits
   - Path validation

2. **Resource Protection**
   - Connection timeouts
   - Buffer size limits
   - Message size limits

3. **Error Handling**
   - Graceful degradation
   - Resource cleanup
   - Error logging

# Style
To navigate this project, I am choosing to use uber-go standard

