# Server-Sent Events (SSE) Broker

You can create broker with either env variables, or programmatically

### Programmatically
```go
broker.NewBrokerWithOptions(
    broker.WithTimeout(30),
    broker.WithBufferSize(10),
    broker.WithMaxTopicSize(50),
    broker.WithMaxMsgSize(1024*1024),
    broker.WithRetryInterval(10),
)
```

### Env variables
```bash
export CLIENT_TIMEOUT_IN_SECONDS=30
export MESSAGE_BUFFER_SIZE=10
export MAX_TOPIC_NAME_LENGTH=100
export MAX_MESSAGE_SIZE_BYTES=1048576
export RETRY_INTERVAL_IN_SECONDS=3
```
```go
broker.NewBrokerFromEnv()
```

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
curl -X POST http://localhost:8080/infocenter/my-topic -d "message"
```

# Style
To navigate this project, I am choosing to use uber-go standard

