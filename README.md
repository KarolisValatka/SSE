# Aardvark - Server-Sent Events (SSE) Broker

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

# Style
To navigate this project, I am choosing to use uber-go standard

