# SSE broker code guide

What every folder and file in this repo does. Configuration and usage examples are in [../../README.md](../README.md). This page describes the code.

Written 2026-11-16, guide sections added 2026-11-17, against branch `main`.

## What the repo is

A message broker over Server-Sent Events, in Go with the standard library only. A client subscribes to a topic by opening a long lived GET request, and anyone can publish to that topic with a POST. Every subscriber of the topic receives the message as an event.

```
POST /infocenter/my-topic  -d "message"     publish
GET  /infocenter/my-topic                   subscribe, the response stays open
```

## Files

| Path | Lines | What |
|---|---|---|
| `cmd/main.go` | 52 | Builds a broker and serves it over HTTP |
| `pkg/broker/broker.go` | 333 | The broker |
| `pkg/broker/broker_test.go` | 346 | Its tests |
| `internal/utils/config.go` | 68 | Reads the settings from environment variables |
| `aardvark.drawio` | | A diagram |
| `README.md` | | Options, environment variables, curl examples |

## pkg/broker/broker.go

`Broker` implements `http.Handler`. `ServeHTTP` takes the topic from the path and branches on the method: GET subscribes, POST publishes.

- Subscribing sets the `text/event-stream` content type, sends a `retry:` line telling the client how long to wait before reconnecting, and then writes each message as an event. The `Last-Event-ID` header of a reconnecting client is read and logged.
- Each subscriber has a buffered channel of messages and a timer. Every message resets the timer. When the topic stays quiet for the whole timeout, the broker sends a `timeout` event with how long the client was connected, and ends the connection. A subscriber that goes away is removed, and a topic with no subscribers left is deleted.
- Publishing reads the body, refuses a message over the size limit or a topic name over the length limit, gives the message an id, and sends it to every subscriber of the topic without blocking. A subscriber whose buffer is full does not get that message, and the drop is logged.

A broker is made in one of two ways:

- `NewBrokerWithOptions(...)` with functional options: `WithTimeout`, `WithBufferSize`, `WithMaxTopicSize`, `WithMaxMsgSize`, `WithRetryInterval`.
- `NewBrokerFromEnv()`, which reads `CLIENT_TIMEOUT_IN_SECONDS`, `MESSAGE_BUFFER_SIZE`, `MAX_TOPIC_NAME_LENGTH`, `MAX_MESSAGE_SIZE_BYTES` and `RETRY_INTERVAL_IN_SECONDS` through `internal/utils/config.go`.

The README states the code follows Uber's Go style guide.

## Start here

`cmd/main.go` (how the broker is set up), then `pkg/broker/broker.go` from `ServeHTTP` down.

## Build and run

`go run ./cmd` starts the server on :8080. Subscribe with `curl -N localhost:8080/infocenter/news`, publish with `curl -d hello localhost:8080/infocenter/news`. `go test ./...` runs the broker tests.

## Things to know

- `WithTimeout` multiplies its value by `time.Second`, but `main` passes `30 * time.Second`, so the timeout becomes about 950 years and quiet clients are never dropped.
- `main` passes `WithRetryInterval(10)`, ten nanoseconds, so the `retry:` hint sent to clients is 0 milliseconds.
- The Go module is named `Aardvark` while the repo is `SSE`.
