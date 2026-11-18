// Package utils holds the broker's settings and reads them from the environment.
package utils

import (
	"os"
	"strconv"
	"time"
)

// Config is the broker's limits and timings.
type Config struct {
	ClientTimeout       time.Duration
	MessageBufferSize   int
	MaxTopicNameLength  int
	MaxMessageSizeBytes int64
	RetryInterval       time.Duration
}

// NewDefaultConfig is 30 seconds of quiet before disconnecting, 10 buffered messages per
// client, topic names up to 100 characters, messages up to 1 MB, and a 3 second
// reconnect hint.
func NewDefaultConfig() *Config {
	return &Config{
		ClientTimeout:       30 * time.Second,
		MessageBufferSize:   10,
		MaxTopicNameLength:  100,
		MaxMessageSizeBytes: 1024 * 1024,
		RetryInterval:       3 * time.Second,
	}
}

// NewConfigFromEnv starts from the defaults and overrides each value set in the
// environment (CLIENT_TIMEOUT_IN_SECONDS and the others); bad values are ignored.
func NewConfigFromEnv() *Config {
	config := NewDefaultConfig()

	if timeout := os.Getenv("CLIENT_TIMEOUT_IN_SECONDS"); timeout != "" {
		if parsed, err := strconv.ParseInt(timeout, 10, 64); err == nil && parsed > 0 {
			config.ClientTimeout = time.Duration(parsed) * time.Second
		}
	}

	if buffSize := os.Getenv("MESSAGE_BUFFER_SIZE"); buffSize != "" {
		if parsed, err := strconv.Atoi(buffSize); err == nil && parsed > 0 {
			config.MessageBufferSize = parsed
		}
	}

	if maxTopicLength := os.Getenv("MAX_TOPIC_NAME_LENGTH"); maxTopicLength != "" {
		if parsed, err := strconv.Atoi(maxTopicLength); err == nil && parsed > 0 {
			config.MaxTopicNameLength = parsed
		}
	}

	if maxMessageSize := os.Getenv("MAX_MESSAGE_SIZE_BYTES"); maxMessageSize != "" {
		if parsed, err := strconv.ParseInt(maxMessageSize, 10, 64); err == nil && parsed > 0 {
			config.MaxMessageSizeBytes = parsed
		}
	}

	if retryInterval := os.Getenv("RETRY_INTERVAL_IN_SECONDS"); retryInterval != "" {
		if parsed, err := strconv.ParseInt(retryInterval, 10, 64); err == nil && parsed > 0 {
			config.RetryInterval = time.Duration(parsed) * time.Second
		}
	}

	return config
}
