package utils

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	ClientTimeout       time.Duration
	MessageBufferSize   int
	MaxTopicNameLength  int
	MaxMessageSizeBytes int64
}

func NewDefaultConfig() *Config {
	return &Config{
		ClientTimeout:       30 * time.Second,
		MessageBufferSize:   10,
		MaxTopicNameLength:  100,
		MaxMessageSizeBytes: 1024 * 1024,
	}
}

func NewConfigFromEnv() *Config {
	config := NewDefaultConfig()

	if timeout := os.Getenv("CLIENT_TIMEOUT_IN_SECONDS"); timeout != "" {
		if parsed, err := time.ParseDuration(timeout); err == nil && parsed.Seconds() > 0.0 {
			config.ClientTimeout = parsed
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

	return config
}
