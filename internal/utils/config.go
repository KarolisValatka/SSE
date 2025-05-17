package utils

import (
	"os"
	"strconv"
)

type Config struct {
	Port                string
	MessageBufferSize   int
	MaxTopicNameLength  int
	MaxMessageSizeBytes int64
}

func NewDefaultConfig() *Config {
	return &Config{
		Port:                "8080",
		MessageBufferSize:   10,
		MaxTopicNameLength:  100,
		MaxMessageSizeBytes: 1024 * 1024,
	}
}

func NewConfigFromEnv() *Config {
	config := NewDefaultConfig()

	if port := os.Getenv("PORT"); port != "" {
		config.Port = port
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
