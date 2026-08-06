package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pitshifer/notifier/internal/consumer"
	"github.com/pitshifer/notifier/internal/tg"
)

type Config struct {
	KafkaConsumer consumer.Config
	Telegram      tg.Config
}

func Load() (*Config, error) {
	kafkaBrokers, err := getEnv("KAFKA_BROKERS", func(s string) ([]string, error) {
		return strings.Split(s, ","), nil
	})
	if err != nil {
		return nil, err
	}

	kafkaTopic, err := getEnv("KAFKA_TOPIC", identity)
	if err != nil {
		return nil, err
	}

	kafkaGroupID, err := getEnv("KAFKA_GROUP_ID", identity)
	if err != nil {
		return nil, err
	}

	tgToken, err := getEnv("TELEGRAM_TOKEN", identity)
	if err != nil {
		return nil, err
	}

	tgChatID, err := getEnv("TELEGRAM_CHAT_ID", func(s string) (int64, error) {
		return strconv.ParseInt(s, 10, 64)
	})
	if err != nil {
		return nil, err
	}

	tgMaxRetries, err := getEnv("TELEGRAM_MAX_RETRIES", func(s string) (int, error) {
		return strconv.Atoi(s)
	})
	if err != nil {
		return nil, err
	}

	tgBaseDelay, err := getEnv("TELEGRAM_BASE_DELAY", func(s string) (time.Duration, error) {
		seconds, err := strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
		delay := time.Duration(seconds) * time.Second
		return delay, nil
	})
	if err != nil {
		return nil, err
	}

	return &Config{
		KafkaConsumer: consumer.Config{
			Brokers: kafkaBrokers,
			Topic:   kafkaTopic,
			GroupID: kafkaGroupID,
		},
		Telegram: tg.Config{
			Token:      tgToken,
			ChatID:     tgChatID,
			MaxRetries: tgMaxRetries,
			BaseDelay:  tgBaseDelay,
		},
	}, nil
}

func getEnv[T any](key string, parse func(string) (T, error)) (T, error) {
	var zero T

	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return zero, fmt.Errorf("%s is not set", key)
	}

	v, err := parse(raw)
	if err != nil {
		return zero, fmt.Errorf("failed to parse %s: %v", key, err)
	}

	return v, nil
}

func identity(s string) (string, error) {
	return s, nil
}
