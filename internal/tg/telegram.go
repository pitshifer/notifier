package tg

import "time"

type Client struct {
}

type Config struct {
	Token      string
	ChatID     int64
	MaxRetries int
	BaseDelay  time.Duration
}
