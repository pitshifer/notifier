package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/pitshifer/notifier/internal/model"
	"github.com/pitshifer/notifier/internal/tg"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

func NewConsumer(config Config) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: config.Brokers,
			Topic:   config.Topic,
			GroupID: config.GroupID,
		}),
	}
}

func (c *Consumer) Run(ctx context.Context, tgClient *tg.Client) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			slog.Error("read failed", "error", err)
			continue
		}
		slog.Info("message received", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key), "value", string(msg.Value))

		if err := c.Commit(ctx, msg); err != nil {
			slog.Error("commit failed", "offset", msg.Offset, "error", err)
			continue
		}
		slog.Info("offset commited", "offset", msg.Offset)

		var alert model.VolatilityAlert
		if err := json.Unmarshal(msg.Value, &alert); err != nil {
			slog.Error("parse failed", "offset", msg.Offset, "error", err)
			continue
		}

		if err := tgClient.Send(ctx, formatAlert(alert)); err != nil {
			slog.Error("telegram send failed - message lost", "offset", msg.Offset, "error", err)
			continue
		}

		slog.Info("notification sent", "offset", msg.Offset)
	}
}

func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func formatAlert(a model.VolatilityAlert) string {
	return fmt.Sprintf("⚠ %s\nvolatility: %.2f%% (threshold: %.2f%%)\n%s",
		strings.ToUpper(a.Symbol), a.Volatility*100, a.Threshold*100,
		a.Timestamp.Format(time.RFC3339))
}
