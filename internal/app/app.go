package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/pitshifer/notifier/internal/kafka"
	"github.com/pitshifer/notifier/internal/model"
	"github.com/pitshifer/notifier/internal/tg"
)

type Service struct {
	consumer *kafka.Consumer
	producer *kafka.Producer
	tgClient *tg.Client
}

func New(consumer *kafka.Consumer, producer *kafka.Producer, tgClient *tg.Client) *Service {
	return &Service{
		consumer: consumer,
		producer: producer,
		tgClient: tgClient,
	}
}

func (s *Service) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := s.consumer.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			slog.Error("read failed", "error", err)
			continue
		}
		slog.Info("message received", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key), "value", string(msg.Value))

		// send alert to telegram
		var alert model.VolatilityAlert
		if err := json.Unmarshal(msg.Value, &alert); err != nil {
			slog.Error("parse failed", "offset", msg.Offset, "error", err)
			continue
		}
		if err := s.tgClient.Send(ctx, alert.FormatAlert()); err != nil {
			slog.Error("telegram send failed", "offset", msg.Offset, "error", err)

			dlqEnvelope := model.NewDLQEnvelope(msg.Value, msg.Offset, string(msg.Key), err.Error())
			dlqErr := s.producer.Send(ctx, dlqEnvelope)
			if dlqErr != nil {
				slog.Error("failed to send message to DQL queque", "message", msg.Value, "offset", msg.Offset, "error", dlqErr)
				continue
			}
			slog.Warn("message routed to DLQ", "offset", msg.Offset)
		} else {
			slog.Info("notification sent", "offset", msg.Offset)
		}

		if err := s.consumer.Commit(ctx, msg); err != nil {
			slog.Error("commit failed", "offset", msg.Offset, "error", err)
			continue
		}
		slog.Info("offset commited", "offset", msg.Offset)
	}
}
