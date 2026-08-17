package kafka

import (
	"context"
	"encoding/json"

	"github.com/pitshifer/notifier/internal/model"
	kfk "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kfk.Writer
}

func NewProducer(cfg Config) *Producer {
	return &Producer{
		writer: &kfk.Writer{
			Addr:                   kfk.TCP(cfg.Brokers...),
			Topic:                  cfg.TopicDLQ,
			MaxAttempts:            3,
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *Producer) Send(ctx context.Context, msg model.DLQEnvelope) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(ctx, kfk.Message{
		Key:   []byte(msg.OriginalKey),
		Value: payload,
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
