package kafka

import (
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

func (p *Producer) Send(msg model.DLQEnvelope) error {
	return nil
}
