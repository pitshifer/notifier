package app

import (
	"context"

	"github.com/pitshifer/notifier/internal/kafka"
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
	return nil
}
