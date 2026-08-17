package kafka

import (
	"context"

	kfk "github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kfk.Reader
}

type Config struct {
	Brokers  []string
	Topic    string
	TopicDLQ string
	GroupID  string
}

func NewConsumer(config Config) *Consumer {
	return &Consumer{
		reader: kfk.NewReader(kfk.ReaderConfig{
			Brokers: config.Brokers,
			Topic:   config.Topic,
			GroupID: config.GroupID,
		}),
	}
}

func (c *Consumer) FetchMessage(ctx context.Context) (kfk.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) Commit(ctx context.Context, msg kfk.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
