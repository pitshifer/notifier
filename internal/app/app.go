package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/pitshifer/notifier/internal/kafka"
	"github.com/pitshifer/notifier/internal/model"
	"github.com/pitshifer/notifier/internal/tg"
	"go.etcd.io/bbolt"
)

type Service struct {
	consumer *kafka.Consumer
	producer *kafka.Producer
	tgClient *tg.Client
	dedup    dedup
}

type dedup struct {
	db         *bbolt.DB
	bucketName string
}

func New(consumer *kafka.Consumer, producer *kafka.Producer, tgClient *tg.Client, db *bbolt.DB) *Service {
	return &Service{
		consumer: consumer,
		producer: producer,
		tgClient: tgClient,
		dedup: dedup{
			db:         db,
			bucketName: "processed",
		},
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

		dedupKey := fmt.Sprintf("%d:%d", msg.Partition, msg.Offset)
		done, err := s.dedup.isProcessed(dedupKey)
		if done {
			slog.Info("already processed, skipping send", "offset", msg.Offset)
			s.consumer.Commit(ctx, msg)
			continue
		}
		if err != nil {
			slog.Error("dedup key is not saved", "error", err)
		}

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
				slog.Error("failed to send message to DQL queque", "message", string(msg.Value), "offset", msg.Offset, "error", dlqErr)
				continue
			}
			slog.Warn("message routed to DLQ", "offset", msg.Offset)
		} else {
			slog.Info("notification sent", "offset", msg.Offset)
		}

		err = s.dedup.MarkProcessed(dedupKey)
		if err != nil {
			slog.Error("failed to mark processed", "offset", msg.Offset, "error", err)
		}

		if err := s.consumer.Commit(ctx, msg); err != nil {
			slog.Error("commit failed", "offset", msg.Offset, "error", err)
			continue
		}
		slog.Info("offset commited", "offset", msg.Offset)
	}
}

func (d *dedup) isProcessed(key string) (bool, error) {
	var found bool
	err := d.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(d.bucketName))
		found = b != nil && b.Get([]byte(key)) != nil
		return nil
	})
	return found, err
}

func (d *dedup) MarkProcessed(key string) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(d.bucketName))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), nil)
	})
}
