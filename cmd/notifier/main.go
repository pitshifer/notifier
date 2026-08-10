package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pitshifer/notifier/internal/config"
	"github.com/pitshifer/notifier/internal/consumer"
	"github.com/pitshifer/notifier/internal/tg"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	slog.Info("config loaded")

	loggerHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	})
	logger := slog.New(loggerHandler)
	slog.SetDefault(logger)

	// Telegram bot
	tgClient, err := tg.NewClient(cfg.Telegram)
	if err != nil {
		slog.Error("failed to create Telegram client", "error", err)
		os.Exit(1)
	}
	slog.Info("Telegram client created")

	// Kafka consumer
	kafkaConsumer := consumer.NewConsumer(cfg.KafkaConsumer)
	defer kafkaConsumer.Close()
	go kafkaConsumer.Run(ctx, tgClient)
	slog.Info("Kafka consumer started")

	slog.Info("notifier started")

	<-ctx.Done()
	slog.Info("notifier stopped")
}
