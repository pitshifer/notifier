package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pitshifer/notifier/internal/app"
	"github.com/pitshifer/notifier/internal/config"
	kafka "github.com/pitshifer/notifier/internal/kafka"
	"github.com/pitshifer/notifier/internal/tg"
	"go.etcd.io/bbolt"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	log.Println("config loaded")

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

	// bbolt storage
	dedupDB, err := bbolt.Open(cfg.DedupDBPath, 0600, nil)
	if err != nil {
		slog.Error("failed to open dedup db", "error", err)
		os.Exit(1)
	}
	defer dedupDB.Close()

	// Kafka consumer
	kafkaConsumer := kafka.NewConsumer(cfg.Kafka)
	defer kafkaConsumer.Close()
	slog.Info("Kafka consumer created")

	// Kafka producer
	kafkaProducer := kafka.NewProducer(cfg.Kafka)
	defer kafkaProducer.Close()
	slog.Info("Kafka producer created")

	app := app.New(kafkaConsumer, kafkaProducer, tgClient, dedupDB)
	go app.Run(ctx)
	slog.Info("Application started")

	slog.Info("notifier started")

	<-ctx.Done()
	slog.Info("notifier stopped")
}
