package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	_, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, os.Interrupt)
	defer stop()

}
