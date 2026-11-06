package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-system/streamforge/internal/broker"
	"github.com/distributed-system/streamforge/internal/config"
	"github.com/distributed-system/streamforge/internal/transport/server"
)

func main() {
	cfg := config.ParseFlags()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	b, err := broker.New(cfg, logger)
	if err != nil {
		logger.Error("broker init", "err", err)
		os.Exit(1)
	}
	defer b.Close()

	srv := server.New(b, cfg.ListenAddr, cfg.HTTPAddr, logger)
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		srv.SetTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
	}
	if err := srv.Start(); err != nil {
		logger.Error("server start", "err", err)
		os.Exit(1)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Stop(ctx)
	logger.Info("shutdown complete")
}
