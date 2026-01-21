package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"citus-mcp/internal/config"
	"citus-mcp/internal/db"
	"citus-mcp/internal/logging"
	"citus-mcp/internal/mcpserver"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

const (
	serverName    = "citus-mcp"
	serverVersion = "0.1.0"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		// fallback logger
		zap.NewExample().Fatal("failed to load config", zap.Error(err))
	}
	logger, err := logging.NewLogger(cfg.Logging)
	if err != nil {
		zap.NewExample().Fatal("failed to init logger", zap.Error(err))
	}
	defer logger.Sync()

	pool, err := db.NewPool(ctx, cfg.DB, logger)
	if err != nil {
		logger.Fatal("failed to connect pool", zap.Error(err))
	}
	defer pool.Close()

	impl := &mcp.Implementation{Name: serverName, Version: serverVersion}
	srv, err := mcpserver.New(ctx, impl, cfg, logger, pool)
	if err != nil {
		logger.Fatal("failed to create server", zap.Error(err))
	}
	transport := &mcp.StdioTransport{}
	logger.Info("starting citus-mcp server", zap.String("name", serverName), zap.String("version", serverVersion))
	if err := srv.Run(ctx, transport); err != nil {
		logger.Error("server exited with error", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("server stopped")
}
