package main

import (
    "context"
    "os"

    "citus-mcp/internal/config"
    "citus-mcp/internal/db"
    "citus-mcp/internal/logging"
    "citus-mcp/internal/mcpserver"
    "go.uber.org/zap"
)

func main() {
    ctx := context.Background()

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

    srv, err := mcpserver.New(ctx, cfg, logger, pool)
    if err != nil {
        logger.Fatal("failed to create server", zap.Error(err))
    }

    if err := srv.Run(ctx); err != nil {
        logger.Error("server exited with error", zap.Error(err))
        os.Exit(1)
    }
}
