package db

import (
    "context"
    "fmt"
    "time"

    "citus-mcp/internal/config"
    "go.uber.org/zap"
    "github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, cfg config.DBConfig, logger *zap.Logger) (*pgxpool.Pool, error) {
    pcfg, err := pgxpool.ParseConfig(cfg.URL)
    if err != nil {
        return nil, fmt.Errorf("parse pool config: %w", err)
    }
    if cfg.MaxConns > 0 {
        pcfg.MaxConns = cfg.MaxConns
    }
    if cfg.MinConns > 0 {
        pcfg.MinConns = cfg.MinConns
    }
    if cfg.HealthCheck > 0 {
        pcfg.HealthCheckPeriod = cfg.HealthCheck
    } else {
        pcfg.HealthCheckPeriod = 30 * time.Second
    }
    pcfg.ConnConfig.Tracer = nil
    pool, err := pgxpool.NewWithConfig(ctx, pcfg)
    if err != nil {
        return nil, fmt.Errorf("pgxpool new: %w", err)
    }
    return pool, nil
}
