package db

import (
	"context"
	"fmt"
	"time"

	"citus-mcp/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.CoordinatorDSN)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	pcfg.ConnConfig.ConnectTimeout = time.Duration(cfg.ConnectTimeoutSeconds) * time.Second
	if pcfg.ConnConfig.RuntimeParams == nil {
		pcfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	pcfg.ConnConfig.RuntimeParams["application_name"] = cfg.AppName
	pcfg.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", cfg.StatementTimeoutMs)

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool new: %w", err)
	}
	return pool, nil
}
