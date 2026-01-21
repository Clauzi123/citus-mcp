package db

import (
	"context"
	"fmt"
	"time"

	"citus-mcp/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// NewPool initializes a pgxpool for the coordinator, applying connection and statement timeouts and app name.
func NewPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.CoordinatorDSN)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	if cfg.CoordinatorUser != "" {
		pcfg.ConnConfig.User = cfg.CoordinatorUser
	}
	if cfg.CoordinatorPassword != "" {
		pcfg.ConnConfig.Password = cfg.CoordinatorPassword
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

// Query executes a query with per-call timeout and max rows enforcement via SQL wrapper.
// It returns rows; caller must Close().
func Query(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger, cfg config.Config, query string, args ...any) (pgx.Rows, error) {
	ctx, cancel := ensureTimeout(ctx, cfg.StatementTimeoutMs)
	defer cancel()
	wrapped := wrapWithLimit(query, cfg.MaxRows)
	start := time.Now()
	rows, err := pool.Query(ctx, wrapped, args...)
	dur := time.Since(start)
	if err != nil {
		logger.Debug("db.query.error", zap.Error(err), zap.Duration("duration", dur), zap.String("query", abbreviate(query, cfg.MaxTextBytes)))
		return nil, err
	}
	logger.Debug("db.query", zap.Duration("duration", dur), zap.String("query", abbreviate(query, cfg.MaxTextBytes)))
	return rows, nil
}

// QueryCollect runs a query and collects rows into a slice of []any, returning field descriptions and row count.
func QueryCollect(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger, cfg config.Config, query string, args ...any) ([][]any, []pgconn.FieldDescription, error) {
	rows, err := Query(ctx, pool, logger, cfg, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	fds := rows.FieldDescriptions()
	var out [][]any
	maxRows := cfg.MaxRows
	if maxRows <= 0 {
		maxRows = 1000
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, err
		}
		out = append(out, values)
		if len(out) >= maxRows {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	logger.Debug("db.query.rows", zap.Int("rows", len(out)))
	return out, fds, nil
}

func ensureTimeout(ctx context.Context, ms int) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	d := time.Duration(ms) * time.Millisecond
	if d <= 0 {
		d = 30 * time.Second
	}
	return context.WithTimeout(ctx, d)
}

func wrapWithLimit(query string, maxRows int) string {
	if maxRows <= 0 {
		return query
	}
	return fmt.Sprintf("SELECT * FROM (%s) AS __mcp__ LIMIT %d", query, maxRows)
}

func abbreviate(s string, max int) string {
	if max <= 0 {
		max = 2000
	}
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
