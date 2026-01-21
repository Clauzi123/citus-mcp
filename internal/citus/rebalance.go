package citus

import (
    "context"
    "fmt"

    "github.com/jackc/pgx/v5/pgxpool"
)

type RebalancePlan struct {
    Table string `json:"table"`
    Hint  string `json:"hint"`
}

func PlanRebalanceTable(ctx context.Context, pool *pgxpool.Pool, table string) (*RebalancePlan, error) {
    return &RebalancePlan{Table: table, Hint: "Will call rebalance_table_shards('" + table + "')"}, nil
}

func ExecuteRebalanceTable(ctx context.Context, pool *pgxpool.Pool, table string) error {
    const q = `SELECT rebalance_table_shards($1)`
    if _, err := pool.Exec(ctx, q, table); err != nil {
        return fmt.Errorf("rebalance_table_shards: %w", err)
    }
    return nil
}
