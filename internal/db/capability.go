package db

import (
    "context"

    "github.com/jackc/pgx/v5/pgxpool"
)

// Capabilities represent optional UDFs/features available in Citus.
type Capabilities struct {
    HasGetActiveWorkerNodes bool `json:"has_get_active_worker_nodes"`
    HasRebalanceTableShards bool `json:"has_rebalance_table_shards"`
    HasMoveShardPlacement   bool `json:"has_move_shard_placement"`
}

func DetectCapabilities(ctx context.Context, exec func(context.Context, string) (bool, error)) (*Capabilities, error) {
    check := func(fn string) (bool, error) {
        return exec(ctx, "SELECT to_regproc('"+fn+"') IS NOT NULL")
    }
    getActive, err := check("citus_get_active_worker_nodes")
    if err != nil { return nil, err }
    rebalance, err := check("rebalance_table_shards")
    if err != nil { return nil, err }
    move, err := check("citus_move_shard_placement")
    if err != nil { return nil, err }
    return &Capabilities{
        HasGetActiveWorkerNodes: getActive,
        HasRebalanceTableShards: rebalance,
        HasMoveShardPlacement:   move,
    }, nil
}

func DetectCapabilitiesWithPool(ctx context.Context, pool *pgxpool.Pool) (*Capabilities, error) {
    return DetectCapabilities(ctx, func(ctx context.Context, q string) (bool, error) {
        var ok bool
        if err := pool.QueryRow(ctx, q).Scan(&ok); err != nil {
            return false, err
        }
        return ok, nil
    })
}
