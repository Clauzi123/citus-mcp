package citus

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type RebalancePlan struct {
	Table string `json:"table"`
	Hint  string `json:"hint"`
}

type RebalanceMove struct {
	TableName  string `json:"table_name"`
	ShardID    int64  `json:"shard_id"`
	ShardSize  int64  `json:"shard_size"`
	SourceName string `json:"source_name"`
	SourcePort int32  `json:"source_port"`
	TargetName string `json:"target_name"`
	TargetPort int32  `json:"target_port"`
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

// StartRebalance uses citus_rebalance_start (if available) to start a cluster/table rebalance.
func StartRebalance(ctx context.Context, pool *pgxpool.Pool, table *string, threshold *float64, maxMoves *int, excludedShards []int64, drainOnly *bool) error {
	var args [5]any
	if table != nil && *table != "" {
		args[0] = *table
	}
	if threshold != nil {
		args[1] = *threshold
	}
	if maxMoves != nil {
		args[2] = *maxMoves
	}
	if len(excludedShards) > 0 {
		args[3] = excludedShards
	} else {
		args[3] = []int64{}
	}
	if drainOnly != nil {
		args[4] = *drainOnly
	}
	const q = `SELECT citus_rebalance_start($1,$2,$3,$4,$5)`
	_, err := pool.Exec(ctx, q, args[0], args[1], args[2], args[3], args[4])
	return err
}

// IsRebalanceRunning best-effort checks if a rebalance is running.
func IsRebalanceRunning(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	rows, err := pool.Query(ctx, "SELECT * FROM citus_rebalance_status()")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), nil
}

// GetRebalancePlan calls get_rebalance_table_shards_plan when available.
func GetRebalancePlan(ctx context.Context, pool *pgxpool.Pool, table *string, threshold *float64, maxMoves *int, excludedShards []int64, drainOnly *bool) ([]RebalanceMove, error) {
	var args [5]any
	if table != nil && *table != "" {
		args[0] = *table
	}
	if threshold != nil {
		args[1] = *threshold
	}
	if maxMoves != nil {
		args[2] = *maxMoves
	}
	if len(excludedShards) > 0 {
		args[3] = excludedShards
	} else {
		args[3] = []int64{} // empty array instead of NULL
	}
	if drainOnly != nil {
		args[4] = *drainOnly
	} else {
		args[4] = false // default to false instead of NULL
	}

	const q = `SELECT table_name, shardid, shard_size, sourcename, sourceport, targetname, targetport FROM get_rebalance_table_shards_plan($1,$2,$3,$4,$5)`
	rows, err := pool.Query(ctx, q, args[0], args[1], args[2], args[3], args[4])
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var moves []RebalanceMove
	for rows.Next() {
		var m RebalanceMove
		if err := rows.Scan(&m.TableName, &m.ShardID, &m.ShardSize, &m.SourceName, &m.SourcePort, &m.TargetName, &m.TargetPort); err != nil {
			return nil, err
		}
		moves = append(moves, m)
	}
	return moves, rows.Err()
}

// HasReplicaIdentityDefault returns true if table replica identity is DEFAULT (none).
func HasReplicaIdentityDefault(ctx context.Context, pool *pgxpool.Pool, table string) (bool, error) {
	schema, rel, err := parseSchemaTable(table)
	if err != nil {
		return false, err
	}
	const q = `SELECT c.relreplident FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = $1 AND c.relname = $2`
	var replIdent string
	if err := pool.QueryRow(ctx, q, schema, rel).Scan(&replIdent); err != nil {
		return false, err
	}
	return replIdent == "n", nil // 'n' = DEFAULT
}

func parseSchemaTable(s string) (string, string, error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid table: %s", s)
	}
	return parts[0], parts[1], nil
}
