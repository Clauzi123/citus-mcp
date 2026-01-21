package citus

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DistributedTable represents metadata about a distributed table.
type DistributedTable struct {
	LogicalRelID string `json:"logical_relid"`
	PartMethod   string `json:"part_method"`
	ColocationID int32  `json:"colocation_id"`
	Replication  string `json:"replication_model"`
	ShardCount   int32  `json:"shard_count"`
}

func ListDistributedTables(ctx context.Context, pool *pgxpool.Pool) ([]DistributedTable, error) {
	const q = `SELECT p.logicalrelid::text, p.partmethod::text, p.colocationid, COUNT(s.shardid)::int, 'na' AS replication_model
		FROM pg_dist_partition p
		LEFT JOIN pg_dist_shard s ON p.logicalrelid = s.logicalrelid
		GROUP BY p.logicalrelid, p.partmethod, p.colocationid
		ORDER BY p.logicalrelid::text`
	rows, err := pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []DistributedTable
	for rows.Next() {
		var t DistributedTable
		if err := rows.Scan(&t.LogicalRelID, &t.PartMethod, &t.ColocationID, &t.ShardCount, &t.Replication); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}
