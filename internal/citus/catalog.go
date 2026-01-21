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
}

func ListDistributedTables(ctx context.Context, pool *pgxpool.Pool) ([]DistributedTable, error) {
	const q = `SELECT logicalrelid::text, partmethod::text, colocationid, 'na' AS replication_model FROM pg_dist_partition ORDER BY logicalrelid::text`
	rows, err := pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []DistributedTable
	for rows.Next() {
		var t DistributedTable
		if err := rows.Scan(&t.LogicalRelID, &t.PartMethod, &t.ColocationID, &t.Replication); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}
