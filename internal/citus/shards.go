package citus

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Shard struct {
	ShardID      int64             `json:"shard_id"`
	ShardStorage string            `json:"shard_storage"`
	ShardMin     *string           `json:"shard_min_value,omitempty"`
	ShardMax     *string           `json:"shard_max_value,omitempty"`
	TableName    string            `json:"table_name"`
	SchemaName   string            `json:"schema_name"`
	Placements   []ShardNodeInfo   `json:"placements,omitempty"`
}

type ShardNodeInfo struct {
	NodeName string `json:"node_name"`
	NodePort int32  `json:"node_port"`
}

type ShardPlacement struct {
	ShardID     int64  `json:"shard_id"`
	NodeName    string `json:"node_name"`
	NodePort    int32  `json:"node_port"`
	PlacementID int64  `json:"placement_id"`
	ShardState  int16  `json:"shard_state"`
}

func ListShards(ctx context.Context, pool *pgxpool.Pool) ([]Shard, error) {
	// First, get all shards with table info
	const shardQuery = `
SELECT 
    ds.shardid,
    ds.shardstorage::text,
    ds.shardminvalue,
    ds.shardmaxvalue,
    c.relname AS table_name,
    n.nspname AS schema_name
FROM pg_dist_shard ds
JOIN pg_class c ON c.oid = ds.logicalrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
ORDER BY ds.shardid`

	rows, err := pool.Query(ctx, shardQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	shardMap := make(map[int64]*Shard)
	var shardIDs []int64

	for rows.Next() {
		var s Shard
		if err := rows.Scan(&s.ShardID, &s.ShardStorage, &s.ShardMin, &s.ShardMax, &s.TableName, &s.SchemaName); err != nil {
			return nil, err
		}
		s.Placements = []ShardNodeInfo{} // Initialize to empty slice
		shardMap[s.ShardID] = &s
		shardIDs = append(shardIDs, s.ShardID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Now get placements for all shards (nodename/nodeport are directly on pg_dist_shard_placement)
	const placementQuery = `
SELECT shardid, nodename, nodeport
FROM pg_dist_shard_placement
ORDER BY shardid, placementid`

	placementRows, err := pool.Query(ctx, placementQuery)
	if err != nil {
		return nil, err
	}
	defer placementRows.Close()

	for placementRows.Next() {
		var shardID int64
		var nodeName string
		var nodePort int32
		if err := placementRows.Scan(&shardID, &nodeName, &nodePort); err != nil {
			return nil, err
		}
		if s, ok := shardMap[shardID]; ok {
			s.Placements = append(s.Placements, ShardNodeInfo{NodeName: nodeName, NodePort: nodePort})
		}
	}
	if err := placementRows.Err(); err != nil {
		return nil, err
	}

	// Build result slice in order
	shards := make([]Shard, 0, len(shardIDs))
	for _, id := range shardIDs {
		shards = append(shards, *shardMap[id])
	}

	return shards, nil
}

func ListShardPlacements(ctx context.Context, pool *pgxpool.Pool, shardID int64) ([]ShardPlacement, error) {
	const q = `
SELECT shardid, nodename, nodeport, placementid, shardstate
FROM pg_dist_shard_placement
WHERE shardid = $1
ORDER BY placementid`
	rows, err := pool.Query(ctx, q, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var placements []ShardPlacement
	for rows.Next() {
		var p ShardPlacement
		if err := rows.Scan(&p.ShardID, &p.NodeName, &p.NodePort, &p.PlacementID, &p.ShardState); err != nil {
			return nil, err
		}
		placements = append(placements, p)
	}
	return placements, rows.Err()
}
