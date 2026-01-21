package citus

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Shard struct {
	ShardID      int64   `json:"shard_id"`
	ShardStorage string  `json:"shard_storage"`
	ShardMin     *string `json:"shard_min_value,omitempty"`
	ShardMax     *string `json:"shard_max_value,omitempty"`
}

type ShardPlacement struct {
	ShardID     int64  `json:"shard_id"`
	NodeName    string `json:"node_name"`
	NodePort    int32  `json:"node_port"`
	PlacementID int64  `json:"placement_id"`
	ShardState  int16  `json:"shard_state"`
}

func ListShards(ctx context.Context, pool *pgxpool.Pool) ([]Shard, error) {
	const q = `SELECT shardid, shardstorage::text, shardminvalue, shardmaxvalue FROM pg_dist_shard ORDER BY shardid`
	rows, err := pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var shards []Shard
	for rows.Next() {
		var s Shard
		if err := rows.Scan(&s.ShardID, &s.ShardStorage, &s.ShardMin, &s.ShardMax); err != nil {
			return nil, err
		}
		shards = append(shards, s)
	}
	return shards, rows.Err()
}

func ListShardPlacements(ctx context.Context, pool *pgxpool.Pool, shardID int64) ([]ShardPlacement, error) {
	const q = `
SELECT p.shardid, n.nodename, n.nodeport, p.placementid, p.shardstate
FROM pg_dist_shard_placement p
JOIN pg_dist_node n ON p.nodeid = n.nodeid
WHERE p.shardid = $1
ORDER BY p.placementid`
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
