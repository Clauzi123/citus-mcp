// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Data collection for snapshot advisor analysis.

package snapshotadvisor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

func collectWorkers(ctx context.Context, deps Dependencies, in Input) ([]WorkerMetrics, []Warning, error) {
	infos, err := deps.WorkerManager.Topology(ctx)
	if err != nil {
		return nil, nil, err
	}
	exclude := make(map[string]struct{}, len(in.ExcludeNodes))
	for _, e := range in.ExcludeNodes {
		exclude[e] = struct{}{}
	}

	shardCounts, err := fetchShardCounts(ctx, deps)
	if err != nil {
		return nil, []Warning{{Code: "PERMISSION", Message: fmt.Sprintf("failed to fetch shard counts: %v", err)}}, nil
	}

	pools, _, _ := deps.WorkerManager.Pools(ctx)
	reachable := make(map[int32]bool, len(pools))
	unreachableWarnings := []Warning{}
	var reachMu sync.Mutex

	// Ping workers concurrently (limit 4)
	eg, ctxPing := errgroup.WithContext(ctx)
	sem := make(chan struct{}, 4)
	for _, info := range infos {
		info := info
		key := fmt.Sprintf("%s:%d", info.NodeName, info.NodePort)
		if _, skip := exclude[key]; skip {
			continue
		}
		pool := pools[info.NodeID]
		if pool == nil {
			reachable[info.NodeID] = false
			continue
		}
		sem <- struct{}{}
		eg.Go(func() error {
			defer func() { <-sem }()
			var one int
			if err := pool.QueryRow(ctxPing, "SELECT 1").Scan(&one); err != nil {
				reachMu.Lock()
				reachable[info.NodeID] = false
				unreachableWarnings = append(unreachableWarnings, Warning{Code: "PARTIAL_RESULTS", Message: fmt.Sprintf("worker %s:%d unreachable: %v", info.NodeName, info.NodePort, err)})
				reachMu.Unlock()
			} else {
				reachMu.Lock()
				reachable[info.NodeID] = true
				reachMu.Unlock()
			}
			return nil
		})
	}
	_ = eg.Wait()

	// Collect bytes (best-effort)
	bytesMap, bytesWarnings := fetchBytes(ctx, deps)

	workers := []WorkerMetrics{}
	for _, w := range infos {
		key := fmt.Sprintf("%s:%d", w.NodeName, w.NodePort)
		if _, skip := exclude[key]; skip {
			continue
		}
		if in.RequireReachable {
			if r, ok := reachable[w.NodeID]; ok && !r {
				continue
			}
		}
		metrics := WorkerMetrics{
			Node:             NodeRef{NodeID: w.NodeID, Host: w.NodeName, Port: w.NodePort},
			Reachable:        reachable[w.NodeID],
			IsActive:         w.IsActive,
			ShouldHaveShards: w.ShouldHaveShards,
			ShardCount:       shardCounts[w.NodeID],
		}
		if b, ok := bytesMap[w.NodeID]; ok {
			metrics.Bytes = &b
		}
		workers = append(workers, metrics)
	}

	warnings := append(unreachableWarnings, bytesWarnings...)
	return workers, warnings, nil
}

func fetchShardCounts(ctx context.Context, deps Dependencies) (map[int32]int, error) {
	rows, err := deps.Pool.Query(ctx, "SELECT pn.nodeid, count(*) FROM pg_dist_placement dp JOIN pg_dist_node pn ON dp.groupid = pn.groupid GROUP BY pn.nodeid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int32]int)
	for rows.Next() {
		var nodeID int32
		var cnt int
		if err := rows.Scan(&nodeID, &cnt); err != nil {
			return nil, err
		}
		out[nodeID] = cnt
	}
	return out, rows.Err()
}

func fetchBytes(ctx context.Context, deps Dependencies) (map[int32]int64, []Warning) {
	if !deps.CollectBytes {
		return map[int32]int64{}, nil
	}
	// Try cache
	if deps.Cache != nil && deps.Config.EnableCaching {
		if val, ok := deps.Cache.Get("snapshotadvisor:bytes"); ok {
			if m, ok := val.(map[int32]int64); ok {
				return m, nil
			}
		}
	}
	// Prefer citus_shard_sizes if available
	if deps.Capabilities != nil && deps.Capabilities.HasShardSizes {
		if m, w := fetchBytesFromShardSizes(ctx, deps); m != nil {
			if deps.Cache != nil && deps.Config.EnableCaching {
				deps.Cache.Set("snapshotadvisor:bytes", m, time.Duration(deps.Config.CacheTTLSeconds)*time.Second)
			}
			return m, w
		}
	}
	// Fallback: query workers individually
	m, warnings := fetchBytesFromWorkers(ctx, deps)
	if deps.Cache != nil && deps.Config.EnableCaching {
		deps.Cache.Set("snapshotadvisor:bytes", m, time.Duration(deps.Config.CacheTTLSeconds)*time.Second)
	}
	return m, warnings
}

func fetchBytesFromShardSizes(ctx context.Context, deps Dependencies) (map[int32]int64, []Warning) {
	rows, err := deps.Pool.Query(ctx, "SELECT nodename, nodeport, sum(shard_size) FROM citus_shard_sizes() GROUP BY 1,2")
	if err != nil {
		return nil, []Warning{{Code: "PARTIAL_RESULTS", Message: fmt.Sprintf("citus_shard_sizes failed: %v", err)}}
	}
	defer rows.Close()
	out := make(map[int32]int64)
	// Map nodename:port -> nodeid by querying pg_dist_node once
	nodes := map[string]int32{}
	nodeRows, err := deps.Pool.Query(ctx, "SELECT nodeid, nodename, nodeport FROM pg_dist_node")
	if err == nil {
		defer nodeRows.Close()
		for nodeRows.Next() {
			var id int32
			var name string
			var port int32
			_ = nodeRows.Scan(&id, &name, &port)
			nodes[fmt.Sprintf("%s:%d", name, port)] = id
		}
	}
	for rows.Next() {
		var name string
		var port int32
		var sz int64
		if err := rows.Scan(&name, &port, &sz); err != nil {
			return nil, []Warning{{Code: "PARTIAL_RESULTS", Message: fmt.Sprintf("citus_shard_sizes scan failed: %v", err)}}
		}
		key := fmt.Sprintf("%s:%d", name, port)
		nodeID := nodes[key]
		out[nodeID] = sz
	}
	return out, nil
}

func fetchBytesFromWorkers(ctx context.Context, deps Dependencies) (map[int32]int64, []Warning) {
	pools, infos, _ := deps.WorkerManager.Pools(ctx)
	out := make(map[int32]int64)
	warnings := []Warning{}
	var mu sync.Mutex
	eg, ctxq := errgroup.WithContext(ctx)
	sem := make(chan struct{}, 4)
	for _, info := range infos {
		info := info
		pool := pools[info.NodeID]
		if pool == nil {
			continue
		}
		sem <- struct{}{}
		eg.Go(func() error {
			defer func() { <-sem }()
			var total *int64
			if err := pool.QueryRow(ctxq, "SELECT sum(pg_total_relation_size(c.oid))::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') AND c.relkind='r'").Scan(&total); err != nil {
				mu.Lock()
				warnings = append(warnings, Warning{Code: "PARTIAL_RESULTS", Message: fmt.Sprintf("bytes fetch failed for %s:%d: %v", info.NodeName, info.NodePort, err)})
				mu.Unlock()
				return nil
			}
			if total != nil {
				mu.Lock()
				out[info.NodeID] = *total
				mu.Unlock()
			}
			return nil
		})
	}
	_ = eg.Wait()
	if len(out) == 0 {
		warnings = append(warnings, Warning{Code: "PARTIAL_RESULTS", Message: "bytes not available; falling back to shard counts"})
	}
	return out, warnings
}
