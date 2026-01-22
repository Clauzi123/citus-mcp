// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_shard_heatmap tool for hot shard detection.

package tools

import (
	"context"
	"fmt"
	"sort"
	"time"

	"citus-mcp/internal/db"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

// ShardHeatmapInput defines input for citus_shard_heatmap.
type ShardHeatmapInput struct {
	Table            string `json:"table,omitempty"` // schema.table
	Limit            int    `json:"limit,omitempty"`
	Metric           string `json:"metric,omitempty"`   // bytes|shard_count
	GroupBy          string `json:"group_by,omitempty"` // node|table|colocation
	IncludeNextSteps bool   `json:"include_next_steps,omitempty"`
}

// ShardHeatmapOutput defines output.
type ShardHeatmapOutput struct {
	Summary   ShardHeatmapSummary `json:"summary"`
	PerNode   []NodeHeat          `json:"per_node"`
	PerTable  []TableHeat         `json:"per_table"`
	HotShards []HeatmapShard      `json:"hot_shards"`
	Warnings  []string            `json:"warnings,omitempty"`
	NextSteps []HeatmapNextStep   `json:"next_steps,omitempty"`
}

type ShardHeatmapSummary struct {
	Tables       int     `json:"tables"`
	Shards       int     `json:"shards"`
	MaxNodeRatio float64 `json:"max_node_ratio"`
	GeneratedAt  string  `json:"generated_at"`
}

type NodeHeat struct {
	Node          string  `json:"node"`
	Host          string  `json:"host"`
	Port          int32   `json:"port"`
	Shards        int64   `json:"shards"`
	Bytes         int64   `json:"bytes"`
	BytesPerShard float64 `json:"bytes_per_shard"`
}

type TableHeat struct {
	Table        string  `json:"table"`
	ColocationID int32   `json:"colocation_id"`
	Shards       int64   `json:"shards"`
	Bytes        int64   `json:"bytes"`
	MaxNodeRatio float64 `json:"max_node_ratio"`
}

type HeatmapShard struct {
	ShardID int64            `json:"shard_id"`
	Table   string           `json:"table"`
	Bytes   int64            `json:"bytes"`
	Nodes   []ShardPlacement `json:"placements"`
}

type HeatmapNextStep struct {
	Tool string                 `json:"tool"`
	Args map[string]interface{} `json:"args,omitempty"`
}

func shardHeatmapTool(ctx context.Context, deps Dependencies, input ShardHeatmapInput) (*mcp.CallToolResult, ShardHeatmapOutput, error) {
	metric := input.Metric
	if metric == "" {
		metric = "bytes"
	}
	metricBytes := metric == "bytes"
	if input.Limit <= 0 {
		input.Limit = 20
	}
	if input.Limit > 200 {
		input.Limit = 200
	}
	if input.GroupBy == "" {
		input.GroupBy = "node"
	}
	// ensure read-only
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), ShardHeatmapOutput{PerNode: []NodeHeat{}, PerTable: []TableHeat{}, HotShards: []HeatmapShard{}}, nil
	}

	// validate citus
	ext, err := db.GetExtensionInfo(ctx, deps.Pool)
	if err != nil || ext == nil {
		return callError(serr.CodeNotCitus, "citus extension not found", "enable citus extension"), ShardHeatmapOutput{PerNode: []NodeHeat{}, PerTable: []TableHeat{}, HotShards: []HeatmapShard{}}, nil
	}

	schema, table, err := parseOptionalSchemaTable(input.Table)
	if err != nil {
		return callError(serr.CodeInvalidInput, "invalid table format", "use schema.table"), ShardHeatmapOutput{PerNode: []NodeHeat{}, PerTable: []TableHeat{}, HotShards: []HeatmapShard{}}, nil
	}

	// collect shards
	shards, warnings := collectShards(ctx, deps, schema, table)
	if len(shards) == 0 {
		if warnings == nil {
			warnings = []string{}
		}
		warnings = append(warnings, "no shards found")
		return nil, ShardHeatmapOutput{
			Warnings:  warnings,
			Summary:   ShardHeatmapSummary{GeneratedAt: time.Now().UTC().Format(time.RFC3339)},
			PerNode:   []NodeHeat{},
			PerTable:  []TableHeat{},
			HotShards: []HeatmapShard{},
		}, nil
	}

	// aggregate per node and per table
	perNode := aggregatePerNode(shards)
	perTable := aggregatePerTable(shards)

	// compute ratios
	maxNodeRatio := computeMaxNodeRatio(perNode, metricBytes)

	// hot shards
	hot := topShards(shards, input.Limit, metricBytes)

	// next steps
	var next []HeatmapNextStep
	if input.IncludeNextSteps {
		next = append(next, HeatmapNextStep{Tool: "citus_shard_skew_report", Args: map[string]interface{}{"metric": metric}})
		next = append(next, HeatmapNextStep{Tool: "citus_rebalance_plan"})
		if table != "" {
			next = append(next, HeatmapNextStep{Tool: "citus_validate_rebalance_prereqs", Args: map[string]interface{}{"table": schema + "." + table}})
		}
	}

	out := ShardHeatmapOutput{
		Summary: ShardHeatmapSummary{
			Tables:       len(perTable),
			Shards:       len(shards),
			MaxNodeRatio: maxNodeRatio,
			GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		},
		PerNode:   perNode,
		PerTable:  perTable,
		HotShards: hot,
		Warnings:  warnings,
		NextSteps: next,
	}
	return nil, out, nil
}

func parseOptionalSchemaTable(s string) (string, string, error) {
	if s == "" {
		return "", "", nil
	}
	return parseSchemaTable(s)
}

// shardRecord represents a shard with placement and size.
type shardRecord struct {
	Table   string
	ShardID int64
	Host    string
	Port    int32
	Bytes   int64
}

func collectShards(ctx context.Context, deps Dependencies, schema, table string) ([]shardRecord, []string) {
	warnings := []string{}
	// prefer citus_shards view
	var ok bool
	shards := []shardRecord{}
	if err := deps.Pool.QueryRow(ctx, "SELECT to_regclass('pg_catalog.citus_shards') IS NOT NULL").Scan(&ok); err == nil && ok {
		q := `SELECT table_name, shardid, nodename, nodeport, shard_size FROM pg_catalog.citus_shards WHERE ($1='' OR table_name = $1||'.'||$2)`
		rows, err := deps.Pool.Query(ctx, q, schema, table)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var r shardRecord
				var tbl string
				if err := rows.Scan(&tbl, &r.ShardID, &r.Host, &r.Port, &r.Bytes); err != nil {
					continue
				}
				r.Table = tbl
				shards = append(shards, r)
			}
		} else {
			warnings = append(warnings, "failed to read citus_shards view; falling back to placements")
			deps.Logger.Warn("failed to read citus_shards view", zap.Error(err))
			// fallback to placements + citus_shard_sizes()
			placements, err2 := fetchShardPlacements(ctx, deps, schema, table)
			if err2 != nil {
				warnings = append(warnings, "failed to fetch shard placements")
				return shards, warnings
			}
			sizes, err3 := fetchShardSizes(ctx, deps)
			if err3 != nil {
				warnings = append(warnings, "failed to fetch shard sizes; using shard_count")
			}
			for _, p := range placements {
				tbl := ""
				if schema != "" && table != "" {
					tbl = schema + "." + table
				}
				r := shardRecord{Table: tbl, ShardID: p.ShardID, Host: p.Host, Port: p.Port}
				if sizes != nil {
					r.Bytes = sizes[p.ShardID]
				}
				shards = append(shards, r)
			}
		}
	} else {
		// fallback: placements + citus_shard_sizes
		placements, err := fetchShardPlacements(ctx, deps, schema, table)
		if err != nil {
			warnings = append(warnings, "failed to fetch shard placements")
			return shards, warnings
		}
		sizes, err := fetchShardSizes(ctx, deps)
		if err != nil {
			warnings = append(warnings, "failed to fetch shard sizes; using shard_count")
		}
		for _, p := range placements {
			r := shardRecord{ShardID: p.ShardID, Host: p.Host, Port: p.Port}
			if sizes != nil {
				r.Bytes = sizes[p.ShardID]
			}
			// table name unknown in fallback; we can attempt to derive via pg_dist_shard query with table filter (already in fetchShardPlacements)
			if schema != "" && table != "" {
				r.Table = schema + "." + table
			}
			shards = append(shards, r)
		}
	}
	return shards, warnings
}

func aggregatePerNode(shards []shardRecord) []NodeHeat {
	m := map[string]*NodeHeat{}
	for _, s := range shards {
		key := s.Host + fmt.Sprintf(":%d", s.Port)
		nh, ok := m[key]
		if !ok {
			nh = &NodeHeat{Node: key, Host: s.Host, Port: s.Port}
			m[key] = nh
		}
		nh.Shards++
		nh.Bytes += s.Bytes
	}
	out := make([]NodeHeat, 0, len(m))
	for _, v := range m {
		if v.Shards > 0 {
			v.BytesPerShard = float64(v.Bytes) / float64(v.Shards)
		}
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

func aggregatePerTable(shards []shardRecord) []TableHeat {
	m := map[string]*TableHeat{}
	// collect per-table per-node to compute max node ratio per table
	perTableNode := map[string]map[string]int64{}
	for _, s := range shards {
		if s.Table == "" {
			continue
		}
		th, ok := m[s.Table]
		if !ok {
			th = &TableHeat{Table: s.Table}
			m[s.Table] = th
		}
		th.Shards++
		th.Bytes += s.Bytes
		if perTableNode[s.Table] == nil {
			perTableNode[s.Table] = map[string]int64{}
		}
		key := s.Host + fmt.Sprintf(":%d", s.Port)
		perTableNode[s.Table][key] += s.Bytes
	}
	out := make([]TableHeat, 0, len(m))
	for tbl, v := range m {
		// compute max node ratio per table (bytes)
		nodes := perTableNode[tbl]
		var maxv, minv float64
		for _, b := range nodes {
			val := float64(b)
			if maxv == 0 || val > maxv {
				maxv = val
			}
			if minv == 0 || val < minv {
				minv = val
			}
		}
		if minv > 0 {
			v.MaxNodeRatio = maxv / minv
		}
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	return out
}

func computeMaxNodeRatio(nodes []NodeHeat, metricBytes bool) float64 {
	if len(nodes) == 0 {
		return 0
	}
	var maxv, minv float64
	for _, n := range nodes {
		v := float64(n.Shards)
		if metricBytes {
			v = float64(n.Bytes)
		}
		if maxv == 0 || v > maxv {
			maxv = v
		}
		if minv == 0 || v < minv {
			minv = v
		}
	}
	if minv <= 0 {
		return 0
	}
	return maxv / minv
}

func topShards(shards []shardRecord, limit int, metricBytes bool) []HeatmapShard {
	if len(shards) == 0 {
		return []HeatmapShard{}
	}
	sort.Slice(shards, func(i, j int) bool {
		if metricBytes {
			if shards[i].Bytes != shards[j].Bytes {
				return shards[i].Bytes > shards[j].Bytes
			}
			return shards[i].ShardID < shards[j].ShardID // stable tie-breaker
		}
		return shards[i].ShardID < shards[j].ShardID
	})
	if len(shards) > limit {
		shards = shards[:limit]
	}
	out := make([]HeatmapShard, 0, len(shards))
	for _, s := range shards {
		hs := HeatmapShard{ShardID: s.ShardID, Table: s.Table, Bytes: s.Bytes}
		hs.Nodes = append(hs.Nodes, ShardPlacement{Host: s.Host, Port: s.Port})
		out = append(out, hs)
	}
	return out
}
