package tools

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"citus-mcp/internal/db"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

// ShardSkewInput for citus.shard_skew_report.
type ShardSkewInput struct {
	Table            string `json:"table,omitempty"`  // schema.name
	Metric           string `json:"metric,omitempty"` // shard_count | bytes
	IncludeTopShards bool   `json:"include_top_shards,omitempty"`
}

// ShardSkewOutput result.
type ShardSkewOutput struct {
	PerNode   []NodeSkewSummary `json:"per_node"`
	Skew      SkewScore         `json:"skew"`
	TopShards []TopShard        `json:"top_shards,omitempty"`
	Warnings  []string          `json:"warnings,omitempty"`
}

type NodeSkewSummary struct {
	Host             string  `json:"host"`
	Port             int32   `json:"port"`
	ShardCount       int64   `json:"shard_count"`
	BytesTotal       int64   `json:"bytes_total"`
	BytesAvgPerShard float64 `json:"bytes_avg_per_shard"`
}

type SkewScore struct {
	Metric  string  `json:"metric"`
	Max     float64 `json:"max"`
	Min     float64 `json:"min"`
	Stddev  float64 `json:"stddev"`
	Warning string  `json:"warning_level"`
}

type TopShard struct {
	ShardID    int64            `json:"shard_id"`
	Bytes      int64            `json:"bytes"`
	Placements []ShardPlacement `json:"placements"`
}

type ShardPlacement struct {
	Host string `json:"host"`
	Port int32  `json:"port"`
}

func shardSkewReportTool(ctx context.Context, deps Dependencies, input ShardSkewInput) (*mcp.CallToolResult, ShardSkewOutput, error) {
	metric := input.Metric
	if metric == "" {
		metric = "bytes"
	}
	metricBytes := metric == "bytes"
	// ensure read-only
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), ShardSkewOutput{}, nil
	}

	// validate citus
	ext, err := db.GetExtensionInfo(ctx, deps.Pool)
	if err != nil || ext == nil {
		return callError(serr.CodeNotCitus, "citus extension not found", "enable citus extension"), ShardSkewOutput{}, nil
	}

	var schema, table string
	if input.Table != "" {
		schema, table, err = parseSchemaTable(input.Table)
		if err != nil {
			return callError(serr.CodeInvalidInput, "invalid table format", "use schema.table"), ShardSkewOutput{}, nil
		}
	}

	// fetch worker topology
	infos, err := deps.WorkerManager.Topology(ctx)
	warnings := []string{}
	if err != nil {
		deps.Logger.Warn("topology fetch failed", zap.Error(err))
		warnings = append(warnings, "failed to fetch worker topology")
	}
	if len(infos) == 0 {
		warnings = append(warnings, "no workers")
	}

	// detect bytes capability
	hasBytes := false
	if metricBytes {
		// detect citus_shard_sizes
		var ok bool
		err := deps.Pool.QueryRow(ctx, "SELECT to_regproc('citus_shard_sizes') IS NOT NULL").Scan(&ok)
		if err == nil && ok {
			hasBytes = true
		}
	}

	// fetch shards and placements
	shards, err := fetchShardPlacements(ctx, deps, schema, table)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "fetch shard placements"), ShardSkewOutput{}, nil
	}
	if len(shards) == 0 {
		warnings = append(warnings, "no shards found")
	}

	shardBytes := map[int64]int64{}
	if hasBytes {
		sz, err := fetchShardSizes(ctx, deps)
		if err != nil {
			warnings = append(warnings, "failed to fetch shard sizes; falling back to shard_count")
		} else {
			shardBytes = sz
		}
	}
	if metricBytes && !hasBytes {
		warnings = append(warnings, "citus_shard_sizes not available; using shard_count")
		metricBytes = false
		metric = "shard_count"
	}

	perNode := map[string]*NodeSkewSummary{}
	for _, shp := range shards {
		key := shp.Host + ":" + strconv.Itoa(int(shp.Port))
		ns, ok := perNode[key]
		if !ok {
			ns = &NodeSkewSummary{Host: shp.Host, Port: shp.Port}
			perNode[key] = ns
		}
		ns.ShardCount++
		if metricBytes && shardBytes[shp.ShardID] > 0 {
			ns.BytesTotal += shardBytes[shp.ShardID]
		}
	}

	nodeSummaries := make([]NodeSkewSummary, 0, len(perNode))
	metricVals := []float64{}
	for _, ns := range perNode {
		if ns.ShardCount > 0 {
			ns.BytesAvgPerShard = float64(ns.BytesTotal) / float64(ns.ShardCount)
		}
		nodeSummaries = append(nodeSummaries, *ns)
		if metricBytes {
			metricVals = append(metricVals, float64(ns.BytesTotal))
		} else {
			metricVals = append(metricVals, float64(ns.ShardCount))
		}
	}
	sort.Slice(nodeSummaries, func(i, j int) bool {
		if nodeSummaries[i].Host == nodeSummaries[j].Host {
			return nodeSummaries[i].Port < nodeSummaries[j].Port
		}
		return nodeSummaries[i].Host < nodeSummaries[j].Host
	})

	skew := computeSkew(metricVals, metric)

	var topShards []TopShard
	if input.IncludeTopShards && len(shardBytes) > 0 {
		topShards = buildTopShards(shardBytes, shards, 10)
	}

	// ensure non-nil slices for JSON (avoid null)
	if nodeSummaries == nil {
		nodeSummaries = []NodeSkewSummary{}
	}
	if topShards == nil && input.IncludeTopShards {
		topShards = []TopShard{}
	}
	if warnings == nil {
		warnings = []string{}
	}
	if len(warnings) > 0 {
		return nil, ShardSkewOutput{PerNode: nodeSummaries, Skew: skew, TopShards: topShards, Warnings: warnings}, nil
	}
	return nil, ShardSkewOutput{PerNode: nodeSummaries, Skew: skew, TopShards: topShards}, nil
}

// ShardSkewReport is exported for resources.
func ShardSkewReport(ctx context.Context, deps Dependencies, input ShardSkewInput) (*mcp.CallToolResult, ShardSkewOutput, error) {
	return shardSkewReportTool(ctx, deps, input)
}

type shardPlacementRow struct {
	ShardID int64
	Host    string
	Port    int32
}

func fetchShardPlacements(ctx context.Context, deps Dependencies, schema, table string) ([]shardPlacementRow, error) {
	q := `
SELECT s.shardid, p.nodename, p.nodeport
FROM pg_dist_shard s
JOIN pg_dist_shard_placement p USING (shardid)
JOIN pg_class c ON c.oid = s.logicalrelid
JOIN pg_namespace ns ON ns.oid = c.relnamespace
WHERE ($1 = '' OR (ns.nspname = $1::name AND c.relname = $2::name))
`
	rows, err := deps.Pool.Query(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []shardPlacementRow
	for rows.Next() {
		var r shardPlacementRow
		if err := rows.Scan(&r.ShardID, &r.Host, &r.Port); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func fetchShardSizes(ctx context.Context, deps Dependencies) (map[int64]int64, error) {
	q := `SELECT shardid, shard_size FROM citus_shard_sizes()`
	rows, err := deps.Pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sizes := map[int64]int64{}
	for rows.Next() {
		var shardid int64
		var size int64
		if err := rows.Scan(&shardid, &size); err != nil {
			return nil, err
		}
		sizes[shardid] = size
	}
	return sizes, rows.Err()
}

func computeSkew(vals []float64, metric string) SkewScore {
	if len(vals) == 0 {
		return SkewScore{Metric: metric}
	}
	maxv, minv := vals[0], vals[0]
	var sum float64
	for _, v := range vals {
		if v > maxv {
			maxv = v
		}
		if v < minv {
			minv = v
		}
		sum += v
	}
	mean := sum / float64(len(vals))
	var variance float64
	for _, v := range vals {
		variance += (v - mean) * (v - mean)
	}
	variance /= float64(len(vals))
	stddev := math.Sqrt(variance)

	warning := "low"
	ratio := 1.0
	if minv > 0 {
		ratio = maxv / minv
	} else if maxv > 0 {
		ratio = math.Inf(1)
	}
	if ratio >= 2.0 || stddev > mean {
		warning = "high"
	} else if ratio >= 1.5 || stddev > mean*0.5 {
		warning = "medium"
	}

	return SkewScore{Metric: metric, Max: maxv, Min: minv, Stddev: stddev, Warning: warning}
}

func buildTopShards(shardBytes map[int64]int64, placements []shardPlacementRow, limit int) []TopShard {
	type pair struct {
		id int64
		b  int64
	}
	pairs := make([]pair, 0, len(shardBytes))
	for id, b := range shardBytes {
		pairs = append(pairs, pair{id: id, b: b})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].b > pairs[j].b })
	if len(pairs) > limit {
		pairs = pairs[:limit]
	}
	// map placements
	placeByShard := map[int64][]ShardPlacement{}
	for _, p := range placements {
		placeByShard[p.ShardID] = append(placeByShard[p.ShardID], ShardPlacement{Host: p.Host, Port: p.Port})
	}

	out := make([]TopShard, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, TopShard{ShardID: p.id, Bytes: p.b, Placements: placeByShard[p.id]})
	}
	return out
}

func parseSchemaTable(s string) (string, string, error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid table: %s", s)
	}
	return parts[0], parts[1], nil
}
