package tools

import (
	"context"
	"fmt"
	"strings"

	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TableInspectorInput for citus_table_inspector.
type TableInspectorInput struct {
	Table          string `json:"table" jsonschema:"required"`
	IncludeIndexes bool   `json:"include_indexes,omitempty"`
	IncludeShards  bool   `json:"include_shards,omitempty"`
}

// TableInspectorOutput detailed metadata about a distributed/reference table.
type TableInspectorOutput struct {
	Table       string            `json:"table"`
	Type        string            `json:"table_type"`
	DistColumn  string            `json:"distribution_column"`
	PartMethod  string            `json:"part_method"`
	Colocation  int32             `json:"colocation_id"`
	ShardCount  int32             `json:"shard_count"`
	Replication int32             `json:"replication_factor"`
	Sizes       TableSizes        `json:"sizes"`
	PerNode     []NodeHeat        `json:"per_node"`
	Shards      []HeatmapShard    `json:"shards,omitempty"`
	Indexes     []IndexInfo       `json:"indexes,omitempty"`
	IndexDrift  []IndexDriftIssue `json:"index_drift,omitempty"`
	Stats       TableStats        `json:"stats"`
	Constraints TableConstraints  `json:"constraints"`
	Warnings    []string          `json:"warnings,omitempty"`
}

type TableSizes struct {
	TotalBytes int64 `json:"total_bytes"`
}

type IndexInfo struct {
	Name           string `json:"name"`
	Def            string `json:"definition"`
	Unique         bool   `json:"unique"`
	IncludeDistKey bool   `json:"include_dist_key"`
}

type IndexDriftIssue struct {
	IndexName string   `json:"index_name"`
	MissingOn []string `json:"missing_on_nodes"`
}

type TableStats struct {
	LastAnalyze     *string `json:"last_analyze,omitempty"`
	LastAutoanalyze *string `json:"last_autoanalyze,omitempty"`
	NLiveTup        int64   `json:"n_live_tup"`
}

type TableConstraints struct {
	HasPrimaryKey   bool   `json:"has_primary_key"`
	ReplicaIdentity string `json:"replica_identity"`
}

func tableInspectorTool(ctx context.Context, deps Dependencies, input TableInspectorInput) (*mcp.CallToolResult, TableInspectorOutput, error) {
	if strings.TrimSpace(input.Table) == "" {
		return callError(serr.CodeInvalidInput, "table is required", ""), TableInspectorOutput{}, nil
	}
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), TableInspectorOutput{}, nil
	}
	schema, rel, err := parseSchemaTable(input.Table)
	if err != nil {
		return callError(serr.CodeInvalidInput, "invalid table format", "use schema.table"), TableInspectorOutput{}, nil
	}

	out := TableInspectorOutput{Table: schema + "." + rel, Warnings: []string{}}

	// fetch citus table metadata
	var partmethod string
	var coloc int32
	var tableType string
	qMeta := `SELECT p.partmethod, p.colocationid, p.logicalrelid::regclass::text, CASE WHEN p.partmethod='n' THEN 'reference' ELSE 'distributed' END
FROM pg_dist_partition p
JOIN pg_class c ON c.oid = p.logicalrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname=$1::name AND c.relname=$2::name`
	var tblreg string
	if err := deps.Pool.QueryRow(ctx, qMeta, schema, rel).Scan(&partmethod, &coloc, &tblreg, &tableType); err != nil {
		return callError(serr.CodeInternalError, err.Error(), "metadata"), TableInspectorOutput{}, nil
	}
	out.PartMethod = partmethod
	out.Colocation = coloc
	out.Type = tableType

	// dist column
	distCol, _ := fetchDistributionColumn(ctx, deps, schema, rel)
	out.DistColumn = distCol

	// shard count and replication
	var shardCount int32
	_ = deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_dist_shard s JOIN pg_class c ON c.oid=s.logicalrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`, schema, rel).Scan(&shardCount)
	out.ShardCount = shardCount
	var repl int32
	_ = deps.Pool.QueryRow(ctx, `SELECT COALESCE(max(cnt),0) FROM (SELECT shardid, count(*) cnt FROM pg_dist_placement pl JOIN pg_dist_shard s USING(shardid) JOIN pg_class c ON c.oid=s.logicalrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 GROUP BY shardid) x`, schema, rel).Scan(&repl)
	if repl == 0 {
		repl = 1
	}
	out.Replication = repl

	// sizes via citus_shards
	sizes, perNode, shards := collectTableShards(ctx, deps, schema, rel, input.IncludeShards)
	out.Sizes = TableSizes{TotalBytes: sizes}
	out.PerNode = perNode
	out.Shards = shards

	// stats
	var lastAnalyze, lastAutoanalyze *string
	var nLiveTup int64
	_ = deps.Pool.QueryRow(ctx, `SELECT to_char(last_analyze, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), to_char(last_autoanalyze, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), COALESCE(n_live_tup,0) FROM pg_stat_all_tables WHERE schemaname=$1 AND relname=$2`, schema, rel).Scan(&lastAnalyze, &lastAutoanalyze, &nLiveTup)
	out.Stats = TableStats{LastAnalyze: lastAnalyze, LastAutoanalyze: lastAutoanalyze, NLiveTup: nLiveTup}

	// constraints
	var hasPK bool
	_ = deps.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND i.indisprimary)`, schema, rel).Scan(&hasPK)
	var replIdent string
	_ = deps.Pool.QueryRow(ctx, `SELECT c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`, schema, rel).Scan(&replIdent)
	out.Constraints = TableConstraints{HasPrimaryKey: hasPK, ReplicaIdentity: replIdent}

	// indexes
	if input.IncludeIndexes {
		idxs, drift, warn := collectIndexInfo(ctx, deps, schema, rel, distCol)
		out.Indexes = idxs
		out.IndexDrift = drift
		if warn != "" {
			out.Warnings = append(out.Warnings, warn)
		}
	}

	return nil, out, nil
}

func collectTableShards(ctx context.Context, deps Dependencies, schema, rel string, includeShards bool) (totalBytes int64, perNode []NodeHeat, shards []HeatmapShard) {
	records, warnings := collectShards(ctx, deps, schema, rel)
	_ = warnings
	for _, r := range records {
		totalBytes += r.Bytes
	}
	perNode = aggregatePerNode(records)
	if includeShards {
		shards = topShards(records, 200, true)
	}
	return
}

func collectIndexInfo(ctx context.Context, deps Dependencies, schema, rel, distCol string) ([]IndexInfo, []IndexDriftIssue, string) {
	idxs := []IndexInfo{}
	drift := []IndexDriftIssue{}
	// expected indexes
	rows, err := deps.Pool.Query(ctx, `SELECT indexname, indexdef, indisunique FROM pg_indexes i JOIN pg_class c ON c.relname=i.tablename JOIN pg_namespace n ON n.nspname=i.schemaname JOIN pg_index pi ON pi.indexrelid = i.indexname::regclass WHERE schemaname=$1 AND tablename=$2 ORDER BY indexname`, schema, rel)
	if err != nil {
		return idxs, drift, "failed to fetch indexes"
	}
	defer rows.Close()
	expected := []string{}
	for rows.Next() {
		var name, def string
		var unique bool
		if err := rows.Scan(&name, &def, &unique); err != nil {
			continue
		}
		includeDist := distCol != "" && strings.Contains(def, fmt.Sprintf("(%s)", distCol))
		idxs = append(idxs, IndexInfo{Name: name, Def: def, Unique: unique, IncludeDistKey: includeDist})
		expected = append(expected, name)
	}
	// drift detection via citus_shard_indexes_on_worker
	rows2, err := deps.Pool.Query(ctx, `SELECT index_name, nodename||':'||nodeport as node FROM pg_catalog.citus_shard_indexes_on_worker WHERE logicalrelid=$1::regclass`, schema+"."+rel)
	if err == nil {
		present := map[string]map[string]bool{}
		for rows2.Next() {
			var idx, node string
			if err := rows2.Scan(&idx, &node); err != nil {
				continue
			}
			if present[idx] == nil {
				present[idx] = map[string]bool{}
			}
			present[idx][node] = true
		}
		rows2.Close()
		for _, idx := range expected {
			nodes := present[idx]
			// naive drift: if nodes count < replication factor * shards?
			// We only report missing nodes if absent entirely
			if len(nodes) == 0 {
				// cannot detect missing per-node easily; skip
				continue
			}
		}
	}
	// TODO: improve index drift detection per placement
	return idxs, drift, ""
}

// TableInspector is exported
func TableInspector(ctx context.Context, deps Dependencies, input TableInspectorInput) (*mcp.CallToolResult, TableInspectorOutput, error) {
	return tableInspectorTool(ctx, deps, input)
}
