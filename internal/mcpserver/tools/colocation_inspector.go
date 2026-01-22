// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_colocation_inspector tool for colocation group analysis.

package tools

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ColocationInspectorInput for citus_colocation_inspector tool.
type ColocationInspectorInput struct {
	ColocationID *int32 `json:"colocation_id,omitempty"` // nil means all groups
	Limit        int    `json:"limit,omitempty"`
}

// ColocationInspectorOutput shows colocation group details.
type ColocationInspectorOutput struct {
	Summary    ColocationSummary   `json:"summary"`
	Groups     []ColocationGroup   `json:"groups"`
	Warnings   []string            `json:"warnings,omitempty"`
}

type ColocationSummary struct {
	TotalGroups      int    `json:"total_groups"`
	TotalTables      int    `json:"total_tables"`
	LargestGroupID   int32  `json:"largest_group_id"`
	LargestGroupSize int    `json:"largest_group_tables"`
	GeneratedAt      string `json:"generated_at"`
}

type ColocationGroup struct {
	ColocationID    int32              `json:"colocation_id"`
	ShardCount      int32              `json:"shard_count"`
	ReplicationFactor int32            `json:"replication_factor"`
	DistColumn      string             `json:"distribution_column_type"`
	Tables          []ColocationTable  `json:"tables"`
	TotalBytes      int64              `json:"total_bytes"`
	SkewRatio       float64            `json:"skew_ratio"`
}

type ColocationTable struct {
	Schema     string `json:"schema"`
	Name       string `json:"name"`
	DistColumn string `json:"distribution_column"`
	Bytes      int64  `json:"bytes"`
}

func colocationInspectorTool(ctx context.Context, deps Dependencies, input ColocationInspectorInput) (*mcp.CallToolResult, ColocationInspectorOutput, error) {
	emptyOutput := ColocationInspectorOutput{Groups: []ColocationGroup{}}

	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), emptyOutput, nil
	}

	limit := input.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	warnings := []string{}

	// Get colocation groups
	q := `
SELECT 
	colocationid,
	shardcount,
	replicationfactor,
	COALESCE(distributioncolumntype::text, 'unknown') as dist_col_type
FROM pg_dist_colocation
`

	var rows pgx.Rows
	var err error

	if input.ColocationID != nil {
		q += " WHERE colocationid = $1 ORDER BY colocationid LIMIT $2"
		rows, err = deps.Pool.Query(ctx, q, *input.ColocationID, limit)
	} else {
		q += " ORDER BY colocationid LIMIT $1"
		rows, err = deps.Pool.Query(ctx, q, limit)
	}

	if err != nil {
		// Try fallback via pg_dist_partition
		warnings = append(warnings, "pg_dist_colocation not available, using fallback")
		return fallbackColocationInspector(ctx, deps, input, limit, warnings)
	}
	defer rows.Close()

	groups := []ColocationGroup{}
	var totalTables int
	var largestGroupID int32
	var largestGroupSize int

	for rows.Next() {
		var g ColocationGroup
		if err := rows.Scan(&g.ColocationID, &g.ShardCount, &g.ReplicationFactor, &g.DistColumn); err != nil {
			warnings = append(warnings, "scan error: "+err.Error())
			continue
		}

		// Fetch tables in this colocation group
		tables, totalBytes := fetchColocationTables(ctx, deps, g.ColocationID)
		g.Tables = tables
		g.TotalBytes = totalBytes

		// Calculate skew ratio
		g.SkewRatio = calculateColocationSkew(ctx, deps, g.ColocationID)

		tableCount := len(tables)
		totalTables += tableCount
		if tableCount > largestGroupSize {
			largestGroupSize = tableCount
			largestGroupID = g.ColocationID
		}

		groups = append(groups, g)
	}

	if err := rows.Err(); err != nil {
		warnings = append(warnings, "error reading colocation groups: "+err.Error())
	}

	out := ColocationInspectorOutput{
		Summary: ColocationSummary{
			TotalGroups:      len(groups),
			TotalTables:      totalTables,
			LargestGroupID:   largestGroupID,
			LargestGroupSize: largestGroupSize,
			GeneratedAt:      time.Now().UTC().Format(time.RFC3339),
		},
		Groups:   groups,
		Warnings: warnings,
	}

	if len(warnings) == 0 {
		out.Warnings = nil
	}

	return nil, out, nil
}

func fetchColocationTables(ctx context.Context, deps Dependencies, colocationID int32) ([]ColocationTable, int64) {
	tables := []ColocationTable{}
	var totalBytes int64

	// Simple query to get tables in colocation group
	q := `
SELECT 
	n.nspname as schema,
	c.relname as name,
	COALESCE(pg_total_relation_size(p.logicalrelid), 0) as bytes
FROM pg_dist_partition p
JOIN pg_class c ON c.oid = p.logicalrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE p.colocationid = $1
ORDER BY n.nspname, c.relname`

	rows, err := deps.Pool.Query(ctx, q, colocationID)
	if err != nil {
		return tables, 0
	}
	defer rows.Close()

	for rows.Next() {
		var t ColocationTable
		if err := rows.Scan(&t.Schema, &t.Name, &t.Bytes); err != nil {
			continue
		}
		// Fetch distribution column separately
		var distCol *string
		_ = deps.Pool.QueryRow(ctx, `SELECT column_to_column_name(logicalrelid, partkey) FROM pg_dist_partition WHERE logicalrelid = $1::regclass`, t.Schema+"."+t.Name).Scan(&distCol)
		if distCol != nil {
			t.DistColumn = *distCol
		} else {
			t.DistColumn = "<none>"
		}
		totalBytes += t.Bytes
		tables = append(tables, t)
	}

	return tables, totalBytes
}

func calculateColocationSkew(ctx context.Context, deps Dependencies, colocationID int32) float64 {
	// Get shard distribution across nodes for this colocation group
	q := `
WITH shard_nodes AS (
	SELECT 
		sp.nodename || ':' || sp.nodeport as node,
		COUNT(*) as shard_count
	FROM pg_dist_shard s
	JOIN pg_dist_shard_placement sp ON sp.shardid = s.shardid
	JOIN pg_dist_partition p ON p.logicalrelid = s.logicalrelid
	WHERE p.colocationid = $1
	GROUP BY sp.nodename, sp.nodeport
)
SELECT 
	CASE WHEN MIN(shard_count) > 0 
		THEN MAX(shard_count)::float / MIN(shard_count)::float 
		ELSE 1.0 
	END as skew_ratio
FROM shard_nodes`

	var skewRatio float64
	if err := deps.Pool.QueryRow(ctx, q, colocationID).Scan(&skewRatio); err != nil {
		return 1.0
	}
	return skewRatio
}

func fallbackColocationInspector(ctx context.Context, deps Dependencies, input ColocationInspectorInput, limit int, warnings []string) (*mcp.CallToolResult, ColocationInspectorOutput, error) {
	// Use pg_dist_partition to get colocation info
	q := `
SELECT DISTINCT colocationid
FROM pg_dist_partition
WHERE colocationid IS NOT NULL
ORDER BY colocationid
LIMIT $1`

	rows, err := deps.Pool.Query(ctx, q, limit)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), ColocationInspectorOutput{Groups: []ColocationGroup{}, Warnings: warnings}, nil
	}
	defer rows.Close()

	groups := []ColocationGroup{}
	var totalTables int
	var largestGroupID int32
	var largestGroupSize int

	for rows.Next() {
		var colocationID int32
		if err := rows.Scan(&colocationID); err != nil {
			continue
		}

		if input.ColocationID != nil && colocationID != *input.ColocationID {
			continue
		}

		// Fetch tables and infer colocation properties
		tables, totalBytes := fetchColocationTables(ctx, deps, colocationID)

		g := ColocationGroup{
			ColocationID: colocationID,
			Tables:       tables,
			TotalBytes:   totalBytes,
			SkewRatio:    calculateColocationSkew(ctx, deps, colocationID),
		}

		// Try to get shard count from first table
		if len(tables) > 0 {
			var shardCount int32
			_ = deps.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_dist_shard s JOIN pg_class c ON c.oid = s.logicalrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2`, tables[0].Schema, tables[0].Name).Scan(&shardCount)
			g.ShardCount = shardCount
		}

		tableCount := len(tables)
		totalTables += tableCount
		if tableCount > largestGroupSize {
			largestGroupSize = tableCount
			largestGroupID = colocationID
		}

		groups = append(groups, g)
	}

	out := ColocationInspectorOutput{
		Summary: ColocationSummary{
			TotalGroups:      len(groups),
			TotalTables:      totalTables,
			LargestGroupID:   largestGroupID,
			LargestGroupSize: largestGroupSize,
			GeneratedAt:      time.Now().UTC().Format(time.RFC3339),
		},
		Groups:   groups,
		Warnings: warnings,
	}

	return nil, out, nil
}
