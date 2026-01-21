package tools

import (
	"context"
	"encoding/base64"
	"sort"
	"strings"

	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ListDistributedTablesV2Input for citus_list_distributed_tables tool.
type ListDistributedTablesV2Input struct {
	Schema    string `json:"schema,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Cursor    string `json:"cursor,omitempty"`
	TableType string `json:"table_type,omitempty" enum:"distributed" enum:"reference" enum:"all"`
}

// ListDistributedTablesV2Output contains tables and pagination cursor.
type ListDistributedTablesV2Output struct {
	Tables []DistributedTable `json:"tables"`
	Next   string             `json:"next_cursor,omitempty"`
}

type DistributedTable struct {
	Schema             string `json:"schema"`
	Name               string `json:"name"`
	DistributionColumn string `json:"distribution_column"`
	DistributionMethod string `json:"distribution_method"`
	ColocationID       int32  `json:"colocation_id"`
	ShardCount         int32  `json:"shard_count"`
	ReplicationFactor  int32  `json:"replication_factor"`
	TableType          string `json:"table_type"`
}

func listDistributedTablesV2(ctx context.Context, deps Dependencies, input ListDistributedTablesV2Input) (*mcp.CallToolResult, ListDistributedTablesV2Output, error) {
	limit := input.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	schemaFilter := strings.TrimSpace(input.Schema)
	tableType := strings.TrimSpace(input.TableType)
	if tableType == "" {
		tableType = "distributed"
	}
	if tableType != "distributed" && tableType != "reference" && tableType != "all" {
		return callError(serr.CodeInvalidInput, "table_type must be one of [distributed, reference, all]", ""), ListDistributedTablesV2Output{}, nil
	}

	// parse cursor
	var cursorSchema, cursorName string
	if input.Cursor != "" {
		decoded, err := base64.StdEncoding.DecodeString(input.Cursor)
		if err != nil {
			return callError(serr.CodeInvalidInput, "invalid cursor", ""), ListDistributedTablesV2Output{}, nil
		}
		parts := strings.SplitN(string(decoded), "\t", 2)
		if len(parts) == 2 {
			cursorSchema, cursorName = parts[0], parts[1]
		}
	}

	// Prefer citus_tables view (Citus 14+) for consistent typing
	q := `
WITH t AS (
	SELECT
		table_name::text AS qualified_name,
		split_part(table_name::text, '.', 1) AS schema,
		split_part(table_name::text, '.', 2) AS name,
		distribution_column,
		colocation_id,
		shard_count,
		citus_table_type
	FROM citus_tables
	WHERE citus_table_type IN ('distributed','reference')
		AND ($1 = '' OR split_part(table_name::text, '.', 1) = $1)
)
SELECT schema, name, distribution_column, citus_table_type AS distribution_method, colocation_id, shard_count, citus_table_type AS table_type
FROM t
WHERE ($2 = '' AND $3 = '') OR (qualified_name > ($2 || '.' || $3))
ORDER BY qualified_name
LIMIT $4
`

	rows, err := deps.Pool.Query(ctx, q, schemaFilter, cursorSchema, cursorName, limit+1)
	if err != nil {
		// fallback to legacy if pagination query fails
		return fallbackListDistributedTables(ctx, deps, tableType, schemaFilter)
	}
	defer rows.Close()

	tables := make([]DistributedTable, 0, limit)
	var nextCursor string
	for rows.Next() {
		if len(tables) == limit {
			// one extra row for next cursor
			var s, name string
			if err := rows.Scan(&s, &name, new(string), new(string), new(int32), new(int32), new(string)); err == nil {
				nextCursor = encodeCursor(s, name)
			}
			break
		}
		var t DistributedTable
		if err := rows.Scan(&t.Schema, &t.Name, &t.DistributionColumn, &t.DistributionMethod, &t.ColocationID, &t.ShardCount, &t.TableType); err != nil {
			return callError(serr.CodeInternalError, err.Error(), "scan error"), ListDistributedTablesV2Output{}, nil
		}
		// replication_factor not available directly; default 1
		t.ReplicationFactor = 1
		if tableType == "all" || tableType == t.TableType || (tableType == "reference" && (t.TableType == "" && t.ShardCount == 1)) {
			tables = append(tables, t)
		}
	}
	if err := rows.Err(); err != nil {
		// fallback on error
		return fallbackListDistributedTables(ctx, deps, tableType, schemaFilter)
	}

	// stable ordering already ensured by ORDER BY
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema == tables[j].Schema {
			return tables[i].Name < tables[j].Name
		}
		return tables[i].Schema < tables[j].Schema
	})

	return nil, ListDistributedTablesV2Output{Tables: tables, Next: nextCursor}, nil
}

// ListDistributedTablesV2 is exported for resources.
func ListDistributedTablesV2(ctx context.Context, deps Dependencies, input ListDistributedTablesV2Input) (*mcp.CallToolResult, ListDistributedTablesV2Output, error) {
	return listDistributedTablesV2(ctx, deps, input)
}

// fallbackListDistributedTables maps legacy output to v2 shape when pagination query fails.
func fallbackListDistributedTables(ctx context.Context, deps Dependencies, tableType string, schemaFilter string) (*mcp.CallToolResult, ListDistributedTablesV2Output, error) {
	_, legacy, err := ListDistributedTables(ctx, deps, ListDistributedTablesInput{})
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), ListDistributedTablesV2Output{}, nil
	}
	out := ListDistributedTablesV2Output{Tables: []DistributedTable{}}
	for _, t := range legacy.Tables {
		schema, name := splitSchemaTable(t.LogicalRelID)
		if schemaFilter != "" && schema != schemaFilter {
			continue
		}
		tbl := DistributedTable{
			Schema:             schema,
			Name:               name,
			DistributionMethod: t.PartMethod,
			ColocationID:       t.ColocationID,
			ReplicationFactor:  1,
			TableType:          "distributed",
			ShardCount:         t.ShardCount,
		}
		if t.PartMethod == "n" || t.PartMethod == "none" {
			tbl.TableType = "reference"
			if tbl.ShardCount == 0 {
				tbl.ShardCount = 1
			}
		}
		if tableType != "all" && tableType != tbl.TableType {
			continue
		}
		out.Tables = append(out.Tables, tbl)
	}
	return nil, out, nil
}

func splitSchemaTable(relid string) (string, string) {
	idx := strings.LastIndex(relid, ".")
	if idx <= 0 {
		return "public", relid
	}
	return relid[:idx], relid[idx+1:]
}

func encodeCursor(schema, name string) string {
	return base64.StdEncoding.EncodeToString([]byte(schema + "\t" + name))
}
