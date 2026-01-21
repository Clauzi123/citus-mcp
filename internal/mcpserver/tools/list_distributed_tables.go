package tools

import (
	"context"
	"encoding/base64"
	"sort"
	"strings"

	serr "citus-mcp/internal/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ListDistributedTablesV2Input for citus.list_distributed_tables tool.
type ListDistributedTablesV2Input struct {
	Schema string `json:"schema,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
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

	// Build query with pagination on (schema,name)
	// Using row values for pagination key comparison
	q := `
SELECT
    n.nspname AS schema,
    c.relname AS name,
    a.attname AS distribution_column,
    p.partmethod AS distribution_method,
    p.colocationid AS colocation_id,
    (SELECT count(*) FROM pg_dist_shard s WHERE s.logicalrelid = c.oid) AS shard_count,
    COALESCE(p.replication_model, 'na') AS replication_model,
    CASE
        WHEN p.partmethod = 'n' THEN 'reference'
        WHEN p.partmethod IS NOT NULL THEN 'distributed'
        ELSE 'local'
    END AS table_type,
    COALESCE((SELECT count(*) FROM pg_dist_shard_placement sp JOIN pg_dist_shard s ON sp.shardid = s.shardid WHERE s.logicalrelid = c.oid) / GREATEST(1, (SELECT count(*) FROM pg_dist_shard s WHERE s.logicalrelid = c.oid)), 1) AS replication_factor
FROM pg_dist_partition p
JOIN pg_class c ON c.oid = p.logicalrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = p.partkey
WHERE ($1 = '' OR n.nspname = $1)
  AND (( $2 = '' AND $3 = '' ) OR (n.nspname, c.relname) > ($2, $3))
ORDER BY n.nspname, c.relname
LIMIT $4
`

	rows, err := deps.Pool.Query(ctx, q, schemaFilter, cursorSchema, cursorName, limit+1)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), ListDistributedTablesV2Output{}, nil
	}
	defer rows.Close()

	tables := make([]DistributedTable, 0, limit)
	var nextCursor string
	for rows.Next() {
		if len(tables) == limit {
			// one extra row for next cursor
			var s, name string
			if err := rows.Scan(&s, &name, new(string), new(string), new(int32), new(pgtype.Int4), new(string), new(string), new(pgtype.Int4)); err == nil {
				nextCursor = encodeCursor(s, name)
			}
			break
		}
		var t DistributedTable
		var replModel string
		var replFactor pgtype.Int4
		if err := rows.Scan(&t.Schema, &t.Name, &t.DistributionColumn, &t.DistributionMethod, &t.ColocationID, &t.ShardCount, &replModel, &t.TableType, &replFactor); err != nil {
			return callError(serr.CodeInternalError, err.Error(), "scan error"), ListDistributedTablesV2Output{}, nil
		}
		if replFactor.Valid {
			t.ReplicationFactor = replFactor.Int32
		} else {
			t.ReplicationFactor = 1
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), ListDistributedTablesV2Output{}, nil
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

func encodeCursor(schema, name string) string {
	return base64.StdEncoding.EncodeToString([]byte(schema + "\t" + name))
}
