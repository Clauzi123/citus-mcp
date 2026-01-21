package tools

import (
	"context"
	"encoding/base64"
	"strconv"

	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RebalanceStatusInput for citus_rebalance_status.
type RebalanceStatusInput struct {
	Verbose bool   `json:"verbose,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	Cursor  string `json:"cursor,omitempty"`
}

// RebalanceStatusOutput out.
type RebalanceStatusOutput struct {
	Rows []map[string]any `json:"rows"`
	Next string           `json:"next_cursor,omitempty"`
}

func rebalanceStatusTool(ctx context.Context, deps Dependencies, input RebalanceStatusInput) (*mcp.CallToolResult, RebalanceStatusOutput, error) {
	limit := input.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	var cursor string
	if input.Cursor != "" {
		decoded, err := base64.StdEncoding.DecodeString(input.Cursor)
		if err == nil {
			cursor = string(decoded)
		}
	}

	// Try citus_rebalance_status first
	if deps.Capabilities != nil && deps.Capabilities.SupportsRebalanceStatus() {
		return rebalanceStatusQuery(ctx, deps, "SELECT * FROM citus_rebalance_status()", limit, cursor)
	}
	// fallback
	if deps.Capabilities != nil && deps.Capabilities.SupportsRebalanceProgress() {
		return rebalanceStatusQuery(ctx, deps, "SELECT * FROM get_rebalance_progress()", limit, cursor)
	}
	return callError(serr.CodeCapabilityMissing, "rebalance status not supported", "Upgrade Citus"), RebalanceStatusOutput{Rows: []map[string]any{}}, nil
}

func rebalanceStatusQuery(ctx context.Context, deps Dependencies, q string, limit int, cursor string) (*mcp.CallToolResult, RebalanceStatusOutput, error) {
	// Very simple pagination: assume rows have an implicit order; we fetch limit+1 and use row offset cursor.
	offset := 0
	if cursor != "" {
		if off, err := strconv.Atoi(cursor); err == nil && off >= 0 {
			offset = off
		}
	}
	qWithLimit := q + " OFFSET " + strconv.Itoa(offset) + " LIMIT " + strconv.Itoa(limit+1)
	rows, err := deps.Pool.Query(ctx, qWithLimit)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), RebalanceStatusOutput{Rows: []map[string]any{}}, nil
	}
	defer rows.Close()

	fds := rows.FieldDescriptions()
	cols := make([]string, len(fds))
	for i, fd := range fds {
		cols[i] = string(fd.Name)
	}

	var outRows []map[string]any
	nextCursor := ""
	for rows.Next() {
		if len(outRows) == limit {
			nextCursor = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset + limit)))
			break
		}
		vals, err := rows.Values()
		if err != nil {
			return callError(serr.CodeInternalError, err.Error(), "scan error"), RebalanceStatusOutput{Rows: []map[string]any{}}, nil
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			row[c] = vals[i]
		}
		outRows = append(outRows, row)
	}
	if err := rows.Err(); err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), RebalanceStatusOutput{Rows: []map[string]any{}}, nil
	}

	// Stable ordering is provided by the functions themselves; we rely on default order.
	if outRows == nil {
		outRows = []map[string]any{}
	}
	return nil, RebalanceStatusOutput{Rows: outRows, Next: nextCursor}, nil
}
