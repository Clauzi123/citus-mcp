package tools

import (
	"context"

	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// LockInspectorInput controls lock inspection options.
type LockInspectorInput struct {
	Limit        int  `json:"limit,omitempty"`
	IncludeLocks bool `json:"include_locks,omitempty"`
}

// LockWait represents a blocking relationship.
type LockWait struct {
	WaitingGPID       int64  `json:"waiting_gpid"`
	BlockingGPID      int64  `json:"blocking_gpid"`
	WaitingNodeID     int32  `json:"waiting_nodeid"`
	BlockingNodeID    int32  `json:"blocking_nodeid"`
	BlockedStatement  string `json:"blocked_statement"`
	BlockingStatement string `json:"blocking_statement"`
}

// LockItem represents a lock entry.
type LockItem struct {
	GlobalPID    int64   `json:"global_pid"`
	NodeID       int32   `json:"nodeid"`
	LockType     string  `json:"locktype"`
	RelationName *string `json:"relation_name,omitempty"`
	Mode         string  `json:"mode"`
	Granted      bool    `json:"granted"`
	WaitStart    *string `json:"waitstart,omitempty"`
}

// LockInspectorOutput aggregates lock waits and optional locks.
type LockInspectorOutput struct {
	Waits []LockWait `json:"waits"`
	Locks []LockItem `json:"locks,omitempty"`
}

func citusLockInspectorTool(ctx context.Context, deps Dependencies, input LockInspectorInput) (*mcp.CallToolResult, LockInspectorOutput, error) {
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), LockInspectorOutput{}, nil
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 50
	}

	out := LockInspectorOutput{Waits: []LockWait{}, Locks: []LockItem{}}

	// Fetch waits
	qWaits := `SELECT waiting_gpid, blocking_gpid, waiting_nodeid, blocking_nodeid, left(blocked_statement, 2000), left(current_statement_in_blocking_process, 2000)
FROM pg_catalog.citus_lock_waits LIMIT $1`
	rows, err := deps.Pool.Query(ctx, qWaits, limit)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var w LockWait
			if err := rows.Scan(&w.WaitingGPID, &w.BlockingGPID, &w.WaitingNodeID, &w.BlockingNodeID, &w.BlockedStatement, &w.BlockingStatement); err != nil {
				continue
			}
			out.Waits = append(out.Waits, w)
		}
	}

	// Optionally fetch locks
	if input.IncludeLocks {
		qLocks := `SELECT global_pid, nodeid, locktype, relation_name, mode, granted, to_char(waitstart, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
FROM pg_catalog.citus_locks ORDER BY waitstart NULLS LAST LIMIT $1`
		rows2, err := deps.Pool.Query(ctx, qLocks, limit)
		if err == nil {
			defer rows2.Close()
			for rows2.Next() {
				var l LockItem
				if err := rows2.Scan(&l.GlobalPID, &l.NodeID, &l.LockType, &l.RelationName, &l.Mode, &l.Granted, &l.WaitStart); err != nil {
					continue
				}
				out.Locks = append(out.Locks, l)
			}
		}
	}

	return nil, out, nil
}
