// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_activity tool for cluster-wide query monitoring.

package tools

import (
	"context"
	"time"

	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ActivityInput for citus_activity tool.
type ActivityInput struct {
	Limit           int  `json:"limit,omitempty"`
	IncludeIdle     bool `json:"include_idle,omitempty"`
	MinDurationSecs int  `json:"min_duration_secs,omitempty"`
}

// ActivityOutput shows cluster-wide activity.
type ActivityOutput struct {
	Summary    ActivitySummary  `json:"summary"`
	Activities []ActivityRecord `json:"activities"`
	TopByTime  []ActivityRecord `json:"top_by_time,omitempty"`
	Warnings   []string         `json:"warnings,omitempty"`
}

type ActivitySummary struct {
	TotalConnections   int     `json:"total_connections"`
	ActiveQueries      int     `json:"active_queries"`
	IdleConnections    int     `json:"idle_connections"`
	WaitingOnLocks     int     `json:"waiting_on_locks"`
	LongestRunningMins float64 `json:"longest_running_mins"`
	GeneratedAt        string  `json:"generated_at"`
}

type ActivityRecord struct {
	PID           int32   `json:"pid"`
	User          string  `json:"user"`
	Database      string  `json:"database"`
	State         string  `json:"state"`
	WaitEventType *string `json:"wait_event_type,omitempty"`
	WaitEvent     *string `json:"wait_event,omitempty"`
	Query         string  `json:"query"`
	DurationSecs  float64 `json:"duration_secs"`
	BackendStart  string  `json:"backend_start,omitempty"`
	QueryStart    *string `json:"query_start,omitempty"`
	NodeName      string  `json:"node_name,omitempty"`
	NodePort      int32   `json:"node_port,omitempty"`
}

func activityTool(ctx context.Context, deps Dependencies, input ActivityInput) (*mcp.CallToolResult, ActivityOutput, error) {
	emptyOutput := ActivityOutput{Activities: []ActivityRecord{}, TopByTime: []ActivityRecord{}}

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

	minDuration := input.MinDurationSecs
	warnings := []string{}

	// Try citus_stat_activity first (cluster-wide)
	q := `
SELECT 
	pid,
	usename,
	datname,
	state,
	wait_event_type,
	wait_event,
	LEFT(query, 500) as query,
	EXTRACT(EPOCH FROM (now() - query_start))::float8 as duration_secs,
	to_char(backend_start, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as backend_start,
	to_char(query_start, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as query_start,
	COALESCE(nodename, 'coordinator') as node_name,
	COALESCE(nodeport, 0) as node_port
FROM citus_stat_activity
WHERE pid <> pg_backend_pid()
`
	if !input.IncludeIdle {
		q += " AND state <> 'idle'"
	}
	if minDuration > 0 {
		q += " AND EXTRACT(EPOCH FROM (now() - query_start)) >= " + string(rune(minDuration+'0'))
	}
	q += " ORDER BY duration_secs DESC NULLS LAST LIMIT $1"

	rows, err := deps.Pool.Query(ctx, q, limit)
	if err != nil {
		// Fallback to pg_stat_activity (coordinator only)
		warnings = append(warnings, "citus_stat_activity unavailable; showing coordinator only")
		return fallbackActivity(ctx, deps, input, limit, warnings)
	}
	defer rows.Close()

	activities := []ActivityRecord{}
	var totalConns, activeQueries, idleConns, waitingLocks int
	var longestRunning float64

	for rows.Next() {
		var r ActivityRecord
		var user, db, state, query, backendStart, nodeName *string
		var queryStart, waitEventType, waitEvent *string
		var durationSecs *float64
		var nodePort *int32

		if err := rows.Scan(&r.PID, &user, &db, &state, &waitEventType, &waitEvent, &query, &durationSecs, &backendStart, &queryStart, &nodeName, &nodePort); err != nil {
			continue
		}

		if user != nil {
			r.User = *user
		}
		if db != nil {
			r.Database = *db
		}
		if state != nil {
			r.State = *state
		}
		if query != nil {
			r.Query = *query
		}
		if backendStart != nil {
			r.BackendStart = *backendStart
		}
		if queryStart != nil {
			r.QueryStart = queryStart
		}
		if nodeName != nil {
			r.NodeName = *nodeName
		}
		if nodePort != nil {
			r.NodePort = *nodePort
		}
		r.WaitEventType = waitEventType
		r.WaitEvent = waitEvent
		if durationSecs != nil {
			r.DurationSecs = *durationSecs
			if *durationSecs > longestRunning {
				longestRunning = *durationSecs
			}
		}

		totalConns++
		if r.State == "active" {
			activeQueries++
		} else if r.State == "idle" {
			idleConns++
		}
		if r.WaitEventType != nil && *r.WaitEventType == "Lock" {
			waitingLocks++
		}

		activities = append(activities, r)
	}

	if err := rows.Err(); err != nil {
		warnings = append(warnings, "error reading activity: "+err.Error())
	}

	// Get top by time (first 10)
	topByTime := []ActivityRecord{}
	for i, a := range activities {
		if i >= 10 {
			break
		}
		if a.State == "active" {
			topByTime = append(topByTime, a)
		}
	}

	out := ActivityOutput{
		Summary: ActivitySummary{
			TotalConnections:   totalConns,
			ActiveQueries:      activeQueries,
			IdleConnections:    idleConns,
			WaitingOnLocks:     waitingLocks,
			LongestRunningMins: longestRunning / 60.0,
			GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
		},
		Activities: activities,
		TopByTime:  topByTime,
		Warnings:   warnings,
	}

	if len(warnings) == 0 {
		out.Warnings = nil
	}

	return nil, out, nil
}

func fallbackActivity(ctx context.Context, deps Dependencies, input ActivityInput, limit int, warnings []string) (*mcp.CallToolResult, ActivityOutput, error) {
	emptyOutput := ActivityOutput{Activities: []ActivityRecord{}, TopByTime: []ActivityRecord{}, Warnings: warnings}

	q := `
SELECT 
	pid,
	usename,
	datname,
	state,
	wait_event_type,
	wait_event,
	LEFT(query, 500) as query,
	EXTRACT(EPOCH FROM (now() - query_start))::float8 as duration_secs,
	to_char(backend_start, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as backend_start,
	to_char(query_start, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as query_start
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
`
	if !input.IncludeIdle {
		q += " AND state <> 'idle'"
	}
	q += " ORDER BY query_start ASC NULLS LAST LIMIT $1"

	rows, err := deps.Pool.Query(ctx, q, limit)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), emptyOutput, nil
	}
	defer rows.Close()

	activities := []ActivityRecord{}
	var totalConns, activeQueries, idleConns, waitingLocks int
	var longestRunning float64

	for rows.Next() {
		var r ActivityRecord
		var user, db, state, query, backendStart *string
		var queryStart, waitEventType, waitEvent *string
		var durationSecs *float64

		if err := rows.Scan(&r.PID, &user, &db, &state, &waitEventType, &waitEvent, &query, &durationSecs, &backendStart, &queryStart); err != nil {
			continue
		}

		if user != nil {
			r.User = *user
		}
		if db != nil {
			r.Database = *db
		}
		if state != nil {
			r.State = *state
		}
		if query != nil {
			r.Query = *query
		}
		if backendStart != nil {
			r.BackendStart = *backendStart
		}
		r.QueryStart = queryStart
		r.WaitEventType = waitEventType
		r.WaitEvent = waitEvent
		r.NodeName = "coordinator"
		if durationSecs != nil {
			r.DurationSecs = *durationSecs
			if *durationSecs > longestRunning {
				longestRunning = *durationSecs
			}
		}

		totalConns++
		if r.State == "active" {
			activeQueries++
		} else if r.State == "idle" {
			idleConns++
		}
		if r.WaitEventType != nil && *r.WaitEventType == "Lock" {
			waitingLocks++
		}

		activities = append(activities, r)
	}

	topByTime := []ActivityRecord{}
	for i, a := range activities {
		if i >= 10 {
			break
		}
		if a.State == "active" {
			topByTime = append(topByTime, a)
		}
	}

	out := ActivityOutput{
		Summary: ActivitySummary{
			TotalConnections:   totalConns,
			ActiveQueries:      activeQueries,
			IdleConnections:    idleConns,
			WaitingOnLocks:     waitingLocks,
			LongestRunningMins: longestRunning / 60.0,
			GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
		},
		Activities: activities,
		TopByTime:  topByTime,
		Warnings:   warnings,
	}

	return nil, out, nil
}
