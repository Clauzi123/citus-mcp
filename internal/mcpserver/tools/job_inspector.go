// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_job_inspector tool for background job monitoring.

package tools

import (
	"context"
	"time"

	serr "citus-mcp/internal/errors"
	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// JobInspectorInput for citus_job_inspector tool.
type JobInspectorInput struct {
	Limit        int    `json:"limit,omitempty"`
	State        string `json:"state,omitempty"` // running, scheduled, finished, all
	IncludeTasks bool   `json:"include_tasks,omitempty"`
}

// JobInspectorOutput shows background jobs.
type JobInspectorOutput struct {
	Summary  JobSummary  `json:"summary"`
	Jobs     []JobRecord `json:"jobs"`
	Warnings []string    `json:"warnings,omitempty"`
}

type JobSummary struct {
	TotalJobs     int    `json:"total_jobs"`
	RunningJobs   int    `json:"running_jobs"`
	ScheduledJobs int    `json:"scheduled_jobs"`
	FailedJobs    int    `json:"failed_jobs"`
	GeneratedAt   string `json:"generated_at"`
}

type JobRecord struct {
	JobID       int64     `json:"job_id"`
	JobType     string    `json:"job_type"`
	State       string    `json:"state"`
	StartedAt   *string   `json:"started_at,omitempty"`
	FinishedAt  *string   `json:"finished_at,omitempty"`
	Details     string    `json:"details,omitempty"`
	Tasks       []JobTask `json:"tasks,omitempty"`
	TaskCount   int       `json:"task_count"`
	FailedTasks int       `json:"failed_tasks"`
}

type JobTask struct {
	TaskID     int64   `json:"task_id"`
	State      string  `json:"state"`
	Message    *string `json:"message,omitempty"`
	RetryCount int     `json:"retry_count"`
}

func jobInspectorTool(ctx context.Context, deps Dependencies, input JobInspectorInput) (*mcp.CallToolResult, JobInspectorOutput, error) {
	emptyOutput := JobInspectorOutput{Jobs: []JobRecord{}}

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

	state := input.State
	if state == "" {
		state = "all"
	}

	warnings := []string{}

	// Query pg_dist_background_job
	var rows pgx.Rows
	var err error

	if state != "all" {
		q := `
SELECT 
	job_id,
	COALESCE(job_type::text, 'unknown') as job_type,
	COALESCE(state::text, 'unknown') as state,
	to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as started_at,
	to_char(finished_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as finished_at,
	COALESCE(details, '') as details
FROM pg_dist_background_job
WHERE state = $2
ORDER BY job_id DESC LIMIT $1`
		rows, err = deps.Pool.Query(ctx, q, limit, state)
	} else {
		q := `
SELECT 
	job_id,
	COALESCE(job_type::text, 'unknown') as job_type,
	COALESCE(state::text, 'unknown') as state,
	to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as started_at,
	to_char(finished_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as finished_at,
	COALESCE(details, '') as details
FROM pg_dist_background_job
ORDER BY job_id DESC LIMIT $1`
		rows, err = deps.Pool.Query(ctx, q, limit)
	}

	if err != nil {
		warnings = append(warnings, "pg_dist_background_job not available: "+err.Error())
		return nil, JobInspectorOutput{
			Summary: JobSummary{
				GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			},
			Jobs:     []JobRecord{},
			Warnings: warnings,
		}, nil
	}
	defer rows.Close()

	jobs := []JobRecord{}
	var totalJobs, runningJobs, scheduledJobs, failedJobs int

	for rows.Next() {
		var j JobRecord
		var startedAt, finishedAt *string

		if err := rows.Scan(&j.JobID, &j.JobType, &j.State, &startedAt, &finishedAt, &j.Details); err != nil {
			warnings = append(warnings, "scan error: "+err.Error())
			continue
		}

		j.StartedAt = startedAt
		j.FinishedAt = finishedAt

		totalJobs++
		switch j.State {
		case "running":
			runningJobs++
		case "scheduled":
			scheduledJobs++
		case "failed":
			failedJobs++
		}

		if input.IncludeTasks {
			tasks, taskCount, failedTaskCount := fetchJobTasks(ctx, deps, j.JobID)
			j.Tasks = tasks
			j.TaskCount = taskCount
			j.FailedTasks = failedTaskCount
		} else {
			taskCount, failedTaskCount := fetchJobTaskCounts(ctx, deps, j.JobID)
			j.TaskCount = taskCount
			j.FailedTasks = failedTaskCount
		}

		jobs = append(jobs, j)
	}

	if err := rows.Err(); err != nil {
		warnings = append(warnings, "error reading jobs: "+err.Error())
	}

	out := JobInspectorOutput{
		Summary: JobSummary{
			TotalJobs:     totalJobs,
			RunningJobs:   runningJobs,
			ScheduledJobs: scheduledJobs,
			FailedJobs:    failedJobs,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		},
		Jobs:     jobs,
		Warnings: warnings,
	}

	if len(warnings) == 0 {
		out.Warnings = nil
	}

	return nil, out, nil
}

func fetchJobTasks(ctx context.Context, deps Dependencies, jobID int64) ([]JobTask, int, int) {
	tasks := []JobTask{}
	var taskCount, failedCount int

	q := `
SELECT 
	task_id,
	COALESCE(status::text, 'unknown') as state,
	message,
	COALESCE(retry_count, 0) as retry_count
FROM pg_dist_background_task
WHERE job_id = $1
ORDER BY task_id
LIMIT 100`

	rows, err := deps.Pool.Query(ctx, q, jobID)
	if err != nil {
		return tasks, 0, 0
	}
	defer rows.Close()

	for rows.Next() {
		var t JobTask
		if err := rows.Scan(&t.TaskID, &t.State, &t.Message, &t.RetryCount); err != nil {
			continue
		}
		taskCount++
		if t.State == "failed" || t.State == "error" {
			failedCount++
		}
		tasks = append(tasks, t)
	}

	return tasks, taskCount, failedCount
}

func fetchJobTaskCounts(ctx context.Context, deps Dependencies, jobID int64) (int, int) {
	var total, failed int

	q := `SELECT COUNT(*), COUNT(*) FILTER (WHERE status IN ('failed', 'error')) FROM pg_dist_background_task WHERE job_id = $1`
	_ = deps.Pool.QueryRow(ctx, q, jobID).Scan(&total, &failed)

	return total, failed
}
