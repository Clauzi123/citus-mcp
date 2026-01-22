// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_explain_query tool for distributed query plans.

package tools

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ExplainQueryInput defines input for citus.explain_query.
type ExplainQueryInput struct {
	SQL           string `json:"sql" jsonschema:"required"`
	Analyze       bool   `json:"analyze,omitempty"`
	Verbose       bool   `json:"verbose,omitempty"`
	Costs         bool   `json:"costs,omitempty"`
	ApprovalToken string `json:"approval_token,omitempty"`
}

// ExplainQueryOutput contains explain text and notes.
type ExplainQueryOutput struct {
	ExplainText string       `json:"explain_text"`
	Notes       ExplainNotes `json:"notes"`
}

type ExplainNotes struct {
	Distributed bool `json:"distributed"`
	Router      bool `json:"router"`
	TaskCount   *int `json:"task_count,omitempty"`
}

func explainQueryTool(ctx context.Context, deps Dependencies, input ExplainQueryInput) (*mcp.CallToolResult, ExplainQueryOutput, error) {
	if strings.TrimSpace(input.SQL) == "" {
		return callError(serr.CodeInvalidInput, "sql is required", ""), ExplainQueryOutput{}, nil
	}
	// Determine tool category
	isExecute := input.Analyze
	if err := deps.Guardrails.RequireToolAllowed("citus.explain_query", isExecute, input.ApprovalToken); err != nil {
		if me, ok := err.(*serr.CitusMCPError); ok {
			return callError(me.Code, me.Message, me.Hint), ExplainQueryOutput{}, nil
		}
		return callError(serr.CodePermissionDenied, err.Error(), ""), ExplainQueryOutput{}, nil
	}
	if !input.Analyze {
		if err := deps.Guardrails.RequireReadOnlySQL(input.SQL); err != nil {
			return callError(serr.CodePermissionDenied, err.Error(), ""), ExplainQueryOutput{}, nil
		}
	}

	query := buildExplainQuery(input)
	rows, err := deps.Pool.Query(ctx, query)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), ExplainQueryOutput{}, nil
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var ln string
		if err := rows.Scan(&ln); err != nil {
			return callError(serr.CodeInternalError, err.Error(), "scan error"), ExplainQueryOutput{}, nil
		}
		lines = append(lines, ln)
	}
	if err := rows.Err(); err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), ExplainQueryOutput{}, nil
	}

	notes := parseExplainNotes(lines)
	return nil, ExplainQueryOutput{ExplainText: strings.Join(lines, "\n"), Notes: notes}, nil
}

// ExplainQuery is exported for integration/tests.
func ExplainQuery(ctx context.Context, deps Dependencies, input ExplainQueryInput) (*mcp.CallToolResult, ExplainQueryOutput, error) {
	return explainQueryTool(ctx, deps, input)
}

func buildExplainQuery(input ExplainQueryInput) string {
	opts := []string{"FORMAT TEXT"}
	if input.Analyze {
		opts = append(opts, "ANALYZE TRUE")
	}
	if input.Verbose {
		opts = append(opts, "VERBOSE TRUE")
	}
	if !input.Costs {
		opts = append(opts, "COSTS FALSE")
	}
	return "EXPLAIN (" + strings.Join(opts, ", ") + ") " + input.SQL
}

var taskCountRegex = regexp.MustCompile(`(?i)Task Count:\s*(\d+)`)

func parseExplainNotes(lines []string) ExplainNotes {
	var notes ExplainNotes
	for _, ln := range lines {
		if strings.Contains(strings.ToLower(ln), "distributed query") {
			notes.Distributed = true
		}
		if strings.Contains(strings.ToLower(ln), "router query") {
			notes.Router = true
		}
		if m := taskCountRegex.FindStringSubmatch(ln); len(m) == 2 {
			if v, err := strconv.Atoi(m[1]); err == nil {
				notes.TaskCount = &v
			}
		}
	}
	return notes
}
