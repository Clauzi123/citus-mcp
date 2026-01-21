package tools

import (
	"context"
	"strings"

	advisor "citus-mcp/internal/citus/advisor"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// CitusAdvisorInput defines input for citus_advisor (alias citus.advisor).
type CitusAdvisorInput struct {
	Focus              string `json:"focus,omitempty" enum:"rebalance" enum:"skew" enum:"performance" enum:"metadata" enum:"ops" enum:"all"`
	Schema             string `json:"schema,omitempty"`
	Table              string `json:"table,omitempty"`
	MaxTables          int    `json:"max_tables,omitempty"`
	MaxFindings        int    `json:"max_findings,omitempty"`
	IncludeSQLFixes    bool   `json:"include_sql_fixes,omitempty"`
	IncludeNextSteps   bool   `json:"include_next_steps,omitempty"`
	AllowQuerySampling bool   `json:"allow_query_sampling,omitempty"`
}

func normalizeAdvisorInput(in CitusAdvisorInput) advisor.Input {
	focus := strings.TrimSpace(in.Focus)
	if focus == "" {
		focus = "all"
	}
	if in.MaxTables <= 0 {
		in.MaxTables = 20
	}
	if in.MaxTables > 100 {
		in.MaxTables = 100
	}
	if in.MaxFindings <= 0 {
		in.MaxFindings = 25
	}
	if in.MaxFindings > 200 {
		in.MaxFindings = 200
	}
	return advisor.Input{
		Focus:              focus,
		Schema:             strings.TrimSpace(in.Schema),
		Table:              strings.TrimSpace(in.Table),
		MaxTables:          in.MaxTables,
		MaxFindings:        in.MaxFindings,
		IncludeSQLFixes:    in.IncludeSQLFixes,
		IncludeNextSteps:   in.IncludeNextSteps,
		AllowQuerySampling: in.AllowQuerySampling,
	}
}

func citusAdvisorTool(ctx context.Context, deps Dependencies, input CitusAdvisorInput) (*mcp.CallToolResult, advisor.Output, error) {
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), advisor.Output{}, nil
	}
	in := normalizeAdvisorInput(input)
	out, warnings, err := advisor.Run(ctx, deps.Pool, deps.Config, in)
	if err != nil {
		if me, ok := err.(*serr.CitusMCPError); ok {
			return callError(me.Code, me.Message, me.Hint), advisor.Output{}, nil
		}
		return callError(serr.CodeInternalError, err.Error(), ""), advisor.Output{}, nil
	}
	// append warnings
	if len(warnings) > 0 {
		out.Warnings = append(out.Warnings, warnings...)
	}
	return nil, out, nil
}

// CitusAdvisor exported
func CitusAdvisor(ctx context.Context, deps Dependencies, input CitusAdvisorInput) (*mcp.CallToolResult, advisor.Output, error) {
	return citusAdvisorTool(ctx, deps, input)
}
