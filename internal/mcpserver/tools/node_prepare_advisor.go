// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_node_prepare_advisor for pre-flight node checks.

package tools

import (
	"context"

	"citus-mcp/internal/citus/nodeprep"
	serr "citus-mcp/internal/errors"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NodePrepareInput defines input for the citus_node_prepare_advisor tool.
type NodePrepareInput struct {
	Host             string `json:"host"`
	Port             int    `json:"port,omitempty"`
	Database         string `json:"database,omitempty"`
	User             string `json:"user,omitempty"`
	Password         string `json:"password,omitempty"`
	ConnectionString string `json:"connection_string,omitempty"`
	GenerateScript   bool   `json:"generate_script,omitempty"`
	SSLMode          string `json:"sslmode,omitempty"`
}

// NodePrepareOutput wraps nodeprep.Output for tool output.
type NodePrepareOutput struct {
	Ready                    bool                   `json:"ready"`
	Summary                  nodeprep.Summary       `json:"summary"`
	Checks                   []nodeprep.CheckResult `json:"checks"`
	PreparationScript        *nodeprep.PrepScript   `json:"preparation_script,omitempty"`
	EstimatedIssuesPrevented []string               `json:"estimated_issues_prevented,omitempty"`
	ConnectionError          string                 `json:"connection_error,omitempty"`
	Warnings                 []string               `json:"warnings,omitempty"`
}

func nodePrepareAdvisorTool(ctx context.Context, deps Dependencies, input NodePrepareInput) (*mcp.CallToolResult, NodePrepareOutput, error) {
	emptyOutput := NodePrepareOutput{
		Checks:                   []nodeprep.CheckResult{},
		EstimatedIssuesPrevented: []string{},
		Warnings:                 []string{},
	}

	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), emptyOutput, nil
	}

	// Validate input
	if input.Host == "" && input.ConnectionString == "" {
		return callError(serr.CodeInvalidInput, "host or connection_string is required", ""), emptyOutput, nil
	}

	// Set defaults
	if input.Port == 0 {
		input.Port = 5432
	}
	if input.Database == "" {
		input.Database = "postgres"
	}
	if input.SSLMode == "" {
		input.SSLMode = "prefer"
	}

	advisor := nodeprep.NewAdvisor(deps.Pool)
	out, err := advisor.Run(ctx, nodeprep.Input{
		Host:             input.Host,
		Port:             input.Port,
		Database:         input.Database,
		User:             input.User,
		Password:         input.Password,
		ConnectionString: input.ConnectionString,
		GenerateScript:   input.GenerateScript,
		SSLMode:          input.SSLMode,
	})
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), emptyOutput, nil
	}

	// Convert to output type
	output := NodePrepareOutput{
		Ready:                    out.Ready,
		Summary:                  out.Summary,
		Checks:                   out.Checks,
		PreparationScript:        out.PreparationScript,
		EstimatedIssuesPrevented: out.EstimatedIssuesPrevented,
		ConnectionError:          out.ConnectionError,
		Warnings:                 out.Warnings,
	}

	// Ensure non-nil slices for JSON
	if output.Checks == nil {
		output.Checks = []nodeprep.CheckResult{}
	}
	if output.EstimatedIssuesPrevented == nil {
		output.EstimatedIssuesPrevented = []string{}
	}
	if output.Warnings == nil {
		output.Warnings = []string{}
	}

	return nil, output, nil
}

// CitusNodePrepareAdvisor is the exported function for the tool.
func CitusNodePrepareAdvisor(ctx context.Context, deps Dependencies, input NodePrepareInput) (*mcp.CallToolResult, NodePrepareOutput, error) {
	return nodePrepareAdvisorTool(ctx, deps, input)
}
