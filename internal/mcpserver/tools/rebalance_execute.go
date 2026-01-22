// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_rebalance_execute tool for cluster rebalancing.

package tools

import (
	"context"
	"time"

	"citus-mcp/internal/citus"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RebalanceExecuteInput for citus_rebalance_execute.
type RebalanceExecuteInput struct {
	ApprovalToken     string   `json:"approval_token" jsonschema:"required"`
	Table             string   `json:"table,omitempty"`
	Threshold         *float64 `json:"threshold,omitempty"`
	MaxShardMoves     *int     `json:"max_shard_moves,omitempty"`
	ExcludedShardList []int64  `json:"excluded_shard_list,omitempty"`
	DrainOnly         *bool    `json:"drain_only,omitempty"`
}

// RebalanceExecuteOutput result.
type RebalanceExecuteOutput struct {
	Started      bool       `json:"started"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	Instructions string     `json:"instructions,omitempty"`
}

func rebalanceExecuteTool(ctx context.Context, deps Dependencies, input RebalanceExecuteInput) (*mcp.CallToolResult, RebalanceExecuteOutput, error) {
	// Execute tool: require token & allow_execute
	if err := deps.Guardrails.RequireToolAllowed("citus_rebalance_execute", true, input.ApprovalToken); err != nil {
		if me, ok := err.(*serr.CitusMCPError); ok {
			return callError(me.Code, me.Message, me.Hint), RebalanceExecuteOutput{}, nil
		}
		return callError(serr.CodeExecuteDisabled, err.Error(), ""), RebalanceExecuteOutput{}, nil
	}

	// Capability check
	if deps.Capabilities == nil || !deps.Capabilities.SupportsRebalanceStart() {
		return callError(serr.CodeCapabilityMissing, "citus_rebalance_start not available", "Upgrade Citus or use plan/execute manually"), RebalanceExecuteOutput{}, nil
	}

	// Check already running
	if running, err := citus.IsRebalanceRunning(ctx, deps.Pool); err == nil && running {
		return callError(serr.CodeInternalError, "rebalance already running", "use citus_rebalance_status"), RebalanceExecuteOutput{}, nil
	}

	var tablePtr *string
	if input.Table != "" {
		table := input.Table
		tablePtr = &table
	}
	if err := citus.StartRebalance(ctx, deps.Pool, tablePtr, input.Threshold, input.MaxShardMoves, input.ExcludedShardList, input.DrainOnly); err != nil {
		return callError(serr.CodeInternalError, err.Error(), "insufficient privileges or running"), RebalanceExecuteOutput{}, nil
	}
	now := time.Now()
	return nil, RebalanceExecuteOutput{Started: true, StartedAt: &now, Instructions: "Use citus_rebalance_status to check progress"}, nil
}
