// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_move_shard_execute tool for shard migration.

package tools

import (
	"context"
	"time"

	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MoveShardExecuteInput for citus_move_shard_execute.
type MoveShardExecuteInput struct {
	ApprovalToken string `json:"approval_token" jsonschema:"required"`
	ShardID       int64  `json:"shard_id" jsonschema:"required"`
	SourceHost    string `json:"source_host" jsonschema:"required"`
	SourcePort    int    `json:"source_port" jsonschema:"required"`
	TargetHost    string `json:"target_host" jsonschema:"required"`
	TargetPort    int    `json:"target_port" jsonschema:"required"`
	Colocated     bool   `json:"colocated,omitempty"`
	DropMethod    string `json:"drop_method,omitempty"`
}

// MoveShardExecuteOutput result.
type MoveShardExecuteOutput struct {
	Started      bool       `json:"started"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	Message      string     `json:"message"`
	Instructions string     `json:"instructions,omitempty"`
}

func moveShardExecuteTool(ctx context.Context, deps Dependencies, input MoveShardExecuteInput) (*mcp.CallToolResult, MoveShardExecuteOutput, error) {
	// Execute tool guard
	if err := deps.Guardrails.RequireToolAllowed("citus_move_shard_execute", true, input.ApprovalToken); err != nil {
		if me, ok := err.(*serr.CitusMCPError); ok {
			return callError(me.Code, me.Message, me.Hint), MoveShardExecuteOutput{}, nil
		}
		return callError(serr.CodeExecuteDisabled, err.Error(), ""), MoveShardExecuteOutput{}, nil
	}

	if deps.Capabilities == nil || !deps.Capabilities.SupportsShardMove() {
		// If enterprise-only function exists, hint
		if deps.Capabilities != nil && deps.Capabilities.SupportsMasterMoveShardPlacement() {
			return callError(serr.CodeCapabilityMissing, "citus_move_shard_placement unavailable", "Enterprise master_move_shard_placement detected; feature not supported"), MoveShardExecuteOutput{}, nil
		}
		return callError(serr.CodeCapabilityMissing, "citus_move_shard_placement not available", "Upgrade Citus"), MoveShardExecuteOutput{}, nil
	}

	// call citus_move_shard_placement
	const q = `SELECT citus_move_shard_placement($1,$2,$3,$4,$5)`
	if _, err := deps.Pool.Exec(ctx, q, input.ShardID, input.SourceHost, input.SourcePort, input.TargetHost, input.TargetPort); err != nil {
		return callError(serr.CodeInternalError, err.Error(), "failed to move shard"), MoveShardExecuteOutput{}, nil
	}

	now := time.Now()
	return nil, MoveShardExecuteOutput{Started: true, StartedAt: &now, Message: "Shard move started", Instructions: "Verify placement via list_shards or citus_rebalance_status"}, nil
}
