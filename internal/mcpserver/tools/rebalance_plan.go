// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_rebalance_plan tool for rebalance preview.

package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"citus-mcp/internal/citus"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RebalancePlanInput for citus_rebalance_plan.
type RebalancePlanInput struct {
	Table             string   `json:"table,omitempty"`
	Threshold         *float64 `json:"threshold,omitempty"`
	MaxShardMoves     *int     `json:"max_shard_moves,omitempty"`
	ExcludedShardList []int64  `json:"excluded_shard_list,omitempty"`
	DrainOnly         *bool    `json:"drain_only,omitempty"`
}

// RebalancePlanOutput out.
type RebalancePlanOutput struct {
	PlanID   string                `json:"plan_id"`
	Moves    []citus.RebalanceMove `json:"moves"`
	Summary  RebalancePlanSummary  `json:"summary"`
	Warnings []string              `json:"warnings,omitempty"`
}

type RebalancePlanSummary struct {
	TotalMoves int   `json:"total_moves"`
	TotalBytes int64 `json:"total_bytes"`
}

func rebalancePlanTool(ctx context.Context, deps Dependencies, input RebalancePlanInput) (*mcp.CallToolResult, RebalancePlanOutput, error) {
	// Check capability
	if deps.Capabilities == nil || !deps.Capabilities.SupportsRebalancePlan() {
		return callError(serr.CodeCapabilityMissing, "get_rebalance_table_shards_plan not available", "Upgrade Citus or use manual plan mode"), RebalancePlanOutput{}, nil
	}

	// For plan only, read-only is fine.
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), RebalancePlanOutput{}, nil
	}

	var tablePtr *string
	if input.Table != "" {
		table := input.Table
		tablePtr = &table
	}

	maxMoves := input.MaxShardMoves
	if maxMoves == nil {
		defaultMax := 100
		maxMoves = &defaultMax
	}
	moves, err := citus.GetRebalancePlan(ctx, deps.Pool, tablePtr, input.Threshold, maxMoves, input.ExcludedShardList, input.DrainOnly)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "citus plan error"), RebalancePlanOutput{Moves: []citus.RebalanceMove{}}, nil
	}
	if moves == nil {
		moves = []citus.RebalanceMove{}
	}

	totalBytes := int64(0)
	for _, m := range moves {
		totalBytes += m.ShardSize
	}
	planID := hashMoves(moves)

	warnings := []string{}
	if tablePtr != nil {
		if def, err := citus.HasReplicaIdentityDefault(ctx, deps.Pool, *tablePtr); err == nil && def {
			warnings = append(warnings, "table has default replica identity")
		}
	}

	out := RebalancePlanOutput{
		PlanID: planID,
		Moves:  moves,
		Summary: RebalancePlanSummary{
			TotalMoves: len(moves),
			TotalBytes: totalBytes,
		},
	}
	if len(warnings) > 0 {
		out.Warnings = warnings
	}
	return nil, out, nil
}

// RebalancePlan exported for integration/tests.
func RebalancePlan(ctx context.Context, deps Dependencies, input RebalancePlanInput) (*mcp.CallToolResult, RebalancePlanOutput, error) {
	return rebalancePlanTool(ctx, deps, input)
}

func hashMoves(moves []citus.RebalanceMove) string {
	h := sha256.New()
	for _, m := range moves {
		h.Write([]byte(m.TableName))
		h.Write([]byte("|"))
		h.Write([]byte(fmt.Sprintf("%d", m.ShardID)))
		h.Write([]byte("|"))
		h.Write([]byte(fmt.Sprintf("%d", m.ShardSize)))
		h.Write([]byte("|"))
		h.Write([]byte(m.SourceName))
		h.Write([]byte("|"))
		h.Write([]byte(fmt.Sprintf("%d", m.SourcePort)))
		h.Write([]byte("|"))
		h.Write([]byte(m.TargetName))
		h.Write([]byte("|"))
		h.Write([]byte(fmt.Sprintf("%d", m.TargetPort)))
		h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil))
}
