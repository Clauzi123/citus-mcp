package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	serr "citus-mcp/internal/errors"
	"citus-mcp/internal/logging"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MoveShardPlanInput input for citus_move_shard_plan.
type MoveShardPlanInput struct {
	ShardID    int64  `json:"shard_id" jsonschema:"required"`
	SourceHost string `json:"source_host" jsonschema:"required"`
	SourcePort int    `json:"source_port" jsonschema:"required"`
	TargetHost string `json:"target_host" jsonschema:"required"`
	TargetPort int    `json:"target_port" jsonschema:"required"`
	Colocated  bool   `json:"colocated,omitempty"`
}

// MoveShardPlanOutput result.
type MoveShardPlanOutput struct {
	PlanID     string               `json:"plan_id"`
	Operations []MoveShardOperation `json:"operations"`
	Warnings   []string             `json:"warnings,omitempty"`
}

type MoveShardOperation struct {
	Function   string `json:"function"`
	Parameters []any  `json:"parameters"`
}

func moveShardPlanTool(ctx context.Context, deps Dependencies, input MoveShardPlanInput) (*mcp.CallToolResult, MoveShardPlanOutput, error) {
	// read-only tool; plan only
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), MoveShardPlanOutput{}, nil
	}

	if deps.Capabilities == nil || !deps.Capabilities.SupportsShardMove() {
		return callError(serr.CodeCapabilityMissing, "citus_move_shard_placement not available", "Upgrade Citus"), MoveShardPlanOutput{}, nil
	}

	warnings := []string{}
	// verify shard exists and placements
	placements, err := deps.Pool.Query(ctx, `
SELECT p.shardid, n.nodename, n.nodeport
FROM pg_dist_shard_placement p
JOIN pg_dist_node n ON n.nodeid = p.nodeid
WHERE p.shardid = $1
`, input.ShardID)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "db error"), MoveShardPlanOutput{}, nil
	}
	defer placements.Close()
	matches := false
	for placements.Next() {
		var sid int64
		var hn string
		var pt int32
		if err := placements.Scan(&sid, &hn, &pt); err != nil {
			return callError(serr.CodeInternalError, err.Error(), ""), MoveShardPlanOutput{}, nil
		}
		if hn == input.SourceHost && int(pt) == input.SourcePort {
			matches = true
		}
	}
	if err := placements.Err(); err != nil {
		return callError(serr.CodeInternalError, err.Error(), ""), MoveShardPlanOutput{}, nil
	}
	if !matches {
		warnings = append(warnings, "shard not found on source")
	}

	// check target node status
	if infos, err := deps.WorkerManager.Topology(ctx); err == nil {
		inactive := false
		for _, info := range infos {
			if info.NodeName == input.TargetHost && int(info.NodePort) == input.TargetPort {
				if !info.IsActive {
					inactive = true
				}
				break
			}
		}
		if inactive {
			warnings = append(warnings, "target node inactive")
		}
	}

	ops := []MoveShardOperation{{
		Function:   "citus_move_shard_placement",
		Parameters: []any{input.ShardID, input.SourceHost, input.SourcePort, input.TargetHost, input.TargetPort},
	}}

	if input.Colocated {
		warnings = append(warnings, "colocated shard groups may require sequential moves")
	}

	planID := hashMovePlan(opsToString(ops))
	return nil, MoveShardPlanOutput{PlanID: planID, Operations: ops, Warnings: warnings}, nil
}

func opsToString(ops []MoveShardOperation) string {
	var sb strings.Builder
	for _, op := range ops {
		sb.WriteString(op.Function)
		sb.WriteString("(")
		for i, p := range op.Parameters {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(logging.RedactDSN(fmt.Sprint(p)))
		}
		sb.WriteString(")\n")
	}
	return sb.String()
}

func hashMovePlan(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
