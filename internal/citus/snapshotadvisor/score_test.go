// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for candidate scoring.

package snapshotadvisor

import "testing"

func TestScoreCandidatesShardOnly(t *testing.T) {
	workers := []WorkerMetrics{{ShardCount: 10, Reachable: true, ShouldHaveShards: true}, {ShardCount: 6, Reachable: true, ShouldHaveShards: true}}
	before := computeClusterMetrics(workers)
	ideal := computeIdealTarget(before)
	candidates, warnings := scoreCandidates(workers, before, ideal, StrategyByShardCount)
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", warnings)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(candidates))
	}
	if candidates[0].Score < candidates[1].Score {
		t.Fatalf("expected first candidate to have higher or equal score")
	}
}

func TestScoreCandidatesBytesFallback(t *testing.T) {
	workers := []WorkerMetrics{{ShardCount: 10}, {ShardCount: 6}}
	before := computeClusterMetrics(workers)
	ideal := computeIdealTarget(before)
	_, warnings := scoreCandidates(workers, before, ideal, StrategyByDiskSize)
	if len(warnings) == 0 {
		t.Fatalf("expected warning for missing bytes")
	}
}
