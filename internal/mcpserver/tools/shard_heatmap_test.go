// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for shard heatmap aggregation logic.

package tools

import "testing"

func TestAggregatePerNode(t *testing.T) {
	shards := []shardRecord{
		{ShardID: 1, Host: "n1", Port: 1, Bytes: 10},
		{ShardID: 2, Host: "n1", Port: 1, Bytes: 20},
		{ShardID: 3, Host: "n2", Port: 1, Bytes: 5},
	}
	nodes := aggregatePerNode(shards)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes")
	}
}

func TestAggregatePerTable(t *testing.T) {
	shards := []shardRecord{{Table: "public.t1"}, {Table: "public.t1"}, {Table: "public.t2"}}
	tables := aggregatePerTable(shards)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables")
	}
}

func TestComputeMaxNodeRatio(t *testing.T) {
	nodes := []NodeHeat{{Shards: 10, Bytes: 100}, {Shards: 5, Bytes: 50}}
	ratio := computeMaxNodeRatio(nodes, false)
	if ratio != 2 {
		t.Fatalf("expected ratio 2, got %f", ratio)
	}
}
