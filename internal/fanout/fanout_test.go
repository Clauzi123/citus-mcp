// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for parallel query fanout.

package fanout

import (
	"context"
	"testing"

	"citus-mcp/internal/db"
)

func TestFanout(t *testing.T) {
	nodes := []db.Node{{NodeID: 1}, {NodeID: 2}}
	res, err := Fanout[int32](context.Background(), nodes, func(ctx context.Context, n db.Node) (int32, error) {
		return n.NodeID, nil
	})
	if err != nil {
		t.Fatalf("Fanout error: %v", err)
	}
	if len(res) != len(nodes) {
		t.Fatalf("expected %d results, got %d", len(nodes), len(res))
	}
}
