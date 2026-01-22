// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for advisor scoring logic.

package advisor

import "testing"

func TestScoreFinding(t *testing.T) {
	f := Finding{Severity: "warning"}
	score := ScoreFinding(f, nil, 1.0)
	if score == 0 {
		t.Fatalf("expected non-zero score")
	}
	if score != 60 {
		t.Fatalf("expected 60, got %d", score)
	}

	tmeta := &TableMeta{ShardCount: 500}
	score2 := ScoreFinding(f, tmeta, 2.5)
	if score2 <= score {
		t.Fatalf("expected higher score with shards and skew, got %d", score2)
	}
	if score2 > 100 {
		t.Fatalf("score should be clamped <= 100, got %d", score2)
	}
}
