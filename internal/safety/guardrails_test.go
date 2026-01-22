// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for guardrails and read-only checks.

package safety

import "testing"

func TestQueryIsReadOnly(t *testing.T) {
	cases := []struct {
		q  string
		ro bool
	}{
		{"SELECT 1", true},
		{"\n  -- comment\nSELECT * FROM t", true},
		{"WITH x AS (SELECT 1) SELECT * FROM x", true},
		{"SHOW work_mem", true},
		{"EXPLAIN SELECT 1", true},
		{"INSERT INTO t VALUES (1)", false},
		{"UPDATE t SET a=1", false},
		{"DELETE FROM t", false},
		{"CALL foo()", false},
		{"COPY t TO '/tmp/x'", false},
		{"CREATE TABLE t()", false},
	}
	for _, c := range cases {
		if QueryIsReadOnly(c.q) != c.ro {
			t.Fatalf("expected %v for %q", c.ro, c.q)
		}
	}
}
