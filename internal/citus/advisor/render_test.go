// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for advisor output rendering.

package advisor

import "testing"

func TestRenderOrdering(t *testing.T) {
	ctx := &AdvisorContext{Focus: "all", MaxFindings: 10, Skew: SkewSnapshot{Ratio: 1.0}}
	ctx.TableMeta = map[string]*TableMeta{"public.t1": {Name: "public.t1"}}
	findings := []Finding{
		MakeFinding("r1", "warning", "performance", "table", "public.t1", "Missing index", "", "", "", Evidence{}, nil, nil),
		MakeFinding("r2", "critical", "health", "cluster", "cluster", "Citus missing", "", "", "", Evidence{}, nil, nil),
		MakeFinding("r3", "info", "metadata", "table", "public.t1", "Stale stats", "", "", "", Evidence{}, nil, nil),
	}
	out := Render(ctx, findings)
	if out.Findings[0].Severity != "critical" {
		t.Fatalf("expected critical first")
	}
	if out.Summary.ClusterHealth != "critical" {
		t.Fatalf("expected cluster health critical")
	}
}
