package advisor

import "testing"

func TestRuleLongRunningQueries(t *testing.T) {
	rule := &RuleLongRunningQueries{}
	ctx := &AdvisorContext{}
	ctx.Ops.Activity = []ActivityRow{{AgeSeconds: 120, State: "active", Query: "select 1"}}
	findings := rule.Evaluate(ctx)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].Severity == "" {
		t.Fatalf("severity should be set")
	}
}

func TestRuleTenantHotspots(t *testing.T) {
	rule := &RuleTenantHotspots{}
	ctx := &AdvisorContext{}
	ctx.Ops.TenantStats = []TenantStatRow{{TenantAttr: "t1", CPUUsage: 90}, {TenantAttr: "t2", CPUUsage: 10}}
	findings := rule.Evaluate(ctx)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
}
