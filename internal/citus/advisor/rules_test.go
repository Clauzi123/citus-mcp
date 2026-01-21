package advisor

import (
	"testing"
	"time"

	"citus-mcp/internal/db"
)

func TestRuleCitusMissing(t *testing.T) {
	ctx := &AdvisorContext{Capabilities: &db.Capabilities{HasCitusExtension: false}}
	fs := (&RuleCitusMissing{}).Evaluate(ctx)
	if len(fs) != 1 || fs[0].Severity != "critical" {
		t.Fatalf("expected critical finding for missing citus")
	}
}

func TestRuleMissingDistKeyIndex(t *testing.T) {
	tm := TableMeta{Name: "public.t1", PartMethod: "h", DistColumn: "id", DistKeyIndexed: false}
	ctx := &AdvisorContext{Tables: []TableMeta{tm}, IncludeSQL: true}
	fs := (&RuleMissingDistKeyIndex{}).Evaluate(ctx)
	if len(fs) != 1 {
		t.Fatalf("expected one finding")
	}
	if len(fs[0].SuggestedSQL) == 0 {
		t.Fatalf("expected suggested SQL")
	}
}

func TestRuleStatsStale(t *testing.T) {
	now := time.Now().Add(-48 * time.Hour)
	tm := TableMeta{Name: "public.t1", PartMethod: "h", LastAnalyze: &now}
	ctx := &AdvisorContext{Tables: []TableMeta{tm}, IncludeSQL: true}
	fs := (&RuleStatsStale{}).Evaluate(ctx)
	if len(fs) != 1 {
		t.Fatalf("expected stale stats finding")
	}
}

func TestRuleHotShardCandidates(t *testing.T) {
	ctx := &AdvisorContext{HotShardsByTable: map[string][]HotShardInfo{
		"public.t1": {
			{ShardID: 1, Table: "public.t1", Bytes: 0, Node: "n1"},
			{ShardID: 2, Table: "public.t1", Bytes: 0, Node: "n2"},
			{ShardID: 3, Table: "public.t1", Bytes: 9000, Node: "n3"},
		},
	}}
	fs := (&RuleHotShardCandidates{}).Evaluate(ctx)
	if len(fs) != 1 {
		t.Fatalf("expected hot shard finding")
	}
	if fs[0].Severity != "warning" {
		t.Fatalf("expected warning severity")
	}
}
