// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Advisor rule definitions for skew and performance analysis.

package advisor

import (
	"fmt"
	"sort"
	"time"
)

// Rule defines advisor rule interface.
type Rule interface {
	ID() string
	Category() string
	Evaluate(ctx *AdvisorContext) []Finding
}

// EvaluateRules evaluates all rules for the given context.
func EvaluateRules(ctx *AdvisorContext) []Finding {
	rules := []Rule{
		&RuleCitusMissing{},
		&RuleNoWorkers{},
		&RuleWorkerUnreachable{},
		&RuleInactiveNodes{},
		&RuleRebalancePrereqs{},
		&RuleRebalanceNotSupported{},
		&RuleHighShardSkew{},
		&RuleHotShardCandidates{},
		&RuleNonColocatedJoins{},
		&RuleStatsStale{},
		&RuleMissingDistKeyIndex{},
		&RuleLongRunningQueries{},
		&RuleLockWaits{},
		&RuleFailingJobs{},
		&RuleTenantHotspots{},
	}
	focus := ctx.Focus
	var findings []Finding
	for _, r := range rules {
		if focus != "all" && r.Category() != focus {
			continue
		}
		fs := r.Evaluate(ctx)
		findings = append(findings, fs...)
	}
	return findings
}

// Finding is the core finding structure.
type Finding struct {
	ID             string     `json:"id"`
	Severity       string     `json:"severity"` // info|warning|critical
	Category       string     `json:"category"`
	Scope          string     `json:"scope"`
	Target         string     `json:"target"`
	Title          string     `json:"title"`
	Problem        string     `json:"problem"`
	Impact         string     `json:"impact"`
	Recommendation string     `json:"recommendation"`
	SuggestedSQL   []string   `json:"suggested_sql,omitempty"`
	Evidence       Evidence   `json:"evidence"`
	NextSteps      []NextStep `json:"next_steps,omitempty"`
	Score          int        `json:"-"`
}

type NextStep struct {
	Tool string                 `json:"tool"`
	Args map[string]interface{} `json:"args,omitempty"`
}

// Helper to create findings.
func MakeFinding(ruleID, severity, category, scope, target, title, problem, impact, recommendation string, evidence Evidence, sqls []string, next []NextStep) Finding {
	f := Finding{
		Severity:       severity,
		Category:       category,
		Scope:          scope,
		Target:         target,
		Title:          title,
		Problem:        problem,
		Impact:         impact,
		Recommendation: recommendation,
		Evidence:       evidence,
		SuggestedSQL:   sqls,
		NextSteps:      next,
	}
	f.ID = StableID(ruleID, target, evidence)
	return f
}

func MakeCriticalFinding(ruleID, scope, target, title, problem, recommendation string, evidence Evidence, sqls []string, next []NextStep) Finding {
	return MakeFinding(ruleID, "critical", "health", scope, target, title, problem, "", recommendation, evidence, sqls, next)
}

// Rule implementations

// RuleCitusMissing detects missing Citus extension.
type RuleCitusMissing struct{}

func (r *RuleCitusMissing) ID() string       { return "rule.citus_missing" }
func (r *RuleCitusMissing) Category() string { return "health" }
func (r *RuleCitusMissing) Evaluate(ctx *AdvisorContext) []Finding {
	if ctx.Capabilities == nil || !ctx.Capabilities.HasCitusExtension {
		return []Finding{MakeCriticalFinding(r.ID(), "cluster", "cluster", "Citus extension missing", "Citus extension not installed", "Install citus extension", Evidence{}, nil, nil)}
	}
	return nil
}

// RuleNoWorkers warns when no workers.
type RuleNoWorkers struct{}

func (r *RuleNoWorkers) ID() string       { return "rule.no_workers" }
func (r *RuleNoWorkers) Category() string { return "health" }
func (r *RuleNoWorkers) Evaluate(ctx *AdvisorContext) []Finding {
	if len(ctx.Cluster.Workers) == 0 {
		return []Finding{MakeFinding(r.ID(), "warning", r.Category(), "cluster", "cluster", "No workers configured", "Single-node Citus", "Limited distribution benefits", "Add workers", Evidence{"workers": 0}, nil, nil)}
	}
	return nil
}

// RuleWorkerUnreachable checks reachability data.
type RuleWorkerUnreachable struct{}

func (r *RuleWorkerUnreachable) ID() string       { return "rule.worker_unreachable" }
func (r *RuleWorkerUnreachable) Category() string { return "health" }
func (r *RuleWorkerUnreachable) Evaluate(ctx *AdvisorContext) []Finding {
	// For MVP, use IsActive flag
	var unreachable []WorkerSummary
	for _, w := range ctx.Cluster.Workers {
		if !w.IsActive {
			unreachable = append(unreachable, w)
		}
	}
	if len(unreachable) > 0 {
		sev := "warning"
		if len(unreachable) == len(ctx.Cluster.Workers) {
			sev = "critical"
		}
		evidence := Evidence{"unreachable": unreachable}
		next := []NextStep{{Tool: "citus_cluster_summary"}}
		return []Finding{MakeFinding(r.ID(), sev, r.Category(), "cluster", "cluster", "Worker(s) unreachable", "Workers not active", "Rebalance and queries may fail", "Investigate worker connectivity", evidence, nil, next)}
	}
	return nil
}

// RuleInactiveNodes checks ShouldHaveShards false.
type RuleInactiveNodes struct{}

func (r *RuleInactiveNodes) ID() string       { return "rule.inactive_nodes" }
func (r *RuleInactiveNodes) Category() string { return "health" }
func (r *RuleInactiveNodes) Evaluate(ctx *AdvisorContext) []Finding {
	var inactive []WorkerSummary
	for _, w := range ctx.Cluster.Workers {
		if !w.ShouldHaveShards {
			inactive = append(inactive, w)
		}
	}
	if len(inactive) > 0 {
		evidence := Evidence{"inactive": inactive}
		return []Finding{MakeFinding(r.ID(), "warning", r.Category(), "cluster", "cluster", "Workers without shards", "Workers marked should_have_shards=false", "Imbalanced cluster", "Rebalance shards to workers", evidence, []string{}, []NextStep{{Tool: "citus_rebalance_plan"}})}
	}
	return nil
}

// RuleRebalancePrereqs checks prereqs per table.
type RuleRebalancePrereqs struct{}

func (r *RuleRebalancePrereqs) ID() string       { return "rule.rebalance_prereqs" }
func (r *RuleRebalancePrereqs) Category() string { return "rebalance" }
func (r *RuleRebalancePrereqs) Evaluate(ctx *AdvisorContext) []Finding {
	var findings []Finding
	for _, t := range ctx.Tables {
		pr, ok := ctx.Prereqs[t.Name]
		if !ok {
			continue
		}
		if pr.Ready {
			continue
		}
		sev := "warning"
		evidence := Evidence{"issues": pr.Issues}
		sqls := []string{}
		if ctx.IncludeSQL {
			for _, issue := range pr.Issues {
				if issue.SuggestedFixSQL != "" {
					sqls = append(sqls, issue.SuggestedFixSQL)
				}
			}
		}
		next := []NextStep{{Tool: "citus_validate_rebalance_prereqs", Args: map[string]interface{}{"table": t.Name}}}
		findings = append(findings, MakeFinding(r.ID(), sev, r.Category(), "table", t.Name, "Rebalance prerequisites not met", "Primary key/replica identity issues", "Rebalance may fail or cause data mismatches", "Fix PK/replica identity", evidence, sqls, next))
	}
	return findings
}

// RuleRebalanceNotSupported warns when capabilities missing.
type RuleRebalanceNotSupported struct{}

func (r *RuleRebalanceNotSupported) ID() string       { return "rule.rebalance_not_supported" }
func (r *RuleRebalanceNotSupported) Category() string { return "rebalance" }
func (r *RuleRebalanceNotSupported) Evaluate(ctx *AdvisorContext) []Finding {
	caps := ctx.Capabilities
	if caps == nil {
		return nil
	}
	if !(caps.SupportsRebalancePlan() && caps.SupportsRebalanceStart() && caps.SupportsRebalanceStatus()) {
		evidence := Evidence{"supports_plan": caps.SupportsRebalancePlan(), "supports_start": caps.SupportsRebalanceStart(), "supports_status": caps.SupportsRebalanceStatus()}
		return []Finding{MakeFinding(r.ID(), "info", r.Category(), "cluster", "cluster", "Rebalance UDFs missing", "Rebalance helper functions not available", "Cannot plan/execute rebalance", "Upgrade Citus or enable UDFs", evidence, nil, nil)}
	}
	return nil
}

// RuleHighShardSkew detects shard count skew.
type RuleHighShardSkew struct{}

func (r *RuleHighShardSkew) ID() string       { return "rule.high_shard_skew" }
func (r *RuleHighShardSkew) Category() string { return "skew" }
func (r *RuleHighShardSkew) Evaluate(ctx *AdvisorContext) []Finding {
	if len(ctx.Skew.PerNode) == 0 {
		return nil
	}
	ratio := ctx.Skew.Ratio
	if ctx.Skew.BytesRatio > 0 {
		ratio = ctx.Skew.BytesRatio
	}
	var sev string
	switch {
	case ratio >= 3:
		sev = "critical"
	case ratio >= 2:
		sev = "warning"
	case ratio > 1:
		sev = "info"
	default:
		sev = "info"
	}
	if ratio <= 1.1 {
		return nil
	}
	evidence := Evidence{"ratio": ratio, "nodes": ctx.Skew.PerNode}
	next := []NextStep{{Tool: "citus_rebalance_plan"}, {Tool: "citus_shard_skew_report"}}
	return []Finding{MakeFinding(r.ID(), sev, r.Category(), "cluster", "cluster", "Shard distribution skew detected", fmt.Sprintf("Shard count ratio %.2f", ratio), "Uneven load across workers", "Plan rebalance and review distribution keys", evidence, nil, next)}
}

// RuleHotShardCandidates detects very large shards vs average (bytes).
type RuleHotShardCandidates struct{}

func (r *RuleHotShardCandidates) ID() string       { return "rule.hot_shard_candidates" }
func (r *RuleHotShardCandidates) Category() string { return "skew" }
func (r *RuleHotShardCandidates) Evaluate(ctx *AdvisorContext) []Finding {
	if ctx.HotShardsByTable == nil {
		return nil
	}
	var findings []Finding
	for tbl, shards := range ctx.HotShardsByTable {
		if len(shards) == 0 {
			continue
		}
		var sum int64
		var max int64
		var maxShard HotShardInfo
		for _, sh := range shards {
			sum += sh.Bytes
			if sh.Bytes > max {
				max = sh.Bytes
				maxShard = sh
			}
		}
		avg := float64(sum) / float64(len(shards))
		if avg == 0 {
			continue
		}
		ratio := float64(max) / avg
		if ratio < 3 {
			continue
		}
		severity := "warning"
		if ratio >= 5 {
			severity = "critical"
		}
		evidence := Evidence{"hot_shard": maxShard, "avg_bytes": avg, "ratio": ratio}
		next := []NextStep{{Tool: "citus_shard_heatmap", Args: map[string]interface{}{"table": tbl}}, {Tool: "citus_rebalance_plan"}}
		findings = append(findings, MakeFinding(r.ID(), severity, r.Category(), "table", tbl, "Hot shard detected", "Shard significantly larger than average", "Uneven storage/load", "Consider rebalance or distribution key review", evidence, nil, next))
	}
	return findings
}

// RuleNonColocatedJoins heuristic.
type RuleNonColocatedJoins struct{}

func (r *RuleNonColocatedJoins) ID() string       { return "rule.non_colocated_likely_joins" }
func (r *RuleNonColocatedJoins) Category() string { return "metadata" }
func (r *RuleNonColocatedJoins) Evaluate(ctx *AdvisorContext) []Finding {
	// group by dist column name
	byCol := map[string][]TableMeta{}
	for _, t := range ctx.Tables {
		if t.DistColumn == "" {
			continue
		}
		byCol[t.DistColumn] = append(byCol[t.DistColumn], t)
	}
	var findings []Finding
	for col, list := range byCol {
		// check colocation IDs
		ids := map[int32]struct{}{}
		for _, t := range list {
			ids[t.ColocationID] = struct{}{}
		}
		if len(ids) > 1 {
			sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
			evidence := Evidence{"dist_column": col, "tables": list}
			next := []NextStep{{Tool: "citus_list_distributed_tables"}}
			findings = append(findings, MakeFinding(r.ID(), "info", r.Category(), "cluster", "cluster", "Tables share dist column but differ colocation", "Likely joined tables not colocated", "Joins may route through coordinator", "Consider colocating tables", evidence, nil, next))
		}
	}
	return findings
}

// RuleStatsStale warns stale stats.
type RuleStatsStale struct{}

func (r *RuleStatsStale) ID() string       { return "rule.stats_stale" }
func (r *RuleStatsStale) Category() string { return "performance" }
func (r *RuleStatsStale) Evaluate(ctx *AdvisorContext) []Finding {
	cutoff := time.Now().Add(-24 * time.Hour)
	var findings []Finding
	for _, t := range ctx.Tables {
		if t.LastAnalyze == nil {
			evidence := Evidence{"table": t.Name, "last_analyze": nil}
			sqls := []string{}
			if ctx.IncludeSQL {
				sqls = append(sqls, fmt.Sprintf("ANALYZE %s;", t.Name))
			}
			findings = append(findings, MakeFinding(r.ID(), "info", r.Category(), "table", t.Name, "Table never analyzed", "Statistics missing", "Planner estimates may be poor", "Analyze table", evidence, sqls, nil))
			continue
		}
		if t.LastAnalyze.Before(cutoff) {
			evidence := Evidence{"table": t.Name, "last_analyze": t.LastAnalyze}
			sqls := []string{}
			if ctx.IncludeSQL {
				sqls = append(sqls, fmt.Sprintf("ANALYZE %s;", t.Name))
			}
			findings = append(findings, MakeFinding(r.ID(), "info", r.Category(), "table", t.Name, "Stale statistics", "Statistics older than 24h", "Planner estimates may be poor", "Analyze table", evidence, sqls, nil))
		}
	}
	return findings
}

// RuleMissingDistKeyIndex warns when distribution column has no index.
type RuleMissingDistKeyIndex struct{}

func (r *RuleMissingDistKeyIndex) ID() string       { return "rule.missing_dist_key_index" }
func (r *RuleMissingDistKeyIndex) Category() string { return "performance" }
func (r *RuleMissingDistKeyIndex) Evaluate(ctx *AdvisorContext) []Finding {
	var findings []Finding
	for _, t := range ctx.Tables {
		if t.PartMethod == "n" { // reference
			continue
		}
		if !t.DistKeyIndexed {
			evidence := Evidence{"table": t.Name, "dist_key": t.DistColumn}
			sqls := []string{}
			if ctx.IncludeSQL && t.DistColumn != "" {
				sqls = append(sqls, fmt.Sprintf("CREATE INDEX ON %s (%s);", t.Name, t.DistColumn))
			}
			findings = append(findings, MakeFinding(r.ID(), "warning", r.Category(), "table", t.Name, "Missing index on distribution column", "Distribution column not indexed", "Routing joins and filters may be slow", "Create index on distribution column", evidence, sqls, nil))
		}
	}
	return findings
}

// RuleLongRunningQueries surfaces long-running queries.
type RuleLongRunningQueries struct{}

func (r *RuleLongRunningQueries) ID() string       { return "rule.ops.long_running_queries" }
func (r *RuleLongRunningQueries) Category() string { return "ops" }
func (r *RuleLongRunningQueries) Evaluate(ctx *AdvisorContext) []Finding {
	act := ctx.Ops.Activity
	if len(act) == 0 {
		return nil
	}
	threshold := 60.0 // seconds
	var long []ActivityRow
	for _, a := range act {
		if a.AgeSeconds >= threshold {
			long = append(long, a)
		}
	}
	if len(long) == 0 {
		return nil
	}
	sev := "warning"
	for _, a := range long {
		if a.AgeSeconds >= 300 {
			sev = "critical"
			break
		}
	}
	evidence := Evidence{"count": len(long), "top": summarizeActivity(long, 5)}
	next := []NextStep{{Tool: "citus_lock_inspector"}}
	return []Finding{MakeFinding(r.ID(), sev, r.Category(), "cluster", "queries", "Long running queries detected", fmt.Sprintf("%d queries >60s", len(long)), "Resource contention and lock buildup", "Investigate and optimize or terminate long-running queries", evidence, nil, next)}
}

// RuleLockWaits surfaces lock waits.
type RuleLockWaits struct{}

func (r *RuleLockWaits) ID() string       { return "rule.ops.lock_waits" }
func (r *RuleLockWaits) Category() string { return "ops" }
func (r *RuleLockWaits) Evaluate(ctx *AdvisorContext) []Finding {
	waits := ctx.Ops.LockWaits
	if len(waits) == 0 {
		return nil
	}
	evidence := Evidence{"count": len(waits), "top": summarizeLockWaits(waits, 5)}
	next := []NextStep{{Tool: "citus_lock_inspector"}}
	return []Finding{MakeFinding(r.ID(), "warning", r.Category(), "cluster", "locks", "Lock waits detected", fmt.Sprintf("%d lock waits", len(waits)), "Queries are blocked", "Identify blockers and resolve contention", evidence, nil, next)}
}

// RuleFailingJobs surfaces failing or unfinished background jobs.
type RuleFailingJobs struct{}

func (r *RuleFailingJobs) ID() string       { return "rule.ops.failing_jobs" }
func (r *RuleFailingJobs) Category() string { return "ops" }
func (r *RuleFailingJobs) Evaluate(ctx *AdvisorContext) []Finding {
	jobs := ctx.Ops.BackgroundJobs
	if len(jobs) == 0 {
		return nil
	}
	sev := "warning"
	for _, j := range jobs {
		if j.State == "failed" {
			sev = "critical"
			break
		}
	}
	evidence := Evidence{"count": len(jobs), "top": summarizeJobs(jobs, 5)}
	return []Finding{MakeFinding(r.ID(), sev, r.Category(), "cluster", "jobs", "Background jobs require attention", "Jobs not finished or failed", "Maintenance or rebalance jobs may be stuck", "Investigate pg_dist_background_job tasks", evidence, nil, nil)}
}

// RuleTenantHotspots surfaces tenant hotspots by CPU usage.
type RuleTenantHotspots struct{}

func (r *RuleTenantHotspots) ID() string       { return "rule.ops.tenant_hotspots" }
func (r *RuleTenantHotspots) Category() string { return "ops" }
func (r *RuleTenantHotspots) Evaluate(ctx *AdvisorContext) []Finding {
	stats := ctx.Ops.TenantStats
	if len(stats) == 0 {
		return nil
	}
	totalCPU := 0.0
	for _, s := range stats {
		totalCPU += s.CPUUsage
	}
	if totalCPU <= 0 {
		return nil
	}
	top := stats[0]
	ratio := top.CPUUsage / totalCPU
	if ratio < 0.5 {
		return nil
	}
	sev := "warning"
	if ratio >= 0.8 {
		sev = "critical"
	}
	evidence := Evidence{"top": top, "ratio": ratio, "total_cpu": totalCPU}
	next := []NextStep{{Tool: "citus_shard_heatmap"}}
	return []Finding{MakeFinding(r.ID(), sev, r.Category(), "cluster", "tenants", "Tenant hotspot detected", fmt.Sprintf("Tenant uses %.0f%% CPU", ratio*100), "Tenant may degrade multi-tenant QoS", "Consider tenant isolation or rebalancing", evidence, nil, next)}
}

// helpers
func summarizeActivity(rows []ActivityRow, n int) []map[string]interface{} {
	res := []map[string]interface{}{}
	for i, a := range rows {
		if i >= n {
			break
		}
		res = append(res, map[string]interface{}{
			"nodeid":      a.NodeID,
			"age_seconds": a.AgeSeconds,
			"state":       a.State,
			"wait_event":  a.WaitEvent,
			"query":       truncate(a.Query, 200),
		})
	}
	return res
}

func summarizeLockWaits(rows []LockWaitRow, n int) []map[string]interface{} {
	res := []map[string]interface{}{}
	for i, w := range rows {
		if i >= n {
			break
		}
		res = append(res, map[string]interface{}{
			"waiting_gpid":  w.WaitingGPID,
			"blocking_gpid": w.BlockingGPID,
			"blocked":       truncate(w.BlockedStatement, 120),
			"blocking":      truncate(w.BlockingStatement, 120),
		})
	}
	return res
}

func summarizeJobs(rows []BackgroundJobRow, n int) []map[string]interface{} {
	res := []map[string]interface{}{}
	for i, j := range rows {
		if i >= n {
			break
		}
		res = append(res, map[string]interface{}{
			"job_id":     j.JobID,
			"state":      j.State,
			"job_type":   j.JobType,
			"started_at": j.StartedAt,
		})
	}
	return res
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
