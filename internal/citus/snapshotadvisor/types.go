// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Type definitions for snapshot advisor.

package snapshotadvisor

import "time"

// Strategy defines scoring strategy.
type Strategy string

const (
	StrategyHybrid       Strategy = "hybrid"
	StrategyByShardCount Strategy = "by_shard_count"
	StrategyByDiskSize   Strategy = "by_disk_size"
)

// Input defines advisor parameters.
type Input struct {
	Strategy          Strategy `json:"strategy,omitempty"`
	MaxCandidates     int      `json:"max_candidates,omitempty"`
	ExcludeNodes      []string `json:"exclude_nodes,omitempty"`
	RequireReachable  bool     `json:"require_reachable,omitempty"`
	IncludeSimulation bool     `json:"include_simulation,omitempty"`
	IncludeNextSteps  bool     `json:"include_next_steps,omitempty"`
}

// Warning represents a non-fatal advisory warning.
type Warning struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// NodeRef identifies a worker node.
type NodeRef struct {
	NodeID int32  `json:"node_id"`
	Host   string `json:"host"`
	Port   int32  `json:"port"`
}

// WorkerMetrics holds per-worker stats.
type WorkerMetrics struct {
	Node             NodeRef `json:"node"`
	Reachable        bool    `json:"reachable"`
	IsActive         bool    `json:"is_active"`
	ShouldHaveShards bool    `json:"should_have_shards"`
	ShardCount       int     `json:"shard_count"`
	Bytes            *int64  `json:"bytes,omitempty"`
}

// ClusterMetricsBefore represents pre-addition metrics.
type ClusterMetricsBefore struct {
	WorkerCount     int      `json:"worker_count"`
	TotalBytes      *int64   `json:"total_bytes,omitempty"`
	TotalShards     int      `json:"total_shards"`
	SkewRatioBytes  *float64 `json:"skew_ratio_bytes,omitempty"`
	SkewRatioShards float64  `json:"skew_ratio_shards"`
}

// IdealTargetAfterAddition holds target per-worker metrics after adding a node.
type IdealTargetAfterAddition struct {
	WorkerCountAfter      int      `json:"worker_count_after"`
	TargetBytesPerWorker  *float64 `json:"target_bytes_per_worker,omitempty"`
	TargetShardsPerWorker float64  `json:"target_shards_per_worker"`
}

// PredictedAfter holds predicted cluster imbalance post-addition for a candidate.
type PredictedAfter struct {
	SkewRatioBytes     *float64 `json:"skew_ratio_bytes,omitempty"`
	SkewRatioShards    float64  `json:"skew_ratio_shards"`
	MaxDeviationBytes  *float64 `json:"max_deviation_bytes,omitempty"`
	MaxDeviationShards float64  `json:"max_deviation_shards"`
}

// Candidate represents a candidate source worker.
type Candidate struct {
	Source         NodeRef        `json:"source"`
	Score          float64        `json:"score"`
	PredictedAfter PredictedAfter `json:"predicted_after"`
	Why            []string       `json:"why"`

	reachable        bool `json:"-"`
	shouldHaveShards bool `json:"-"`
}

// Recommendation represents the chosen candidate.
type Recommendation struct {
	Source         NodeRef        `json:"source"`
	Score          float64        `json:"score"`
	Reasons        []string       `json:"reasons"`
	PredictedAfter PredictedAfter `json:"predicted_after"`
}

// Summary is top-level summary info.
type Summary struct {
	AdvisorVersion    string    `json:"advisor_version"`
	Strategy          Strategy  `json:"strategy"`
	WorkersConsidered int       `json:"workers_considered"`
	GeneratedAt       time.Time `json:"generated_at"`
	Note              string    `json:"note"`
}

// NextStep represents suggested follow-ups.
type NextStep struct {
	Tool string         `json:"tool"`
	Args map[string]any `json:"args,omitempty"`
}

// Output is the full advisor output.
type Output struct {
	Summary              Summary                  `json:"summary"`
	ClusterMetricsBefore ClusterMetricsBefore     `json:"cluster_metrics_before"`
	IdealTargetAfter     IdealTargetAfterAddition `json:"ideal_target_after_addition"`
	Recommendation       *Recommendation          `json:"recommendation,omitempty"`
	Candidates           []Candidate              `json:"candidates,omitempty"`
	NextSteps            []NextStep               `json:"next_steps,omitempty"`
	Warnings             []Warning                `json:"warnings,omitempty"`
}
