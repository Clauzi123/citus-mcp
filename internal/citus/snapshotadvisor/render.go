package snapshotadvisor

import (
	"fmt"
	"sort"
)

func sortAndTruncate(candidates []Candidate, max int) []Candidate {
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		if candidates[i].reachable != candidates[j].reachable {
			return candidates[i].reachable && !candidates[j].reachable
		}
		if candidates[i].shouldHaveShards != candidates[j].shouldHaveShards {
			return candidates[i].shouldHaveShards && !candidates[j].shouldHaveShards
		}
		if candidates[i].Source.Host != candidates[j].Source.Host {
			return candidates[i].Source.Host < candidates[j].Source.Host
		}
		if candidates[i].Source.Port != candidates[j].Source.Port {
			return candidates[i].Source.Port < candidates[j].Source.Port
		}
		return candidates[i].Source.NodeID < candidates[j].Source.NodeID
	})
	if max <= 0 || max >= len(candidates) {
		return candidates
	}
	return candidates[:max]
}

func buildReasons(before ClusterMetricsBefore, ideal IdealTargetAfterAddition, c Candidate, strategy Strategy) []string {
	reasons := []string{}
	reasons = append(reasons, fmt.Sprintf("Splitting %s:%d reduces shard skew from %.2f to %.2f", c.Source.Host, c.Source.Port, before.SkewRatioShards, c.PredictedAfter.SkewRatioShards))
	if c.PredictedAfter.MaxDeviationShards > 0 {
		reasons = append(reasons, fmt.Sprintf("Max deviation from target shards (%.1f) becomes %.1f", ideal.TargetShardsPerWorker, c.PredictedAfter.MaxDeviationShards))
	}
	if c.PredictedAfter.SkewRatioBytes != nil && before.SkewRatioBytes != nil {
		reasons = append(reasons, fmt.Sprintf("Bytes skew improves from %.2f to %.2f", *before.SkewRatioBytes, *c.PredictedAfter.SkewRatioBytes))
	}
	reasons = append(reasons, "Workflow: Create clone from source; register via citus_add_clone_node(...); optionally preview with get_snapshot_based_node_split_plan(...); promote with citus_promote_clone_and_rebalance(...)")
	return reasons
}

func defaultNextSteps() []NextStep {
	return []NextStep{
		{Tool: "citus.shard_skew_report", Args: map[string]any{"metric": "bytes", "include_top_shards": true}},
		{Tool: "citus.rebalance_plan"},
		{Tool: "citus.cluster_summary"},
	}
}
