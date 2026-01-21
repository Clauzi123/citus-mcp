package snapshotadvisor

import "testing"

func TestComputeMetricsAndIdeal(t *testing.T) {
	workers := []WorkerMetrics{{ShardCount: 10}, {ShardCount: 6}, {ShardCount: 4}}
	before := computeClusterMetrics(workers)
	if before.TotalShards != 20 {
		t.Fatalf("total shards expected 20, got %d", before.TotalShards)
	}
	if before.SkewRatioShards <= 0 {
		t.Fatalf("skew ratio shards should be >0, got %f", before.SkewRatioShards)
	}
	ideal := computeIdealTarget(before)
	if ideal.WorkerCountAfter != 4 {
		t.Fatalf("expected worker_count_after=4, got %d", ideal.WorkerCountAfter)
	}
	if ideal.TargetShardsPerWorker != 5 {
		t.Fatalf("expected target shards per worker 5, got %f", ideal.TargetShardsPerWorker)
	}
}

func TestSimulateSplit(t *testing.T) {
	workers := []WorkerMetrics{{ShardCount: 10}, {ShardCount: 6}}
	after := simulateSplit(workers, 0)
	if len(after) != 3 {
		t.Fatalf("expected 3 workers after split, got %d", len(after))
	}
	if after[0].ShardCount == workers[0].ShardCount {
		t.Fatalf("source not split")
	}
}
