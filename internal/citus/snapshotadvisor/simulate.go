// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Cluster metrics simulation for scaling scenarios.

package snapshotadvisor

import "math"

func computeClusterMetrics(workers []WorkerMetrics) ClusterMetricsBefore {
	var totalShards int
	var totalBytes *int64
	maxShards := 0
	minShards := math.MaxInt
	var maxBytes int64
	minBytes := int64(math.MaxInt64)
	hasBytes := false

	for _, w := range workers {
		totalShards += w.ShardCount
		if w.ShardCount > maxShards {
			maxShards = w.ShardCount
		}
		if w.ShardCount > 0 && w.ShardCount < minShards {
			minShards = w.ShardCount
		}
		if w.Bytes != nil {
			hasBytes = true
			if totalBytes == nil {
				tb := int64(0)
				totalBytes = &tb
			}
			*totalBytes += *w.Bytes
			if *w.Bytes > maxBytes {
				maxBytes = *w.Bytes
			}
			if *w.Bytes > 0 && *w.Bytes < minBytes {
				minBytes = *w.Bytes
			}
		}
	}
	var skewShards float64
	if minShards == math.MaxInt || minShards == 0 {
		skewShards = 0
	} else {
		skewShards = float64(maxShards) / float64(minShards)
	}
	var skewBytes *float64
	if hasBytes && minBytes > 0 && maxBytes > 0 {
		v := float64(maxBytes) / float64(minBytes)
		skewBytes = &v
	}
	return ClusterMetricsBefore{
		WorkerCount:     len(workers),
		TotalBytes:      totalBytes,
		TotalShards:     totalShards,
		SkewRatioBytes:  skewBytes,
		SkewRatioShards: skewShards,
	}
}

func computeIdealTarget(before ClusterMetricsBefore) IdealTargetAfterAddition {
	denom := float64(before.WorkerCount + 1)
	targetShards := 0.0
	if denom > 0 {
		targetShards = float64(before.TotalShards) / denom
	}
	var targetBytes *float64
	if before.TotalBytes != nil {
		v := float64(*before.TotalBytes) / denom
		targetBytes = &v
	}
	return IdealTargetAfterAddition{WorkerCountAfter: before.WorkerCount + 1, TargetBytesPerWorker: targetBytes, TargetShardsPerWorker: targetShards}
}

func simulateSplit(workers []WorkerMetrics, idx int) []WorkerMetrics {
	after := make([]WorkerMetrics, len(workers)+1)
	copy(after, workers)
	source := workers[idx]
	halfShards := float64(source.ShardCount) / 2.0
	var halfBytes *float64
	if source.Bytes != nil {
		hb := float64(*source.Bytes) / 2.0
		halfBytes = &hb
	}
	// update source
	after[idx].ShardCount = int(math.Round(halfShards))
	if halfBytes != nil {
		hb := int64(math.Round(*halfBytes))
		after[idx].Bytes = &hb
	}
	// add clone
	clone := source
	clone.Node.NodeID = -source.Node.NodeID // placeholder; not used for scoring except identification
	clone.Node.Host = source.Node.Host + "-clone"
	clone.ShardCount = int(math.Round(halfShards))
	if halfBytes != nil {
		hb := int64(math.Round(*halfBytes))
		clone.Bytes = &hb
	} else {
		clone.Bytes = nil
	}
	after[len(after)-1] = clone
	return after
}

// computeMetrics returns skew ratio and max deviation from target
func computeMetrics(vals []float64, target float64) (float64, float64) {
	if len(vals) == 0 {
		return 0, 0
	}
	maxVal := -math.MaxFloat64
	minVal := math.MaxFloat64
	maxDev := 0.0
	for _, v := range vals {
		if v > maxVal {
			maxVal = v
		}
		if v > 0 && v < minVal {
			minVal = v
		}
		d := math.Abs(v - target)
		if d > maxDev {
			maxDev = d
		}
	}
	var skew float64
	if minVal <= 0 || minVal == math.MaxFloat64 {
		skew = 0
	} else {
		skew = maxVal / minVal
	}
	return skew, maxDev
}

func toFloatSliceShards(workers []WorkerMetrics) []float64 {
	out := make([]float64, len(workers))
	for i, w := range workers {
		out[i] = float64(w.ShardCount)
	}
	return out
}

func toFloatSliceBytes(workers []WorkerMetrics) ([]float64, bool) {
	out := make([]float64, 0, len(workers))
	has := false
	for _, w := range workers {
		if w.Bytes == nil {
			out = append(out, 0)
			continue
		}
		out = append(out, float64(*w.Bytes))
		if *w.Bytes > 0 {
			has = true
		}
	}
	return out, has
}
