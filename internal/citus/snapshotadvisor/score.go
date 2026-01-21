package snapshotadvisor

// no imports

func scoreCandidates(workers []WorkerMetrics, before ClusterMetricsBefore, ideal IdealTargetAfterAddition, strategy Strategy) ([]Candidate, []Warning) {
	// Compute before deviations
	shardsVals := toFloatSliceShards(workers)
	beforeSkewShards, beforeDevShards := computeMetrics(shardsVals, ideal.TargetShardsPerWorker)
	bytesVals, hasBytes := toFloatSliceBytes(workers)
	var beforeSkewBytes, beforeDevBytes float64
	if hasBytes {
		beforeSkewBytes, beforeDevBytes = computeMetrics(bytesVals, deref(ideal.TargetBytesPerWorker))
	}

	candidates := make([]Candidate, 0, len(workers))
	bytesAvailable := hasBytes
	warnedBytesMissing := false
	warnings := []Warning{}

	for i, w := range workers {
		afterWorkers := simulateSplit(workers, i)
		// Shards metrics
		afterShardsVals := toFloatSliceShards(afterWorkers)
		afterSkewShards, afterDevShards := computeMetrics(afterShardsVals, ideal.TargetShardsPerWorker)

		// Bytes metrics
		afterSkewBytes := 0.0
		afterDevBytes := 0.0
		if bytesAvailable {
			afterBytesVals, _ := toFloatSliceBytes(afterWorkers)
			afterSkewBytes, afterDevBytes = computeMetrics(afterBytesVals, deref(ideal.TargetBytesPerWorker))
		}

		scoreShards := scoreComponent(beforeSkewShards, afterSkewShards, beforeDevShards, afterDevShards)
		scoreBytes := scoreComponent(beforeSkewBytes, afterSkewBytes, beforeDevBytes, afterDevBytes)

		var score float64
		switch strategy {
		case StrategyByDiskSize:
			if bytesAvailable {
				score = scoreBytes
			} else {
				score = scoreShards
				if !warnedBytesMissing {
					warnings = append(warnings, Warning{Code: "PARTIAL_RESULTS", Message: "bytes not available; using shard_count for scoring"})
					warnedBytesMissing = true
				}
			}
		case StrategyByShardCount:
			score = scoreShards
		case StrategyHybrid:
			if bytesAvailable {
				score = 0.6*scoreBytes + 0.4*scoreShards
			} else {
				score = scoreShards
			}
		default:
			score = scoreShards
		}

		var pred PredictedAfter
		pred.SkewRatioShards = afterSkewShards
		pred.MaxDeviationShards = afterDevShards
		if bytesAvailable {
			pred.SkewRatioBytes = ptr(afterSkewBytes)
			pred.MaxDeviationBytes = ptr(afterDevBytes)
		}

		candidates = append(candidates, Candidate{
			Source:           w.Node,
			Score:            clamp(score, 0, 100),
			PredictedAfter:   pred,
			Why:              nil, // filled later in buildReasons
			reachable:        w.Reachable,
			shouldHaveShards: w.ShouldHaveShards,
		})
	}
	return candidates, warnings
}

func scoreComponent(beforeSkew, afterSkew, beforeDev, afterDev float64) float64 {
	if beforeSkew <= 0 {
		return 0
	}
	skewImprovement := (beforeSkew - afterSkew) / beforeSkew
	if skewImprovement < 0 {
		skewImprovement = 0
	}
	devImprovement := 0.0
	if beforeDev > 0 {
		devImprovement = (beforeDev - afterDev) / beforeDev
		if devImprovement < 0 {
			devImprovement = 0
		}
	}
	return (skewImprovement*0.7 + devImprovement*0.3) * 100
}

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

func ptr[T any](v T) *T { return &v }

func deref(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}
