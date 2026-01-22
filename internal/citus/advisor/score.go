// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Scoring logic for advisor findings prioritization.

package advisor

// Severity to base score mapping.
var severityScore = map[string]int{
	"critical": 90,
	"warning":  60,
	"info":     25,
}

// ScoreFinding computes a score based on severity and impact inputs.
func ScoreFinding(f Finding, tmeta *TableMeta, skewRatio float64) int {
	base := severityScore[f.Severity]
	// shard multiplier
	if tmeta != nil && tmeta.ShardCount > 0 {
		mult := float64(tmeta.ShardCount)
		if mult > 1000 {
			mult = 1000
		}
		factor := 1.0 + mult/1000.0*0.3 // cap at 1.3
		base = int(float64(base) * factor)
	}
	// skew ratio adjustment
	if skewRatio > 1 {
		add := int((skewRatio - 1) * 10)
		if add > 20 {
			add = 20
		}
		base += add
	}
	if base > 100 {
		base = 100
	}
	if base < 0 {
		base = 0
	}
	return base
}
