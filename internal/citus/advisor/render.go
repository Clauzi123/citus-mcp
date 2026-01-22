// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Advisor output rendering and sorting.

package advisor

import (
	"sort"
	"time"
)

// Render builds the Output with ordering and scoring.
func Render(ctx *AdvisorContext, findings []Finding) Output {
	// Score and aggregate
	tableScores := map[string]int{}
	tableReasons := map[string][]string{}
	for i := range findings {
		f := &findings[i]
		var tmeta *TableMeta
		if f.Scope == "table" && ctx.TableMeta != nil {
			tmeta = ctx.TableMeta[f.Target]
		}
		skewRatio := ctx.Skew.Ratio
		f.Score = ScoreFinding(*f, tmeta, skewRatio)
		if f.Scope == "table" {
			tableScores[f.Target] += f.Score
			tableReasons[f.Target] = append(tableReasons[f.Target], f.Title)
		}
	}

	// Sort findings: severity desc, score desc, category asc, target asc, id asc
	sort.Slice(findings, func(i, j int) bool {
		fi, fj := findings[i], findings[j]
		if fi.Severity != fj.Severity {
			return severityRank(fi.Severity) < severityRank(fj.Severity)
		}
		if fi.Score != fj.Score {
			return fi.Score > fj.Score
		}
		if fi.Category != fj.Category {
			return fi.Category < fj.Category
		}
		if fi.Target != fj.Target {
			return fi.Target < fj.Target
		}
		return fi.ID < fj.ID
	})

	// Table rankings
	rankings := []TableRanking{}
	for tbl, score := range tableScores {
		rankings = append(rankings, TableRanking{Table: tbl, ImpactScore: score, Reasons: tableReasons[tbl]})
	}
	sort.Slice(rankings, func(i, j int) bool {
		if rankings[i].ImpactScore != rankings[j].ImpactScore {
			return rankings[i].ImpactScore > rankings[j].ImpactScore
		}
		return rankings[i].Table < rankings[j].Table
	})

	// Cluster observations: pick cluster-scope findings
	obs := []FindingSnippet{}
	for _, f := range findings {
		if f.Scope == "cluster" {
			obs = append(obs, FindingSnippet{Severity: f.Severity, Title: f.Title, Details: f.Problem, Evidence: f.Evidence})
		}
	}

	// Cluster health
	clusterHealth := "ok"
	highSeverity := 0
	for _, f := range findings {
		if f.Severity == "critical" {
			clusterHealth = "critical"
			highSeverity++
		} else if f.Severity == "warning" && clusterHealth == "ok" {
			clusterHealth = "warning"
			highSeverity++
		}
	}

	// Limit findings to MaxFindings
	if ctx.MaxFindings > 0 && len(findings) > ctx.MaxFindings {
		findings = findings[:ctx.MaxFindings]
	}
	if findings == nil {
		findings = []Finding{}
	}

	return Output{
		Summary: Summary{
			AdvisorVersion: "v1",
			Focus:          ctx.Focus,
			ClusterHealth:  clusterHealth,
			TablesAnalyzed: len(ctx.Tables),
			Findings:       len(findings),
			HighSeverity:   highSeverity,
			GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
		},
		ClusterObservations: obs,
		TableRankings:       rankings,
		Findings:            findings,
	}
}

func severityRank(sev string) int {
	switch sev {
	case "critical":
		return 0
	case "warning":
		return 1
	default:
		return 2
	}
}
