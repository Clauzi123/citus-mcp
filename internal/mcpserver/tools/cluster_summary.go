// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Implements citus_cluster_summary tool for cluster overview and health.

package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"citus-mcp/internal/citus/guc"
	"citus-mcp/internal/db"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

// ClusterSummaryInput defines input for citus_cluster_summary.
type ClusterSummaryInput struct {
	IncludeWorkers bool `json:"include_workers,omitempty"`
	IncludeGUCs    bool `json:"include_gucs,omitempty"`
	IncludeConfig  bool `json:"include_config,omitempty"`
}

// ClusterSummaryOutput defines output structure.
type ClusterSummaryOutput struct {
	Coordinator       CoordinatorSummary   `json:"coordinator"`
	Workers           []WorkerSummary      `json:"workers,omitempty"`
	Counts            CountsSummary        `json:"counts"`
	Warnings          []string             `json:"warnings,omitempty"`
	GUCs              map[string]string    `json:"gucs,omitempty"`
	Configuration     *ConfigurationReport `json:"configuration,omitempty"`
}

// ConfigurationReport provides a summary of important configuration settings.
type ConfigurationReport struct {
	Summary           string                     `json:"summary"`
	OverallHealth     string                     `json:"overall_health"`
	CriticalIssues    int                        `json:"critical_issues"`
	Warnings          int                        `json:"warnings"`
	CitusSettings     map[string]ConfigValue     `json:"citus_settings"`
	PostgresSettings  map[string]ConfigValue     `json:"postgres_settings"`
	Recommendations   []string                   `json:"recommendations,omitempty"`
}

// ConfigValue represents a configuration parameter value with context.
type ConfigValue struct {
	Value       string `json:"value"`
	Unit        string `json:"unit,omitempty"`
	IsDefault   bool   `json:"is_default"`
	Status      string `json:"status,omitempty"` // ok, warning, critical
}

type CoordinatorSummary struct {
	Host            string `json:"host"`
	Port            int32  `json:"port"`
	PostgresVersion string `json:"postgres_version"`
	CitusVersion    string `json:"citus_version"`
}

type WorkerSummary struct {
	Host             string `json:"host"`
	Port             int32  `json:"port"`
	IsActive         bool   `json:"is_active"`
	ShouldHaveShards bool   `json:"should_have_shards"`
}

type CountsSummary struct {
	DistributedTables int `json:"distributed_tables"`
	ReferenceTables   int `json:"reference_tables"`
	ShardsTotal       int `json:"shards_total"`
	PlacementsTotal   int `json:"placements_total"`
}

func clusterSummaryTool(ctx context.Context, deps Dependencies, input ClusterSummaryInput) (*mcp.CallToolResult, ClusterSummaryOutput, error) {
	// defaults - include workers and config by default
	if !input.IncludeWorkers {
		input.IncludeWorkers = true
	}
	// Include config by default for comprehensive summary
	input.IncludeConfig = true

	// read-only guard for potential SQLs
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), ClusterSummaryOutput{}, nil
	}

	// validate citus extension
	ext, err := db.GetExtensionInfo(ctx, deps.Pool)
	if err != nil || ext == nil {
		return callError(serr.CodeNotCitus, "citus extension not found", "enable citus extension"), ClusterSummaryOutput{}, nil
	}

	serverInfo, err := db.GetServerInfo(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "server info"), ClusterSummaryOutput{}, nil
	}

	coordHost := deps.Pool.Config().ConnConfig.Host
	coordPort := int32(deps.Pool.Config().ConnConfig.Port)

	counts, err := fetchCounts(ctx, deps)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "counts"), ClusterSummaryOutput{}, nil
	}

	out := ClusterSummaryOutput{
		Coordinator: CoordinatorSummary{Host: coordHost, Port: coordPort, PostgresVersion: serverInfo.PostgresVersion, CitusVersion: serverInfo.CitusVersion},
		Counts:      counts,
	}

	warnings := []string{}

	if input.IncludeWorkers {
		workers, wWarnings := fetchWorkers(ctx, deps)
		out.Workers = workers
		warnings = append(warnings, wWarnings...)
	}

	if input.IncludeGUCs {
		gucs, err := fetchGUCs(ctx, deps)
		if err == nil {
			out.GUCs = gucs
		}
	}

	// Include configuration report
	if input.IncludeConfig {
		configReport, configWarnings := fetchConfigurationReport(ctx, deps, len(out.Workers))
		out.Configuration = configReport
		warnings = append(warnings, configWarnings...)
	}

	if len(warnings) > 0 {
		out.Warnings = warnings
	}
	return nil, out, nil
}

// ClusterSummary is exported for reuse by resources.
func ClusterSummary(ctx context.Context, deps Dependencies, input ClusterSummaryInput) (*mcp.CallToolResult, ClusterSummaryOutput, error) {
	return clusterSummaryTool(ctx, deps, input)
}

func fetchCounts(ctx context.Context, deps Dependencies) (CountsSummary, error) {
	var counts CountsSummary
	// distributed tables
	if err := deps.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_partition WHERE partmethod <> 'n'").Scan(&counts.DistributedTables); err != nil {
		return counts, err
	}
	if err := deps.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_partition WHERE partmethod = 'n'").Scan(&counts.ReferenceTables); err != nil {
		return counts, err
	}
	if err := deps.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_shard").Scan(&counts.ShardsTotal); err != nil {
		return counts, err
	}
	if err := deps.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_shard_placement").Scan(&counts.PlacementsTotal); err != nil {
		return counts, err
	}
	return counts, nil
}

func fetchWorkers(ctx context.Context, deps Dependencies) ([]WorkerSummary, []string) {
	infos, err := deps.WorkerManager.Topology(ctx)
	if err != nil {
		deps.Logger.Warn("topology fetch failed", zap.Error(err))
		return nil, []string{"failed to fetch worker topology"}
	}
	workers := make([]WorkerSummary, 0, len(infos))
	warnings := []string{}
	if len(infos) == 0 {
		warnings = append(warnings, "no workers")
	}
	for _, info := range infos {
		if !info.IsActive {
			warnings = append(warnings, "inactive worker: "+info.NodeName)
		}
		workers = append(workers, WorkerSummary{Host: info.NodeName, Port: info.NodePort, IsActive: info.IsActive, ShouldHaveShards: info.ShouldHaveShards})
	}
	sort.Slice(workers, func(i, j int) bool {
		if workers[i].Host == workers[j].Host {
			return workers[i].Port < workers[j].Port
		}
		return workers[i].Host < workers[j].Host
	})

	// mixed worker versions (best-effort)
	if pools, infos2, err := deps.WorkerManager.Pools(ctx); err == nil {
		versions := map[string]struct{}{}
		for id, pool := range pools {
			if pool == nil {
				continue
			}
			if _, ok := workerInfoByID(infos2, id); ok {
				if v, err := db.GetServerInfo(ctx, pool); err == nil {
					versions[v.PostgresVersion+"|"+v.CitusVersion] = struct{}{}
				}
			}
		}
		if len(versions) > 1 {
			warnings = append(warnings, "mixed worker versions")
		}
	}

	return workers, warnings
}

func workerInfoByID(infos []db.WorkerInfo, id int32) (db.WorkerInfo, bool) {
	for _, w := range infos {
		if w.NodeID == id {
			return w, true
		}
	}
	return db.WorkerInfo{}, false
}

func fetchGUCs(ctx context.Context, deps Dependencies) (map[string]string, error) {
	rows, err := deps.Pool.Query(ctx, "SELECT name, setting FROM pg_settings WHERE name IN ('citus.shard_count','max_connections')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	gucs := map[string]string{}
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			return nil, err
		}
		gucs[name] = setting
	}
	return gucs, rows.Err()
}

// fetchConfigurationReport builds a comprehensive configuration report.
func fetchConfigurationReport(ctx context.Context, deps Dependencies, workerCount int) (*ConfigurationReport, []string) {
	warnings := []string{}

	// Fetch Citus GUCs
	citusGUCs, err := guc.FetchCitusGUCs(ctx, deps.Pool)
	if err != nil {
		warnings = append(warnings, "failed to fetch Citus GUCs")
		return nil, warnings
	}

	// Fetch PostgreSQL GUCs
	postgresGUCs, err := guc.FetchRelevantPostgresGUCs(ctx, deps.Pool)
	if err != nil {
		warnings = append(warnings, "failed to fetch PostgreSQL GUCs")
		return nil, warnings
	}

	// Key Citus settings to include
	keyCitusGUCs := []string{
		"citus.shard_count",
		"citus.shard_replication_factor",
		"citus.max_adaptive_executor_pool_size",
		"citus.multi_shard_modify_mode",
		"citus.enable_repartition_joins",
		"citus.node_connection_timeout",
		"citus.max_background_task_executors_per_node",
		"citus.use_secondary_nodes",
	}

	// Key PostgreSQL settings to include
	keyPostgresGUCs := []string{
		"max_connections",
		"shared_buffers",
		"work_mem",
		"maintenance_work_mem",
		"max_worker_processes",
		"wal_level",
		"max_wal_senders",
		"max_replication_slots",
		"shared_preload_libraries",
		"statement_timeout",
	}

	// Build analysis context for rules
	analysisCtx := &guc.AnalysisContext{
		CitusGUCs:     citusGUCs,
		PostgresGUCs:  postgresGUCs,
		WorkerCount:   workerCount,
		IsCoordinator: true,
	}

	// Run analysis rules
	findings := guc.EvaluateAllRules(analysisCtx)

	// Count issues
	criticalCount := 0
	warningCount := 0
	recommendations := []string{}

	for _, f := range findings {
		switch f.Severity {
		case guc.SeverityCritical:
			criticalCount++
			recommendations = append(recommendations, fmt.Sprintf("[CRITICAL] %s: %s", f.Title, f.Recommendation))
		case guc.SeverityWarning:
			warningCount++
			if len(recommendations) < 5 {
				recommendations = append(recommendations, fmt.Sprintf("[WARNING] %s: %s", f.Title, f.Recommendation))
			}
		}
	}

	// Build Citus settings map
	citusSettings := make(map[string]ConfigValue)
	for _, name := range keyCitusGUCs {
		if g, ok := citusGUCs[name]; ok {
			unit := ""
			if g.Unit != nil {
				unit = *g.Unit
			}
			status := "ok"
			// Check if there's a finding for this GUC
			for _, f := range findings {
				for _, affected := range f.AffectedGUCs {
					if affected == name {
						status = string(f.Severity)
						break
					}
				}
			}
			citusSettings[name] = ConfigValue{
				Value:     g.Setting,
				Unit:      unit,
				IsDefault: g.Source == "default",
				Status:    status,
			}
		}
	}

	// Build PostgreSQL settings map
	postgresSettings := make(map[string]ConfigValue)
	for _, name := range keyPostgresGUCs {
		if g, ok := postgresGUCs[name]; ok {
			unit := ""
			if g.Unit != nil {
				unit = *g.Unit
			}
			// Format memory values nicely
			value := g.Setting
			if unit == "8kB" || unit == "kB" {
				if bytes, err := guc.ParseBytes(g.Setting, unit); err == nil {
					value = guc.FormatBytes(bytes)
					unit = ""
				}
			}
			status := "ok"
			// Check if there's a finding for this GUC
			for _, f := range findings {
				for _, affected := range f.AffectedGUCs {
					if affected == name {
						if f.Severity == guc.SeverityCritical || f.Severity == guc.SeverityWarning {
							status = string(f.Severity)
						}
						break
					}
				}
			}
			postgresSettings[name] = ConfigValue{
				Value:     value,
				Unit:      unit,
				IsDefault: g.Source == "default",
				Status:    status,
			}
		}
	}

	// Determine overall health
	overallHealth := "healthy"
	if criticalCount > 0 {
		overallHealth = "critical"
	} else if warningCount > 0 {
		overallHealth = "needs_attention"
	}

	// Build summary paragraph
	summary := buildConfigSummary(citusGUCs, postgresGUCs, workerCount, criticalCount, warningCount)

	return &ConfigurationReport{
		Summary:          summary,
		OverallHealth:    overallHealth,
		CriticalIssues:   criticalCount,
		Warnings:         warningCount,
		CitusSettings:    citusSettings,
		PostgresSettings: postgresSettings,
		Recommendations:  recommendations,
	}, warnings
}

// buildConfigSummary creates a human-readable summary paragraph about the cluster configuration.
func buildConfigSummary(citusGUCs, postgresGUCs map[string]guc.GUCValue, workerCount, criticalCount, warningCount int) string {
	var parts []string

	// Shard configuration
	if g, ok := citusGUCs["citus.shard_count"]; ok {
		shardCount := g.Setting
		shardsPerWorker := "N/A"
		if workerCount > 0 {
			if sc, err := guc.ParseInt(shardCount); err == nil {
				shardsPerWorker = fmt.Sprintf("%d", sc/int64(workerCount))
			}
		}
		parts = append(parts, fmt.Sprintf("The cluster is configured with %s shards per distributed table (%s shards per worker)", shardCount, shardsPerWorker))
	}

	// Replication factor
	if g, ok := citusGUCs["citus.shard_replication_factor"]; ok {
		if g.Setting == "1" {
			parts = append(parts, "using single-copy replication (recommended for PostgreSQL HA setups)")
		} else {
			parts = append(parts, fmt.Sprintf("using %s-way shard replication for high availability", g.Setting))
		}
	}

	// Execution mode
	if g, ok := citusGUCs["citus.multi_shard_modify_mode"]; ok {
		parts = append(parts, fmt.Sprintf("Multi-shard modifications run in %s mode", g.Setting))
	}

	// WAL level check
	if g, ok := postgresGUCs["wal_level"]; ok {
		if strings.ToLower(g.Setting) == "logical" {
			parts = append(parts, "WAL level is correctly set to 'logical' enabling shard moves and rebalancing")
		} else {
			parts = append(parts, fmt.Sprintf("WARNING: wal_level='%s' - shard moves and rebalancing will not work", g.Setting))
		}
	}

	// Memory configuration
	if g, ok := postgresGUCs["shared_buffers"]; ok {
		if g.Unit != nil {
			if bytes, err := guc.ParseBytes(g.Setting, *g.Unit); err == nil {
				parts = append(parts, fmt.Sprintf("Memory: shared_buffers=%s", guc.FormatBytes(bytes)))
			}
		}
	}

	// Connection capacity
	if g, ok := postgresGUCs["max_connections"]; ok {
		parts = append(parts, fmt.Sprintf("max_connections=%s", g.Setting))
	}

	// Health status
	if criticalCount > 0 {
		parts = append(parts, fmt.Sprintf("ATTENTION: %d critical configuration issues detected that should be addressed immediately", criticalCount))
	} else if warningCount > 0 {
		parts = append(parts, fmt.Sprintf("Note: %d configuration warnings detected - consider reviewing recommendations", warningCount))
	} else {
		parts = append(parts, "Configuration appears healthy with no critical issues detected")
	}

	return strings.Join(parts, ". ") + "."
}
