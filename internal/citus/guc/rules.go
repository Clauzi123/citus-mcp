// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Configuration analysis rules for Citus and PostgreSQL GUCs.

package guc

import (
	"fmt"
	"strings"
)

// Severity levels for findings.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// ConfigFinding represents a configuration issue or recommendation.
type ConfigFinding struct {
	ID              string         `json:"id"`
	Severity        Severity       `json:"severity"`
	Category        Category       `json:"category"`
	Title           string         `json:"title"`
	Problem         string         `json:"problem"`
	Impact          string         `json:"impact"`
	Recommendation  string         `json:"recommendation"`
	CurrentValue    string         `json:"current_value"`
	RecommendedVal  string         `json:"recommended_value,omitempty"`
	FixSQL          string         `json:"fix_sql,omitempty"`
	Evidence        map[string]any `json:"evidence,omitempty"`
	AffectedGUCs    []string       `json:"affected_gucs,omitempty"`
	RequiresRestart bool           `json:"requires_restart"`
	DocLink         string         `json:"doc_link,omitempty"`
}

// Rule defines a configuration analysis rule.
type Rule interface {
	ID() string
	Category() Category
	Evaluate(ctx *AnalysisContext) []ConfigFinding
}

// AnalysisContext holds all data needed for rule evaluation.
type AnalysisContext struct {
	CitusGUCs     map[string]GUCValue
	PostgresGUCs  map[string]GUCValue
	AllGUCs       map[string]GUCValue
	WorkerCount   int
	ShardCount    int
	TotalRAMBytes int64
	IsCoordinator bool
}

// EvaluateAllRules runs all configuration rules and returns findings.
func EvaluateAllRules(ctx *AnalysisContext) []ConfigFinding {
	rules := []Rule{
		// Critical rules
		&RuleWalLevel{},
		&RuleSharedPreloadLibraries{},
		&RuleMaxWorkerProcesses{},
		&RuleMaxReplicationSlots{},
		&RuleMaxWalSenders{},

		// Memory rules
		&RuleSharedBuffers{},
		&RuleWorkMem{},
		&RuleMaintenanceWorkMem{},
		&RuleEffectiveCacheSize{},

		// Connection rules
		&RuleMaxConnections{},
		&RuleConnectionsVsMemory{},

		// Citus-specific rules
		&RuleShardCount{},
		&RuleShardReplicationFactor{},
		&RuleMaxAdaptiveExecutorPoolSize{},
		&RuleBackgroundTaskExecutors{},
		&RuleNodeConnectionTimeout{},

		// Performance rules
		&RuleRandomPageCost{},
		&RuleEffectiveIOConcurrency{},
		&RuleStatisticsTarget{},

		// Operations rules
		&RuleStatementTimeout{},
		&RuleLockTimeout{},
		&RuleIdleInTransactionTimeout{},
		&RuleLogMinDurationStatement{},

		// Parallelism rules
		&RuleParallelWorkers{},

		// HA rules
		&RuleUseSecondaryNodes{},
	}

	var findings []ConfigFinding
	for _, rule := range rules {
		fs := rule.Evaluate(ctx)
		findings = append(findings, fs...)
	}
	return findings
}

// Helper to get GUC value or default
func (ctx *AnalysisContext) GetGUC(name string) (GUCValue, bool) {
	if v, ok := ctx.CitusGUCs[name]; ok {
		return v, true
	}
	if v, ok := ctx.PostgresGUCs[name]; ok {
		return v, true
	}
	if v, ok := ctx.AllGUCs[name]; ok {
		return v, true
	}
	return GUCValue{}, false
}

func (ctx *AnalysisContext) GetGUCInt(name string) (int64, bool) {
	if g, ok := ctx.GetGUC(name); ok {
		if v, err := ParseInt(g.Setting); err == nil {
			return v, true
		}
	}
	return 0, false
}

func (ctx *AnalysisContext) GetGUCBool(name string) (bool, bool) {
	if g, ok := ctx.GetGUC(name); ok {
		return ParseBool(g.Setting), true
	}
	return false, false
}

func (ctx *AnalysisContext) GetGUCBytes(name string) (int64, bool) {
	if g, ok := ctx.GetGUC(name); ok {
		unit := ""
		if g.Unit != nil {
			unit = *g.Unit
		}
		if v, err := ParseBytes(g.Setting, unit); err == nil {
			return v, true
		}
	}
	return 0, false
}

// =============================================================================
// Critical Rules
// =============================================================================

// RuleWalLevel checks wal_level is set to logical for Citus operations.
type RuleWalLevel struct{}

func (r *RuleWalLevel) ID() string         { return "rule.config.wal_level" }
func (r *RuleWalLevel) Category() Category { return CategoryReplication }
func (r *RuleWalLevel) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	g, ok := ctx.GetGUC("wal_level")
	if !ok {
		return nil
	}
	if strings.ToLower(g.Setting) != "logical" {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityCritical,
			Category:        r.Category(),
			Title:           "wal_level must be 'logical' for Citus",
			Problem:         fmt.Sprintf("wal_level is '%s', but Citus requires 'logical' for shard moves and rebalancing", g.Setting),
			Impact:          "Shard moves, rebalancing, and online schema changes will fail. Cluster cannot be safely modified.",
			Recommendation:  "Set wal_level = 'logical' in postgresql.conf and restart PostgreSQL",
			CurrentValue:    g.Setting,
			RecommendedVal:  "logical",
			FixSQL:          "ALTER SYSTEM SET wal_level = 'logical'; -- Requires restart",
			RequiresRestart: true,
			AffectedGUCs:    []string{"wal_level"},
			DocLink:         "https://docs.citusdata.com/en/stable/admin_guide/cluster_management.html",
		}}
	}
	return nil
}

// RuleSharedPreloadLibraries checks citus is in shared_preload_libraries.
type RuleSharedPreloadLibraries struct{}

func (r *RuleSharedPreloadLibraries) ID() string         { return "rule.config.shared_preload_libraries" }
func (r *RuleSharedPreloadLibraries) Category() Category { return CategoryOperations }
func (r *RuleSharedPreloadLibraries) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	g, ok := ctx.GetGUC("shared_preload_libraries")
	if !ok {
		return nil
	}
	libs := strings.ToLower(g.Setting)
	if !strings.Contains(libs, "citus") {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityCritical,
			Category:        r.Category(),
			Title:           "'citus' missing from shared_preload_libraries",
			Problem:         fmt.Sprintf("shared_preload_libraries='%s' does not include 'citus'", g.Setting),
			Impact:          "Citus extension cannot function properly without being preloaded.",
			Recommendation:  "Add 'citus' to shared_preload_libraries and restart PostgreSQL",
			CurrentValue:    g.Setting,
			RecommendedVal:  "citus",
			FixSQL:          "ALTER SYSTEM SET shared_preload_libraries = 'citus'; -- Requires restart",
			RequiresRestart: true,
			AffectedGUCs:    []string{"shared_preload_libraries"},
		}}
	}
	return nil
}

// RuleMaxWorkerProcesses checks worker processes are sufficient.
type RuleMaxWorkerProcesses struct{}

func (r *RuleMaxWorkerProcesses) ID() string         { return "rule.config.max_worker_processes" }
func (r *RuleMaxWorkerProcesses) Category() Category { return CategoryParallelism }
func (r *RuleMaxWorkerProcesses) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	maxWorkers, ok := ctx.GetGUCInt("max_worker_processes")
	if !ok {
		return nil
	}

	// Citus needs workers for: maintenance daemon, background tasks, parallel queries
	// Minimum: 8 + (workers * 2) for a healthy setup
	minRequired := int64(16)
	if ctx.WorkerCount > 0 {
		minRequired = int64(8 + ctx.WorkerCount*2)
	}

	if maxWorkers < minRequired {
		severity := SeverityWarning
		if maxWorkers < 8 {
			severity = SeverityCritical
		}
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        severity,
			Category:        r.Category(),
			Title:           "max_worker_processes may be too low for Citus",
			Problem:         fmt.Sprintf("max_worker_processes=%d is below recommended %d for a %d-worker cluster", maxWorkers, minRequired, ctx.WorkerCount),
			Impact:          "Background maintenance, shard rebalancing, and parallel queries may be limited or fail.",
			Recommendation:  fmt.Sprintf("Set max_worker_processes >= %d", minRequired),
			CurrentValue:    fmt.Sprintf("%d", maxWorkers),
			RecommendedVal:  fmt.Sprintf("%d", minRequired),
			FixSQL:          fmt.Sprintf("ALTER SYSTEM SET max_worker_processes = %d; -- Requires restart", minRequired),
			RequiresRestart: true,
			AffectedGUCs:    []string{"max_worker_processes"},
			Evidence:        map[string]any{"worker_count": ctx.WorkerCount, "min_required": minRequired},
		}}
	}
	return nil
}

// RuleMaxReplicationSlots checks replication slots are sufficient.
type RuleMaxReplicationSlots struct{}

func (r *RuleMaxReplicationSlots) ID() string         { return "rule.config.max_replication_slots" }
func (r *RuleMaxReplicationSlots) Category() Category { return CategoryReplication }
func (r *RuleMaxReplicationSlots) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	slots, ok := ctx.GetGUCInt("max_replication_slots")
	if !ok {
		return nil
	}

	// Each concurrent shard move uses replication slots
	// Recommend at least 10 + (background_task_executors * workers)
	bgExecutors, _ := ctx.GetGUCInt("citus.max_background_task_executors_per_node")
	if bgExecutors == 0 {
		bgExecutors = 1
	}
	minRequired := int64(10)
	if ctx.WorkerCount > 0 {
		minRequired = 10 + bgExecutors*int64(ctx.WorkerCount)
	}

	if slots < minRequired {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityWarning,
			Category:        r.Category(),
			Title:           "max_replication_slots may limit shard operations",
			Problem:         fmt.Sprintf("max_replication_slots=%d, recommend at least %d for parallel shard moves", slots, minRequired),
			Impact:          "Concurrent shard moves during rebalancing may be limited.",
			Recommendation:  fmt.Sprintf("Set max_replication_slots >= %d", minRequired),
			CurrentValue:    fmt.Sprintf("%d", slots),
			RecommendedVal:  fmt.Sprintf("%d", minRequired),
			FixSQL:          fmt.Sprintf("ALTER SYSTEM SET max_replication_slots = %d; -- Requires restart", minRequired),
			RequiresRestart: true,
			AffectedGUCs:    []string{"max_replication_slots"},
		}}
	}
	return nil
}

// RuleMaxWalSenders checks WAL senders are sufficient.
type RuleMaxWalSenders struct{}

func (r *RuleMaxWalSenders) ID() string         { return "rule.config.max_wal_senders" }
func (r *RuleMaxWalSenders) Category() Category { return CategoryReplication }
func (r *RuleMaxWalSenders) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	senders, ok := ctx.GetGUCInt("max_wal_senders")
	if !ok {
		return nil
	}

	minRequired := int64(10)
	if ctx.WorkerCount > 2 {
		minRequired = int64(ctx.WorkerCount * 3)
	}

	if senders < minRequired {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityWarning,
			Category:        r.Category(),
			Title:           "max_wal_senders may limit replication",
			Problem:         fmt.Sprintf("max_wal_senders=%d, recommend at least %d", senders, minRequired),
			Impact:          "Logical replication for shard moves may be limited.",
			Recommendation:  fmt.Sprintf("Set max_wal_senders >= %d", minRequired),
			CurrentValue:    fmt.Sprintf("%d", senders),
			RecommendedVal:  fmt.Sprintf("%d", minRequired),
			FixSQL:          fmt.Sprintf("ALTER SYSTEM SET max_wal_senders = %d; -- Requires restart", minRequired),
			RequiresRestart: true,
			AffectedGUCs:    []string{"max_wal_senders"},
		}}
	}
	return nil
}

// =============================================================================
// Memory Rules
// =============================================================================

// RuleSharedBuffers analyzes shared_buffers setting.
type RuleSharedBuffers struct{}

func (r *RuleSharedBuffers) ID() string         { return "rule.config.shared_buffers" }
func (r *RuleSharedBuffers) Category() Category { return CategoryMemory }
func (r *RuleSharedBuffers) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	bytes, ok := ctx.GetGUCBytes("shared_buffers")
	if !ok {
		return nil
	}

	// Check if less than 256MB
	minBytes := int64(256 * 1024 * 1024)
	if bytes < minBytes {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityWarning,
			Category:        r.Category(),
			Title:           "shared_buffers is below recommended minimum",
			Problem:         fmt.Sprintf("shared_buffers=%s is below recommended 256MB minimum", FormatBytes(bytes)),
			Impact:          "Insufficient buffer cache leads to excessive disk I/O and poor query performance.",
			Recommendation:  "Set shared_buffers to 15-25% of available RAM, minimum 256MB",
			CurrentValue:    FormatBytes(bytes),
			RecommendedVal:  "256MB - 25% of RAM",
			FixSQL:          "ALTER SYSTEM SET shared_buffers = '1GB'; -- Adjust based on RAM, requires restart",
			RequiresRestart: true,
			AffectedGUCs:    []string{"shared_buffers"},
		}}
	}

	// Check if RAM info available and shared_buffers > 25%
	if ctx.TotalRAMBytes > 0 {
		maxRecommended := ctx.TotalRAMBytes / 4 // 25%
		if bytes > maxRecommended {
			return []ConfigFinding{{
				ID:              r.ID(),
				Severity:        SeverityWarning,
				Category:        r.Category(),
				Title:           "shared_buffers may be too high",
				Problem:         fmt.Sprintf("shared_buffers=%s exceeds 25%% of RAM (%s)", FormatBytes(bytes), FormatBytes(ctx.TotalRAMBytes)),
				Impact:          "Excessive shared_buffers can starve OS file system cache, reducing overall performance.",
				Recommendation:  "Set shared_buffers to 15-25% of available RAM",
				CurrentValue:    FormatBytes(bytes),
				RecommendedVal:  FormatBytes(maxRecommended),
				FixSQL:          fmt.Sprintf("ALTER SYSTEM SET shared_buffers = '%s'; -- Requires restart", FormatBytes(maxRecommended)),
				RequiresRestart: true,
				AffectedGUCs:    []string{"shared_buffers"},
			}}
		}
	}
	return nil
}

// RuleWorkMem analyzes work_mem setting.
type RuleWorkMem struct{}

func (r *RuleWorkMem) ID() string         { return "rule.config.work_mem" }
func (r *RuleWorkMem) Category() Category { return CategoryMemory }
func (r *RuleWorkMem) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	bytes, ok := ctx.GetGUCBytes("work_mem")
	if !ok {
		return nil
	}

	// Check if less than 4MB
	minBytes := int64(4 * 1024 * 1024)
	if bytes < minBytes {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityInfo,
			Category:        r.Category(),
			Title:           "work_mem is at default value",
			Problem:         fmt.Sprintf("work_mem=%s is at PostgreSQL default", FormatBytes(bytes)),
			Impact:          "Complex sorts and hash joins may spill to disk, reducing performance.",
			Recommendation:  "Consider increasing work_mem to 16-64MB for distributed query workloads",
			CurrentValue:    FormatBytes(bytes),
			RecommendedVal:  "16MB - 64MB",
			FixSQL:          "ALTER SYSTEM SET work_mem = '32MB';",
			RequiresRestart: false,
			AffectedGUCs:    []string{"work_mem"},
		}}
	}

	// Check for dangerous high values
	maxConns, hasConns := ctx.GetGUCInt("max_connections")
	if hasConns && bytes > 256*1024*1024 { // > 256MB
		potentialUsage := bytes * maxConns * 4 // 4 ops per connection worst case
		if ctx.TotalRAMBytes > 0 && potentialUsage > ctx.TotalRAMBytes {
			return []ConfigFinding{{
				ID:              r.ID(),
				Severity:        SeverityCritical,
				Category:        r.Category(),
				Title:           "work_mem * max_connections risks OOM",
				Problem:         fmt.Sprintf("work_mem=%s with max_connections=%d could use up to %s per query", FormatBytes(bytes), maxConns, FormatBytes(potentialUsage)),
				Impact:          "Risk of out-of-memory errors under load.",
				Recommendation:  "Reduce work_mem or max_connections to prevent OOM",
				CurrentValue:    FormatBytes(bytes),
				RecommendedVal:  "16MB - 64MB",
				FixSQL:          "ALTER SYSTEM SET work_mem = '64MB';",
				RequiresRestart: false,
				AffectedGUCs:    []string{"work_mem", "max_connections"},
				Evidence:        map[string]any{"max_connections": maxConns, "potential_usage": FormatBytes(potentialUsage)},
			}}
		}
	}
	return nil
}

// RuleMaintenanceWorkMem analyzes maintenance_work_mem.
type RuleMaintenanceWorkMem struct{}

func (r *RuleMaintenanceWorkMem) ID() string         { return "rule.config.maintenance_work_mem" }
func (r *RuleMaintenanceWorkMem) Category() Category { return CategoryMemory }
func (r *RuleMaintenanceWorkMem) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	bytes, ok := ctx.GetGUCBytes("maintenance_work_mem")
	if !ok {
		return nil
	}

	minBytes := int64(256 * 1024 * 1024) // 256MB
	if bytes < minBytes {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityInfo,
			Category:        r.Category(),
			Title:           "maintenance_work_mem could be increased",
			Problem:         fmt.Sprintf("maintenance_work_mem=%s is below recommended 256MB", FormatBytes(bytes)),
			Impact:          "VACUUM, CREATE INDEX, and other maintenance operations may be slower.",
			Recommendation:  "Set maintenance_work_mem to 256MB-1GB for faster maintenance",
			CurrentValue:    FormatBytes(bytes),
			RecommendedVal:  "256MB - 1GB",
			FixSQL:          "ALTER SYSTEM SET maintenance_work_mem = '512MB';",
			RequiresRestart: false,
			AffectedGUCs:    []string{"maintenance_work_mem"},
		}}
	}
	return nil
}

// RuleEffectiveCacheSize analyzes effective_cache_size.
type RuleEffectiveCacheSize struct{}

func (r *RuleEffectiveCacheSize) ID() string         { return "rule.config.effective_cache_size" }
func (r *RuleEffectiveCacheSize) Category() Category { return CategoryMemory }
func (r *RuleEffectiveCacheSize) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	bytes, ok := ctx.GetGUCBytes("effective_cache_size")
	if !ok {
		return nil
	}

	if ctx.TotalRAMBytes > 0 {
		recommended := ctx.TotalRAMBytes * 3 / 4 // 75%
		if bytes < recommended/2 {
			return []ConfigFinding{{
				ID:              r.ID(),
				Severity:        SeverityInfo,
				Category:        r.Category(),
				Title:           "effective_cache_size may be underestimated",
				Problem:         fmt.Sprintf("effective_cache_size=%s is less than 50%% of RAM", FormatBytes(bytes)),
				Impact:          "Query planner may not choose optimal plans, preferring sequential scans over index scans.",
				Recommendation:  "Set effective_cache_size to 50-75% of total RAM",
				CurrentValue:    FormatBytes(bytes),
				RecommendedVal:  FormatBytes(recommended),
				FixSQL:          fmt.Sprintf("ALTER SYSTEM SET effective_cache_size = '%s';", FormatBytes(recommended)),
				RequiresRestart: false,
				AffectedGUCs:    []string{"effective_cache_size"},
			}}
		}
	}
	return nil
}

// =============================================================================
// Connection Rules
// =============================================================================

// RuleMaxConnections analyzes max_connections.
type RuleMaxConnections struct{}

func (r *RuleMaxConnections) ID() string         { return "rule.config.max_connections" }
func (r *RuleMaxConnections) Category() Category { return CategoryConnections }
func (r *RuleMaxConnections) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	conns, ok := ctx.GetGUCInt("max_connections")
	if !ok {
		return nil
	}

	// For coordinator: need connections for clients + internal Citus connections
	// For workers: need connections for coordinator + other workers
	minRequired := int64(100)
	if ctx.WorkerCount > 0 && ctx.IsCoordinator {
		// Coordinator needs: client connections + (workers * internal_pool_size)
		poolSize, _ := ctx.GetGUCInt("citus.max_adaptive_executor_pool_size")
		if poolSize == 0 {
			poolSize = 16
		}
		minRequired = 100 + int64(ctx.WorkerCount)*poolSize
	}

	if conns < minRequired {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityWarning,
			Category:        r.Category(),
			Title:           "max_connections may be insufficient",
			Problem:         fmt.Sprintf("max_connections=%d, recommend at least %d for cluster with %d workers", conns, minRequired, ctx.WorkerCount),
			Impact:          "Connection exhaustion may cause query failures during high load.",
			Recommendation:  fmt.Sprintf("Set max_connections >= %d", minRequired),
			CurrentValue:    fmt.Sprintf("%d", conns),
			RecommendedVal:  fmt.Sprintf("%d", minRequired),
			FixSQL:          fmt.Sprintf("ALTER SYSTEM SET max_connections = %d; -- Requires restart", minRequired),
			RequiresRestart: true,
			AffectedGUCs:    []string{"max_connections"},
			Evidence:        map[string]any{"worker_count": ctx.WorkerCount, "is_coordinator": ctx.IsCoordinator},
		}}
	}

	// Check for very high connections
	if conns > 500 {
		return []ConfigFinding{{
			ID:             r.ID(),
			Severity:       SeverityInfo,
			Category:       r.Category(),
			Title:          "High max_connections - consider connection pooling",
			Problem:        fmt.Sprintf("max_connections=%d is high", conns),
			Impact:         "Each connection uses memory. High values may require memory tuning.",
			Recommendation: "Consider using PgBouncer for connection pooling if not already in use",
			CurrentValue:   fmt.Sprintf("%d", conns),
			AffectedGUCs:   []string{"max_connections"},
		}}
	}
	return nil
}

// RuleConnectionsVsMemory checks memory per connection.
type RuleConnectionsVsMemory struct{}

func (r *RuleConnectionsVsMemory) ID() string         { return "rule.config.connections_vs_memory" }
func (r *RuleConnectionsVsMemory) Category() Category { return CategoryMemory }
func (r *RuleConnectionsVsMemory) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	conns, hasConns := ctx.GetGUCInt("max_connections")
	workMem, hasWork := ctx.GetGUCBytes("work_mem")
	sharedBuf, hasShared := ctx.GetGUCBytes("shared_buffers")

	if !hasConns || !hasWork || !hasShared || ctx.TotalRAMBytes == 0 {
		return nil
	}

	// Estimate worst-case memory: shared_buffers + (connections * work_mem * 4)
	worstCase := sharedBuf + (conns * workMem * 4)
	if worstCase > ctx.TotalRAMBytes*9/10 { // > 90% of RAM
		return []ConfigFinding{{
			ID:             r.ID(),
			Severity:       SeverityWarning,
			Category:       r.Category(),
			Title:          "Memory settings may risk OOM under load",
			Problem:        fmt.Sprintf("shared_buffers + (max_connections × work_mem) = %s could exceed available RAM", FormatBytes(worstCase)),
			Impact:         "Risk of out-of-memory conditions under heavy parallel workloads.",
			Recommendation: "Reduce work_mem or max_connections, or add RAM",
			CurrentValue:   fmt.Sprintf("max_connections=%d, work_mem=%s, shared_buffers=%s", conns, FormatBytes(workMem), FormatBytes(sharedBuf)),
			AffectedGUCs:   []string{"max_connections", "work_mem", "shared_buffers"},
			Evidence:       map[string]any{"worst_case_memory": FormatBytes(worstCase), "total_ram": FormatBytes(ctx.TotalRAMBytes)},
		}}
	}
	return nil
}

// =============================================================================
// Citus-Specific Rules
// =============================================================================

// RuleShardCount analyzes citus.shard_count.
type RuleShardCount struct{}

func (r *RuleShardCount) ID() string         { return "rule.config.shard_count" }
func (r *RuleShardCount) Category() Category { return CategoryDistribution }
func (r *RuleShardCount) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	shardCount, ok := ctx.GetGUCInt("citus.shard_count")
	if !ok {
		return nil
	}

	// Recommend: shard_count >= worker_count * 8 for good distribution
	if ctx.WorkerCount > 0 {
		minRecommended := int64(ctx.WorkerCount * 8)
		if shardCount < minRecommended {
			return []ConfigFinding{{
				ID:              r.ID(),
				Severity:        SeverityInfo,
				Category:        r.Category(),
				Title:           "shard_count may limit parallelism",
				Problem:         fmt.Sprintf("citus.shard_count=%d with %d workers. Recommend at least %d shards.", shardCount, ctx.WorkerCount, minRecommended),
				Impact:          "Fewer shards per worker reduces parallelism and may cause uneven data distribution.",
				Recommendation:  fmt.Sprintf("Set citus.shard_count = %d or higher for new tables", minRecommended),
				CurrentValue:    fmt.Sprintf("%d", shardCount),
				RecommendedVal:  fmt.Sprintf("%d", minRecommended),
				FixSQL:          fmt.Sprintf("ALTER SYSTEM SET citus.shard_count = %d;", minRecommended),
				RequiresRestart: false,
				AffectedGUCs:    []string{"citus.shard_count"},
				Evidence:        map[string]any{"worker_count": ctx.WorkerCount, "shards_per_worker": shardCount / int64(ctx.WorkerCount)},
			}}
		}

		// Check for very high shard count
		maxRecommended := int64(ctx.WorkerCount * 64)
		if shardCount > maxRecommended {
			return []ConfigFinding{{
				ID:             r.ID(),
				Severity:       SeverityInfo,
				Category:       r.Category(),
				Title:          "High shard_count increases overhead",
				Problem:        fmt.Sprintf("citus.shard_count=%d creates many shards per worker", shardCount),
				Impact:         "Too many shards increases planning overhead and metadata size.",
				Recommendation: "Consider 32-128 shards per worker as optimal range",
				CurrentValue:   fmt.Sprintf("%d", shardCount),
				AffectedGUCs:   []string{"citus.shard_count"},
			}}
		}
	}
	return nil
}

// RuleShardReplicationFactor analyzes replication factor.
type RuleShardReplicationFactor struct{}

func (r *RuleShardReplicationFactor) ID() string         { return "rule.config.shard_replication_factor" }
func (r *RuleShardReplicationFactor) Category() Category { return CategoryHA }
func (r *RuleShardReplicationFactor) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	replFactor, ok := ctx.GetGUCInt("citus.shard_replication_factor")
	if !ok {
		return nil
	}

	if replFactor > 1 {
		return []ConfigFinding{{
			ID:             r.ID(),
			Severity:       SeverityInfo,
			Category:       r.Category(),
			Title:          "shard_replication_factor > 1 (legacy HA mode)",
			Problem:        fmt.Sprintf("citus.shard_replication_factor=%d creates multiple copies of each shard", replFactor),
			Impact:         "Storage usage multiplied by replication factor. Writes slower. Consider PostgreSQL streaming replication instead.",
			Recommendation: "Modern Citus deployments typically use replication_factor=1 with PostgreSQL HA (streaming replication)",
			CurrentValue:   fmt.Sprintf("%d", replFactor),
			AffectedGUCs:   []string{"citus.shard_replication_factor"},
		}}
	}
	return nil
}

// RuleMaxAdaptiveExecutorPoolSize analyzes executor pool size.
type RuleMaxAdaptiveExecutorPoolSize struct{}

func (r *RuleMaxAdaptiveExecutorPoolSize) ID() string {
	return "rule.config.max_adaptive_executor_pool_size"
}
func (r *RuleMaxAdaptiveExecutorPoolSize) Category() Category { return CategoryExecution }
func (r *RuleMaxAdaptiveExecutorPoolSize) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	poolSize, ok := ctx.GetGUCInt("citus.max_adaptive_executor_pool_size")
	if !ok {
		return nil
	}

	maxConns, hasConns := ctx.GetGUCInt("max_connections")
	if hasConns && ctx.WorkerCount > 0 {
		// Check if pool_size * workers could exhaust worker connections
		totalPoolConns := poolSize * int64(ctx.WorkerCount)
		if totalPoolConns > maxConns*8/10 { // > 80% of max_connections
			return []ConfigFinding{{
				ID:             r.ID(),
				Severity:       SeverityWarning,
				Category:       r.Category(),
				Title:          "Executor pool size may exhaust worker connections",
				Problem:        fmt.Sprintf("citus.max_adaptive_executor_pool_size=%d × %d workers = %d potential connections", poolSize, ctx.WorkerCount, totalPoolConns),
				Impact:         "Parallel queries may exhaust worker max_connections, causing failures.",
				Recommendation: "Ensure worker max_connections can handle pool_size × concurrent_queries",
				CurrentValue:   fmt.Sprintf("%d", poolSize),
				AffectedGUCs:   []string{"citus.max_adaptive_executor_pool_size", "max_connections"},
				Evidence:       map[string]any{"total_pool_connections": totalPoolConns, "worker_max_connections": maxConns},
			}}
		}
	}
	return nil
}

// RuleBackgroundTaskExecutors analyzes background task parallelism.
type RuleBackgroundTaskExecutors struct{}

func (r *RuleBackgroundTaskExecutors) ID() string         { return "rule.config.background_task_executors" }
func (r *RuleBackgroundTaskExecutors) Category() Category { return CategoryOperations }
func (r *RuleBackgroundTaskExecutors) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	executors, ok := ctx.GetGUCInt("citus.max_background_task_executors_per_node")
	if !ok {
		return nil
	}

	if executors == 1 {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityInfo,
			Category:        r.Category(),
			Title:           "Background task executors at default",
			Problem:         "citus.max_background_task_executors_per_node=1 limits parallel shard operations",
			Impact:          "Rebalancing and shard moves will be sequential, taking longer to complete.",
			Recommendation:  "Set to 2-4 for faster rebalancing (ensure sufficient replication slots)",
			CurrentValue:    "1",
			RecommendedVal:  "2-4",
			FixSQL:          "ALTER SYSTEM SET citus.max_background_task_executors_per_node = 2;",
			RequiresRestart: false,
			AffectedGUCs:    []string{"citus.max_background_task_executors_per_node"},
		}}
	}
	return nil
}

// RuleNodeConnectionTimeout analyzes node connection timeout.
type RuleNodeConnectionTimeout struct{}

func (r *RuleNodeConnectionTimeout) ID() string         { return "rule.config.node_connection_timeout" }
func (r *RuleNodeConnectionTimeout) Category() Category { return CategoryOperations }
func (r *RuleNodeConnectionTimeout) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	g, ok := ctx.GetGUC("citus.node_connection_timeout")
	if !ok {
		return nil
	}

	// Parse timeout (format: "5s" or similar)
	timeout := g.Setting
	if strings.HasSuffix(timeout, "ms") {
		val, _ := ParseInt(strings.TrimSuffix(timeout, "ms"))
		if val < 1000 {
			return []ConfigFinding{{
				ID:              r.ID(),
				Severity:        SeverityWarning,
				Category:        r.Category(),
				Title:           "Node connection timeout very short",
				Problem:         fmt.Sprintf("citus.node_connection_timeout=%s may cause spurious failures", timeout),
				Impact:          "Transient network delays may cause query failures.",
				Recommendation:  "Set to at least 5s unless you have very low latency network",
				CurrentValue:    timeout,
				RecommendedVal:  "5s",
				FixSQL:          "ALTER SYSTEM SET citus.node_connection_timeout = '5s';",
				RequiresRestart: false,
				AffectedGUCs:    []string{"citus.node_connection_timeout"},
			}}
		}
	}
	return nil
}

// =============================================================================
// Performance Rules
// =============================================================================

// RuleRandomPageCost analyzes random_page_cost for SSDs.
type RuleRandomPageCost struct{}

func (r *RuleRandomPageCost) ID() string         { return "rule.config.random_page_cost" }
func (r *RuleRandomPageCost) Category() Category { return CategoryPerformance }
func (r *RuleRandomPageCost) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	g, ok := ctx.GetGUC("random_page_cost")
	if !ok {
		return nil
	}

	// Default is 4.0, but SSDs should use 1.1-1.5
	if g.Setting == "4" || g.Setting == "4.0" {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityInfo,
			Category:        r.Category(),
			Title:           "random_page_cost at HDD default",
			Problem:         "random_page_cost=4.0 assumes spinning disks",
			Impact:          "Query planner may avoid index scans that would be faster on SSDs.",
			Recommendation:  "Set random_page_cost=1.1 for SSDs, 1.5-2.0 for fast SANs",
			CurrentValue:    g.Setting,
			RecommendedVal:  "1.1 (SSD) or 1.5 (fast storage)",
			FixSQL:          "ALTER SYSTEM SET random_page_cost = 1.1;",
			RequiresRestart: false,
			AffectedGUCs:    []string{"random_page_cost"},
		}}
	}
	return nil
}

// RuleEffectiveIOConcurrency analyzes I/O concurrency.
type RuleEffectiveIOConcurrency struct{}

func (r *RuleEffectiveIOConcurrency) ID() string         { return "rule.config.effective_io_concurrency" }
func (r *RuleEffectiveIOConcurrency) Category() Category { return CategoryPerformance }
func (r *RuleEffectiveIOConcurrency) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	conc, ok := ctx.GetGUCInt("effective_io_concurrency")
	if !ok {
		return nil
	}

	if conc <= 2 {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityInfo,
			Category:        r.Category(),
			Title:           "effective_io_concurrency is low",
			Problem:         fmt.Sprintf("effective_io_concurrency=%d assumes spinning disks", conc),
			Impact:          "PostgreSQL won't prefetch data efficiently on SSDs.",
			Recommendation:  "Set effective_io_concurrency=200 for SSDs, 2 for spinning disks",
			CurrentValue:    fmt.Sprintf("%d", conc),
			RecommendedVal:  "200 (SSD)",
			FixSQL:          "ALTER SYSTEM SET effective_io_concurrency = 200;",
			RequiresRestart: false,
			AffectedGUCs:    []string{"effective_io_concurrency"},
		}}
	}
	return nil
}

// RuleStatisticsTarget analyzes statistics granularity.
type RuleStatisticsTarget struct{}

func (r *RuleStatisticsTarget) ID() string         { return "rule.config.statistics_target" }
func (r *RuleStatisticsTarget) Category() Category { return CategoryPerformance }
func (r *RuleStatisticsTarget) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	target, ok := ctx.GetGUCInt("default_statistics_target")
	if !ok {
		return nil
	}

	if target < 100 {
		return []ConfigFinding{{
			ID:             r.ID(),
			Severity:       SeverityInfo,
			Category:       r.Category(),
			Title:          "Statistics target below default",
			Problem:        fmt.Sprintf("default_statistics_target=%d is below default 100", target),
			Impact:         "Query planner estimates may be less accurate.",
			Recommendation: "Keep at 100 or increase to 200-500 for complex workloads",
			CurrentValue:   fmt.Sprintf("%d", target),
			AffectedGUCs:   []string{"default_statistics_target"},
		}}
	}
	return nil
}

// =============================================================================
// Operations Rules
// =============================================================================

// RuleStatementTimeout analyzes statement timeout.
type RuleStatementTimeout struct{}

func (r *RuleStatementTimeout) ID() string         { return "rule.config.statement_timeout" }
func (r *RuleStatementTimeout) Category() Category { return CategoryOperations }
func (r *RuleStatementTimeout) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	timeout, ok := ctx.GetGUCInt("statement_timeout")
	if !ok {
		return nil
	}

	if timeout == 0 {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityInfo,
			Category:        r.Category(),
			Title:           "No statement timeout configured",
			Problem:         "statement_timeout=0 allows queries to run indefinitely",
			Impact:          "Runaway queries can consume resources and block others.",
			Recommendation:  "Consider setting a reasonable timeout (e.g., 30s-5min) for production",
			CurrentValue:    "0 (disabled)",
			RecommendedVal:  "30s - 5min depending on workload",
			FixSQL:          "ALTER SYSTEM SET statement_timeout = '60s';",
			RequiresRestart: false,
			AffectedGUCs:    []string{"statement_timeout"},
		}}
	}
	return nil
}

// RuleLockTimeout analyzes lock timeout.
type RuleLockTimeout struct{}

func (r *RuleLockTimeout) ID() string         { return "rule.config.lock_timeout" }
func (r *RuleLockTimeout) Category() Category { return CategoryOperations }
func (r *RuleLockTimeout) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	timeout, ok := ctx.GetGUCInt("lock_timeout")
	if !ok {
		return nil
	}

	if timeout == 0 {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityInfo,
			Category:        r.Category(),
			Title:           "No lock timeout configured",
			Problem:         "lock_timeout=0 allows indefinite lock waits",
			Impact:          "Queries may wait forever for locks, causing application timeouts.",
			Recommendation:  "Set lock_timeout to prevent indefinite waits (e.g., 10s-30s)",
			CurrentValue:    "0 (disabled)",
			RecommendedVal:  "10s - 30s",
			FixSQL:          "ALTER SYSTEM SET lock_timeout = '10s';",
			RequiresRestart: false,
			AffectedGUCs:    []string{"lock_timeout"},
		}}
	}
	return nil
}

// RuleIdleInTransactionTimeout analyzes idle transaction timeout.
type RuleIdleInTransactionTimeout struct{}

func (r *RuleIdleInTransactionTimeout) ID() string         { return "rule.config.idle_in_transaction_timeout" }
func (r *RuleIdleInTransactionTimeout) Category() Category { return CategoryOperations }
func (r *RuleIdleInTransactionTimeout) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	timeout, ok := ctx.GetGUCInt("idle_in_transaction_session_timeout")
	if !ok {
		return nil
	}

	if timeout == 0 {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityWarning,
			Category:        r.Category(),
			Title:           "No idle-in-transaction timeout",
			Problem:         "idle_in_transaction_session_timeout=0 allows abandoned transactions",
			Impact:          "Idle transactions hold locks and prevent VACUUM, causing table bloat.",
			Recommendation:  "Set idle_in_transaction_session_timeout (e.g., 5min) to terminate abandoned transactions",
			CurrentValue:    "0 (disabled)",
			RecommendedVal:  "5min",
			FixSQL:          "ALTER SYSTEM SET idle_in_transaction_session_timeout = '5min';",
			RequiresRestart: false,
			AffectedGUCs:    []string{"idle_in_transaction_session_timeout"},
		}}
	}
	return nil
}

// RuleLogMinDurationStatement analyzes slow query logging.
type RuleLogMinDurationStatement struct{}

func (r *RuleLogMinDurationStatement) ID() string         { return "rule.config.log_slow_queries" }
func (r *RuleLogMinDurationStatement) Category() Category { return CategoryOperations }
func (r *RuleLogMinDurationStatement) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	duration, ok := ctx.GetGUCInt("log_min_duration_statement")
	if !ok {
		return nil
	}

	if duration == -1 {
		return []ConfigFinding{{
			ID:              r.ID(),
			Severity:        SeverityInfo,
			Category:        r.Category(),
			Title:           "Slow query logging disabled",
			Problem:         "log_min_duration_statement=-1 disables slow query logging",
			Impact:          "Cannot identify slow queries from logs for optimization.",
			Recommendation:  "Set log_min_duration_statement=1000 (1s) or lower to log slow queries",
			CurrentValue:    "-1 (disabled)",
			RecommendedVal:  "1000ms (1 second)",
			FixSQL:          "ALTER SYSTEM SET log_min_duration_statement = 1000;",
			RequiresRestart: false,
			AffectedGUCs:    []string{"log_min_duration_statement"},
		}}
	}
	return nil
}

// =============================================================================
// Parallelism Rules
// =============================================================================

// RuleParallelWorkers analyzes parallel query configuration.
type RuleParallelWorkers struct{}

func (r *RuleParallelWorkers) ID() string         { return "rule.config.parallel_workers" }
func (r *RuleParallelWorkers) Category() Category { return CategoryParallelism }
func (r *RuleParallelWorkers) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	maxParallel, ok := ctx.GetGUCInt("max_parallel_workers")
	if !ok {
		return nil
	}
	maxWorkerProcs, hasProcs := ctx.GetGUCInt("max_worker_processes")

	if hasProcs && maxParallel > maxWorkerProcs {
		return []ConfigFinding{{
			ID:             r.ID(),
			Severity:       SeverityWarning,
			Category:       r.Category(),
			Title:          "max_parallel_workers exceeds max_worker_processes",
			Problem:        fmt.Sprintf("max_parallel_workers=%d but max_worker_processes=%d", maxParallel, maxWorkerProcs),
			Impact:         "Parallel workers limited by max_worker_processes anyway.",
			Recommendation: "Set max_parallel_workers <= max_worker_processes",
			CurrentValue:   fmt.Sprintf("max_parallel_workers=%d, max_worker_processes=%d", maxParallel, maxWorkerProcs),
			AffectedGUCs:   []string{"max_parallel_workers", "max_worker_processes"},
		}}
	}
	return nil
}

// =============================================================================
// HA Rules
// =============================================================================

// RuleUseSecondaryNodes analyzes secondary node usage.
type RuleUseSecondaryNodes struct{}

func (r *RuleUseSecondaryNodes) ID() string         { return "rule.config.use_secondary_nodes" }
func (r *RuleUseSecondaryNodes) Category() Category { return CategoryHA }
func (r *RuleUseSecondaryNodes) Evaluate(ctx *AnalysisContext) []ConfigFinding {
	g, ok := ctx.GetGUC("citus.use_secondary_nodes")
	if !ok {
		return nil
	}

	if g.Setting == "always" {
		return []ConfigFinding{{
			ID:             r.ID(),
			Severity:       SeverityInfo,
			Category:       r.Category(),
			Title:          "Read replica routing enabled",
			Problem:        "citus.use_secondary_nodes='always' routes SELECTs to replicas",
			Impact:         "Reads may return slightly stale data due to replication lag.",
			Recommendation: "Ensure application tolerates eventual consistency for reads",
			CurrentValue:   g.Setting,
			AffectedGUCs:   []string{"citus.use_secondary_nodes"},
		}}
	}
	return nil
}
