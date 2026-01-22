// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Package metadata provides Citus metadata health detection and repair suggestions.
package metadata

// CheckLevel defines the depth of metadata health checks.
type CheckLevel string

const (
	// CheckLevelBasic performs fast coordinator-only checks.
	CheckLevelBasic CheckLevel = "basic"
	// CheckLevelThorough performs all coordinator checks including expensive ones.
	CheckLevelThorough CheckLevel = "thorough"
	// CheckLevelDeep performs cross-node validation (requires worker connections).
	CheckLevelDeep CheckLevel = "deep"
)

// Severity levels for issues.
type Severity string

const (
	SeverityCritical Severity = "critical"
	SeverityWarning  Severity = "warning"
	SeverityInfo     Severity = "info"
)

// Category groups related checks.
type Category string

const (
	CategoryOrphans     Category = "orphans"
	CategoryConsistency Category = "consistency"
	CategorySync        Category = "sync"
	CategoryOperational Category = "operational"
	CategoryCrossNode   Category = "cross_node"
)

// RiskLevel for fix suggestions.
type RiskLevel string

const (
	RiskNone   RiskLevel = "none"
	RiskLow    RiskLevel = "low"
	RiskMedium RiskLevel = "medium"
	RiskHigh   RiskLevel = "high"
)

// Input for the metadata health check.
type Input struct {
	CheckLevel          CheckLevel `json:"check_level"`
	IncludeFixes        bool       `json:"include_fixes"`
	IncludeVerification bool       `json:"include_verification"`
	TargetNodes         []string   `json:"target_nodes,omitempty"` // "all", "coordinator", "workers", "node:host:port"
}

// Output from the metadata health check.
type Output struct {
	Summary      Summary  `json:"summary"`
	Checks       []Check  `json:"checks"`
	Issues       []Issue  `json:"issues"`
	Healthy      bool     `json:"healthy"`
	CheckedNodes []string `json:"checked_nodes"`
	Warnings     []string `json:"warnings,omitempty"`
}

// Summary provides counts of issues by severity.
type Summary struct {
	TotalChecks int `json:"total_checks"`
	Passed      int `json:"passed"`
	Failed      int `json:"failed"`
	Critical    int `json:"critical"`
	Warning     int `json:"warning"`
	Info        int `json:"info"`
	Skipped     int `json:"skipped"`
}

// Check represents a single check that was performed.
type Check struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Category    Category `json:"category"`
	Description string   `json:"description"`
	Status      string   `json:"status"` // passed, failed, skipped
	Duration    string   `json:"duration,omitempty"`
	IssueCount  int      `json:"issue_count"`
}

// Issue represents a detected metadata problem.
type Issue struct {
	ID              string           `json:"id"`
	CheckID         string           `json:"check_id"`
	Severity        Severity         `json:"severity"`
	Category        Category         `json:"category"`
	Title           string           `json:"title"`
	Description     string           `json:"description"`
	AffectedObjects []AffectedObject `json:"affected_objects"`
	Impact          string           `json:"impact"`
	Fix             *Fix             `json:"fix,omitempty"`
}

// AffectedObject describes an object affected by an issue.
type AffectedObject struct {
	Type       string                 `json:"type"` // shard, placement, table, node, etc.
	Identifier string                 `json:"identifier"`
	Details    map[string]interface{} `json:"details,omitempty"`
}

// Fix provides repair suggestions for an issue.
type Fix struct {
	Approach         string   `json:"approach"`
	RiskLevel        RiskLevel `json:"risk_level"`
	RequiresDowntime bool     `json:"requires_downtime"`
	RequiresBackup   bool     `json:"requires_backup"`
	SQLCommands      []string `json:"sql_commands,omitempty"`
	VerificationSQL  string   `json:"verification_sql,omitempty"`
	Notes            string   `json:"notes,omitempty"`
	ManualSteps      []string `json:"manual_steps,omitempty"`
}

// OrphanedShard represents a shard without placements.
type OrphanedShard struct {
	ShardID   int64  `json:"shard_id"`
	TableName string `json:"table_name"`
	TableOID  int64  `json:"table_oid"`
}

// OrphanedPlacement represents a placement for a non-existent shard.
type OrphanedPlacement struct {
	PlacementID int64  `json:"placement_id"`
	ShardID     int64  `json:"shard_id"`
	GroupID     int32  `json:"group_id"`
	NodeName    string `json:"node_name,omitempty"`
	NodePort    int32  `json:"node_port,omitempty"`
}

// MissingRelation represents pg_dist_partition referencing a missing table.
type MissingRelation struct {
	LogicalRelID  int64  `json:"logical_rel_id"`
	PartMethod    string `json:"part_method"`
	ColocationID  int32  `json:"colocation_id"`
	ReplicationModel string `json:"replication_model"`
}

// InvalidNodeRef represents a placement referencing a non-existent node.
type InvalidNodeRef struct {
	PlacementID int64 `json:"placement_id"`
	ShardID     int64 `json:"shard_id"`
	GroupID     int32 `json:"group_id"`
}

// StaleCleanup represents a cleanup record for a removed node.
type StaleCleanup struct {
	RecordID    int64  `json:"record_id"`
	ObjectName  string `json:"object_name"`
	ObjectType  int32  `json:"object_type"`
	NodeGroupID int32  `json:"node_group_id"`
}

// ColocationMismatch represents tables in same colocation with different shard counts.
type ColocationMismatch struct {
	ColocationID int32  `json:"colocation_id"`
	Table1       string `json:"table1"`
	ShardCount1  int    `json:"shard_count1"`
	Table2       string `json:"table2"`
	ShardCount2  int    `json:"shard_count2"`
}

// UnsyncedNode represents a node with pending metadata sync.
type UnsyncedNode struct {
	NodeID       int32  `json:"node_id"`
	NodeName     string `json:"node_name"`
	NodePort     int32  `json:"node_port"`
	HasMetadata  bool   `json:"has_metadata"`
	MetadataSynced bool `json:"metadata_synced"`
}

// ShardRangeGap represents a gap in shard ranges for a table.
type ShardRangeGap struct {
	TableName  string `json:"table_name"`
	PrevShard  int64  `json:"prev_shard"`
	NextShard  int64  `json:"next_shard"`
	GapStart   int64  `json:"gap_start"`
	GapEnd     int64  `json:"gap_end"`
}

// ShardRangeOverlap represents overlapping shard ranges.
type ShardRangeOverlap struct {
	TableName string `json:"table_name"`
	Shard1    int64  `json:"shard1"`
	Shard2    int64  `json:"shard2"`
	Range1    string `json:"range1"`
	Range2    string `json:"range2"`
}

// ReferenceTableIssue represents a reference table with insufficient placements.
type ReferenceTableIssue struct {
	TableName      string `json:"table_name"`
	PlacementCount int    `json:"placement_count"`
	ActiveNodes    int    `json:"active_nodes"`
	MissingNodes   []string `json:"missing_nodes,omitempty"`
}

// CrossNodeShardIssue represents a shard existence mismatch between coordinator and worker.
type CrossNodeShardIssue struct {
	ShardID       int64  `json:"shard_id"`
	ShardName     string `json:"shard_name"`
	NodeName      string `json:"node_name"`
	NodePort      int32  `json:"node_port"`
	InCoordinator bool   `json:"in_coordinator"`
	InWorker      bool   `json:"in_worker"`
}

// ExtensionMismatch represents different extension versions across nodes.
type ExtensionMismatch struct {
	ExtensionName     string `json:"extension_name"`
	CoordinatorVersion string `json:"coordinator_version"`
	NodeName          string `json:"node_name"`
	NodePort          int32  `json:"node_port"`
	NodeVersion       string `json:"node_version"`
}
