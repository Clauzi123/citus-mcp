// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Cross-node metadata drift detection.

package metadata

import (
	"context"
	"fmt"
	"strings"

	"citus-mcp/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CrossNodeChecker performs cross-node validation.
type CrossNodeChecker struct {
	coordinatorPool *pgxpool.Pool
	workerManager   *db.WorkerManager
	includeFixes    bool
}

// NewCrossNodeChecker creates a new cross-node checker.
func NewCrossNodeChecker(coordinatorPool *pgxpool.Pool, workerManager *db.WorkerManager, includeFixes bool) *CrossNodeChecker {
	return &CrossNodeChecker{
		coordinatorPool: coordinatorPool,
		workerManager:   workerManager,
		includeFixes:    includeFixes,
	}
}

// CheckShardExistence validates that shards exist on workers as expected.
func (c *CrossNodeChecker) CheckShardExistence(ctx context.Context) CheckResult {
	return runCheck(ctx, "cross_node_shard_existence", "Cross-Node Shard Existence", CategoryCrossNode,
		"Validates that shard tables exist on workers as recorded in coordinator metadata",
		func() ([]Issue, error) {
			if c.workerManager == nil {
				return nil, nil // No worker connections configured
			}

			// Get worker pools
			pools, workerInfos, err := c.workerManager.Pools(ctx)
			if err != nil {
				return nil, fmt.Errorf("get worker pools: %w", err)
			}

			if len(pools) == 0 {
				return nil, nil // No worker pools available
			}

			var allIssues []CrossNodeShardIssue

			for _, workerInfo := range workerInfos {
				pool, ok := pools[workerInfo.NodeID]
				if !ok {
					continue // Pool not available for this worker
				}

				// Get expected shards on this worker
				expectedRows, err := c.coordinatorPool.Query(ctx, QueryExpectedShardPlacements, workerInfo.NodeName, workerInfo.NodePort)
				if err != nil {
					continue
				}

				expectedShards := make(map[string]int64) // shard_name -> shard_id
				for expectedRows.Next() {
					var shardID int64
					var tableName, schemaName, shardName string
					if err := expectedRows.Scan(&shardID, &tableName, &schemaName, &shardName); err != nil {
						expectedRows.Close()
						continue
					}
					expectedShards[schemaName+"."+shardName] = shardID
				}
				expectedRows.Close()

				// Get actual shard tables on worker
				actualRows, err := pool.Query(ctx, QueryWorkerShardTables)
				if err != nil {
					continue
				}

				actualShards := make(map[string]bool)
				for actualRows.Next() {
					var schemaName, tableName string
					if err := actualRows.Scan(&schemaName, &tableName); err != nil {
						actualRows.Close()
						continue
					}
					actualShards[schemaName+"."+tableName] = true
				}
				actualRows.Close()

				// Find mismatches - expected but not on worker
				for shardName, shardID := range expectedShards {
					if !actualShards[shardName] {
						allIssues = append(allIssues, CrossNodeShardIssue{
							ShardID:       shardID,
							ShardName:     shardName,
							NodeName:      workerInfo.NodeName,
							NodePort:      workerInfo.NodePort,
							InCoordinator: true,
							InWorker:      false,
						})
					}
				}

				// Find orphans on worker - on worker but not expected
				for shardName := range actualShards {
					if _, expected := expectedShards[shardName]; !expected {
						// This could be a legitimate orphan or just a different table
						// Only report if it looks like a shard (ends with _digits)
						allIssues = append(allIssues, CrossNodeShardIssue{
							ShardID:       0, // Unknown
							ShardName:     shardName,
							NodeName:      workerInfo.NodeName,
							NodePort:      workerInfo.NodePort,
							InCoordinator: false,
							InWorker:      true,
						})
					}
				}
			}

			if len(allIssues) == 0 {
				return nil, nil
			}

			// Group issues
			missingOnWorker := []CrossNodeShardIssue{}
			orphanedOnWorker := []CrossNodeShardIssue{}
			for _, issue := range allIssues {
				if issue.InCoordinator && !issue.InWorker {
					missingOnWorker = append(missingOnWorker, issue)
				} else if !issue.InCoordinator && issue.InWorker {
					orphanedOnWorker = append(orphanedOnWorker, issue)
				}
			}

			var issues []Issue

			if len(missingOnWorker) > 0 {
				affected := make([]AffectedObject, 0, len(missingOnWorker))
				for _, m := range missingOnWorker {
					affected = append(affected, AffectedObject{
						Type:       "shard",
						Identifier: m.ShardName,
						Details: map[string]interface{}{
							"shard_id":  m.ShardID,
							"node_name": m.NodeName,
							"node_port": m.NodePort,
							"status":    "missing_on_worker",
						},
					})
				}

				issue := Issue{
					ID:              fmt.Sprintf("shards_missing_on_workers_%d", len(missingOnWorker)),
					CheckID:         "cross_node_shard_existence",
					Severity:        SeverityCritical,
					Category:        CategoryCrossNode,
					Title:           fmt.Sprintf("%d Shards Missing on Workers", len(missingOnWorker)),
					Description:     "Coordinator metadata references shards that don't exist on the expected worker nodes.",
					AffectedObjects: affected,
					Impact:          "Queries to these shards will fail.",
				}

				if c.includeFixes {
					issue.Fix = &Fix{
						Approach:         "Repair shard placements or remove stale metadata",
						RiskLevel:        RiskHigh,
						RequiresDowntime: false,
						RequiresBackup:   true,
						ManualSteps: []string{
							"1. Determine if shard data is recoverable from another replica",
							"2. If replicas exist, use master_copy_shard_placement to repair",
							"3. If no replicas, remove the placement metadata",
							"4. May need to restore from backup",
						},
						Notes: "This indicates data loss or incomplete shard move. Investigate cause.",
					}
				}
				issues = append(issues, issue)
			}

			if len(orphanedOnWorker) > 0 {
				affected := make([]AffectedObject, 0, len(orphanedOnWorker))
				for _, o := range orphanedOnWorker {
					affected = append(affected, AffectedObject{
						Type:       "shard",
						Identifier: o.ShardName,
						Details: map[string]interface{}{
							"node_name": o.NodeName,
							"node_port": o.NodePort,
							"status":    "orphaned_on_worker",
						},
					})
				}

				issue := Issue{
					ID:              fmt.Sprintf("orphaned_shards_on_workers_%d", len(orphanedOnWorker)),
					CheckID:         "cross_node_shard_existence",
					Severity:        SeverityWarning,
					Category:        CategoryCrossNode,
					Title:           fmt.Sprintf("%d Potential Orphaned Shards on Workers", len(orphanedOnWorker)),
					Description:     "Shard-like tables exist on workers but are not tracked in coordinator metadata.",
					AffectedObjects: affected,
					Impact:          "Wastes storage space. May be leftover from failed operations.",
				}

				if c.includeFixes {
					issue.Fix = &Fix{
						Approach:         "Verify and clean up orphaned shard tables",
						RiskLevel:        RiskMedium,
						RequiresDowntime: false,
						RequiresBackup:   false,
						ManualSteps: []string{
							"1. Verify these are actually orphaned shards (not legitimate tables)",
							"2. Check if they contain data that should be preserved",
							"3. Drop orphaned tables: DROP TABLE schema.tablename;",
						},
						Notes: "Manually verify before dropping. Some may be legitimate non-shard tables.",
					}
				}
				issues = append(issues, issue)
			}

			return issues, nil
		})
}

// CheckExtensionVersions validates extension versions match across nodes.
func (c *CrossNodeChecker) CheckExtensionVersions(ctx context.Context) CheckResult {
	return runCheck(ctx, "cross_node_extension_versions", "Cross-Node Extension Versions", CategoryCrossNode,
		"Validates that extension versions match between coordinator and workers",
		func() ([]Issue, error) {
			if c.workerManager == nil {
				return nil, nil
			}

			// Get coordinator extensions
			coordRows, err := c.coordinatorPool.Query(ctx, QueryCoordinatorExtensions)
			if err != nil {
				return nil, fmt.Errorf("query coordinator extensions: %w", err)
			}
			defer coordRows.Close()

			coordExtensions := make(map[string]string)
			for coordRows.Next() {
				var name, version string
				if err := coordRows.Scan(&name, &version); err != nil {
					return nil, fmt.Errorf("scan coordinator extension: %w", err)
				}
				coordExtensions[name] = version
			}
			if coordRows.Err() != nil {
				return nil, coordRows.Err()
			}

			// Get worker pools
			pools, workerInfos, err := c.workerManager.Pools(ctx)
			if err != nil {
				return nil, fmt.Errorf("get worker pools: %w", err)
			}

			var mismatches []ExtensionMismatch

			for _, workerInfo := range workerInfos {
				pool, ok := pools[workerInfo.NodeID]
				if !ok {
					continue
				}

				workerExtRows, err := pool.Query(ctx, QueryNodeExtensions)
				if err != nil {
					continue
				}

				workerExtensions := make(map[string]string)
				for workerExtRows.Next() {
					var name, version string
					if err := workerExtRows.Scan(&name, &version); err != nil {
						workerExtRows.Close()
						continue
					}
					workerExtensions[name] = version
				}
				workerExtRows.Close()

				// Compare versions
				for extName, coordVersion := range coordExtensions {
					workerVersion, exists := workerExtensions[extName]
					if !exists {
						mismatches = append(mismatches, ExtensionMismatch{
							ExtensionName:      extName,
							CoordinatorVersion: coordVersion,
							NodeName:           workerInfo.NodeName,
							NodePort:           workerInfo.NodePort,
							NodeVersion:        "(missing)",
						})
					} else if workerVersion != coordVersion {
						mismatches = append(mismatches, ExtensionMismatch{
							ExtensionName:      extName,
							CoordinatorVersion: coordVersion,
							NodeName:           workerInfo.NodeName,
							NodePort:           workerInfo.NodePort,
							NodeVersion:        workerVersion,
						})
					}
				}
			}

			if len(mismatches) == 0 {
				return nil, nil
			}

			// Group by extension
			byExtension := make(map[string][]ExtensionMismatch)
			for _, m := range mismatches {
				byExtension[m.ExtensionName] = append(byExtension[m.ExtensionName], m)
			}

			var issues []Issue
			for extName, extMismatches := range byExtension {
				severity := SeverityWarning
				if extName == "citus" {
					severity = SeverityCritical
				}

				affected := make([]AffectedObject, 0, len(extMismatches)+1)
				affected = append(affected, AffectedObject{
					Type:       "extension",
					Identifier: extName + "@coordinator",
					Details: map[string]interface{}{
						"version": coordExtensions[extName],
						"node":    "coordinator",
					},
				})
				for _, m := range extMismatches {
					affected = append(affected, AffectedObject{
						Type:       "extension",
						Identifier: fmt.Sprintf("%s@%s:%d", extName, m.NodeName, m.NodePort),
						Details: map[string]interface{}{
							"version":             m.NodeVersion,
							"coordinator_version": m.CoordinatorVersion,
						},
					})
				}

				issue := Issue{
					ID:              fmt.Sprintf("extension_mismatch_%s", extName),
					CheckID:         "cross_node_extension_versions",
					Severity:        severity,
					Category:        CategoryCrossNode,
					Title:           fmt.Sprintf("Extension %s Version Mismatch (%d nodes)", extName, len(extMismatches)),
					Description:     fmt.Sprintf("Extension %s has different versions across cluster nodes.", extName),
					AffectedObjects: affected,
					Impact:          "Can cause query failures, feature incompatibilities, or add_node failures.",
				}

				if c.includeFixes {
					issue.Fix = &Fix{
						Approach:         "Update extensions to match coordinator version",
						RiskLevel:        RiskMedium,
						RequiresDowntime: extName == "citus",
						RequiresBackup:   true,
						SQLCommands: []string{
							fmt.Sprintf("-- Run on each mismatched worker:\nALTER EXTENSION %s UPDATE TO '%s';", extName, coordExtensions[extName]),
						},
						ManualSteps: []string{
							"1. Plan maintenance window if updating citus extension",
							"2. Update extension on each worker to match coordinator",
							"3. Restart PostgreSQL if required by extension",
							"4. Verify versions match: SELECT extname, extversion FROM pg_extension;",
						},
						Notes: "Extension updates may require PostgreSQL restart.",
					}
				}
				issues = append(issues, issue)
			}

			return issues, nil
		})
}

// CheckMetadataSync validates metadata tables are in sync between coordinator and workers.
func (c *CrossNodeChecker) CheckMetadataSync(ctx context.Context) CheckResult {
	return runCheck(ctx, "cross_node_metadata_sync", "Cross-Node Metadata Sync", CategoryCrossNode,
		"Validates that Citus metadata tables are synchronized between coordinator and metadata workers",
		func() ([]Issue, error) {
			if c.workerManager == nil {
				return nil, nil
			}

			// Get workers with metadata
			workerRows, err := c.coordinatorPool.Query(ctx, `
				SELECT nodeid, nodename, nodeport 
				FROM pg_dist_node 
				WHERE hasmetadata = true AND metadatasynced = true AND isactive = true`)
			if err != nil {
				return nil, fmt.Errorf("query metadata workers: %w", err)
			}
			defer workerRows.Close()

			type metadataWorker struct {
				nodeID int32
				name   string
				port   int32
			}
			var metadataWorkers []metadataWorker
			for workerRows.Next() {
				var w metadataWorker
				if err := workerRows.Scan(&w.nodeID, &w.name, &w.port); err != nil {
					return nil, err
				}
				metadataWorkers = append(metadataWorkers, w)
			}

			if len(metadataWorkers) == 0 {
				return nil, nil // No metadata workers
			}

			// Get worker pools
			pools, _, err := c.workerManager.Pools(ctx)
			if err != nil {
				return nil, fmt.Errorf("get worker pools: %w", err)
			}

			// Get coordinator metadata counts
			var coordPartitionCount, coordShardCount, coordPlacementCount, coordNodeCount int64
			if err := c.coordinatorPool.QueryRow(ctx, QueryDistTableCount).Scan(&coordPartitionCount); err != nil {
				return nil, err
			}
			if err := c.coordinatorPool.QueryRow(ctx, QueryShardCount).Scan(&coordShardCount); err != nil {
				return nil, err
			}
			if err := c.coordinatorPool.QueryRow(ctx, QueryPlacementCount).Scan(&coordPlacementCount); err != nil {
				return nil, err
			}
			if err := c.coordinatorPool.QueryRow(ctx, QueryNodeCount).Scan(&coordNodeCount); err != nil {
				return nil, err
			}

			var issues []Issue
			var driftNodes []string

			for _, worker := range metadataWorkers {
				pool, ok := pools[worker.nodeID]
				if !ok {
					continue
				}

				var workerPartitionCount, workerShardCount, workerPlacementCount, workerNodeCount int64
				if err := pool.QueryRow(ctx, QueryDistTableCount).Scan(&workerPartitionCount); err != nil {
					continue
				}
				if err := pool.QueryRow(ctx, QueryShardCount).Scan(&workerShardCount); err != nil {
					continue
				}
				if err := pool.QueryRow(ctx, QueryPlacementCount).Scan(&workerPlacementCount); err != nil {
					continue
				}
				if err := pool.QueryRow(ctx, QueryNodeCount).Scan(&workerNodeCount); err != nil {
					continue
				}

				if workerPartitionCount != coordPartitionCount ||
					workerShardCount != coordShardCount ||
					workerPlacementCount != coordPlacementCount ||
					workerNodeCount != coordNodeCount {
					driftNodes = append(driftNodes, fmt.Sprintf("%s:%d", worker.name, worker.port))
				}
			}

			if len(driftNodes) > 0 {
				affected := make([]AffectedObject, 0, len(driftNodes))
				for _, node := range driftNodes {
					affected = append(affected, AffectedObject{
						Type:       "node",
						Identifier: node,
						Details: map[string]interface{}{
							"status": "metadata_drift",
						},
					})
				}

				issue := Issue{
					ID:              fmt.Sprintf("metadata_drift_%d", len(driftNodes)),
					CheckID:         "cross_node_metadata_sync",
					Severity:        SeverityWarning,
					Category:        CategoryCrossNode,
					Title:           fmt.Sprintf("Metadata Drift Detected on %d Node(s)", len(driftNodes)),
					Description:     fmt.Sprintf("Metadata workers %s have different metadata counts than coordinator.", strings.Join(driftNodes, ", ")),
					AffectedObjects: affected,
					Impact:          "MX queries on these workers may see stale or incorrect metadata.",
				}

				if c.includeFixes {
					issue.Fix = &Fix{
						Approach:         "Force metadata resync",
						RiskLevel:        RiskLow,
						RequiresDowntime: false,
						RequiresBackup:   false,
						SQLCommands: []string{
							"-- Trigger metadata resync\nSELECT citus_finish_citus_upgrade();",
						},
						VerificationSQL: "SELECT nodename, nodeport, metadatasynced FROM pg_dist_node WHERE hasmetadata = true;",
						Notes:           "If drift persists, may need to stop_metadata_sync_to_node and restart sync.",
					}
				}
				issues = append(issues, issue)
			}

			return issues, nil
		})
}
