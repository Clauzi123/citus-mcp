package metadata

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckResult holds the result of a single check.
type CheckResult struct {
	Check  Check
	Issues []Issue
	Error  error
}

// runCheck executes a check and returns the result.
func runCheck(ctx context.Context, id, name string, category Category, description string, fn func() ([]Issue, error)) CheckResult {
	start := time.Now()
	issues, err := fn()
	duration := time.Since(start)

	status := "passed"
	if err != nil {
		status = "error"
	} else if len(issues) > 0 {
		status = "failed"
	}

	return CheckResult{
		Check: Check{
			ID:          id,
			Name:        name,
			Category:    category,
			Description: description,
			Status:      status,
			Duration:    duration.Round(time.Millisecond).String(),
			IssueCount:  len(issues),
		},
		Issues: issues,
		Error:  err,
	}
}

// checkOrphanedShards finds shards without placements.
func checkOrphanedShards(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "orphaned_shards", "Orphaned Shards", CategoryOrphans,
		"Detects shards in pg_dist_shard that have no placements in pg_dist_placement",
		func() ([]Issue, error) {
			rows, err := pool.Query(ctx, QueryOrphanedShards)
			if err != nil {
				return nil, fmt.Errorf("query orphaned shards: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var orphans []OrphanedShard
			for rows.Next() {
				var o OrphanedShard
				if err := rows.Scan(&o.ShardID, &o.TableOID, &o.TableName); err != nil {
					return nil, fmt.Errorf("scan orphaned shard: %w", err)
				}
				orphans = append(orphans, o)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			if len(orphans) > 0 {
				affected := make([]AffectedObject, 0, len(orphans))
				shardIDs := make([]string, 0, len(orphans))
				for _, o := range orphans {
					affected = append(affected, AffectedObject{
						Type:       "shard",
						Identifier: fmt.Sprintf("%d", o.ShardID),
						Details: map[string]interface{}{
							"table_name": o.TableName,
							"table_oid":  o.TableOID,
						},
					})
					shardIDs = append(shardIDs, fmt.Sprintf("%d", o.ShardID))
				}

				issue := Issue{
					ID:              fmt.Sprintf("orphaned_shards_%d", len(orphans)),
					CheckID:         "orphaned_shards",
					Severity:        SeverityCritical,
					Category:        CategoryOrphans,
					Title:           fmt.Sprintf("%d Orphaned Shards Detected", len(orphans)),
					Description:     "Shards exist in pg_dist_shard but have no corresponding entries in pg_dist_placement. These shards cannot be accessed.",
					AffectedObjects: affected,
					Impact:          "Queries may fail or return incomplete results. Data in these shards is inaccessible.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Remove orphaned shard metadata entries",
						RiskLevel:        RiskMedium,
						RequiresDowntime: false,
						RequiresBackup:   true,
						SQLCommands: []string{
							fmt.Sprintf("-- Remove orphaned shard entries\nDELETE FROM pg_dist_shard WHERE shardid IN (%s);", strings.Join(shardIDs, ", ")),
						},
						VerificationSQL: "SELECT COUNT(*) FROM pg_dist_shard s LEFT JOIN pg_dist_placement p ON s.shardid = p.shardid WHERE p.shardid IS NULL;",
						Notes:           "WARNING: This removes metadata only. If the actual shard data exists on workers, it will become orphaned. Verify shard data status before proceeding.",
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}

// checkOrphanedPlacements finds placements for non-existent shards.
func checkOrphanedPlacements(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "orphaned_placements", "Orphaned Placements", CategoryOrphans,
		"Detects placements in pg_dist_placement that reference non-existent shards",
		func() ([]Issue, error) {
			rows, err := pool.Query(ctx, QueryOrphanedPlacements)
			if err != nil {
				return nil, fmt.Errorf("query orphaned placements: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var orphans []OrphanedPlacement
			for rows.Next() {
				var o OrphanedPlacement
				if err := rows.Scan(&o.PlacementID, &o.ShardID, &o.GroupID); err != nil {
					return nil, fmt.Errorf("scan orphaned placement: %w", err)
				}
				orphans = append(orphans, o)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			if len(orphans) > 0 {
				affected := make([]AffectedObject, 0, len(orphans))
				placementIDs := make([]string, 0, len(orphans))
				for _, o := range orphans {
					affected = append(affected, AffectedObject{
						Type:       "placement",
						Identifier: fmt.Sprintf("%d", o.PlacementID),
						Details: map[string]interface{}{
							"shard_id": o.ShardID,
							"group_id": o.GroupID,
						},
					})
					placementIDs = append(placementIDs, fmt.Sprintf("%d", o.PlacementID))
				}

				issue := Issue{
					ID:              fmt.Sprintf("orphaned_placements_%d", len(orphans)),
					CheckID:         "orphaned_placements",
					Severity:        SeverityCritical,
					Category:        CategoryOrphans,
					Title:           fmt.Sprintf("%d Orphaned Placements Detected", len(orphans)),
					Description:     "Placement records exist for shards that don't exist in pg_dist_shard.",
					AffectedObjects: affected,
					Impact:          "Can cause errors during shard operations and rebalancing.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Remove orphaned placement records",
						RiskLevel:        RiskLow,
						RequiresDowntime: false,
						RequiresBackup:   true,
						SQLCommands: []string{
							fmt.Sprintf("-- Remove orphaned placement entries\nDELETE FROM pg_dist_placement WHERE placementid IN (%s);", strings.Join(placementIDs, ", ")),
						},
						VerificationSQL: "SELECT COUNT(*) FROM pg_dist_placement p WHERE NOT EXISTS (SELECT 1 FROM pg_dist_shard s WHERE s.shardid = p.shardid);",
						Notes:           "Safe to remove as these reference non-existent shards.",
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}

// checkMissingRelations finds pg_dist_partition entries with no corresponding pg_class.
func checkMissingRelations(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "missing_relations", "Missing Table Relations", CategoryOrphans,
		"Detects pg_dist_partition entries that reference tables which no longer exist",
		func() ([]Issue, error) {
			rows, err := pool.Query(ctx, QueryMissingRelations)
			if err != nil {
				return nil, fmt.Errorf("query missing relations: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var missing []MissingRelation
			for rows.Next() {
				var m MissingRelation
				if err := rows.Scan(&m.LogicalRelID, &m.PartMethod, &m.ColocationID, &m.ReplicationModel); err != nil {
					return nil, fmt.Errorf("scan missing relation: %w", err)
				}
				missing = append(missing, m)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			if len(missing) > 0 {
				affected := make([]AffectedObject, 0, len(missing))
				relIDs := make([]string, 0, len(missing))
				for _, m := range missing {
					affected = append(affected, AffectedObject{
						Type:       "partition_entry",
						Identifier: fmt.Sprintf("OID %d", m.LogicalRelID),
						Details: map[string]interface{}{
							"part_method":       m.PartMethod,
							"colocation_id":     m.ColocationID,
							"replication_model": m.ReplicationModel,
						},
					})
					relIDs = append(relIDs, fmt.Sprintf("%d", m.LogicalRelID))
				}

				issue := Issue{
					ID:              fmt.Sprintf("missing_relations_%d", len(missing)),
					CheckID:         "missing_relations",
					Severity:        SeverityCritical,
					Category:        CategoryOrphans,
					Title:           fmt.Sprintf("%d Missing Table Relations", len(missing)),
					Description:     "Entries in pg_dist_partition reference table OIDs that no longer exist in pg_class.",
					AffectedObjects: affected,
					Impact:          "Can cause errors in metadata queries and cluster operations.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Remove orphaned partition entries and related metadata",
						RiskLevel:        RiskMedium,
						RequiresDowntime: false,
						RequiresBackup:   true,
						SQLCommands: []string{
							fmt.Sprintf("-- Remove orphaned shard entries first\nDELETE FROM pg_dist_shard WHERE logicalrelid IN (%s);", strings.Join(relIDs, ", ")),
							fmt.Sprintf("-- Remove orphaned partition entries\nDELETE FROM pg_dist_partition WHERE logicalrelid IN (%s);", strings.Join(relIDs, ", ")),
						},
						VerificationSQL: "SELECT COUNT(*) FROM pg_dist_partition p WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = p.logicalrelid);",
						Notes:           "Also removes any related shard entries. Cascade cleanup is required.",
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}

// checkInvalidNodeRefs finds placements referencing non-existent nodes.
func checkInvalidNodeRefs(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "invalid_node_refs", "Invalid Node References", CategoryConsistency,
		"Detects placements that reference node groups which no longer exist",
		func() ([]Issue, error) {
			rows, err := pool.Query(ctx, QueryInvalidNodeRefs)
			if err != nil {
				return nil, fmt.Errorf("query invalid node refs: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var invalid []InvalidNodeRef
			for rows.Next() {
				var i InvalidNodeRef
				if err := rows.Scan(&i.PlacementID, &i.ShardID, &i.GroupID); err != nil {
					return nil, fmt.Errorf("scan invalid node ref: %w", err)
				}
				invalid = append(invalid, i)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			if len(invalid) > 0 {
				affected := make([]AffectedObject, 0, len(invalid))
				placementIDs := make([]string, 0, len(invalid))
				groupIDs := make(map[int32]bool)
				for _, i := range invalid {
					affected = append(affected, AffectedObject{
						Type:       "placement",
						Identifier: fmt.Sprintf("%d", i.PlacementID),
						Details: map[string]interface{}{
							"shard_id": i.ShardID,
							"group_id": i.GroupID,
						},
					})
					placementIDs = append(placementIDs, fmt.Sprintf("%d", i.PlacementID))
					groupIDs[i.GroupID] = true
				}

				issue := Issue{
					ID:              fmt.Sprintf("invalid_node_refs_%d", len(invalid)),
					CheckID:         "invalid_node_refs",
					Severity:        SeverityCritical,
					Category:        CategoryConsistency,
					Title:           fmt.Sprintf("%d Placements Reference Invalid Nodes", len(invalid)),
					Description:     fmt.Sprintf("Placements reference %d node group(s) that no longer exist in pg_dist_node.", len(groupIDs)),
					AffectedObjects: affected,
					Impact:          "Queries to affected shards will fail. Rebalancing will not work correctly.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Remove placements for removed nodes OR re-add the missing nodes",
						RiskLevel:        RiskHigh,
						RequiresDowntime: false,
						RequiresBackup:   true,
						SQLCommands: []string{
							fmt.Sprintf("-- OPTION 1: Remove invalid placements (DATA LOSS if shards had data)\nDELETE FROM pg_dist_placement WHERE placementid IN (%s);", strings.Join(placementIDs, ", ")),
						},
						VerificationSQL: "SELECT COUNT(*) FROM pg_dist_placement p LEFT JOIN pg_dist_node n ON p.groupid = n.groupid WHERE n.groupid IS NULL;",
						Notes:           "WARNING: Removing placements may result in data loss. Consider if the node can be re-added instead.",
						ManualSteps: []string{
							"1. Check if the node is recoverable and can be re-added",
							"2. If node is lost, verify if shard data exists elsewhere (replicas)",
							"3. Only delete placements if data is confirmed lost or recoverable from replicas",
						},
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}

// checkStaleCleanupRecords finds cleanup records for removed nodes.
func checkStaleCleanupRecords(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "stale_cleanup_records", "Stale Cleanup Records", CategoryConsistency,
		"Detects cleanup records that reference nodes which no longer exist",
		func() ([]Issue, error) {
			// First check if pg_dist_cleanup exists
			var exists bool
			err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'pg_catalog' AND tablename = 'pg_dist_cleanup')").Scan(&exists)
			if err != nil {
				return nil, fmt.Errorf("check pg_dist_cleanup exists: %w", err)
			}
			if !exists {
				return nil, nil // Table doesn't exist, skip check
			}

			rows, err := pool.Query(ctx, QueryStaleCleanupRecords)
			if err != nil {
				return nil, fmt.Errorf("query stale cleanup records: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var stale []StaleCleanup
			for rows.Next() {
				var s StaleCleanup
				if err := rows.Scan(&s.RecordID, &s.ObjectName, &s.ObjectType, &s.NodeGroupID); err != nil {
					return nil, fmt.Errorf("scan stale cleanup: %w", err)
				}
				stale = append(stale, s)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			if len(stale) > 0 {
				affected := make([]AffectedObject, 0, len(stale))
				recordIDs := make([]string, 0, len(stale))
				for _, s := range stale {
					affected = append(affected, AffectedObject{
						Type:       "cleanup_record",
						Identifier: fmt.Sprintf("%d", s.RecordID),
						Details: map[string]interface{}{
							"object_name":   s.ObjectName,
							"object_type":   s.ObjectType,
							"node_group_id": s.NodeGroupID,
						},
					})
					recordIDs = append(recordIDs, fmt.Sprintf("%d", s.RecordID))
				}

				issue := Issue{
					ID:              fmt.Sprintf("stale_cleanup_%d", len(stale)),
					CheckID:         "stale_cleanup_records",
					Severity:        SeverityWarning,
					Category:        CategoryConsistency,
					Title:           fmt.Sprintf("%d Stale Cleanup Records", len(stale)),
					Description:     "Cleanup records reference nodes that no longer exist. These block certain operations.",
					AffectedObjects: affected,
					Impact:          "May block shard moves and rebalancing operations.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Remove stale cleanup records",
						RiskLevel:        RiskLow,
						RequiresDowntime: false,
						RequiresBackup:   false,
						SQLCommands: []string{
							fmt.Sprintf("-- Remove stale cleanup records\nDELETE FROM pg_dist_cleanup WHERE record_id IN (%s);", strings.Join(recordIDs, ", ")),
						},
						VerificationSQL: "SELECT COUNT(*) FROM pg_dist_cleanup c LEFT JOIN pg_dist_node n ON c.node_group_id = n.groupid WHERE n.groupid IS NULL;",
						Notes:           "Safe operation - these records reference non-existent nodes.",
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}

// checkColocationMismatch finds tables in same colocation with different shard counts.
func checkColocationMismatch(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "colocation_mismatch", "Colocation Shard Count Mismatch", CategoryConsistency,
		"Detects tables in the same colocation group with different shard counts",
		func() ([]Issue, error) {
			rows, err := pool.Query(ctx, QueryColocationMismatch)
			if err != nil {
				return nil, fmt.Errorf("query colocation mismatch: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var mismatches []ColocationMismatch
			for rows.Next() {
				var m ColocationMismatch
				if err := rows.Scan(&m.ColocationID, &m.Table1, &m.ShardCount1, &m.Table2, &m.ShardCount2); err != nil {
					return nil, fmt.Errorf("scan colocation mismatch: %w", err)
				}
				mismatches = append(mismatches, m)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			// Group by colocation ID
			byColocation := make(map[int32][]ColocationMismatch)
			for _, m := range mismatches {
				byColocation[m.ColocationID] = append(byColocation[m.ColocationID], m)
			}

			for colocationID, group := range byColocation {
				affected := make([]AffectedObject, 0)
				tables := make(map[string]int)
				for _, m := range group {
					tables[m.Table1] = m.ShardCount1
					tables[m.Table2] = m.ShardCount2
				}
				for table, count := range tables {
					affected = append(affected, AffectedObject{
						Type:       "table",
						Identifier: table,
						Details: map[string]interface{}{
							"shard_count":   count,
							"colocation_id": colocationID,
						},
					})
				}

				issue := Issue{
					ID:              fmt.Sprintf("colocation_mismatch_%d", colocationID),
					CheckID:         "colocation_mismatch",
					Severity:        SeverityCritical,
					Category:        CategoryConsistency,
					Title:           fmt.Sprintf("Colocation Group %d Has Mismatched Shard Counts", colocationID),
					Description:     "Tables in the same colocation group must have identical shard counts for colocated joins to work.",
					AffectedObjects: affected,
					Impact:          "Colocated joins between these tables will fail or produce incorrect results.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Move tables to separate colocation groups or re-shard to match",
						RiskLevel:        RiskHigh,
						RequiresDowntime: true,
						RequiresBackup:   true,
						ManualSteps: []string{
							"1. Identify which table has the correct shard count",
							"2. Either re-distribute the mismatched table with correct shard count",
							"3. Or use update_distributed_table_colocation to separate them",
						},
						Notes: "This is a serious inconsistency that requires careful planning to fix.",
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}

// checkUnsyncedNodes finds nodes with pending metadata sync.
func checkUnsyncedNodes(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "unsynced_nodes", "Unsynced Metadata Nodes", CategorySync,
		"Detects nodes marked for metadata sync that haven't completed syncing",
		func() ([]Issue, error) {
			rows, err := pool.Query(ctx, QueryUnsyncedNodes)
			if err != nil {
				return nil, fmt.Errorf("query unsynced nodes: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var unsynced []UnsyncedNode
			for rows.Next() {
				var u UnsyncedNode
				if err := rows.Scan(&u.NodeID, &u.NodeName, &u.NodePort, &u.HasMetadata, &u.MetadataSynced); err != nil {
					return nil, fmt.Errorf("scan unsynced node: %w", err)
				}
				unsynced = append(unsynced, u)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			if len(unsynced) > 0 {
				affected := make([]AffectedObject, 0, len(unsynced))
				for _, u := range unsynced {
					affected = append(affected, AffectedObject{
						Type:       "node",
						Identifier: fmt.Sprintf("%s:%d", u.NodeName, u.NodePort),
						Details: map[string]interface{}{
							"node_id":         u.NodeID,
							"has_metadata":    u.HasMetadata,
							"metadata_synced": u.MetadataSynced,
						},
					})
				}

				issue := Issue{
					ID:              fmt.Sprintf("unsynced_nodes_%d", len(unsynced)),
					CheckID:         "unsynced_nodes",
					Severity:        SeverityWarning,
					Category:        CategorySync,
					Title:           fmt.Sprintf("%d Nodes Have Pending Metadata Sync", len(unsynced)),
					Description:     "These nodes have hasmetadata=true but metadatasynced=false.",
					AffectedObjects: affected,
					Impact:          "Metadata changes may not be reflected on these nodes. MX queries may see stale data.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Wait for background sync or trigger manual sync",
						RiskLevel:        RiskNone,
						RequiresDowntime: false,
						RequiresBackup:   false,
						SQLCommands: []string{
							"-- Check if citus maintenance daemon is running and trigger sync",
							"SELECT citus_finish_citus_upgrade(); -- Forces metadata sync",
						},
						VerificationSQL: "SELECT nodename, nodeport FROM pg_dist_node WHERE hasmetadata = true AND metadatasynced = false;",
						Notes:           "Metadata sync happens automatically via the maintenance daemon. Check if daemon is running.",
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}

// checkShardRangeGaps finds gaps in shard ranges.
func checkShardRangeGaps(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "shard_range_gaps", "Shard Range Gaps", CategoryConsistency,
		"Detects gaps in hash shard ranges that could cause data routing issues",
		func() ([]Issue, error) {
			rows, err := pool.Query(ctx, QueryShardRangeGaps)
			if err != nil {
				return nil, fmt.Errorf("query shard range gaps: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var gaps []ShardRangeGap
			for rows.Next() {
				var g ShardRangeGap
				if err := rows.Scan(&g.TableName, &g.PrevShard, &g.NextShard, &g.GapStart, &g.GapEnd); err != nil {
					return nil, fmt.Errorf("scan shard range gap: %w", err)
				}
				gaps = append(gaps, g)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			// Group by table
			byTable := make(map[string][]ShardRangeGap)
			for _, g := range gaps {
				byTable[g.TableName] = append(byTable[g.TableName], g)
			}

			for tableName, tableGaps := range byTable {
				affected := make([]AffectedObject, 0, len(tableGaps))
				for _, g := range tableGaps {
					affected = append(affected, AffectedObject{
						Type:       "shard_range",
						Identifier: fmt.Sprintf("%s [%d-%d]", tableName, g.GapStart, g.GapEnd),
						Details: map[string]interface{}{
							"prev_shard": g.PrevShard,
							"next_shard": g.NextShard,
							"gap_start":  g.GapStart,
							"gap_end":    g.GapEnd,
						},
					})
				}

				issue := Issue{
					ID:              fmt.Sprintf("shard_range_gaps_%s", tableName),
					CheckID:         "shard_range_gaps",
					Severity:        SeverityCritical,
					Category:        CategoryConsistency,
					Title:           fmt.Sprintf("Table %s Has %d Shard Range Gap(s)", tableName, len(tableGaps)),
					Description:     "Hash distribution range has gaps. Data with hash values in these ranges cannot be stored.",
					AffectedObjects: affected,
					Impact:          "INSERTs for rows hashing to gap ranges will fail or be routed incorrectly.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Recreate shards or redistribute the table",
						RiskLevel:        RiskHigh,
						RequiresDowntime: true,
						RequiresBackup:   true,
						ManualSteps: []string{
							"1. This is a serious metadata corruption",
							"2. Export table data",
							"3. Undistribute and redistribute the table",
							"4. Re-import data",
						},
						Notes: "Contact Citus support if in production. This indicates serious metadata corruption.",
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}

// checkShardRangeOverlaps finds overlapping shard ranges.
func checkShardRangeOverlaps(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "shard_range_overlaps", "Shard Range Overlaps", CategoryConsistency,
		"Detects overlapping hash shard ranges that could cause duplicate routing",
		func() ([]Issue, error) {
			rows, err := pool.Query(ctx, QueryShardRangeOverlaps)
			if err != nil {
				return nil, fmt.Errorf("query shard range overlaps: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var overlaps []ShardRangeOverlap
			for rows.Next() {
				var o ShardRangeOverlap
				if err := rows.Scan(&o.TableName, &o.Shard1, &o.Shard2, &o.Range1, &o.Range2); err != nil {
					return nil, fmt.Errorf("scan shard range overlap: %w", err)
				}
				overlaps = append(overlaps, o)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			// Group by table
			byTable := make(map[string][]ShardRangeOverlap)
			for _, o := range overlaps {
				byTable[o.TableName] = append(byTable[o.TableName], o)
			}

			for tableName, tableOverlaps := range byTable {
				affected := make([]AffectedObject, 0, len(tableOverlaps)*2)
				for _, o := range tableOverlaps {
					affected = append(affected, AffectedObject{
						Type:       "shard",
						Identifier: fmt.Sprintf("%d", o.Shard1),
						Details: map[string]interface{}{
							"range": o.Range1,
						},
					})
					affected = append(affected, AffectedObject{
						Type:       "shard",
						Identifier: fmt.Sprintf("%d", o.Shard2),
						Details: map[string]interface{}{
							"range": o.Range2,
						},
					})
				}

				issue := Issue{
					ID:              fmt.Sprintf("shard_range_overlaps_%s", tableName),
					CheckID:         "shard_range_overlaps",
					Severity:        SeverityCritical,
					Category:        CategoryConsistency,
					Title:           fmt.Sprintf("Table %s Has %d Overlapping Shard Ranges", tableName, len(tableOverlaps)),
					Description:     "Multiple shards cover the same hash range values.",
					AffectedObjects: affected,
					Impact:          "Data may be duplicated or queries may return incorrect results.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Identify and remove duplicate shards",
						RiskLevel:        RiskHigh,
						RequiresDowntime: true,
						RequiresBackup:   true,
						ManualSteps: []string{
							"1. This is a serious metadata corruption",
							"2. Identify which shard has the correct/more recent data",
							"3. Remove the duplicate shard placement and metadata",
							"4. Verify data integrity",
						},
						Notes: "Contact Citus support if in production. This indicates serious metadata corruption.",
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}

// checkReferenceTableConsistency finds reference tables with insufficient placements.
func checkReferenceTableConsistency(ctx context.Context, pool *pgxpool.Pool, includeFixes bool) CheckResult {
	return runCheck(ctx, "reference_table_consistency", "Reference Table Consistency", CategoryConsistency,
		"Detects reference tables that are not replicated to all active nodes",
		func() ([]Issue, error) {
			rows, err := pool.Query(ctx, QueryReferenceTablePlacements)
			if err != nil {
				return nil, fmt.Errorf("query reference table placements: %w", err)
			}
			defer rows.Close()

			var issues []Issue
			var refIssues []ReferenceTableIssue
			for rows.Next() {
				var r ReferenceTableIssue
				if err := rows.Scan(&r.TableName, &r.PlacementCount, &r.ActiveNodes); err != nil {
					return nil, fmt.Errorf("scan reference table issue: %w", err)
				}
				refIssues = append(refIssues, r)
			}
			if rows.Err() != nil {
				return nil, rows.Err()
			}

			for _, r := range refIssues {
				affected := []AffectedObject{
					{
						Type:       "table",
						Identifier: r.TableName,
						Details: map[string]interface{}{
							"placement_count": r.PlacementCount,
							"active_nodes":    r.ActiveNodes,
							"missing":         r.ActiveNodes - r.PlacementCount,
						},
					},
				}

				issue := Issue{
					ID:              fmt.Sprintf("ref_table_incomplete_%s", r.TableName),
					CheckID:         "reference_table_consistency",
					Severity:        SeverityCritical,
					Category:        CategoryConsistency,
					Title:           fmt.Sprintf("Reference Table %s Missing on %d Node(s)", r.TableName, r.ActiveNodes-r.PlacementCount),
					Description:     fmt.Sprintf("Reference table has %d placements but there are %d active nodes.", r.PlacementCount, r.ActiveNodes),
					AffectedObjects: affected,
					Impact:          "Joins with this reference table may fail on nodes missing the data.",
				}

				if includeFixes {
					issue.Fix = &Fix{
						Approach:         "Replicate reference table to missing nodes",
						RiskLevel:        RiskLow,
						RequiresDowntime: false,
						RequiresBackup:   false,
						SQLCommands: []string{
							fmt.Sprintf("-- Replicate reference table to all nodes\nSELECT replicate_reference_tables();"),
						},
						VerificationSQL: fmt.Sprintf("SELECT COUNT(DISTINCT groupid) FROM pg_dist_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = '%s'::regclass);", r.TableName),
						Notes:           "replicate_reference_tables() will copy to all missing nodes.",
					}
				}
				issues = append(issues, issue)
			}
			return issues, nil
		})
}
