package metadata

// SQL queries for metadata health checks.
const (
	// QueryOrphanedShards finds shards without any placements.
	QueryOrphanedShards = `
		SELECT s.shardid, s.logicalrelid, s.logicalrelid::regclass::text as table_name
		FROM pg_dist_shard s
		LEFT JOIN pg_dist_placement p ON s.shardid = p.shardid
		WHERE p.shardid IS NULL
		ORDER BY s.logicalrelid, s.shardid`

	// QueryOrphanedPlacements finds placements for non-existent shards.
	QueryOrphanedPlacements = `
		SELECT p.placementid, p.shardid, p.groupid
		FROM pg_dist_placement p
		WHERE NOT EXISTS (SELECT 1 FROM pg_dist_shard s WHERE s.shardid = p.shardid)
		ORDER BY p.shardid`

	// QueryMissingRelations finds pg_dist_partition entries with no corresponding pg_class.
	QueryMissingRelations = `
		SELECT p.logicalrelid, p.partmethod::text, p.colocationid, p.repmodel
		FROM pg_dist_partition p
		WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = p.logicalrelid)
		ORDER BY p.logicalrelid`

	// QueryInvalidNodeRefs finds placements referencing non-existent nodes.
	QueryInvalidNodeRefs = `
		SELECT p.placementid, p.shardid, p.groupid
		FROM pg_dist_placement p
		LEFT JOIN pg_dist_node n ON p.groupid = n.groupid
		WHERE n.groupid IS NULL
		ORDER BY p.groupid, p.shardid`

	// QueryStaleCleanupRecords finds cleanup records for removed nodes.
	QueryStaleCleanupRecords = `
		SELECT c.record_id, c.object_name, c.object_type, c.node_group_id
		FROM pg_dist_cleanup c
		LEFT JOIN pg_dist_node n ON c.node_group_id = n.groupid
		WHERE n.groupid IS NULL
		ORDER BY c.node_group_id`

	// QueryColocationMismatch finds tables in same colocation with different shard counts.
	QueryColocationMismatch = `
		WITH colocation_counts AS (
			SELECT p.colocationid, p.logicalrelid::regclass::text as table_name,
				   COUNT(DISTINCT s.shardid) as shard_count
			FROM pg_dist_partition p
			JOIN pg_dist_shard s ON p.logicalrelid = s.logicalrelid
			WHERE p.colocationid > 0
			GROUP BY p.colocationid, p.logicalrelid
		)
		SELECT c1.colocationid, c1.table_name as table1, c1.shard_count as count1,
			   c2.table_name as table2, c2.shard_count as count2
		FROM colocation_counts c1
		JOIN colocation_counts c2 ON c1.colocationid = c2.colocationid 
								  AND c1.table_name < c2.table_name
		WHERE c1.shard_count != c2.shard_count
		ORDER BY c1.colocationid`

	// QueryUnsyncedNodes finds nodes with pending metadata sync.
	QueryUnsyncedNodes = `
		SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced
		FROM pg_dist_node
		WHERE hasmetadata = true AND metadatasynced = false
		ORDER BY nodeid`

	// QueryShardRangeGaps finds gaps in shard ranges for hash-distributed tables.
	QueryShardRangeGaps = `
		WITH shard_ranges AS (
			SELECT s.logicalrelid, s.logicalrelid::regclass::text as table_name,
				   s.shardid, s.shardminvalue::bigint as minval, 
				   s.shardmaxvalue::bigint as maxval
			FROM pg_dist_shard s
			JOIN pg_dist_partition p ON s.logicalrelid = p.logicalrelid
			WHERE s.shardminvalue IS NOT NULL 
			  AND s.shardmaxvalue IS NOT NULL
			  AND p.partmethod = 'h'
			ORDER BY s.logicalrelid, s.shardminvalue::bigint
		),
		range_with_next AS (
			SELECT *, 
				   LEAD(minval) OVER (PARTITION BY logicalrelid ORDER BY minval) as next_minval,
				   LEAD(shardid) OVER (PARTITION BY logicalrelid ORDER BY minval) as next_shardid
			FROM shard_ranges
		)
		SELECT table_name, shardid as prev_shard, next_shardid as next_shard,
			   maxval as gap_start, next_minval as gap_end
		FROM range_with_next
		WHERE next_minval IS NOT NULL AND maxval + 1 < next_minval
		ORDER BY table_name, gap_start`

	// QueryShardRangeOverlaps finds overlapping shard ranges.
	QueryShardRangeOverlaps = `
		SELECT s1.logicalrelid::regclass::text as table_name, 
			   s1.shardid as shard1, s2.shardid as shard2,
			   s1.shardminvalue || ' - ' || s1.shardmaxvalue as range1,
			   s2.shardminvalue || ' - ' || s2.shardmaxvalue as range2
		FROM pg_dist_shard s1
		JOIN pg_dist_shard s2 ON s1.logicalrelid = s2.logicalrelid AND s1.shardid < s2.shardid
		WHERE s1.shardminvalue IS NOT NULL AND s1.shardmaxvalue IS NOT NULL
		  AND s2.shardminvalue IS NOT NULL AND s2.shardmaxvalue IS NOT NULL
		  AND s1.shardminvalue::bigint <= s2.shardmaxvalue::bigint
		  AND s2.shardminvalue::bigint <= s1.shardmaxvalue::bigint
		ORDER BY table_name, shard1`

	// QueryReferenceTablePlacements finds reference tables with insufficient placements.
	QueryReferenceTablePlacements = `
		WITH active_node_count AS (
			SELECT COUNT(*) as cnt FROM pg_dist_node WHERE isactive = true
		),
		ref_table_placements AS (
			SELECT p.logicalrelid::regclass::text as table_name,
				   COUNT(DISTINCT pl.groupid) as placement_count
			FROM pg_dist_partition p
			JOIN pg_dist_shard s ON p.logicalrelid = s.logicalrelid
			JOIN pg_dist_placement pl ON s.shardid = pl.shardid
			WHERE p.repmodel = 't'
			GROUP BY p.logicalrelid
		)
		SELECT r.table_name, r.placement_count, a.cnt as active_nodes
		FROM ref_table_placements r, active_node_count a
		WHERE r.placement_count < a.cnt
		ORDER BY r.table_name`

	// QueryMissingReferenceTableNodes finds which nodes are missing reference table placements.
	QueryMissingReferenceTableNodes = `
		WITH ref_shards AS (
			SELECT s.shardid, p.logicalrelid::regclass::text as table_name
			FROM pg_dist_shard s
			JOIN pg_dist_partition p ON s.logicalrelid = p.logicalrelid
			WHERE p.repmodel = 't'
		),
		ref_placements AS (
			SELECT rs.table_name, pl.groupid
			FROM ref_shards rs
			JOIN pg_dist_placement pl ON rs.shardid = pl.shardid
		)
		SELECT n.nodename, n.nodeport, 
			   array_agg(DISTINCT dt.table_name) as missing_tables
		FROM pg_dist_node n
		CROSS JOIN (SELECT DISTINCT table_name FROM ref_shards) dt
		LEFT JOIN ref_placements rp ON rp.table_name = dt.table_name AND rp.groupid = n.groupid
		WHERE n.isactive = true AND rp.groupid IS NULL
		GROUP BY n.nodename, n.nodeport
		HAVING COUNT(*) > 0
		ORDER BY n.nodename, n.nodeport`

	// QueryBackgroundJobStatus checks for failed or stuck background jobs.
	QueryBackgroundJobStatus = `
		SELECT job_id, job_type, status, started_at, finished_at,
			   EXTRACT(EPOCH FROM (COALESCE(finished_at, now()) - started_at)) as duration_seconds
		FROM pg_dist_background_job
		WHERE status IN ('failed', 'running', 'runnable')
		ORDER BY started_at DESC
		LIMIT 100`

	// QueryBackgroundTaskStatus checks for stuck or failed tasks.
	QueryBackgroundTaskStatus = `
		SELECT task_id, job_id, status, 
			   EXTRACT(EPOCH FROM (now() - COALESCE(retry_at, now()))) as stuck_seconds
		FROM pg_dist_background_task
		WHERE status IN ('blocked', 'runnable') 
		  AND retry_at < now() - interval '5 minutes'
		ORDER BY retry_at
		LIMIT 100`

	// QueryCleanupRecordsPending checks for pending shard cleanup.
	QueryCleanupRecordsPending = `
		SELECT record_id, object_name, object_type, node_group_id,
			   policy_type
		FROM pg_dist_cleanup
		ORDER BY record_id
		LIMIT 100`

	// QueryNodeExtensions gets extension versions from a node.
	QueryNodeExtensions = `
		SELECT extname, extversion 
		FROM pg_extension 
		ORDER BY extname`

	// QueryCoordinatorExtensions gets extensions from coordinator.
	QueryCoordinatorExtensions = QueryNodeExtensions

	// QueryCheckShardExists checks if a shard table exists on a worker.
	QueryCheckShardExists = `
		SELECT EXISTS (
			SELECT 1 FROM pg_class c
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE c.relname = $1 AND n.nspname = $2
		)`

	// QueryWorkerShardTables lists all shard tables on a worker.
	QueryWorkerShardTables = `
		SELECT n.nspname as schema_name, c.relname as table_name
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE c.relname ~ '_\d+$'
		  AND c.relkind = 'r'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, c.relname`

	// QueryExpectedShardPlacements gets shards expected on a specific node.
	QueryExpectedShardPlacements = `
		SELECT s.shardid, 
			   s.logicalrelid::regclass::text as table_name,
			   n.nspname as schema_name,
			   s.logicalrelid::regclass::text || '_' || s.shardid as shard_name
		FROM pg_dist_shard s
		JOIN pg_dist_placement p ON s.shardid = p.shardid
		JOIN pg_dist_node dn ON p.groupid = dn.groupid
		JOIN pg_class c ON s.logicalrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE dn.nodename = $1 AND dn.nodeport = $2
		ORDER BY s.shardid`

	// QueryTableSchemaHash gets a hash of table schema for comparison.
	QueryTableSchemaHash = `
		SELECT md5(string_agg(
			attname || ':' || atttypid::regtype::text || ':' || attnotnull::text || ':' || COALESCE(attidentity, ''),
			',' ORDER BY attnum
		)) as schema_hash
		FROM pg_attribute
		WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped`

	// QueryDistTableCount gets count of distributed tables.
	QueryDistTableCount = `
		SELECT COUNT(*) FROM pg_dist_partition`

	// QueryShardCount gets total shard count.
	QueryShardCount = `
		SELECT COUNT(*) FROM pg_dist_shard`

	// QueryPlacementCount gets total placement count.
	QueryPlacementCount = `
		SELECT COUNT(*) FROM pg_dist_placement`

	// QueryNodeCount gets active node count.
	QueryNodeCount = `
		SELECT COUNT(*) FROM pg_dist_node WHERE isactive = true`
)
