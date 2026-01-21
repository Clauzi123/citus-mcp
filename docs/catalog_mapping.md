# Citus Catalogs & Views Mapping

> Catalogs/views from `SELECT ... extname='citus'` (57 rows on local dev); demo tables omitted.

| Object | Type | Purpose | Current Tool Coverage | Proposed Tool / Notes |
| --- | --- | --- | --- | --- |
| `pg_dist_node`, `pg_dist_node_metadata` | table | Cluster node inventory & metadata | `list_nodes`, `citus_cluster_summary` | Add health/status derived from `node_state`, lag, pool info |
| `citus_nodes` | view | Node info (host/port/groupid) | `cluster_summary`, `list_nodes` |  |
| `pg_dist_partition` | table | Table distribution metadata | `list_distributed_tables`, `table_inspector` |  |
| `pg_dist_shard`, `pg_dist_placement` | table | Shards and placements | `list_shards`, `shard_heatmap`, `shard_skew_report`, `advisor` | Add `citus_shard_map` (placements for table) |
| `pg_dist_shard_placement` | view | Shard → node mapping | fallback in heatmap |  |
| `citus_shards`, `citus_shards_on_worker` | view | Shard sizes & placements | `shard_heatmap`, `shard_skew_report`, `advisor`, `table_inspector` |  |
| `citus_shard_indexes_on_worker` | view | Index presence on workers | `table_inspector` (drift stub) | Flesh out drift detection, report missing indexes |
| `citus_tables`, `citus_schemas` | view | Table/schema distribution info | `list_distributed_tables` | Potential `citus_schema_overview` |
| `pg_dist_colocation` | table | Colocation groups | `list_distributed_tables` (colocation_id) | Add `citus_colocation_inspector` |
| `pg_dist_background_job`, `_task`, `_depend` | table | Background jobs (rebalance, copy) | `citus_advisor` (ops focus) | Add `citus_job_inspector` (jobs/tasks in progress) |
| `pg_dist_poolinfo` | table | Connection pool usage | none | Add `citus_pool_inspector` (connections per node) |
| `pg_dist_transaction` | table | Distributed transactions | none | Add `citus_txn_inspector` (active prepared txns) |
| `pg_dist_authinfo` | table | Auth info | none | Possibly sensitive; omit or redact |
| `citus_stat_activity`, `citus_dist_stat_activity` | view | Cluster-wide activity | `citus_advisor` (ops focus) | Add `citus_activity` (top by duration, waits) |
| `citus_locks`, `citus_lock_waits` | view | Cluster-wide locks & waits | `citus_lock_inspector`, `citus_advisor` (ops focus) |  |
| `citus_stat_statements` | view | Query stats | none | Add `citus_top_statements` (by calls/mean_time) |
| `citus_stat_counters`, `citus_stats` | view | Internal counters | none | Add to `citus_stats_dashboard` |
| `citus_stat_tenants`, `_local` | view | Tenant metrics | `citus_advisor` (ops focus) | Add `citus_tenant_hotspots` |
| `pg_stat_all_tables` (core) | view | Table stats | `table_inspector` | Add cluster-wide `citus_table_stats` |
| `time_partitions` | view | Time-partition info | none | Add `citus_time_partitions_inspector` |

## Gaps & Priorities

1. **Lock & wait visibility** — `citus_lock_inspector` (high value, low risk)
2. **Activity dashboard** — `citus_activity` (use `citus_stat_activity` + `wait_event`/`relation`)
3. **Background jobs** — `citus_job_inspector` (rebalance/copy progress)
4. **Query hotspots** — `citus_top_statements`
5. **Tenant hotspots** — `citus_tenant_hotspots`
6. **Pool health** — `citus_pool_inspector`
7. **Index drift** — enhance `table_inspector` using `citus_shard_indexes_on_worker`
8. **Shard map** — `citus_shard_map` (placements + sizes for a table)

All tools must remain **read-only** and **deterministic** with guardrails.
