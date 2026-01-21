package dbsql

const (
    QueryCitusExtension    = "SELECT extversion FROM pg_extension WHERE extname = 'citus'"
    QueryServerVersion     = "SHOW server_version"
    QueryPgDistNode        = "SELECT nodeid, nodename, nodeport, noderole::text FROM pg_dist_node ORDER BY nodeid"
    QueryPgDistNodeStatus  = "SELECT nodeid, nodename, nodeport, noderole::text, COALESCE(isactive, true) AS isactive, COALESCE(shouldhaveshards, false) AS shouldhaveshards FROM pg_dist_node ORDER BY nodeid"
    QueryCitusVersion      = "SELECT citus_version()"
    QueryGetActiveWorkers  = "SELECT * FROM citus_get_active_worker_nodes()"
)
