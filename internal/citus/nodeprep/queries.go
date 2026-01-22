// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// SQL queries for node preparation checks.

package nodeprep

// SQL queries for node preparation checks.
const (
	// QueryPostgresVersion gets PostgreSQL version information.
	QueryPostgresVersion = `
		SELECT current_setting('server_version_num')::int as version_num,
		       version() as version_string`

	// QueryPostgresConfig gets relevant PostgreSQL configuration.
	QueryPostgresConfig = `
		SELECT name, setting
		FROM pg_settings
		WHERE name IN (
			'shared_preload_libraries',
			'max_connections',
			'wal_level',
			'max_prepared_transactions',
			'listen_addresses'
		)`

	// QueryExtensions gets all installed extensions.
	QueryExtensions = `
		SELECT extname, extversion
		FROM pg_extension
		ORDER BY extname`

	// QueryCitusVersion gets Citus version if installed.
	QueryCitusVersion = `
		SELECT extversion 
		FROM pg_extension 
		WHERE extname = 'citus'`

	// QueryCitusInstalled checks if Citus extension exists.
	QueryCitusInstalled = `
		SELECT EXISTS (
			SELECT 1 FROM pg_extension WHERE extname = 'citus'
		)`

	// QueryUserSchemas gets non-system schemas.
	QueryUserSchemas = `
		SELECT nspname, pg_get_userbyid(nspowner) as owner
		FROM pg_namespace
		WHERE nspname NOT LIKE 'pg_%'
		  AND nspname NOT IN ('information_schema', 'citus', 'citus_internal', 'columnar', 'columnar_internal')
		ORDER BY nspname`

	// QueryUserTypes gets user-defined types.
	QueryUserTypes = `
		SELECT n.nspname as schema, t.typname as name,
		       CASE t.typtype
		           WHEN 'c' THEN 'composite'
		           WHEN 'e' THEN 'enum'
		           WHEN 'd' THEN 'domain'
		           WHEN 'r' THEN 'range'
		           ELSE t.typtype::text
		       END as type_kind
		FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'citus', 'citus_internal')
		  AND t.typtype IN ('c', 'e', 'd', 'r')
		  AND NOT EXISTS (
		      SELECT 1 FROM pg_class c WHERE c.reltype = t.oid AND c.relkind IN ('r', 'v', 'm', 'p')
		  )
		ORDER BY n.nspname, t.typname`

	// QueryUserFunctions gets user-defined functions (excluding Citus internal).
	QueryUserFunctions = `
		SELECT n.nspname as schema, p.proname as name,
		       pg_get_function_identity_arguments(p.oid) as args
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'citus', 'citus_internal', 'columnar', 'columnar_internal')
		  AND p.prokind IN ('f', 'p', 'a')
		ORDER BY n.nspname, p.proname`

	// QueryRoles gets database roles.
	QueryRoles = `
		SELECT rolname, rolsuper, rolcanlogin, rolcreatedb
		FROM pg_roles
		WHERE rolname NOT LIKE 'pg_%'
		  AND rolname NOT IN ('citus', 'citus_internal')
		ORDER BY rolname`

	// QueryDatabaseExists checks if a database exists.
	QueryDatabaseExists = `
		SELECT EXISTS (
			SELECT 1 FROM pg_database WHERE datname = $1
		)`

	// QueryCurrentDatabase gets current database name.
	QueryCurrentDatabase = `SELECT current_database()`

	// QuerySequences gets sequences that might need syncing.
	QuerySequences = `
		SELECT n.nspname as schema, c.relname as name,
		       pg_get_serial_sequence(n.nspname || '.' || t.relname, a.attname) is not null as is_serial
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
		LEFT JOIN pg_class t ON d.refobjid = t.oid
		LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
		WHERE c.relkind = 'S'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'citus', 'citus_internal')
		ORDER BY n.nspname, c.relname`

	// QueryDistributedSequences gets distributed sequences from Citus.
	QueryDistributedSequences = `
		SELECT objid::regclass::text as sequence_name
		FROM pg_dist_object
		WHERE classid = 'pg_class'::regclass
		  AND objid::regclass::text LIKE '%_seq'`

	// QuerySharedPreloadLibraries gets shared_preload_libraries setting.
	QuerySharedPreloadLibraries = `
		SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'`

	// QueryHBAEntries is a placeholder - pg_hba.conf can't be queried directly.
	// We'll check connectivity instead.
	QueryHBAEntries = `SELECT 1` // Placeholder
)
