package advisor

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"citus-mcp/internal/config"
	"citus-mcp/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// Input parameters for advisor run.
type Input struct {
	Focus              string
	Schema             string
	Table              string
	MaxTables          int
	MaxFindings        int
	IncludeSQLFixes    bool
	IncludeNextSteps   bool
	AllowQuerySampling bool
}

// Output is the advisor output schema.
type Output struct {
	Summary             Summary          `json:"summary"`
	ClusterObservations []FindingSnippet `json:"cluster_observations"`
	TableRankings       []TableRanking   `json:"table_rankings"`
	Findings            []Finding        `json:"findings"`
	Warnings            []Warning        `json:"warnings,omitempty"`
}

type Summary struct {
	AdvisorVersion string `json:"advisor_version"`
	Focus          string `json:"focus"`
	ClusterHealth  string `json:"cluster_health"`
	TablesAnalyzed int    `json:"tables_analyzed"`
	Findings       int    `json:"findings"`
	HighSeverity   int    `json:"high_severity"`
	GeneratedAt    string `json:"generated_at"`
}

type Warning struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Details interface{} `json:"details,omitempty"`
}

type TableRanking struct {
	Table       string   `json:"table"`
	ImpactScore int      `json:"impact_score"`
	Reasons     []string `json:"reasons"`
}

// FindingSnippet mirrors minimal finding fields for cluster observations.
type FindingSnippet struct {
	Severity string      `json:"severity"`
	Title    string      `json:"title"`
	Details  string      `json:"details"`
	Evidence interface{} `json:"evidence,omitempty"`
}

// Run orchestrates data collection, rule evaluation, scoring and rendering.
func Run(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, in Input) (Output, []Warning, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Cap limits
	if in.MaxTables <= 0 {
		in.MaxTables = 20
	}
	if in.MaxTables > 100 {
		in.MaxTables = 100
	}
	if in.MaxFindings <= 0 {
		in.MaxFindings = 25
	}
	if in.MaxFindings > 200 {
		in.MaxFindings = 200
	}
	if in.Focus == "" {
		in.Focus = "all"
	}

	// Capabilities
	caps, err := db.DetectCapabilities(ctx, pool)
	if err != nil {
		return Output{}, []Warning{{Code: "CAPABILITY_MISSING", Message: "failed to detect capabilities", Details: err.Error()}}, nil
	}

	collector := NewCollector(pool, cfg)
	ac := &AdvisorContext{
		Config:       cfg,
		Capabilities: caps,
		Focus:        in.Focus,
		IncludeSQL:   in.IncludeSQLFixes,
		IncludeNext:  in.IncludeNextSteps,
		MaxFindings:  in.MaxFindings,
	}

	// Data collection
	warnings := []Warning{}
	if err := collector.CollectCluster(ctx, ac); err != nil {
		return Output{}, []Warning{{Code: "PARTIAL_RESULTS", Message: "cluster collection failed", Details: err.Error()}}, nil
	}

	// If citus missing, short-circuit with critical finding
	if !ac.Capabilities.HasCitusExtension {
		findings := []Finding{MakeCriticalFinding("rule.citus_missing", "cluster", "cluster", "Citus extension missing", "Citus extension not installed", "Install citus extension", nil, nil, nil)}
		out := Render(ac, findings)
		return out, warnings, nil
	}

	// collect tables concurrently
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4)

	g.Go(func() error { return collector.CollectTables(gctx, ac, in.Schema, in.Table, in.MaxTables) })
	g.Go(func() error { return collector.CollectSkew(gctx, ac) })
	g.Go(func() error { return collector.CollectPrereqs(gctx, ac) })
	if in.AllowQuerySampling {
		g.Go(func() error { return collector.CollectQuerySamples(gctx, ac) })
	}
	if err := g.Wait(); err != nil {
		warnings = append(warnings, Warning{Code: "PARTIAL_RESULTS", Message: "one or more collectors failed", Details: err.Error()})
	}

	// Evaluate rules
	findings := EvaluateRules(ac)
	// Render
	out := Render(ac, findings)
	return out, warnings, nil
}

// AdvisorContext holds collected data for rules.
type AdvisorContext struct {
	Config       config.Config
	Capabilities *db.Capabilities
	Focus        string
	IncludeSQL   bool
	IncludeNext  bool
	MaxFindings  int

	Cluster          ClusterSnapshot
	Tables           []TableMeta
	TableMeta        map[string]*TableMeta // keyed by qualified name
	Skew             SkewSnapshot
	Prereqs          map[string]PrereqOutput
	Queries          map[string][]QuerySample
	HotShardsByTable map[string][]HotShardInfo
}

type QuerySample struct {
	Query string `json:"query"`
	Calls int64  `json:"calls"`
}

type HotShardInfo struct {
	ShardID int64  `json:"shard_id"`
	Table   string `json:"table"`
	Bytes   int64  `json:"bytes"`
	Node    string `json:"node"`
}

// SkewSnapshot captures per-node shard counts/bytes.
type SkewSnapshot struct {
	PerNode    []NodeSkew
	Ratio      float64
	BytesRatio float64
}

type NodeSkew struct {
	Node   string `json:"node"`
	Port   int32  `json:"port"`
	Shards int64  `json:"shards"`
	Bytes  int64  `json:"bytes_total"`
}

// ClusterSnapshot mirrors cluster summary
type ClusterSnapshot struct {
	Coordinator CoordinatorSummary
	Workers     []WorkerSummary
	Counts      CountsSummary
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

// TableMeta represents per-table metadata and metrics.
type TableMeta struct {
	Name           string     `json:"name"` // schema.table
	Schema         string     `json:"schema"`
	Relname        string     `json:"relname"`
	PartMethod     string     `json:"part_method"`
	DistColumn     string     `json:"dist_column"`
	ColocationID   int32      `json:"colocation_id"`
	ShardCount     int32      `json:"shard_count"`
	Replication    int32      `json:"replication_factor"`
	DistKeyIndexed bool       `json:"dist_key_indexed"`
	LastAnalyze    *time.Time `json:"last_analyze,omitempty"`
}

type PrereqOutput struct {
	Ready    bool           `json:"ready"`
	Issues   []PrereqIssue  `json:"issues"`
	Detected PrereqDetected `json:"detected"`
}

type PrereqIssue struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	SuggestedFixSQL string `json:"suggested_fix_sql,omitempty"`
}

type PrereqDetected struct {
	HasPrimaryKey       bool     `json:"has_primary_key"`
	ReplicaIdentity     string   `json:"replica_identity_setting"`
	CandidateUniqueIdxs []string `json:"candidate_unique_indexes_that_include_distribution_column"`
}

// NewCollector returns a collector.
func NewCollector(pool *pgxpool.Pool, cfg config.Config) *Collector {
	return &Collector{pool: pool, cfg: cfg}
}

// Collector gathers data into AdvisorContext.
type Collector struct {
	pool *pgxpool.Pool
	cfg  config.Config
}

// CollectCluster populates cluster snapshot in context.
func (c *Collector) CollectCluster(ctx context.Context, ac *AdvisorContext) error {
	// coordinator info
	coordHost := c.pool.Config().ConnConfig.Host
	coordPort := int32(c.pool.Config().ConnConfig.Port)
	var pgVersion string
	_ = c.pool.QueryRow(ctx, "SHOW server_version").Scan(&pgVersion)
	var citusVersion string
	_ = c.pool.QueryRow(ctx, "SELECT citus_version()::text").Scan(&citusVersion)
	ac.Cluster.Coordinator = CoordinatorSummary{Host: coordHost, Port: coordPort, PostgresVersion: pgVersion, CitusVersion: citusVersion}
	ac.TableMeta = map[string]*TableMeta{}

	// counts
	_ = c.pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_partition WHERE partmethod <> 'n'").Scan(&ac.Cluster.Counts.DistributedTables)
	_ = c.pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_partition WHERE partmethod = 'n'").Scan(&ac.Cluster.Counts.ReferenceTables)
	_ = c.pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_shard").Scan(&ac.Cluster.Counts.ShardsTotal)
	_ = c.pool.QueryRow(ctx, "SELECT count(*) FROM pg_dist_placement").Scan(&ac.Cluster.Counts.PlacementsTotal)

	// workers
	rows, err := c.pool.Query(ctx, "SELECT node_nodename, node_port, isactive, shouldhaveshards FROM pg_dist_node WHERE noderole='worker'")
	if err != nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var w WorkerSummary
		if err := rows.Scan(&w.Host, &w.Port, &w.IsActive, &w.ShouldHaveShards); err != nil {
			continue
		}
		ac.Cluster.Workers = append(ac.Cluster.Workers, w)
	}
	return nil
}

// CollectTables collects distributed/reference tables and metadata.
func (c *Collector) CollectTables(ctx context.Context, ac *AdvisorContext, schema, table string, max int) error {
	q := `
SELECT n.nspname, c.relname, p.partmethod, p.colocationid
FROM pg_dist_partition p
JOIN pg_class c ON c.oid = p.logicalrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE ($1='' OR n.nspname=$1) AND ($2='' OR (n.nspname||'.'||c.relname)=$2)
ORDER BY n.nspname, c.relname
LIMIT $3`
	rows, err := c.pool.Query(ctx, q, schema, table, max)
	if err != nil {
		return err
	}
	defer rows.Close()
	var tables []TableMeta
	for rows.Next() {
		var t TableMeta
		if err := rows.Scan(&t.Schema, &t.Relname, &t.PartMethod, &t.ColocationID); err != nil {
			return err
		}
		t.Name = t.Schema + "." + t.Relname
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Per-table enrich
	for i := range tables {
		t := &tables[i]
		// distribution column
		distCol, _ := fetchDistributionColumn(ctx, c.pool, t.Schema, t.Relname)
		t.DistColumn = distCol
		// shard count & replication factor
		var shardCnt, repl int32
		shardq := `SELECT COALESCE(count(*),0) FROM pg_dist_shard s JOIN pg_dist_partition p ON p.logicalrelid=s.logicalrelid JOIN pg_class c ON c.oid=s.logicalrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`
		_ = c.pool.QueryRow(ctx, shardq, t.Schema, t.Relname).Scan(&shardCnt)
		t.ShardCount = shardCnt
		replq := `SELECT COALESCE(max(cnt),0) FROM (SELECT shardid, count(*) cnt FROM pg_dist_placement pl JOIN pg_dist_shard s ON s.shardid=pl.shardid JOIN pg_class c ON c.oid=s.logicalrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 GROUP BY shardid) sub`
		_ = c.pool.QueryRow(ctx, replq, t.Schema, t.Relname).Scan(&repl)
		if repl == 0 {
			repl = 1
		}
		t.Replication = repl
		// index info
		_, _, idxs, _ := fetchIndexInfo(ctx, c.pool, t.Schema, t.Relname, t.DistColumn)
		t.DistKeyIndexed = len(idxs) > 0
		// last analyze
		var lastAnalyze sql.NullTime
		_ = c.pool.QueryRow(ctx, `SELECT last_analyze FROM pg_stat_all_tables WHERE schemaname=$1 AND relname=$2`, t.Schema, t.Relname).Scan(&lastAnalyze)
		if lastAnalyze.Valid {
			tm := lastAnalyze.Time
			t.LastAnalyze = &tm
		}
		ac.TableMeta[t.Name] = t
	}
	ac.Tables = tables
	return nil
}

// CollectSkew computes shard count skew per node.
func (c *Collector) CollectSkew(ctx context.Context, ac *AdvisorContext) error {
	// shard counts per node
	countQ := `SELECT n.nodename, n.nodeport, count(*)
FROM pg_dist_placement p
JOIN pg_dist_node n ON n.nodeid = p.nodeid
WHERE n.noderole='worker'
GROUP BY n.nodename, n.nodeport
ORDER BY n.nodename, n.nodeport`
	rows, err := c.pool.Query(ctx, countQ)
	if err != nil {
		return err
	}
	defer rows.Close()
	var per []NodeSkew
	var maxCnt, minCnt int64
	perMap := map[string]*NodeSkew{}
	for rows.Next() {
		var ns NodeSkew
		if err := rows.Scan(&ns.Node, &ns.Port, &ns.Shards); err != nil {
			return err
		}
		copy := ns
		per = append(per, copy)
		perMap[ns.Node+fmt.Sprintf(":%d", ns.Port)] = &per[len(per)-1]
		if maxCnt == 0 || ns.Shards > maxCnt {
			maxCnt = ns.Shards
		}
		if minCnt == 0 || ns.Shards < minCnt {
			minCnt = ns.Shards
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	cntRatio := 1.0
	if minCnt > 0 {
		cntRatio = float64(maxCnt) / float64(minCnt)
	}

	// bytes per node via citus_shards if available
	bytesRatio := 0.0
	var maxBytes, minBytes int64
	var hasBytes bool
	var ok bool
	if err := c.pool.QueryRow(ctx, "SELECT to_regclass('pg_catalog.citus_shards') IS NOT NULL").Scan(&ok); err == nil && ok {
		hasBytes = true
		bq := `SELECT nodename, nodeport, sum(shard_size) FROM pg_catalog.citus_shards WHERE true GROUP BY nodename, nodeport`
		brows, err := c.pool.Query(ctx, bq)
		if err == nil {
			for brows.Next() {
				var node string
				var port int32
				var bytes int64
				if err := brows.Scan(&node, &port, &bytes); err != nil {
					continue
				}
				key := node + fmt.Sprintf(":%d", port)
				if ns, exists := perMap[key]; exists {
					ns.Bytes = bytes
					if maxBytes == 0 || bytes > maxBytes {
						maxBytes = bytes
					}
					if minBytes == 0 || bytes < minBytes {
						minBytes = bytes
					}
				}
			}
			brows.Close()
		}
	}
	if hasBytes && minBytes > 0 {
		bytesRatio = float64(maxBytes) / float64(minBytes)
	}

	ac.Skew = SkewSnapshot{PerNode: per, Ratio: cntRatio, BytesRatio: bytesRatio}
	// collect hot shards per table if view available
	if ac.HotShardsByTable == nil {
		ac.HotShardsByTable = map[string][]HotShardInfo{}
	}
	var okHS bool
	if err := c.pool.QueryRow(ctx, "SELECT to_regclass('pg_catalog.citus_shards') IS NOT NULL").Scan(&okHS); err == nil && okHS {
		rows, err := c.pool.Query(ctx, `SELECT table_name, shardid, nodename, nodeport, shard_size FROM pg_catalog.citus_shards ORDER BY shard_size DESC LIMIT 20`)
		if err == nil {
			for rows.Next() {
				var tbl string
				var sh HotShardInfo
				var port int32
				if err := rows.Scan(&tbl, &sh.ShardID, &sh.Node, &port, &sh.Bytes); err != nil {
					continue
				}
				sh.Table = tbl
				// annotate node with port
				sh.Node = fmt.Sprintf("%s:%d", sh.Node, port)
				ac.HotShardsByTable[tbl] = append(ac.HotShardsByTable[tbl], sh)
			}
			rows.Close()
		}
	}
	return nil
}

// CollectPrereqs runs validate_rebalance_prereqs for tables.
func (c *Collector) CollectPrereqs(ctx context.Context, ac *AdvisorContext) error {
	ac.Prereqs = map[string]PrereqOutput{}
	for _, t := range ac.Tables {
		dCol := t.DistColumn
		hasPK, replIdent, idxs, err := fetchIndexInfo(ctx, c.pool, t.Schema, t.Relname, dCol)
		if err != nil {
			continue
		}
		ready := true
		issues := []PrereqIssue{}
		if !hasPK {
			ready = false
			issues = append(issues, PrereqIssue{Code: "NO_PRIMARY_KEY", Message: "Table missing primary key", SuggestedFixSQL: fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s);", t.Name, dCol)})
		}
		if replIdent == "n" {
			ready = false
			fix := ""
			if len(idxs) > 0 {
				fix = fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s;", t.Name, idxs[0])
			} else {
				fix = fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL; -- prefer USING INDEX", t.Name)
			}
			issues = append(issues, PrereqIssue{Code: "REPLICA_IDENTITY_DEFAULT", Message: "Replica identity DEFAULT", SuggestedFixSQL: fix})
		}
		ac.Prereqs[t.Name] = PrereqOutput{Ready: ready, Issues: issues, Detected: PrereqDetected{HasPrimaryKey: hasPK, ReplicaIdentity: replIdent, CandidateUniqueIdxs: idxs}}
	}
	return nil
}

// CollectQuerySamples optionally samples pg_stat_statements.
func (c *Collector) CollectQuerySamples(ctx context.Context, ac *AdvisorContext) error {
	var ext string
	if err := c.pool.QueryRow(ctx, `SELECT extname FROM pg_extension WHERE extname='pg_stat_statements'`).Scan(&ext); err != nil {
		return nil
	}
	ac.Queries = map[string][]QuerySample{}
	for _, t := range ac.Tables {
		// simple pattern match
		qq := `SELECT query, calls FROM pg_stat_statements WHERE query ILIKE '%'||$1||'%' ORDER BY total_exec_time DESC LIMIT 5`
		rows, err := c.pool.Query(ctx, qq, t.Relname)
		if err != nil {
			continue
		}
		var samples []QuerySample
		for rows.Next() {
			var s QuerySample
			if err := rows.Scan(&s.Query, &s.Calls); err != nil {
				continue
			}
			samples = append(samples, s)
		}
		rows.Close()
		if len(samples) > 0 {
			ac.Queries[t.Name] = samples
		}
	}
	return nil
}

func fetchDistributionColumn(ctx context.Context, pool *pgxpool.Pool, schema, rel string) (string, error) {
	const q = `
SELECT column_to_column_name(p.logicalrelid, p.partkey)
FROM pg_dist_partition p
JOIN pg_class c ON c.oid = p.logicalrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1::name AND c.relname = $2::name`
	var col string
	if err := pool.QueryRow(ctx, q, schema, rel).Scan(&col); err != nil {
		return "", err
	}
	return col, nil
}

func fetchIndexInfo(ctx context.Context, pool *pgxpool.Pool, schema, rel, distCol string) (hasPK bool, replicaIdent string, candidateIdxs []string, err error) {
	const pkq = `
SELECT EXISTS (
  SELECT 1
  FROM pg_index i
  JOIN pg_class c ON c.oid = i.indrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = $1::name AND c.relname = $2::name AND i.indisprimary
)`
	if err = pool.QueryRow(ctx, pkq, schema, rel).Scan(&hasPK); err != nil {
		return
	}
	const replq = `
SELECT c.relreplident
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1::name AND c.relname = $2::name`
	if err = pool.QueryRow(ctx, replq, schema, rel).Scan(&replicaIdent); err != nil {
		return
	}
	const uq = `
SELECT ci.relname AS index_name
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class ci ON ci.oid = i.indexrelid
JOIN pg_attribute a ON a.attrelid = c.oid
WHERE n.nspname = $1::name AND c.relname = $2::name AND i.indisunique
  AND a.attname = $3 AND a.attnum = ANY(i.indkey)
ORDER BY ci.relname`
	rows, err2 := pool.Query(ctx, uq, schema, rel, distCol)
	if err2 != nil {
		err = err2
		return
	}
	defer rows.Close()
	for rows.Next() {
		var idx string
		if err = rows.Scan(&idx); err != nil {
			return
		}
		candidateIdxs = append(candidateIdxs, idx)
	}
	err = rows.Err()
	return
}
