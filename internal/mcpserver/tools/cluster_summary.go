package tools

import (
	"context"
	"sort"

	"citus-mcp/internal/db"
	serr "citus-mcp/internal/errors"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

// ClusterSummaryInput defines input for citus_cluster_summary.
type ClusterSummaryInput struct {
	IncludeWorkers bool `json:"include_workers,omitempty"`
	IncludeGUCs    bool `json:"include_gucs,omitempty"`
}

// ClusterSummaryOutput defines output structure.
type ClusterSummaryOutput struct {
	Coordinator CoordinatorSummary `json:"coordinator"`
	Workers     []WorkerSummary    `json:"workers,omitempty"`
	Counts      CountsSummary      `json:"counts"`
	Warnings    []string           `json:"warnings,omitempty"`
	GUCs        map[string]string  `json:"gucs,omitempty"`
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
	// defaults
	if !input.IncludeWorkers {
		input.IncludeWorkers = true
	}

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
