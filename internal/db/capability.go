// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Citus cluster capability detection (version, features).

package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Capabilities represent optional functions/UDFs/features available in Citus.
type Capabilities struct {
	HasCitusExtension           bool   `json:"has_citus_extension"`
	CitusVersion                string `json:"citus_version,omitempty"`
	HasRebalanceStart           bool   `json:"has_rebalance_start"`
	HasRebalanceStatus          bool   `json:"has_rebalance_status"`
	HasRebalancePlan            bool   `json:"has_rebalance_plan"`
	HasRebalanceProgress        bool   `json:"has_rebalance_progress"`
	HasMoveShardPlacement       bool   `json:"has_move_shard_placement"`
	HasMasterMoveShardPlacement bool   `json:"has_master_move_shard_placement"`
	HasGetActiveWorkerNodes     bool   `json:"has_get_active_worker_nodes"`
	HasShardSizes               bool   `json:"has_shard_sizes"`
}

func (c *Capabilities) SupportsRebalancePlan() bool            { return c.HasRebalancePlan }
func (c *Capabilities) SupportsRebalanceStart() bool           { return c.HasRebalanceStart }
func (c *Capabilities) SupportsRebalanceStatus() bool          { return c.HasRebalanceStatus }
func (c *Capabilities) SupportsRebalanceProgress() bool        { return c.HasRebalanceProgress }
func (c *Capabilities) SupportsShardMove() bool                { return c.HasMoveShardPlacement }
func (c *Capabilities) SupportsMasterMoveShardPlacement() bool { return c.HasMasterMoveShardPlacement }
func (c *Capabilities) SupportsGetActiveWorkerNodes() bool     { return c.HasGetActiveWorkerNodes }
func (c *Capabilities) SupportsShardSizes() bool               { return c.HasShardSizes }

// DetectCapabilities probes pg_extension and pg_proc for Citus UDFs.
func DetectCapabilities(ctx context.Context, pool *pgxpool.Pool) (*Capabilities, error) {
	c := &Capabilities{}
	// extension
	if err := pool.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'citus'").Scan(&c.CitusVersion); err == nil {
		c.HasCitusExtension = true
	}

	check := func(fn string) (bool, error) {
		var ok bool
		// Use parameterized query to prevent SQL injection
		if err := pool.QueryRow(ctx, "SELECT to_regproc($1) IS NOT NULL", fn).Scan(&ok); err != nil {
			return false, err
		}
		return ok, nil
	}

	// Functions to detect
	var err error
	if c.HasRebalanceStart, err = check("citus_rebalance_start"); err != nil {
		return nil, err
	}
	if c.HasRebalanceStatus, err = check("citus_rebalance_status"); err != nil {
		return nil, err
	}
	if c.HasRebalancePlan, err = check("get_rebalance_table_shards_plan"); err != nil {
		return nil, err
	}
	if c.HasMoveShardPlacement, err = check("citus_move_shard_placement"); err != nil {
		return nil, err
	}
	if c.HasMasterMoveShardPlacement, err = check("master_move_shard_placement"); err != nil {
		return nil, err
	}
	if c.HasRebalanceProgress, err = check("get_rebalance_progress"); err != nil {
		return nil, err
	}
	if c.HasGetActiveWorkerNodes, err = check("citus_get_active_worker_nodes"); err != nil {
		return nil, err
	}
	if c.HasShardSizes, err = check("citus_shard_sizes"); err != nil {
		return nil, err
	}
	return c, nil
}

// DetectCapabilitiesWithPool is kept for backward compatibility.
func DetectCapabilitiesWithPool(ctx context.Context, pool *pgxpool.Pool) (*Capabilities, error) {
	return DetectCapabilities(ctx, pool)
}
