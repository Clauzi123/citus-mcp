// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Citus cluster metadata retrieval (version, coordinator info).

package citus

import (
	"context"

	"citus-mcp/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Metadata struct {
	Server       *db.ServerInfo    `json:"server"`
	Extension    *db.ExtensionInfo `json:"extension"`
	Capabilities *db.Capabilities  `json:"capabilities"`
	Nodes        []db.Node         `json:"nodes"`
}

func GetMetadata(ctx context.Context, pool *pgxpool.Pool) (*Metadata, error) {
	extension, err := db.GetExtensionInfo(ctx, pool)
	if err != nil {
		return nil, err
	}
	server, err := db.GetServerInfo(ctx, pool)
	if err != nil {
		return nil, err
	}
	capabilities, err := db.DetectCapabilitiesWithPool(ctx, pool)
	if err != nil {
		return nil, err
	}
	nodes, err := db.ListNodes(ctx, pool)
	if err != nil {
		return nil, err
	}
	return &Metadata{Server: server, Extension: extension, Capabilities: capabilities, Nodes: nodes}, nil
}
