package db

import (
    "context"

    dbsql "citus-mcp/internal/db/sql"
    "github.com/jackc/pgx/v5/pgxpool"
)

type Node struct {
    NodeID   int32  `json:"node_id"`
    NodeName string `json:"node_name"`
    NodePort int32  `json:"node_port"`
    NodeRole string `json:"node_role"`
}

func ListNodes(ctx context.Context, pool *pgxpool.Pool) ([]Node, error) {
    rows, err := pool.Query(ctx, dbsql.QueryPgDistNode)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    var nodes []Node
    for rows.Next() {
        var n Node
        if err := rows.Scan(&n.NodeID, &n.NodeName, &n.NodePort, &n.NodeRole); err != nil {
            return nil, err
    }
        nodes = append(nodes, n)
    }
    return nodes, rows.Err()
}

func ListWorkers(ctx context.Context, pool *pgxpool.Pool) ([]Node, error) {
    nodes, err := ListNodes(ctx, pool)
    if err != nil {
        return nil, err
    }
    var workers []Node
    for _, n := range nodes {
        if n.NodeRole != "primary" { // primary typically coordinator
            workers = append(workers, n)
        }
    }
    return workers, nil
}
