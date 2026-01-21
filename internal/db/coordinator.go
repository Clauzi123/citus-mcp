package db

import (
    "context"

    dbsql "citus-mcp/internal/db/sql"
    "github.com/jackc/pgx/v5/pgxpool"
)

type ExtensionInfo struct {
    Version string `json:"version"`
}

type ServerInfo struct {
    PostgresVersion string `json:"postgres_version"`
    CitusVersion    string `json:"citus_version"`
}

func GetExtensionInfo(ctx context.Context, pool *pgxpool.Pool) (*ExtensionInfo, error) {
    var v string
    if err := pool.QueryRow(ctx, dbsql.QueryCitusExtension).Scan(&v); err != nil {
        return nil, err
    }
    return &ExtensionInfo{Version: v}, nil
}

func GetServerVersion(ctx context.Context, pool *pgxpool.Pool) (string, error) {
    var v string
    if err := pool.QueryRow(ctx, dbsql.QueryServerVersion).Scan(&v); err != nil {
        return "", err
    }
    return v, nil
}

func GetCitusVersion(ctx context.Context, pool *pgxpool.Pool) (string, error) {
    var v string
    if err := pool.QueryRow(ctx, dbsql.QueryCitusVersion).Scan(&v); err != nil {
        return "", err
    }
    return v, nil
}

func GetServerInfo(ctx context.Context, pool *pgxpool.Pool) (*ServerInfo, error) {
    pgV, err := GetServerVersion(ctx, pool)
    if err != nil {
        return nil, err
    }
    citusV, err := GetCitusVersion(ctx, pool)
    if err != nil {
        return nil, err
    }
    return &ServerInfo{PostgresVersion: pgV, CitusVersion: citusV}, nil
}
