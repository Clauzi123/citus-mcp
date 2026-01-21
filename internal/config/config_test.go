package config

import "testing"

func TestLoadDefaults(t *testing.T) {
    t.Setenv("CITUS_MCP_DB_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
    cfg, err := Load()
    if err != nil {
        t.Fatalf("Load() error = %v", err)
    }
    if cfg.DB.URL == "" {
        t.Fatalf("expected DB URL to be set")
    }
}
