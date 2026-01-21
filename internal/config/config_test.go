package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("CITUS_MCP_COORDINATOR_DSN", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CoordinatorDSN == "" {
		t.Fatalf("expected coordinator dsn to be set")
	}
	if cfg.Mode != ModeReadOnly {
		t.Fatalf("expected mode read_only, got %s", cfg.Mode)
	}
}
