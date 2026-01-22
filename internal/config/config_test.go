// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for configuration loading.

package config

import (
	"os"
	"path/filepath"
	"testing"
)

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

func TestLoadConfigFileFlag(t *testing.T) {
	t.Setenv("CITUS_MCP_COORDINATOR_DSN", "")
	t.Setenv("CITUS_MCP_CONFIG", "")
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	dir := t.TempDir()
	path := filepath.Join(dir, "citus-mcp.yaml")
	contents := []byte(`coordinator_dsn: postgres://u:p@localhost:5432/postgres?sslmode=disable
coordinator_user: another
coordinator_password: secret
mode: admin
allow_execute: true
approval_secret: foo
`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	os.Args = []string{"cmd", "--config", path}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CoordinatorDSN == "" || cfg.CoordinatorUser != "another" || cfg.CoordinatorPassword != "secret" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestLoadConfigDefaultXDG(t *testing.T) {
	t.Setenv("CITUS_MCP_COORDINATOR_DSN", "")
	t.Setenv("CITUS_MCP_CONFIG", "")
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	dir := t.TempDir()
	configDir := filepath.Join(dir, "citus-mcp")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(configDir, "config.yaml")
	contents := []byte(`coordinator_dsn: postgres://u:p@localhost:5432/postgres?sslmode=disable`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("XDG_CONFIG_HOME", dir)
	os.Args = []string{"cmd"}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CoordinatorDSN == "" {
		t.Fatalf("expected coordinator dsn to be set")
	}
}

func TestLoadPositionalDSN(t *testing.T) {
	t.Setenv("CITUS_MCP_COORDINATOR_DSN", "")
	t.Setenv("CITUS_MCP_CONFIG", "")
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	dsn := "postgres://u:p@localhost:5432/postgres?sslmode=disable"
	os.Args = []string{"cmd", dsn}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CoordinatorDSN != dsn {
		t.Fatalf("expected positional dsn %q, got %q", dsn, cfg.CoordinatorDSN)
	}
}
