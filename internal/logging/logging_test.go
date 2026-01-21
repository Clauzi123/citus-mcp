package logging

import "testing"

func TestRedactDSN(t *testing.T) {
	dsn := "postgres://user:pass@localhost:5432/db"
	red := RedactDSN(dsn)
	if red == dsn || red == "" {
		t.Fatalf("expected redacted dsn, got %s", red)
	}
	if red == dsn {
		t.Fatalf("dsn not redacted")
	}
}
