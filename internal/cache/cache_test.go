// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Unit tests for TTL cache.

package cache

import (
	"testing"
	"time"
)

func TestCacheSetGet(t *testing.T) {
	c := New()
	c.Set("k", "v", time.Second)
	v, ok := c.Get("k")
	if !ok || v.(string) != "v" {
		t.Fatalf("expected v, got %v", v)
	}
}
