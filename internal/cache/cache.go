// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// In-memory TTL cache for query results.

package cache

import (
	"sync"
	"time"
)

type item struct {
	value     any
	expiresAt time.Time
}

type Cache struct {
	mu    sync.RWMutex
	items map[string]item
}

func New() *Cache {
	return &Cache{items: make(map[string]item)}
}

func (c *Cache) Get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	it, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if !it.expiresAt.IsZero() && time.Now().After(it.expiresAt) {
		return nil, false
	}
	return it.value, true
}

func (c *Cache) Set(key string, value any, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	it := item{value: value}
	if ttl > 0 {
		it.expiresAt = time.Now().Add(ttl)
	}
	c.items[key] = it
}
