// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Rate limiting placeholder for future implementation.

package safety

// Limiter is a placeholder rate limiter.
type Limiter struct {}

func NewLimiter() *Limiter { return &Limiter{} }

func (l *Limiter) Allow(action string) bool { return true }
