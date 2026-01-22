// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Sensitive data redaction for DSN and connection strings.

package safety

import (
	"net/url"
	"strings"
)

func RedactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	if u.User != nil {
		if _, hasPwd := u.User.Password(); hasPwd {
			u.User = url.UserPassword(u.User.Username(), "***")
		}
	}
	return u.String()
}

// QuoteIdent performs a minimal identifier quoting for SQL identifiers.
// For safety, it doubles internal quotes.
func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
