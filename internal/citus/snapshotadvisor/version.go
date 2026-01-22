// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Citus version parsing utilities.

package snapshotadvisor

import "regexp"

var verRe = regexp.MustCompile(`^(\d+)\.(\d+)`)

func parseVersion(v string) (int, int) {
	m := verRe.FindStringSubmatch(v)
	if len(m) < 3 {
		return 0, 0
	}
	return atoi(m[1]), atoi(m[2])
}

func atoi(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	return n
}
