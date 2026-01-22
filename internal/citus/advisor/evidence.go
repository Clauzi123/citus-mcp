// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Evidence collection and hashing for advisor findings.

package advisor

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
)

// Evidence is a structured map.
type Evidence map[string]interface{}

// StableID computes a stable hex hash for the rule target and key evidence fields.
func StableID(ruleID, target string, evidence Evidence) string {
	h := sha1.New()
	h.Write([]byte(ruleID))
	h.Write([]byte("|"))
	h.Write([]byte(target))
	if evidence != nil {
		keys := make([]string, 0, len(evidence))
		for k := range evidence {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h.Write([]byte("|"))
			h.Write([]byte(k))
			h.Write([]byte("="))
			h.Write([]byte(fmtSprintEvidence(evidence[k])))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func fmtSprintEvidence(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprintf("%v", v)
	}
}
