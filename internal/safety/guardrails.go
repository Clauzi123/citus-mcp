package safety

import (
	"strings"
	"time"

	"citus-mcp/internal/config"
	serr "citus-mcp/internal/errors"
)

type Guardrails struct {
	allowExecute   bool
	approvalSecret string
	approvalTTL    int64 // seconds
	executeTools   map[string]struct{}
}

func NewGuardrails(cfg config.Config) *Guardrails {
	ttl := int64(5 * 60)
	return &Guardrails{
		allowExecute:   cfg.AllowExecute,
		approvalSecret: cfg.ApprovalSecret,
		approvalTTL:    ttl,
		executeTools:   map[string]struct{}{},
	}
}

// AddExecuteTool registers an execute-category tool name.
func (g *Guardrails) AddExecuteTool(name string) {
	if name == "" {
		return
	}
	g.executeTools[strings.ToLower(name)] = struct{}{}
}

// RequireExecuteAllowed ensures execute mode is enabled and token is valid.
func (g *Guardrails) RequireExecuteAllowed(token string, action string) error {
	if !g.allowExecute {
		return serr.NewExecuteDisabled()
	}
	if token == "" {
		return serr.NewApprovalRequired(action)
	}
	if err := ValidateApprovalToken(g.approvalSecret, action, token); err != nil {
		return serr.New(serr.CodeApprovalRequired, "invalid approval token", err.Error(), map[string]any{"action": action})
	}
	return nil
}

// RequireToolAllowed enforces read-only/execute policy based on tool category.
func (g *Guardrails) RequireToolAllowed(toolName string, isExecute bool, approvalToken string) error {
	if isExecute {
		action := "tool:" + toolName
		return g.RequireExecuteAllowed(approvalToken, action)
	}
	// read-only tool
	if !g.allowExecute {
		return nil
	}
	return nil
}

func (g *Guardrails) GenerateApprovalToken(action string) (string, error) {
	return GenerateApprovalToken(g.approvalSecret, action, time.Duration(g.approvalTTL)*time.Second)
}

// RequireReadOnlySQL blocks non-read queries when in read-only mode.
func (g *Guardrails) RequireReadOnlySQL(sql string) error {
	if g.allowExecute {
		return nil
	}
	if QueryIsReadOnly(sql) {
		return nil
	}
	return serr.New(serr.CodePermissionDenied, "write operation blocked in read-only mode", "use plan/execute tool with approval", nil)
}

// QueryIsReadOnly returns true if the statement appears to be read-only (best-effort classification).
func QueryIsReadOnly(sql string) bool {
	kw := firstKeyword(sql)
	if kw == "" {
		return true
	}
	kw = strings.ToLower(kw)
	switch kw {
	case "select", "show", "explain", "values":
		return true
	case "with":
		// WITH can also be used with INSERT/UPDATE; best-effort allow
		return true
	default:
		return false
	}
}

// firstKeyword strips leading comments/whitespace and returns the first token.
func firstKeyword(sql string) string {
	s := stripLeadingComments(sql)
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// split on whitespace or '(' or ';'
	for i, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '(' || r == ';' {
			return s[:i]
		}
	}
	return s
}

// stripLeadingComments removes leading SQL comments (-- or /* */) and whitespace.
func stripLeadingComments(sql string) string {
	s := sql
	for {
		s = strings.TrimLeft(s, "\t\n\r ")
		if strings.HasPrefix(s, "--") {
			if idx := strings.IndexAny(s, "\n\r"); idx >= 0 {
				s = s[idx:]
			} else {
				return ""
			}
			continue
		}
		if strings.HasPrefix(s, "/*") {
			if idx := strings.Index(s, "*/"); idx >= 0 {
				s = s[idx+2:]
				continue
			}
			return ""
		}
		return s
	}
}
