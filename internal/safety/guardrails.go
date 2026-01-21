package safety

import (
	"time"

	"citus-mcp/internal/config"
	serr "citus-mcp/internal/errors"
)

type Guardrails struct {
	allowExecute   bool
	approvalSecret string
	approvalTTL    int64 // seconds
}

func NewGuardrails(cfg config.Config) *Guardrails {
	ttl := int64(5 * 60)
	return &Guardrails{
		allowExecute:   cfg.AllowExecute,
		approvalSecret: cfg.ApprovalSecret,
		approvalTTL:    ttl,
	}
}

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

func (g *Guardrails) RequireReadOnly(action string) error {
	// When not allowExecute, this ensures we keep read-only. For now, no-op.
	return nil
}

func (g *Guardrails) GenerateApprovalToken(action string) (string, error) {
	return GenerateApprovalToken(g.approvalSecret, action, time.Duration(g.approvalTTL)*time.Second)
}
