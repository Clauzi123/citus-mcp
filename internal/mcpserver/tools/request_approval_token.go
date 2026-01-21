package tools

import (
	"context"
	"time"

	"citus-mcp/internal/config"
	serr "citus-mcp/internal/errors"
	"citus-mcp/internal/safety"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RequestApprovalTokenInput input for citus.request_approval_token.
type RequestApprovalTokenInput struct {
	Action     string `json:"action" jsonschema:"required"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

// RequestApprovalTokenOutput output.
type RequestApprovalTokenOutput struct {
	Token     string     `json:"token"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func requestApprovalTokenTool(ctx context.Context, deps Dependencies, input RequestApprovalTokenInput) (*mcp.CallToolResult, RequestApprovalTokenOutput, error) {
	if deps.Config.Mode != config.ModeAdmin {
		return callError(serr.CodePermissionDenied, "token issuance disabled", "set mode=admin"), RequestApprovalTokenOutput{}, nil
	}
	if input.Action == "" {
		return callError(serr.CodeInvalidInput, "action required", ""), RequestApprovalTokenOutput{}, nil
	}
	ttl := input.TTLSeconds
	if ttl == 0 {
		ttl = 300
	}
	tok, err := safety.GenerateApprovalToken(input.Action, ttl, deps.Config.ApprovalSecret)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "failed to generate token"), RequestApprovalTokenOutput{}, nil
	}
	exp := time.Now().Add(time.Duration(ttl) * time.Second)
	return nil, RequestApprovalTokenOutput{Token: tok, ExpiresAt: &exp}, nil
}
