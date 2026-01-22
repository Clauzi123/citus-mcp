// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Custom error types and error codes for MCP responses.

package errors

import (
	"fmt"
	"strings"
)

type ErrorCode string

const (
	CodeNotCitus          ErrorCode = "NOT_CITUS"
	CodePermissionDenied  ErrorCode = "PERMISSION_DENIED"
	CodeInvalidInput      ErrorCode = "INVALID_INPUT"
	CodeTimeout           ErrorCode = "TIMEOUT"
	CodeWorkerUnreachable ErrorCode = "WORKER_UNREACHABLE"
	CodeExecuteDisabled   ErrorCode = "EXECUTE_DISABLED"
	CodeApprovalRequired  ErrorCode = "APPROVAL_REQUIRED"
	CodeCapabilityMissing ErrorCode = "CAPABILITY_MISSING"
	CodeInternalError     ErrorCode = "INTERNAL_ERROR"
)

type CitusMCPError struct {
	Code    ErrorCode      `json:"code"`
	Message string         `json:"message"`
	Hint    string         `json:"hint,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *CitusMCPError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func New(code ErrorCode, msg, hint string, details map[string]any) *CitusMCPError {
	return &CitusMCPError{Code: code, Message: msg, Hint: hint, Details: sanitize(details)}
}

func NewInvalidInput(msg, hint string, details map[string]any) *CitusMCPError {
	return New(CodeInvalidInput, msg, hint, details)
}

func NewPermissionDenied(msg, hint string) *CitusMCPError {
	return New(CodePermissionDenied, msg, hint, nil)
}

func NewExecuteDisabled() *CitusMCPError {
	return New(CodeExecuteDisabled, "execute mode disabled", "set allow_execute=true to enable", nil)
}

func NewApprovalRequired(action string) *CitusMCPError {
	return New(CodeApprovalRequired, "approval token required", "provide short-lived approval token", map[string]any{"action": action})
}

func NewCapabilityMissing(cap string) *CitusMCPError {
	return New(CodeCapabilityMissing, "capability missing", "upgrade Citus or enable UDF", map[string]any{"capability": cap})
}

func NewTimeout(msg string) *CitusMCPError {
	return New(CodeTimeout, msg, "retry or increase timeout", nil)
}

func NewWorkerUnreachable(worker string) *CitusMCPError {
	return New(CodeWorkerUnreachable, "worker unreachable", "check worker status", map[string]any{"worker": worker})
}

func NewInternal(err error) *CitusMCPError {
	if err == nil {
		return New(CodeInternalError, "internal error", "see logs", nil)
	}
	return New(CodeInternalError, "internal error", "see logs", map[string]any{"cause": scrub(err.Error())})
}

// ToToolError converts any error to a CitusMCPError;
// unknown errors are wrapped as internal error with scrubbed message.
func ToToolError(err error) *CitusMCPError {
	if err == nil {
		return nil
	}
	if me, ok := err.(*CitusMCPError); ok {
		return me
	}
	return NewInternal(err)
}

func sanitize(details map[string]any) map[string]any {
	if details == nil {
		return nil
	}
	out := make(map[string]any, len(details))
	for k, v := range details {
		out[k] = scrub(fmt.Sprint(v))
	}
	return out
}

// scrub best-effort masks secrets/DSNs by replacing common patterns.
func scrub(s string) string {
	// lightweight scrub: do not leak raw DSNs or secrets
	replacements := []struct{ find, repl string }{
		{"postgres://", "postgres://***:***@"},
		{"postgresql://", "postgresql://***:***@"},
		{"password=", "password=***"},
		{"pwd=", "pwd=***"},
	}
	out := s
	for _, r := range replacements {
		out = strings.ReplaceAll(out, r.find, r.repl)
	}
	return out
}
