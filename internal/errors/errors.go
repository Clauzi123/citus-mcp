package errors

import "fmt"

type Code string

const (
    CodeInternal      Code = "internal_error"
    CodeInvalidInput  Code = "invalid_input"
    CodeUnauthorized  Code = "unauthorized"
    CodeForbidden     Code = "forbidden"
    CodeNotFound      Code = "not_found"
    CodeUnavailable   Code = "unavailable"
    CodeNotImplemented Code = "not_implemented"
)

type MCPError struct {
    Code    Code             `json:"code"`
    Message string           `json:"message"`
    Hint    string           `json:"hint,omitempty"`
    Details map[string]any   `json:"details,omitempty"`
}

func (e *MCPError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func New(code Code, msg string, hint string, details map[string]any) *MCPError {
    return &MCPError{Code: code, Message: msg, Hint: hint, Details: details}
}

func Wrap(err error, code Code, msg string) *MCPError {
    return &MCPError{Code: code, Message: msg, Details: map[string]any{"cause": err.Error()}}
}
