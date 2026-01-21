package errors

import (
	"fmt"
	"testing"
)

func TestToToolErrorWrapsUnknown(t *testing.T) {
	err := ToToolError(fmt.Errorf("boom: password=secret"))
	if err.Code != CodeInternalError {
		t.Fatalf("expected internal error code, got %s", err.Code)
	}
	if err.Details["cause"] == "boom: password=secret" {
		t.Fatalf("expected scrubbed cause, got %v", err.Details["cause"])
	}
}

func TestNewInvalidInput(t *testing.T) {
	e := NewInvalidInput("bad", "hint", map[string]any{"field": "x"})
	if e.Code != CodeInvalidInput {
		t.Fatalf("expected %s, got %s", CodeInvalidInput, e.Code)
	}
}
