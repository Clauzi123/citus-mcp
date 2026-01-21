package errors

import "testing"

func TestNewError(t *testing.T) {
    e := New(CodeInvalidInput, "msg", "hint", nil)
    if e.Code != CodeInvalidInput {
        t.Fatalf("expected code %s", CodeInvalidInput)
    }
}
