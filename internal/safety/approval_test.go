package safety

import (
	"testing"
)

func TestApprovalTokenLifecycle(t *testing.T) {
	token, err := GenerateApprovalToken("action", 60, "secret")
	if err != nil {
		t.Fatalf("GenerateApprovalToken error: %v", err)
	}
	if err := ValidateApprovalToken(token, "action", "secret"); err != nil {
		t.Fatalf("ValidateApprovalToken error: %v", err)
	}
}
