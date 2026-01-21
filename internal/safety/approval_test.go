package safety

import (
    "testing"
    "time"
)

func TestApprovalTokenLifecycle(t *testing.T) {
    token, err := GenerateApprovalToken("secret", "action", time.Minute)
    if err != nil {
        t.Fatalf("GenerateApprovalToken error: %v", err)
    }
    if err := ValidateApprovalToken("secret", "action", token); err != nil {
        t.Fatalf("ValidateApprovalToken error: %v", err)
    }
}
