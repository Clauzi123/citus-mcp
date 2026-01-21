package safety

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type approvalPayload struct {
	Action    string `json:"action"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	Nonce     string `json:"nonce"`
}

// GenerateApprovalToken creates a signed token for an action.
// Token format: base64(payload).base64(hmac(payload, secret))
func GenerateApprovalToken(action string, ttlSeconds int, secret string) (string, error) {
	if secret == "" {
		return "", errors.New("approval secret is empty")
	}
	if ttlSeconds <= 0 {
		ttlSeconds = 300
	}
	it := time.Now().Unix()
	exp := time.Now().Add(time.Duration(ttlSeconds) * time.Second).Unix()
	nonce, err := randomNonce()
	if err != nil {
		return "", err
	}
	payload := approvalPayload{Action: action, IssuedAt: it, ExpiresAt: exp, Nonce: nonce}
	plain, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	pb64 := base64.StdEncoding.EncodeToString(plain)
	sig := sign(secret, plain)
	tok := pb64 + "." + base64.StdEncoding.EncodeToString(sig)
	return tok, nil
}

// ValidateApprovalToken validates token for action.
// token is base64(payload).base64(hmac)
func ValidateApprovalToken(token string, action string, secret string) error {
	pb64, sigb64, ok := splitToken(token)
	if !ok {
		return errors.New("invalid token format")
	}
	plain, err := base64.StdEncoding.DecodeString(pb64)
	if err != nil {
		return errors.New("invalid token payload")
	}
	if !verify(secret, plain, sigb64) {
		return errors.New("invalid token signature")
	}
	var payload approvalPayload
	if err := json.Unmarshal(plain, &payload); err != nil {
		return errors.New("invalid token payload json")
	}
	if payload.Action != action {
		return errors.New("token action mismatch")
	}
	now := time.Now().Unix()
	if now > payload.ExpiresAt {
		return errors.New("token expired")
	}
	return nil
}

func splitToken(tok string) (string, string, bool) {
	parts := strings.Split(tok, ".")
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func sign(secret string, msg []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(msg)
	return mac.Sum(nil)
}

func verify(secret string, msg []byte, sigb64 string) bool {
	sig, err := base64.StdEncoding.DecodeString(sigb64)
	if err != nil {
		return false
	}
	expected := sign(secret, msg)
	return hmac.Equal(sig, expected)
}

func randomNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
