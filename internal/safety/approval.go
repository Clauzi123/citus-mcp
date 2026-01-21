package safety

import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/base64"
    "errors"
    "fmt"
    "strconv"
    "strings"
    "time"
)

func GenerateApprovalToken(secret, action string, ttl time.Duration) (string, error) {
    if secret == "" {
        return "", errors.New("approval secret is empty")
    }
    exp := time.Now().Add(ttl).Unix()
    msg := fmt.Sprintf("%s|%d", action, exp)
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write([]byte(msg))
    sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))
    return fmt.Sprintf("%s|%d|%s", action, exp, sig), nil
}

func ValidateApprovalToken(secret, action, token string) error {
    parts := strings.Split(token, "|")
    if len(parts) != 3 {
        return errors.New("invalid token format")
    }
    tAction, tExpStr, tSig := parts[0], parts[1], parts[2]
    if tAction != action {
        return errors.New("token action mismatch")
    }
    exp, err := strconv.ParseInt(tExpStr, 10, 64)
    if err != nil {
        return errors.New("invalid token expiry")
    }
    if time.Now().Unix() > exp {
        return errors.New("token expired")
    }
    msg := fmt.Sprintf("%s|%d", tAction, exp)
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write([]byte(msg))
    expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
    if !hmac.Equal([]byte(expected), []byte(tSig)) {
        return errors.New("invalid token signature")
    }
    return nil
}
