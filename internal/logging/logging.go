package logging

import (
	"fmt"

	"citus-mcp/internal/safety"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger constructs a zap logger with the provided level (default info).
// It uses console encoding and ISO8601 timestamps.
func NewLogger(level string) (*zap.Logger, error) {
	zcfg := zap.NewProductionConfig()
	zcfg.Encoding = "console"
	lvl := level
	if lvl == "" {
		lvl = "info"
	}
	l, err := zapcore.ParseLevel(lvl)
	if err != nil {
		return nil, fmt.Errorf("parse log level: %w", err)
	}
	zcfg.Level = zap.NewAtomicLevelAt(l)
	zcfg.EncoderConfig.TimeKey = "ts"
	zcfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zcfg.EncoderConfig.CallerKey = "caller"
	return zcfg.Build()
}

// Fields bundles common structured fields used across the service.
type Fields struct {
	Component string
	ToolName  string
	RequestID string
	SessionID string
}

// WithFields attaches standard fields to the logger.
func WithFields(logger *zap.Logger, f Fields) *zap.Logger {
	fields := make([]zap.Field, 0, 4)
	if f.Component != "" {
		fields = append(fields, zap.String("component", f.Component))
	}
	if f.ToolName != "" {
		fields = append(fields, zap.String("tool_name", f.ToolName))
	}
	if f.RequestID != "" {
		fields = append(fields, zap.String("request_id", f.RequestID))
	}
	if f.SessionID != "" {
		fields = append(fields, zap.String("session_id", f.SessionID))
	}
	return logger.With(fields...)
}

// WithComponent attaches a component field.
func WithComponent(logger *zap.Logger, component string) *zap.Logger {
	if component == "" {
		return logger
	}
	return logger.With(zap.String("component", component))
}

// WithTool attaches a tool_name field.
func WithTool(logger *zap.Logger, tool string) *zap.Logger {
	if tool == "" {
		return logger
	}
	return logger.With(zap.String("tool_name", tool))
}

// WithRequest attaches request/session IDs.
func WithRequest(logger *zap.Logger, requestID, sessionID string) *zap.Logger {
	fields := make([]zap.Field, 0, 2)
	if requestID != "" {
		fields = append(fields, zap.String("request_id", requestID))
	}
	if sessionID != "" {
		fields = append(fields, zap.String("session_id", sessionID))
	}
	return logger.With(fields...)
}

// RedactDSN safely redacts DSNs by masking user/password.
func RedactDSN(dsn string) string { return safety.RedactDSN(dsn) }

// FieldDSN returns a zap field with a redacted DSN.
func FieldDSN(key, dsn string) zap.Field {
	return zap.String(key, RedactDSN(dsn))
}

// FieldSecret masks secret values.
func FieldSecret(key string) zap.Field {
	return zap.String(key, "***")
}
