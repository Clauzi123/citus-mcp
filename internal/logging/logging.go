package logging

import (
    "fmt"

    "citus-mcp/internal/config"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
)

func NewLogger(cfg config.LoggingConfig) (*zap.Logger, error) {
    zcfg := zap.NewProductionConfig()
    if cfg.Encoding != "" {
        zcfg.Encoding = cfg.Encoding
    } else {
        zcfg.Encoding = "console"
    }
    level, err := zapcore.ParseLevel(cfg.Level)
    if err != nil {
        return nil, fmt.Errorf("parse log level: %w", err)
    }
    zcfg.Level = zap.NewAtomicLevelAt(level)
    zcfg.EncoderConfig.TimeKey = "ts"
    zcfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
    zcfg.EncoderConfig.CallerKey = "caller"
    return zcfg.Build()
}
