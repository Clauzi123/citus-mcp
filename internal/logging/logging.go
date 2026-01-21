package logging

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

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
