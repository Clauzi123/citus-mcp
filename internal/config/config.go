package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Mode string

const (
	ModeReadOnly Mode = "read_only"
	ModeAdmin    Mode = "admin"
)

type Config struct {
	CoordinatorDSN              string   `mapstructure:"coordinator_dsn"`
	WorkerDSNs                  []string `mapstructure:"worker_dsns"`
	ConnectTimeoutSeconds       int      `mapstructure:"connect_timeout_seconds"`
	StatementTimeoutMs          int      `mapstructure:"statement_timeout_ms"`
	AppName                     string   `mapstructure:"app_name"`
	Mode                        Mode     `mapstructure:"mode"`
	AllowExecute                bool     `mapstructure:"allow_execute"`
	ApprovalSecret              string   `mapstructure:"approval_secret"`
	MaxRows                     int      `mapstructure:"max_rows"`
	MaxTextBytes                int      `mapstructure:"max_text_bytes"`
	EnableCaching               bool     `mapstructure:"enable_caching"`
	CacheTTLSeconds             int      `mapstructure:"cache_ttl_seconds"`
	LogLevel                    string   `mapstructure:"log_level"`
	SnapshotAdvisorCollectBytes bool     `mapstructure:"snapshot_advisor_collect_bytes"`
}

func defaults(v *viper.Viper) {
	v.SetDefault("coordinator_dsn", "")
	v.SetDefault("worker_dsns", []string{})
	v.SetDefault("connect_timeout_seconds", 5)
	v.SetDefault("statement_timeout_ms", 30000)
	v.SetDefault("app_name", "citus-mcp")
	v.SetDefault("mode", string(ModeReadOnly))
	v.SetDefault("allow_execute", false)
	v.SetDefault("approval_secret", "")
	v.SetDefault("max_rows", 200)
	v.SetDefault("max_text_bytes", 200000)
	v.SetDefault("enable_caching", true)
	v.SetDefault("cache_ttl_seconds", 5)
	v.SetDefault("log_level", "info")
	v.SetDefault("snapshot_advisor_collect_bytes", true)
}

func Load() (Config, error) {
	v := viper.New()
	defaults(v)
	v.SetEnvPrefix("CITUS_MCP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Config file
	if cfgPath := os.Getenv("CITUS_MCP_CONFIG"); cfgPath != "" {
		v.SetConfigFile(cfgPath)
		if err := v.ReadInConfig(); err != nil {
			return Config{}, fmt.Errorf("read config file %s: %w", cfgPath, err)
		}
	}

	// Flags override
	fs := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	fs.String("coordinator-dsn", "", "Coordinator DSN (postgres://…)")
	fs.StringSlice("worker-dsn", []string{}, "Worker DSNs (repeatable)")
	fs.Int("connect-timeout-seconds", 5, "Connection timeout in seconds")
	fs.Int("statement-timeout-ms", 30000, "Statement timeout in milliseconds")
	fs.String("app-name", "citus-mcp", "Application name")
	fs.String("mode", string(ModeReadOnly), "Mode: read_only|admin")
	fs.Bool("allow-execute", false, "Allow execute tools")
	fs.String("approval-secret", "", "Approval secret (required if allow-execute)")
	fs.Int("max-rows", 200, "Maximum rows returned by tools")
	fs.Int("max-text-bytes", 200000, "Maximum text bytes returned by tools")
	fs.Bool("enable-caching", true, "Enable caching")
	fs.Int("cache-ttl-seconds", 5, "Cache TTL in seconds")
	fs.String("log-level", "info", "Log level")
	fs.Bool("snapshot-advisor-collect-bytes", true, "Collect bytes for snapshot advisor (may be heavy)")

	// pflag -> std flag compatibility
	_ = fs.Parse(os.Args[1:])
	_ = v.BindPFlags(fs)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}
	if err := validate(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validate(cfg Config) error {
	if cfg.CoordinatorDSN == "" {
		return errors.New("config: coordinator_dsn is required")
	}
	if cfg.Mode != ModeReadOnly && cfg.Mode != ModeAdmin {
		return fmt.Errorf("config: mode must be one of [%s,%s]", ModeReadOnly, ModeAdmin)
	}
	if cfg.AllowExecute && cfg.ApprovalSecret == "" {
		return errors.New("config: approval_secret is required when allow_execute=true")
	}
	if cfg.ConnectTimeoutSeconds <= 0 {
		return errors.New("config: connect_timeout_seconds must be > 0")
	}
	if cfg.StatementTimeoutMs <= 0 {
		return errors.New("config: statement_timeout_ms must be > 0")
	}
	if cfg.MaxRows <= 0 {
		return errors.New("config: max_rows must be > 0")
	}
	if cfg.MaxTextBytes <= 0 {
		return errors.New("config: max_text_bytes must be > 0")
	}
	return nil
}
