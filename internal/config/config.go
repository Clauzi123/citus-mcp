package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	CoordinatorUser             string   `mapstructure:"coordinator_user"`
	CoordinatorPassword         string   `mapstructure:"coordinator_password"`
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
	v.SetDefault("coordinator_user", "")
	v.SetDefault("coordinator_password", "")
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

	// Flags override (parse early to locate config file)
	fs := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	var cfgPathFlag string
	fs.StringVarP(&cfgPathFlag, "config", "c", "", "Config file path (yaml|json|toml)")
	fs.String("coordinator-dsn", "", "Coordinator DSN (postgres://…)")
	fs.String("dsn", "", "Coordinator DSN (alias for coordinator-dsn)")
	fs.String("coordinator-user", "", "Coordinator user (optional override)")
	fs.String("coordinator-password", "", "Coordinator password (optional override)")
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

	// Config file resolution
	cfgPath := cfgPathFlag
	if cfgPath == "" {
		cfgPath = os.Getenv("CITUS_MCP_CONFIG")
	}
	if cfgPath != "" {
		if err := readConfigFile(v, cfgPath); err != nil {
			return Config{}, err
		}
	} else {
		_ = readDefaultConfig(v) // best-effort
	}

	// Flags override config
	_ = v.BindPFlags(fs)

	// positional DSN fallback
	if v.GetString("coordinator_dsn") == "" {
		if dsn := v.GetString("dsn"); dsn != "" {
			v.Set("coordinator_dsn", dsn)
		} else if args := fs.Args(); len(args) > 0 && args[0] != "" {
			v.Set("coordinator_dsn", args[0])
		}
	}

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

func readConfigFile(v *viper.Viper, path string) error {
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read config file %s: %w", path, err)
	}
	return nil
}

func readDefaultConfig(v *viper.Viper) error {
	paths := defaultConfigCandidates()
	exts := []string{"yaml", "yml", "json", "toml"}
	for _, base := range paths {
		for _, ext := range exts {
			candidate := base + "." + ext
			if _, err := os.Stat(candidate); err == nil {
				v.SetConfigFile(candidate)
				if err := v.ReadInConfig(); err != nil {
					return fmt.Errorf("read default config %s: %w", candidate, err)
				}
				return nil
			}
		}
	}
	return nil
}

func defaultConfigCandidates() []string {
	var out []string
	cwd, _ := os.Getwd()
	if cwd != "" {
		out = append(out,
			filepath.Join(cwd, "citus-mcp"),
			filepath.Join(cwd, "config", "citus-mcp"),
		)
	}
	xdg := os.Getenv("XDG_CONFIG_HOME")
	if xdg == "" {
		home, _ := os.UserHomeDir()
		if home != "" {
			xdg = filepath.Join(home, ".config")
		}
	}
	if xdg != "" {
		out = append(out, filepath.Join(xdg, "citus-mcp", "config"))
	}
	return out
}
