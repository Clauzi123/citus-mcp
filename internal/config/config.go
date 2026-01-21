package config

import (
    "fmt"
    "strings"
    "time"

    "github.com/spf13/viper"
)

type DBConfig struct {
    URL         string        `mapstructure:"url"`
    MaxConns    int32         `mapstructure:"max_conns"`
    MinConns    int32         `mapstructure:"min_conns"`
    HealthCheck time.Duration `mapstructure:"health_check_interval"`
}

type ApprovalConfig struct {
    Secret string        `mapstructure:"secret"`
    TTL    time.Duration `mapstructure:"ttl"`
}

type LoggingConfig struct {
    Level    string `mapstructure:"level"`
    Encoding string `mapstructure:"encoding"`
}

type CacheConfig struct {
    TTL time.Duration `mapstructure:"ttl"`
}

type LimitsConfig struct {
    DefaultLimit int `mapstructure:"default_limit"`
    MaxLimit     int `mapstructure:"max_limit"`
}

type RateLimitConfig struct {
    RequestsPerMinute int `mapstructure:"requests_per_minute"`
}

type Config struct {
    AllowExecute bool            `mapstructure:"allow_execute"`
    DB           DBConfig        `mapstructure:"db"`
    Approval     ApprovalConfig  `mapstructure:"approval"`
    Logging      LoggingConfig   `mapstructure:"logging"`
    Cache        CacheConfig     `mapstructure:"cache"`
    Limits       LimitsConfig    `mapstructure:"limits"`
    RateLimit    RateLimitConfig `mapstructure:"rate_limit"`
}

func Load() (Config, error) {
    v := viper.New()
    v.SetEnvPrefix("CITUS_MCP")
    v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
    v.AutomaticEnv()

    v.SetDefault("db.url", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
    v.SetDefault("db.max_conns", 10)
    v.SetDefault("db.min_conns", 1)
    v.SetDefault("db.health_check_interval", "30s")

    v.SetDefault("allow_execute", false)
    v.SetDefault("approval.secret", "")
    v.SetDefault("approval.ttl", "5m")

    v.SetDefault("logging.level", "info")
    v.SetDefault("logging.encoding", "console")

    v.SetDefault("cache.ttl", "30s")
    v.SetDefault("limits.default_limit", 100)
    v.SetDefault("limits.max_limit", 1000)
    v.SetDefault("rate_limit.requests_per_minute", 120)

    // Optional config file
    v.SetConfigName("citus-mcp")
    v.SetConfigType("yaml")
    v.AddConfigPath(".")
    v.AddConfigPath("/etc/citus-mcp")

    if err := v.ReadInConfig(); err != nil {
        // ignore not found
        if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
            return Config{}, fmt.Errorf("read config: %w", err)
        }
    }

    var cfg Config
    if err := v.Unmarshal(&cfg); err != nil {
        return Config{}, fmt.Errorf("unmarshal config: %w", err)
    }
    return cfg, nil
}
