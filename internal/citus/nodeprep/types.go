// Package nodeprep provides pre-flight checks for adding nodes to a Citus cluster.
package nodeprep

// CheckStatus represents the result of a single check.
type CheckStatus string

const (
	StatusPassed  CheckStatus = "passed"
	StatusFailed  CheckStatus = "failed"
	StatusWarning CheckStatus = "warning"
	StatusSkipped CheckStatus = "skipped"
)

// Category groups related checks.
type Category string

const (
	CategoryConnection Category = "connection"
	CategoryPostgres   Category = "postgres"
	CategoryCitus      Category = "citus"
	CategoryExtensions Category = "extensions"
	CategorySchema     Category = "schema"
	CategorySecurity   Category = "security"
)

// CommandType for fix scripts.
type CommandType string

const (
	CommandTypeSQL    CommandType = "sql"
	CommandTypeBash   CommandType = "bash"
	CommandTypeConfig CommandType = "config"
)

// Target for fix commands.
type Target string

const (
	TargetNewNode     Target = "new_node"
	TargetCoordinator Target = "coordinator"
	TargetBoth        Target = "both"
)

// Input for the node preparation advisor.
type Input struct {
	Host             string `json:"host"`
	Port             int    `json:"port"`
	Database         string `json:"database"`
	User             string `json:"user,omitempty"`
	Password         string `json:"password,omitempty"`
	ConnectionString string `json:"connection_string,omitempty"`
	GenerateScript   bool   `json:"generate_script"`
	SSLMode          string `json:"sslmode,omitempty"`
}

// Output from the node preparation advisor.
type Output struct {
	Ready                    bool              `json:"ready"`
	Summary                  Summary           `json:"summary"`
	Checks                   []CheckResult     `json:"checks"`
	PreparationScript        *PrepScript       `json:"preparation_script,omitempty"`
	EstimatedIssuesPrevented []string          `json:"estimated_issues_prevented,omitempty"`
	ConnectionError          string            `json:"connection_error,omitempty"`
	Warnings                 []string          `json:"warnings,omitempty"`
}

// Summary of check results.
type Summary struct {
	Passed   int `json:"passed"`
	Failed   int `json:"failed"`
	Warnings int `json:"warnings"`
	Skipped  int `json:"skipped"`
	Total    int `json:"total"`
}

// CheckResult represents the result of a single prerequisite check.
type CheckResult struct {
	Category         Category    `json:"category"`
	Check            string      `json:"check"`
	Name             string      `json:"name"`
	Status           CheckStatus `json:"status"`
	Message          string      `json:"message"`
	CoordinatorValue string      `json:"coordinator_value,omitempty"`
	NewNodeValue     string      `json:"new_node_value,omitempty"`
	Fix              *Fix        `json:"fix,omitempty"`
}

// Fix provides remediation steps for a failed check.
type Fix struct {
	Description string    `json:"description"`
	Commands    []Command `json:"commands,omitempty"`
}

// Command is a single remediation command.
type Command struct {
	Type    CommandType `json:"type"`
	Target  Target      `json:"target"`
	Command string      `json:"command"`
	Note    string      `json:"note,omitempty"`
}

// PrepScript contains all commands needed to prepare a node.
type PrepScript struct {
	BashCommands    []ScriptCommand `json:"bash_commands,omitempty"`
	SQLCommands     []ScriptCommand `json:"sql_commands,omitempty"`
	ConfigChanges   []ConfigChange  `json:"config_changes,omitempty"`
	PostAddCommands []ScriptCommand `json:"post_add_commands,omitempty"`
}

// ScriptCommand is a command in the preparation script.
type ScriptCommand struct {
	Description string `json:"description"`
	Command     string `json:"command"`
	Target      Target `json:"target"`
}

// ConfigChange represents a configuration file change.
type ConfigChange struct {
	File        string `json:"file"`
	Parameter   string `json:"parameter"`
	Value       string `json:"value"`
	Description string `json:"description"`
}

// Extension represents a PostgreSQL extension.
type Extension struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Schema represents a database schema.
type Schema struct {
	Name  string `json:"name"`
	Owner string `json:"owner,omitempty"`
}

// UserType represents a user-defined type.
type UserType struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Type   string `json:"type"` // composite, enum, domain, range
}

// UserFunction represents a user-defined function.
type UserFunction struct {
	Schema    string `json:"schema"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Role represents a database role.
type Role struct {
	Name        string `json:"name"`
	IsSuperuser bool   `json:"is_superuser"`
	CanLogin    bool   `json:"can_login"`
	CanCreateDB bool   `json:"can_create_db"`
}

// PostgresConfig holds PostgreSQL configuration values.
type PostgresConfig struct {
	ServerVersion           int    `json:"server_version"`
	ServerVersionString     string `json:"server_version_string"`
	SharedPreloadLibraries  string `json:"shared_preload_libraries"`
	MaxConnections          int    `json:"max_connections"`
	WalLevel                string `json:"wal_level"`
	MaxPreparedTransactions int    `json:"max_prepared_transactions"`
	ListenAddresses         string `json:"listen_addresses"`
}
