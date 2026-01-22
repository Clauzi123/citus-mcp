<div align="center">

<img src="docs/images/logo.png" alt="citus-mcp logo" width="180"/>

# Citus MCP Server

**An AI-powered MCP server for managing Citus distributed PostgreSQL clusters**

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Citus](https://img.shields.io/badge/Citus-12.x--14.x-336791?logo=postgresql)](https://www.citusdata.com)

[Quick Start](#-quick-start) •
[Features](#-features) •
[Installation](#-installation) •
[Configuration](#-configuration) •
[Tools Reference](#-tools-reference) •
[Examples](#-usage-examples)

</div>

---

## 📖 What is Citus MCP?

Citus MCP is a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that enables AI assistants like GitHub Copilot to interact with your Citus distributed PostgreSQL cluster. It provides:

| Feature | Description |
|---------|-------------|
| 🔍 **Read-only Inspection** | Safely explore distributed tables, shards, nodes, and colocation groups |
| 🤖 **Intelligent Advisors** | Get recommendations for rebalancing, skew analysis, configuration, and operational health |
| 🛡️ **Guarded Operations** | Execute dangerous operations only with explicit approval tokens |
| 📊 **Real-time Monitoring** | View cluster activity, locks, background jobs, and hot shards |

### How It Works

```
┌─────────────────┐                      ┌──────────────┐                ┌─────────────────┐
│  GitHub Copilot │     MCP Protocol     │  citus-mcp   │      SQL       │  Citus Cluster  │
│  (VS Code/CLI)  │ <──────────────────> │    server    │ <────────────> │  (Coordinator)  │
└─────────────────┘      stdio/SSE       └──────────────┘                └─────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- **Go 1.23+** (for building from source)
- **Citus 12.x–14.x** cluster with coordinator access
- **GitHub Copilot** with MCP support (VS Code or CLI)

### 1. Build the Server

```bash
git clone https://github.com/citusdata/citus-mcp.git
cd citus-mcp
make build
# Binary created at ./bin/citus-mcp
```

Or using Go directly:

```bash
go build -o bin/citus-mcp ./cmd/citus-mcp
```

### 2. Configure Your Connection

Create a configuration file at `~/.config/citus-mcp/config.yaml`:

```yaml
# Minimum required configuration
coordinator_dsn: postgres://username:password@localhost:5432/mydb?sslmode=disable
```

Or set the environment variable:

```bash
export CITUS_MCP_COORDINATOR_DSN="postgres://username:password@localhost:5432/mydb?sslmode=disable"
```

### 3. Set Up VS Code

Create `.vscode/mcp.json` in your workspace (or `mcp.json` at the project root):

```json
{
  "mcpServers": {
    "citus-mcp": {
      "command": "/path/to/citus-mcp/bin/citus-mcp",
      "args": [],
      "env": {
        "CITUS_MCP_COORDINATOR_DSN": "postgres://username:password@localhost:5432/mydb?sslmode=disable"
      }
    }
  }
}
```

### 4. Test the Connection

In VS Code Copilot Chat, type:

```
@citus-mcp ping
```

You should see a "pong" response confirming the connection works.

---

## ✨ Features

### 🔍 Cluster Inspection (Read-Only)

| Tool | Description |
|------|-------------|
| `citus_cluster_summary` | Overview of coordinator, workers, table counts, and configuration health |
| `list_nodes` | List all coordinator and worker nodes |
| `list_distributed_tables` | List distributed and reference tables |
| `citus_list_distributed_tables` | Paginated list of distributed tables with filters |
| `citus_list_reference_tables` | Paginated list of reference tables |
| `list_shards` | List shards with placements and sizes |
| `citus_table_inspector` | Deep dive into table metadata, indexes, and statistics |
| `citus_colocation_inspector` | Analyze colocation groups and colocated tables |

### 📊 Monitoring & Analysis

| Tool | Description |
|------|-------------|
| `citus_activity` | Cluster-wide active queries and connections |
| `citus_lock_inspector` | View lock waits and blocking queries |
| `citus_job_inspector` | Background job progress (rebalance, copy) |
| `citus_shard_heatmap` | Hot shards and node distribution |
| `citus_shard_skew_report` | Data skew analysis per node |
| `citus_explain_query` | EXPLAIN distributed queries |

### 🤖 Intelligent Advisors

| Tool | Description |
|------|-------------|
| `citus_advisor` | SRE + performance advisor with actionable recommendations |
| `citus_config_advisor` | Comprehensive Citus and PostgreSQL configuration analysis |
| `citus_snapshot_source_advisor` | Recommend source node for snapshot-based scaling |
| `citus_validate_rebalance_prereqs` | Check if table is ready for rebalancing |
| `citus_metadata_health` | Detect metadata corruption and inconsistencies with fix suggestions |
| `citus_node_prepare_advisor` | Pre-flight checks and preparation script for adding new nodes |

### ⚡ Execute Operations (Requires Approval)

| Tool | Description |
|------|-------------|
| `citus_rebalance_plan` | Preview rebalance operations |
| `citus_rebalance_execute` | Start cluster rebalance |
| `citus_rebalance_status` | Monitor rebalance progress |
| `citus_move_shard_plan` | Preview shard move |
| `citus_move_shard_execute` | Move a shard to different node |
| `citus_request_approval_token` | Request time-limited approval token |

---

## 📦 Installation

### Option 1: Build from Source

```bash
# Clone the repository
git clone https://github.com/citusdata/citus-mcp.git
cd citus-mcp

# Build using Make
make build

# Or build directly with Go
go build -o bin/citus-mcp ./cmd/citus-mcp

# (Optional) Install to your PATH
sudo cp bin/citus-mcp /usr/local/bin/
```

### Option 2: Go Install

```bash
go install github.com/citusdata/citus-mcp/cmd/citus-mcp@latest
```

### Verify Installation

```bash
citus-mcp --help
```

---

## ⚙️ Configuration

### Connection String (DSN)

The most important configuration is the PostgreSQL connection string to your Citus coordinator:

```
postgres://[user]:[password]@[host]:[port]/[database]?sslmode=[mode]
```

**Examples:**

```bash
# Local development (no SSL)
postgres://postgres:secret@localhost:5432/mydb?sslmode=disable

# Production with SSL
postgres://admin:secret@citus-coord.example.com:5432/production?sslmode=require

# With specific schema
postgres://user:pass@host:5432/db?sslmode=require&search_path=myschema
```

### Configuration Methods

Configuration can be provided via (in order of precedence):

1. **Command-line flags**
2. **Environment variables**
3. **Configuration file**

#### Method 1: Environment Variables

```bash
# Required
export CITUS_MCP_COORDINATOR_DSN="postgres://user:pass@localhost:5432/mydb?sslmode=disable"

# Optional
export CITUS_MCP_MODE="read_only"           # read_only (default) or admin
export CITUS_MCP_ALLOW_EXECUTE="false"      # Enable execute operations
export CITUS_MCP_APPROVAL_SECRET="secret"   # Required if allow_execute=true
export CITUS_MCP_LOG_LEVEL="info"           # debug, info, warn, error
```

#### Method 2: Configuration File

Create `~/.config/citus-mcp/config.yaml`:

```yaml
# ===========================================
# Citus MCP Server Configuration
# ===========================================

# Database Connection (REQUIRED)
# -----------------------------
coordinator_dsn: postgres://user:password@localhost:5432/mydb?sslmode=disable

# Optional: Override credentials from DSN
# coordinator_user: myuser
# coordinator_password: mypassword

# Optional: Direct worker connections (comma-separated)
# worker_dsns: postgres://user:pass@worker1:5432/db,postgres://user:pass@worker2:5432/db

# Server Mode
# -----------
# read_only: Only inspection tools available (default, safest)
# admin: All tools available including execute operations
mode: read_only

# Execute Operations (only if mode=admin)
# ---------------------------------------
allow_execute: false
# approval_secret: your-secret-key  # Required if allow_execute=true

# Performance Settings
# --------------------
cache_ttl_seconds: 5          # Cache duration for metadata queries
enable_caching: true          # Set to false to disable caching
max_rows: 200                 # Maximum rows returned per query
max_text_bytes: 200000        # Maximum text size in responses

# Timeouts
# --------
connect_timeout_seconds: 10   # Connection timeout
statement_timeout_ms: 30000   # Query timeout (30 seconds)

# Logging
# -------
log_level: info               # debug, info, warn, error

# Transport (NEW)
# ---------------
# stdio: Standard input/output (default, for VS Code/CLI integration)
# sse: Server-Sent Events over HTTP (for remote/network access)
# streamable: Streamable HTTP transport (for remote/network access)
transport: stdio

# HTTP Settings (only used when transport is sse or streamable)
# http_addr: 127.0.0.1        # Listen address (use 0.0.0.0 for all interfaces)
# http_port: 8080             # Listen port
# http_path: /mcp             # Endpoint path
# sse_keepalive_seconds: 30   # SSE keepalive interval
```

#### Method 3: Command-Line Flags

```bash
# Using flags (note: use underscores in flag names)
bin/citus-mcp --coordinator_dsn "postgres://..." --mode read_only

# Using positional argument for DSN
bin/citus-mcp "postgres://user:pass@localhost:5432/mydb?sslmode=disable"

# Specify config file
bin/citus-mcp --config /path/to/config.yaml

# Start with SSE transport
bin/citus-mcp --transport sse --http_port 8080 --coordinator_dsn "postgres://..."
```

### Configuration File Locations

The server searches for configuration files in this order:

1. `--config` / `-c` flag
2. `CITUS_MCP_CONFIG` environment variable
3. `$XDG_CONFIG_HOME/citus-mcp/config.yaml`
4. `~/.config/citus-mcp/config.yaml`
5. `./citus-mcp.yaml` (current directory)

Supported formats: YAML, JSON, TOML

---

## 🌐 Transport Options

Citus MCP supports three transport modes for different deployment scenarios:

### 1. Stdio Transport (Default)

Standard input/output transport — the server communicates via stdin/stdout. This is the default and is used for direct integration with VS Code and GitHub Copilot CLI.

```bash
# Default - stdio transport
bin/citus-mcp --coordinator_dsn "postgres://..."

# Explicit
bin/citus-mcp --transport stdio --coordinator_dsn "postgres://..."
```

**Use cases:**
- VS Code Copilot Chat integration
- GitHub Copilot CLI
- Local development

### 2. SSE Transport (Server-Sent Events)

HTTP-based transport using Server-Sent Events. The server runs as an HTTP daemon that clients can connect to remotely.

```bash
# Start server on HTTP with SSE
bin/citus-mcp --transport sse --http_addr 0.0.0.0 --http_port 8080 --coordinator_dsn "postgres://..."

# Or via environment variables
export CITUS_MCP_TRANSPORT=sse
export CITUS_MCP_HTTP_ADDR=0.0.0.0
export CITUS_MCP_HTTP_PORT=8080
export CITUS_MCP_COORDINATOR_DSN="postgres://..."
bin/citus-mcp
```

**Endpoints:**
- `GET /mcp` - Establish SSE connection
- `POST /mcp/session/{id}` - Send messages to session
- `GET /health` - Health check

**Use cases:**
- Remote MCP server deployment
- Docker/Kubernetes deployments
- Shared server for multiple clients
- Network-accessible MCP services

### 3. Streamable HTTP Transport

Modern HTTP transport with streaming support. Recommended for new deployments.

```bash
# Start server with streamable HTTP transport
bin/citus-mcp --transport streamable --http_addr 0.0.0.0 --http_port 8080 --coordinator_dsn "postgres://..."
```

**Endpoints:**
- `POST /mcp` - Handle MCP requests with streaming responses
- `GET /health` - Health check

**Use cases:**
- Same as SSE, with better streaming support
- Environments where SSE is not ideal

### Docker Deployment Example

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o citus-mcp ./cmd/citus-mcp

FROM alpine:latest
COPY --from=builder /app/citus-mcp /usr/local/bin/
EXPOSE 8080
CMD ["citus-mcp", "--transport", "sse", "--http-addr", "0.0.0.0", "--http-port", "8080"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  citus-mcp:
    build: .
    ports:
      - "8080:8080"
    environment:
      CITUS_MCP_TRANSPORT: sse
      CITUS_MCP_HTTP_ADDR: 0.0.0.0
      CITUS_MCP_HTTP_PORT: 8080
      CITUS_MCP_COORDINATOR_DSN: postgres://user:pass@citus-coordinator:5432/mydb?sslmode=disable
```

### Connecting to Remote Server

For SSE/Streamable transports, configure your MCP client to connect via HTTP:

```json
{
  "mcpServers": {
    "citus-mcp": {
      "type": "sse",
      "url": "http://citus-mcp-server:8080/mcp"
    }
  }
}
```

---

## 🔌 Setting Up with GitHub Copilot

### VS Code Setup

1. **Install Prerequisites**
   - VS Code with GitHub Copilot extension
   - MCP support enabled in Copilot settings

2. **Create MCP Configuration**

   Create `.vscode/mcp.json` in your workspace:

   ```json
   {
     "mcpServers": {
       "citus-mcp": {
         "command": "/absolute/path/to/bin/citus-mcp",
         "args": [],
         "env": {
           "CITUS_MCP_COORDINATOR_DSN": "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
         }
       }
     }
   }
   ```

   Or for development (using `go run`):

   ```json
   {
     "mcpServers": {
       "citus-mcp": {
         "command": "go",
         "args": ["run", "./cmd/citus-mcp"],
         "cwd": "/path/to/citus-mcp",
         "env": {
           "CITUS_MCP_COORDINATOR_DSN": "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
         }
       }
     }
   }
   ```

3. **Reload VS Code** and open Copilot Chat

4. **Verify Connection**
   ```
   @citus-mcp ping
   ```

### GitHub Copilot CLI Setup

1. **Create Global MCP Config**

   Create `~/.config/github-copilot/mcp.json`:

   ```json
   {
     "mcpServers": {
       "citus-mcp": {
         "command": "/usr/local/bin/citus-mcp",
         "args": [],
         "env": {
           "CITUS_MCP_COORDINATOR_DSN": "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
         }
       }
     }
   }
   ```

2. **Verify Setup**
   ```bash
   copilot mcp list
   copilot mcp test citus-mcp
   ```

3. **Use in CLI**
   ```bash
   copilot -p "Show me the cluster summary"
   ```

---

## 💡 Usage Examples

### Basic Cluster Inspection

```
@citus-mcp Show me the cluster summary
```

```
@citus-mcp List all distributed tables
```

```
@citus-mcp Inspect the public.users table including shards and indexes
```

### Monitoring

```
@citus-mcp Show current cluster activity
```

```
@citus-mcp Are there any lock waits in the cluster?
```

```
@citus-mcp Show background job progress
```

### Analysis

```
@citus-mcp Analyze shard skew for the orders table
```

```
@citus-mcp Show me the shard heatmap grouped by node
```

```
@citus-mcp Explain this query: SELECT * FROM orders WHERE customer_id = 123
```

### Advisor

```
@citus-mcp Run the advisor with focus on skew
```

```
@citus-mcp Check operational health (long queries, locks, jobs)
```

```
@citus-mcp Suggest the best source node for snapshot-based scaling
```

```
@citus-mcp Check metadata health with deep validation across nodes
```

### Configuration Analysis

```
@citus-mcp Analyze cluster configuration and recommend improvements
```

```
@citus-mcp Run config advisor with focus on memory settings
```

### Colocation Analysis

```
@citus-mcp Show all colocation groups
```

```
@citus-mcp Which tables are colocated with the orders table?
```

### Node Addition

```
@citus-mcp Run pre-flight checks for adding node at postgres://user:pass@newworker:5432/db
```

---

## 📚 Tools Reference

### Inspection Tools

| Tool | Parameters | Description |
|------|------------|-------------|
| `ping` | `message?` | Health check |
| `server_info` | — | Server metadata and mode |
| `list_nodes` | `limit?`, `offset?` | Coordinator and workers |
| `list_distributed_tables` | `limit?`, `offset?` | All distributed tables |
| `list_shards` | `limit?`, `offset?` | Shards with placements |
| `citus_cluster_summary` | `include_workers?`, `include_gucs?`, `include_config?` | Full cluster overview with config health |
| `citus_list_distributed_tables` | `schema?`, `table_type?`, `limit?`, `cursor?` | Paginated table list |
| `citus_list_reference_tables` | `schema?`, `limit?`, `cursor?` | Paginated reference table list |
| `citus_table_inspector` | `table` (required), `include_shards?`, `include_indexes?` | Table deep dive |
| `citus_colocation_inspector` | `colocation_id?`, `limit?` | Colocation groups |

### Monitoring Tools

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_activity` | `limit?`, `include_idle?`, `min_duration_secs?` | Active queries |
| `citus_lock_inspector` | `include_locks?`, `limit?` | Lock waits |
| `citus_job_inspector` | `state?`, `include_tasks?`, `limit?` | Background jobs |
| `citus_shard_heatmap` | `table?`, `limit?`, `metric?`, `group_by?` | Hot shards |
| `citus_shard_skew_report` | `table?`, `metric?`, `include_top_shards?` | Skew analysis |
| `citus_explain_query` | `sql` (required), `analyze?`, `verbose?`, `costs?` | EXPLAIN query |

### Advisor Tools

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_advisor` | `focus?` (`skew`/`ops`), `max_tables?`, `include_next_steps?`, `include_sql_fixes?` | SRE advisor |
| `citus_config_advisor` | `include_all_gucs?`, `category?`, `severity_filter?`, `total_ram_gb?` | Configuration analysis |
| `citus_snapshot_source_advisor` | `strategy?`, `max_candidates?`, `include_simulation?` | Node addition advice |
| `citus_validate_rebalance_prereqs` | `table` (required) | Rebalance readiness |
| `citus_metadata_health` | `check_level?` (`basic`/`thorough`/`deep`), `include_fixes?` | Metadata consistency checks |
| `citus_node_prepare_advisor` | `host` (required), `port?`, `database?`, `generate_script?` | Pre-flight node addition checks |

### Execute Tools (Require Approval)

| Tool | Parameters | Description |
|------|------------|-------------|
| `citus_rebalance_plan` | `table?`, `threshold?`, `max_shard_moves?`, `drain_only?` | Preview rebalance |
| `citus_rebalance_execute` | `approval_token` (required), `table?`, `threshold?` | Start rebalance |
| `citus_rebalance_status` | `verbose?`, `limit?`, `cursor?` | Rebalance progress |
| `citus_move_shard_plan` | `shard_id`, `source_host`, `source_port`, `target_host`, `target_port`, `colocated?` | Preview move |
| `citus_move_shard_execute` | `approval_token` (required), `shard_id`, `source_*`, `target_*`, `colocated?`, `drop_method?` | Execute move |
| `citus_request_approval_token` | `action` (required), `ttl_seconds?` | Get approval token |
| `rebalance_table_plan` | `table` (required) | Legacy: plan table rebalance |
| `rebalance_table_execute` | `table` (required), `approval_token` (required) | Legacy: execute table rebalance |

---

## 📋 Built-in Prompts

Use these prompts in Copilot Chat for guided workflows:

| Prompt | Description |
|--------|-------------|
| `/citus.health_check` | Cluster health checklist |
| `/citus.rebalance_workflow` | Step-by-step rebalance guide |
| `/citus.skew_investigation` | Skew investigation playbook |
| `/citus.ops_triage` | Operational triage workflow |

---

## 🔐 Security

### Read-Only Mode (Default)

By default, citus-mcp runs in **read-only mode**. This means:
- ✅ All inspection and monitoring tools work
- ✅ Advisors provide recommendations
- ❌ Execute operations are disabled
- ❌ No data can be modified

### Admin Mode with Approval Tokens

To enable execute operations:

1. **Set admin mode** in configuration:
   ```yaml
   mode: admin
   allow_execute: true
   approval_secret: your-secret-key-here
   ```

2. **Request an approval token** before executing:
   ```
   @citus-mcp Request approval token for rebalance
   ```

3. **Use the token** in the execute command:
   ```
   @citus-mcp Execute rebalance with token: <token>
   ```

Tokens are time-limited and action-specific (HMAC-signed).

---

## 🔧 Troubleshooting

### Connection Issues

**Error: `connection refused`**
- Verify the coordinator host and port are correct
- Check that PostgreSQL is running and accepting connections
- Ensure firewall rules allow the connection

**Error: `authentication failed`**
- Verify username and password in DSN
- Check that the user has permissions on the database
- For SSL issues, try `sslmode=disable` for local testing

### MCP Issues

**Copilot doesn't see citus-mcp**
- Ensure `mcp.json` is in the correct location
- Check that the command path is absolute
- Reload VS Code after changing configuration

**Tools return errors**
- Check logs: `CITUS_MCP_LOG_LEVEL=debug bin/citus-mcp`
- Verify Citus extension is installed: `SELECT * FROM pg_extension WHERE extname = 'citus'`

### Testing Connection

```bash
# Test directly
CITUS_MCP_COORDINATOR_DSN="postgres://..." bin/citus-mcp

# Then send a ping via stdin
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' | bin/citus-mcp
```

---

## 🛠️ Development

### Running Tests

```bash
# Unit tests
make test

# With verbose output
go test -v ./...

# Integration tests (requires Docker)
make docker-up
make integration
make docker-down
```

### Linting

```bash
make lint
```

### Project Structure

```
citus-mcp/
├── cmd/citus-mcp/       # Main entry point
├── internal/
│   ├── mcpserver/       # MCP server implementation
│   │   ├── tools/       # Tool implementations (30+ tools)
│   │   ├── prompts/     # Prompt templates
│   │   └── resources/   # Static resources
│   ├── db/              # Database layer and worker management
│   ├── citus/           # Citus-specific logic and queries
│   │   ├── advisor/     # Advisor implementations
│   │   └── guc/         # GUC (configuration) analysis
│   ├── cache/           # Query result caching
│   ├── config/          # Configuration management
│   ├── errors/          # Error types and codes
│   ├── fanout/          # Parallel query execution
│   ├── logging/         # Structured logging
│   └── safety/          # Guardrails and approval tokens
├── docker/              # Docker Compose setup for testing
├── docs/                # Additional documentation
└── tests/               # Integration tests
```

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

**[⬆ Back to Top](#citus-mcp-server)**

Made with ❤️ for the Citus community

</div>
