# citus-mcp

An MCP server for Citus (PostgreSQL extension), providing read-heavy tooling with guarded execute flows. Read-only by default; disruptive actions require explicit opt-in and HMAC approval tokens.

## Features
- MCP stdio transport for GitHub Copilot Chat & CLI
- `*_plan` / `*_execute` split with approval tokens
- Snapshot source advisor for node addition
- Structured outputs, pagination, caching
- Tested against Citus 14.x

## Tools (highlights)
- `ping`, `server_info`
- `list_nodes`, `list_distributed_tables`, `list_shards`
- `rebalance_table_plan`, `rebalance_table_execute` (requires `allow_execute` + approval)
- `citus.cluster_summary`, `citus.list_distributed_tables`
- `citus.explain_query`, `citus.shard_skew_report`
- `citus.rebalance_plan`, `citus.validate_rebalance_prereqs`, `citus.rebalance_execute`, `citus.rebalance_status`
- `citus.snapshot_source_advisor`
- `citus.move_shard_plan`, `citus.move_shard_execute` (approval)
- `citus.request_approval_token`

## Requirements
- Go 1.22+
- Citus 14.x coordinator DSN (`CITUS_MCP_COORDINATOR_DSN`)
- `citus.snapshot_source_advisor` function available for snapshot advisor

## Build & Run
```bash
go build -o bin/citus-mcp ./cmd/citus-mcp
CITUS_MCP_COORDINATOR_DSN=postgres://localhost:5432/postgres?sslmode=disable bin/citus-mcp
# or pass DSN as flag/positional
bin/citus-mcp --coordinator-dsn postgres://localhost:5432/postgres?sslmode=disable
bin/citus-mcp postgres://localhost:5432/postgres?sslmode=disable
go run ./cmd/citus-mcp --config ~/.config/citus-mcp/config.yaml
```

## Configuration (env / flags / file)
- `CITUS_MCP_COORDINATOR_DSN` (required)
- `--coordinator-dsn` / `--dsn` **or positional** DSN arg
- `CITUS_MCP_CONFIG` or `--config/-c` to point to config file
- `CITUS_MCP_WORKER_DSNS` (optional, comma-separated)
- `CITUS_MCP_COORDINATOR_USER` / `CITUS_MCP_COORDINATOR_PASSWORD` (optional overrides)
- `CITUS_MCP_ALLOW_EXECUTE` (default false)
- `CITUS_MCP_APPROVAL_SECRET` (required if allow_execute)
- `CITUS_MCP_MODE` (`read_only`|`admin`, default `read_only`)
- `CITUS_MCP_CACHE_TTL_SECONDS` (default 5; disable via `CITUS_MCP_ENABLE_CACHING=false`)
- `CITUS_MCP_MAX_ROWS` (default 200)
- `CITUS_MCP_MAX_TEXT_BYTES` (default 200000)
- `CITUS_MCP_LOG_LEVEL` (default info)
- `CITUS_MCP_SNAPSHOT_ADVISOR_COLLECT_BYTES` (default true)

See `internal/config/config.go` for full list and flags.

### Config file
The server looks for a config file in this order:
1. `--config / -c` flag
2. `CITUS_MCP_CONFIG` env var
3. `$XDG_CONFIG_HOME/citus-mcp/config.{yaml,yml,json,toml}`
4. `~/.config/citus-mcp/config.{yaml,yml,json,toml}`
5. `./citus-mcp.{yaml,yml,json,toml}`

Example `~/.config/citus-mcp/config.yaml`:
```yaml
coordinator_dsn: postgres://u:p@localhost:5432/postgres?sslmode=disable
coordinator_user: u
coordinator_password: p
mode: read_only
allow_execute: false
approval_secret: "" # required if allow_execute=true
```

Admin example:
```yaml
coordinator_dsn: postgres://admin:admin@localhost:5432/postgres?sslmode=disable
mode: admin
allow_execute: true
approval_secret: supersecret
```

## Approval Tokens
- Generate: `HMAC(secret, action|expiryUnix)`
- Format: `action|expiryUnix|signature`
- Execute tools require `allow_execute=true` **and** valid token

## Integration Tests
```bash
make docker-up
make integration
make docker-down
```

## VS Code (MCP)
1. Ensure GitHub Copilot Chat extension is enabled with MCP support.
2. Add `.vscode/mcp.json` (see below) or use root `mcp.json`.
3. Run `bin/citus-mcp` (or `go run ./cmd/server`) with `CITUS_MCP_COORDINATOR_DSN` set.
4. In Copilot Chat, run `@citus-mcp ping` (or any tool) to verify.

`.vscode/mcp.json` (VS Code Copilot Chat)
```jsonc
{
	"servers": {
		"citus-mcp": {
			"type": "stdio",
			"command": "bin/citus-mcp", // or "go"
			"args": ["run", "./cmd/citus-mcp"], // if using go run
			"env": {
				"CITUS_MCP_COORDINATOR_DSN": "postgres://localhost:5432/postgres?sslmode=disable"
			}
		}
	}
}
```

## Copilot CLI
```bash
copilot mcp list
copilot mcp test citus-mcp
copilot -p "Run go run ./cmd/integration"
```
The CLI reads `mcp.json` in the workspace root or `~/.config/github-copilot/mcp.json`. Use `COPILOT_MCP_CONFIG` to point elsewhere.

## License
MIT
