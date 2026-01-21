# citus-mcp

An MCP server for Citus (PostgreSQL extension), providing read-heavy tooling with guarded execute flows.

## Features
- MCP stdio transport for VS Code
- Default read-only mode with *_plan/*_execute split
- HMAC approval tokens for disruptive actions
- Structured outputs and errors with pagination
- Citus metadata, shards, placements, nodes, fanout queries

## Tools
- `ping`
- `server_info`
- `list_nodes` (paginated)
- `list_distributed_tables` (paginated)
- `list_shards` (paginated)
- `rebalance_table_plan`
- `rebalance_table_execute` (requires `allow_execute=true` and approval token)

## Requirements
- Go 1.22+
- PostgreSQL with Citus extension

## Quickstart
```bash
make build
# or
make test
```

Run server over stdio (MCP):
```bash
citus-mcp
```

## Configuration
Load order: `CITUS_MCP_CONFIG` file (yaml/json/toml) → env vars `CITUS_MCP_*` → flags.

Environment variables:
- `CITUS_MCP_COORDINATOR_DSN` **(required)**
- `CITUS_MCP_WORKER_DSNS`
- `CITUS_MCP_CONNECT_TIMEOUT_SECONDS` (default 5)
- `CITUS_MCP_STATEMENT_TIMEOUT_MS` (default 30000)
- `CITUS_MCP_APP_NAME` (default citus-mcp)
- `CITUS_MCP_MODE` (read_only|admin, default read_only)
- `CITUS_MCP_ALLOW_EXECUTE` (default false)
- `CITUS_MCP_APPROVAL_SECRET` (required if allow_execute)
- `CITUS_MCP_MAX_ROWS` (default 200)
- `CITUS_MCP_MAX_TEXT_BYTES` (default 200000)
- `CITUS_MCP_ENABLE_CACHING` (default true)
- `CITUS_MCP_CACHE_TTL_SECONDS` (default 5)
- `CITUS_MCP_LOG_LEVEL` (default info)

## Approval Tokens
- Generate: HMAC(secret, action|expiry)
- Format: `action|expiryUnix|signature`
- Execute tools require both `allow_execute=true` and valid token

See `internal/config/config.go` for more.

## Integration Tests
```bash
make docker-up
make integration
make docker-down
```

## License
MIT
