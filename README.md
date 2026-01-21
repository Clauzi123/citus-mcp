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
Environment variables (via Viper):
- `CITUS_MCP_DB_URL`
- `CITUS_MCP_ALLOW_EXECUTE` (default false)
- `CITUS_MCP_APPROVAL_SECRET`
- `CITUS_MCP_APPROVAL_TTL`
- `CITUS_MCP_LOG_LEVEL`
- `CITUS_MCP_CACHE_TTL`
- `CITUS_MCP_DEFAULT_LIMIT`
- `CITUS_MCP_MAX_LIMIT`

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
