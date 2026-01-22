# Contributing to citus-mcp

Thank you for your interest in contributing to citus-mcp! This document provides guidelines and information for contributors.

## Code of Conduct

Please be respectful and constructive in all interactions. We're building something together.

## Getting Started

### Prerequisites

- Go 1.23 or later
- Docker and Docker Compose (for integration tests)
- golangci-lint (for linting)

### Setup

```bash
# Clone the repository
git clone https://github.com/citusdata/citus-mcp.git
cd citus-mcp

# Build
make build

# Run tests
make test

# Run linter
make lint
```

### Running with a Local Citus Cluster

```bash
# Start a local Citus cluster
make docker-up

# Run integration tests
make test-integration

# Stop the cluster
make docker-down
```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

### 2. Make Changes

- Follow existing code style and patterns
- Add tests for new functionality
- Update documentation as needed

### 3. Test Your Changes

```bash
# Run all checks
make check

# Run tests with coverage
make test-coverage
```

### 4. Commit Your Changes

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add new shard inspection tool
fix: correct timeout handling in rebalance
docs: update README with new examples
refactor: simplify advisor scoring logic
test: add unit tests for config loading
chore: update dependencies
```

### 5. Submit a Pull Request

- Provide a clear description of the changes
- Reference any related issues
- Ensure all CI checks pass

## Project Structure

```
citus-mcp/
├── cmd/citus-mcp/       # Main entry point
├── internal/
│   ├── mcpserver/       # MCP server implementation
│   │   ├── tools/       # Tool implementations
│   │   ├── prompts/     # Prompt templates
│   │   └── resources/   # Static resources
│   ├── db/              # Database layer
│   ├── citus/           # Citus-specific logic
│   │   ├── advisor/     # Advisor implementations
│   │   ├── metadata/    # Metadata health checks
│   │   └── guc/         # Configuration analysis
│   ├── cache/           # Query caching
│   ├── config/          # Configuration
│   ├── safety/          # Guardrails and approval
│   ├── version/         # Build version info
│   └── logging/         # Structured logging
├── docker/              # Docker setup for testing
└── tests/               # Integration tests
```

## Adding a New Tool

1. Create a new file in `internal/mcpserver/tools/`
2. Define input/output structs with JSON tags
3. Implement the tool function
4. Register in `tools.go` via `RegisterAll()`
5. Add tests
6. Update README tool catalog

Example:

```go
// internal/mcpserver/tools/my_tool.go

type MyToolInput struct {
    Table string `json:"table" jsonschema:"required"`
    Limit int    `json:"limit,omitempty"`
}

type MyToolOutput struct {
    Results []Result `json:"results"`
    Summary string   `json:"summary"`
}

func myTool(ctx context.Context, deps Dependencies, input MyToolInput) (*mcp.CallToolResult, MyToolOutput, error) {
    // Implementation
}
```

## Code Style

- Use `gofmt` for formatting
- Follow [Effective Go](https://golang.org/doc/effective_go) guidelines
- Keep functions focused and small
- Add comments for exported types and functions
- Use meaningful variable names

## Testing Guidelines

- Write unit tests for business logic
- Use table-driven tests where appropriate
- Mock database connections in unit tests
- Integration tests should use the Docker Citus cluster

## Security

- Never log sensitive data (passwords, tokens)
- Use `safety.RedactDSN()` for connection strings
- Use `logging.FieldSecret()` for secret values
- All execute operations require approval tokens

## Questions?

Open an issue for questions or discussions. We're happy to help!

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
