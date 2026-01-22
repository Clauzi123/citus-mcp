# citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
# SPDX-License-Identifier: MIT

BINARY := citus-mcp
MODULE := citus-mcp
GO ?= go

# Build info
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

# ldflags for version injection
LDFLAGS := -s -w \
	-X $(MODULE)/internal/version.Version=$(VERSION) \
	-X $(MODULE)/internal/version.Commit=$(COMMIT) \
	-X $(MODULE)/internal/version.Date=$(DATE)

# Directories
BIN_DIR := bin
CMD_DIR := cmd/$(BINARY)

.PHONY: all build build-dev clean test test-unit test-integration lint fmt vet \
        docker-up docker-down help

##@ General

all: build ## Default target: build the binary

help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Build

build: ## Build the binary with version info
	@mkdir -p $(BIN_DIR)
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY) ./$(CMD_DIR)
	@echo "Built $(BIN_DIR)/$(BINARY) ($(VERSION))"

build-dev: ## Build without optimizations for development
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/$(BINARY) ./$(CMD_DIR)

clean: ## Remove build artifacts
	rm -rf $(BIN_DIR)
	$(GO) clean -cache -testcache

##@ Testing

test: test-unit ## Run all tests (alias for test-unit)

test-unit: ## Run unit tests
	$(GO) test -race -cover ./...

test-integration: ## Run integration tests (requires Docker)
	$(GO) test -race -tags=integration ./...

test-coverage: ## Run tests with coverage report
	$(GO) test -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

##@ Code Quality

lint: ## Run golangci-lint
	golangci-lint run ./...

fmt: ## Format code
	$(GO) fmt ./...
	@echo "Code formatted"

vet: ## Run go vet
	$(GO) vet ./...

check: fmt vet lint ## Run all checks (fmt, vet, lint)

##@ Docker

docker-up: ## Start Citus cluster for testing
	docker compose -f docker/docker-compose.yml up -d --build
	@echo "Waiting for cluster to be ready..."
	@sleep 5

docker-down: ## Stop Citus cluster
	docker compose -f docker/docker-compose.yml down

docker-logs: ## Show Citus cluster logs
	docker compose -f docker/docker-compose.yml logs -f

##@ Release

version: ## Show version info
	@echo "Version: $(VERSION)"
	@echo "Commit:  $(COMMIT)"
	@echo "Date:    $(DATE)"

