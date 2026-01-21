BINARY=citus-mcp
GO?=go

.PHONY: all build test fmt lint integration docker-up docker-down

all: build

build:
	$(GO) build ./...

test:
	$(GO) test ./...

fmt:
	$(GO) fmt ./...

lint:
	golangci-lint run ./...

integration:
	$(GO) test -tags=integration ./...

docker-up:
	docker compose -f docker/docker-compose.yml up -d --build

docker-down:
	docker compose -f docker/docker-compose.yml down
