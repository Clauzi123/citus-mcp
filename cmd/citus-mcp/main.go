// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Main entry point for the Citus MCP server.

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"citus-mcp/internal/config"
	"citus-mcp/internal/logging"
	"citus-mcp/internal/mcpserver"
	"citus-mcp/internal/version"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

const serverName = "citus-mcp"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		// fallback logger
		zap.NewExample().Fatal("failed to load config", zap.Error(err))
	}
	logger, err := logging.NewLogger(cfg.LogLevel)
	if err != nil {
		zap.NewExample().Fatal("failed to init logger", zap.Error(err))
	}
	defer logger.Sync()

	srv, err := mcpserver.New(cfg, logger)
	if err != nil {
		logger.Fatal("failed to create server", zap.Error(err))
	}
	defer srv.Close()

	switch cfg.Transport {
	case config.TransportStdio:
		runStdio(ctx, srv, logger)
	case config.TransportSSE:
		runSSE(ctx, srv, cfg, logger)
	case config.TransportStreamable:
		runStreamable(ctx, srv, cfg, logger)
	default:
		logger.Fatal("unknown transport", zap.String("transport", string(cfg.Transport)))
	}
}

func runStdio(ctx context.Context, srv *mcpserver.Server, logger *zap.Logger) {
	transport := &mcp.StdioTransport{}
	info := version.Info()
	logger.Info("starting citus-mcp server (stdio)",
		zap.String("name", serverName),
		zap.String("version", info.Version),
		zap.String("commit", info.Commit),
		zap.String("date", info.Date),
	)
	if err := srv.Run(ctx, transport); err != nil {
		logger.Error("server exited with error", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("server stopped")
}

func runSSE(ctx context.Context, srv *mcpserver.Server, cfg config.Config, logger *zap.Logger) {
	addr := fmt.Sprintf("%s:%d", cfg.HTTPAddr, cfg.HTTPPort)
	endpoint := cfg.HTTPPath
	info := version.Info()

	logger.Info("starting citus-mcp server (SSE)",
		zap.String("name", serverName),
		zap.String("version", info.Version),
		zap.String("commit", info.Commit),
		zap.String("date", info.Date),
		zap.String("addr", addr),
		zap.String("endpoint", endpoint),
	)

	// Use SDK's SSEHandler which properly manages sessions
	handler := mcp.NewSSEHandler(func(r *http.Request) *mcp.Server {
		return srv.MCP()
	}, nil)

	mux := http.NewServeMux()
	mux.Handle(endpoint, handler)
	mux.Handle(endpoint+"/", handler) // Handle session sub-paths

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("HTTP server error", zap.Error(err))
	}
	logger.Info("server stopped")
}

func runStreamable(ctx context.Context, srv *mcpserver.Server, cfg config.Config, logger *zap.Logger) {
	addr := fmt.Sprintf("%s:%d", cfg.HTTPAddr, cfg.HTTPPort)
	endpoint := cfg.HTTPPath
	info := version.Info()

	logger.Info("starting citus-mcp server (Streamable HTTP)",
		zap.String("name", serverName),
		zap.String("version", info.Version),
		zap.String("commit", info.Commit),
		zap.String("date", info.Date),
		zap.String("addr", addr),
		zap.String("endpoint", endpoint),
	)

	// Use SDK's StreamableHTTPHandler which properly manages sessions
	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return srv.MCP()
	}, nil)

	mux := http.NewServeMux()
	mux.Handle(endpoint, handler)

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("HTTP server error", zap.Error(err))
	}
	logger.Info("server stopped")
}
