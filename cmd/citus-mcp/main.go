package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"citus-mcp/internal/config"
	"citus-mcp/internal/logging"
	"citus-mcp/internal/mcpserver"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

const (
	serverName    = "citus-mcp"
	serverVersion = "0.1.0"
)

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
	logger.Info("starting citus-mcp server (stdio)", zap.String("name", serverName), zap.String("version", serverVersion))
	if err := srv.Run(ctx, transport); err != nil {
		logger.Error("server exited with error", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("server stopped")
}

func runSSE(ctx context.Context, srv *mcpserver.Server, cfg config.Config, logger *zap.Logger) {
	addr := fmt.Sprintf("%s:%d", cfg.HTTPAddr, cfg.HTTPPort)
	endpoint := cfg.HTTPPath

	logger.Info("starting citus-mcp server (SSE)",
		zap.String("name", serverName),
		zap.String("version", serverVersion),
		zap.String("addr", addr),
		zap.String("endpoint", endpoint),
	)

	// Create HTTP handler for SSE
	mux := http.NewServeMux()

	// SSE endpoint - handles both GET (SSE stream) and POST (messages)
	mux.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// New SSE session
			sessionID := generateSessionID()
			sessionEndpoint := fmt.Sprintf("%s/session/%s", endpoint, sessionID)

			transport := &mcp.SSEServerTransport{
				Endpoint: sessionEndpoint,
				Response: w,
			}

			// Register session handler for POST messages
			mux.Handle(sessionEndpoint, transport)

			logger.Info("new SSE session", zap.String("session_id", sessionID))

			if err := srv.Run(r.Context(), transport); err != nil {
				logger.Error("SSE session error", zap.Error(err), zap.String("session_id", sessionID))
			}
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

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
		server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("HTTP server error", zap.Error(err))
	}
	logger.Info("server stopped")
}

func runStreamable(ctx context.Context, srv *mcpserver.Server, cfg config.Config, logger *zap.Logger) {
	addr := fmt.Sprintf("%s:%d", cfg.HTTPAddr, cfg.HTTPPort)
	endpoint := cfg.HTTPPath

	logger.Info("starting citus-mcp server (Streamable HTTP)",
		zap.String("name", serverName),
		zap.String("version", serverVersion),
		zap.String("addr", addr),
		zap.String("endpoint", endpoint),
	)

	// Create HTTP handler for Streamable transport
	mux := http.NewServeMux()

	// Streamable endpoint
	mux.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		sessionID := generateSessionID()

		transport := &mcp.StreamableServerTransport{
			SessionID: sessionID,
		}

		// The transport itself is an HTTP handler
		// First, connect to establish the session
		go func() {
			if err := srv.Run(r.Context(), transport); err != nil {
				logger.Error("Streamable session error", zap.Error(err), zap.String("session_id", sessionID))
			}
		}()

		// Serve the HTTP request through the transport
		transport.ServeHTTP(w, r)
	})

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
		server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("HTTP server error", zap.Error(err))
	}
	logger.Info("server stopped")
}

func generateSessionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
