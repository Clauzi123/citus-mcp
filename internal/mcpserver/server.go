package mcpserver

import (
	"context"

	"citus-mcp/internal/config"
	"citus-mcp/internal/mcpserver/tools"
	"citus-mcp/internal/safety"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

type Server struct {
	cfg        config.Config
	logger     *zap.Logger
	pool       *pgxpool.Pool
	guardrails *safety.Guardrails
	srv        *mcp.Server
}

func New(ctx context.Context, impl *mcp.Implementation, cfg config.Config, logger *zap.Logger, pool *pgxpool.Pool) (*Server, error) {
	if impl == nil {
		impl = &mcp.Implementation{Name: "citus-mcp", Version: "0.1.0"}
	}
	m := mcp.NewServer(impl, nil)
	guard := safety.NewGuardrails(cfg)
	tools.Register(m, tools.Dependencies{Pool: pool, Logger: logger, Guardrails: guard, Config: cfg})
	return &Server{cfg: cfg, logger: logger, pool: pool, guardrails: guard, srv: m}, nil
}

// Run runs the server with the provided transport (e.g., &mcp.StdioTransport{}).
func (s *Server) Run(ctx context.Context, transport mcp.Transport) error {
	return s.srv.Run(ctx, transport)
}
