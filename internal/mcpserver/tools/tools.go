package tools

import (
	"context"
	"fmt"

	"citus-mcp/internal/citus"
	"citus-mcp/internal/config"
	"citus-mcp/internal/db"
	serr "citus-mcp/internal/errors"
	"citus-mcp/internal/safety"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"
)

type Dependencies struct {
	Pool       *pgxpool.Pool
	Logger     *zap.Logger
	Guardrails *safety.Guardrails
	Config     config.Config
}

func Register(server *mcp.Server, deps Dependencies) {
	mcp.AddTool(server, &mcp.Tool{Name: "ping", Description: "ping the server"}, func(ctx context.Context, req *mcp.CallToolRequest, input PingInput) (*mcp.CallToolResult, PingOutput, error) {
		return Ping(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "server_info", Description: "returns server metadata"}, func(ctx context.Context, req *mcp.CallToolRequest, input ServerInfoInput) (*mcp.CallToolResult, ServerInfoOutput, error) {
		return ServerInfo(ctx, deps)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "list_nodes", Description: "lists coordinator and worker nodes"}, func(ctx context.Context, req *mcp.CallToolRequest, input ListNodesInput) (*mcp.CallToolResult, ListNodesOutput, error) {
		return ListNodes(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "list_distributed_tables", Description: "lists distributed tables"}, func(ctx context.Context, req *mcp.CallToolRequest, input ListDistributedTablesInput) (*mcp.CallToolResult, ListDistributedTablesOutput, error) {
		return ListDistributedTables(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "list_shards", Description: "lists shards"}, func(ctx context.Context, req *mcp.CallToolRequest, input ListShardsInput) (*mcp.CallToolResult, ListShardsOutput, error) {
		return ListShards(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "rebalance_table_plan", Description: "plan rebalance_table_shards"}, func(ctx context.Context, req *mcp.CallToolRequest, input RebalanceTableInput) (*mcp.CallToolResult, RebalanceTablePlanOutput, error) {
		return RebalanceTablePlan(ctx, deps, input)
	})

	mcp.AddTool(server, &mcp.Tool{Name: "rebalance_table_execute", Description: "execute rebalance_table_shards (approval required)"}, func(ctx context.Context, req *mcp.CallToolRequest, input RebalanceTableExecuteInput) (*mcp.CallToolResult, RebalanceTableExecuteOutput, error) {
		return RebalanceTableExecute(ctx, deps, input)
	})
}

// Ping tool

type PingInput struct {
	Message string `json:"message,omitempty" jsonschema:"optional message to echo"`
}

type PingOutput struct {
	Pong string `json:"pong"`
}

func Ping(ctx context.Context, deps Dependencies, input PingInput) (*mcp.CallToolResult, PingOutput, error) {
	msg := input.Message
	if msg == "" {
		msg = "pong"
	}
	return nil, PingOutput{Pong: msg}, nil
}

// ServerInfo tool

type ServerInfoInput struct{}

type ServerInfoOutput struct {
	ReadOnly     bool            `json:"read_only"`
	AllowExecute bool            `json:"allow_execute"`
	Metadata     *citus.Metadata `json:"metadata,omitempty"`
}

func ServerInfo(ctx context.Context, deps Dependencies) (*mcp.CallToolResult, ServerInfoOutput, error) {
	meta, err := citus.GetMetadata(ctx, deps.Pool)
	if err != nil {
		deps.Logger.Warn("server_info metadata failed", zap.Error(err))
		return nil, ServerInfoOutput{ReadOnly: !deps.Config.AllowExecute, AllowExecute: deps.Config.AllowExecute}, nil
	}
	return nil, ServerInfoOutput{ReadOnly: !deps.Config.AllowExecute, AllowExecute: deps.Config.AllowExecute, Metadata: meta}, nil
}

// ListNodes tool
type ListNodesInput struct {
	Limit  int `json:"limit,omitempty" jsonschema:"min=1"`
	Offset int `json:"offset,omitempty" jsonschema:"min=0"`
}
type ListNodesOutput struct {
	Nodes []db.Node `json:"nodes"`
	Meta  Meta      `json:"meta"`
}

// Meta contains pagination metadata.
type Meta struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
	Total  int `json:"total"`
}

func ListNodes(ctx context.Context, deps Dependencies, input ListNodesInput) (*mcp.CallToolResult, ListNodesOutput, error) {
	limit, offset := normalizeLimitOffset(deps.Config, input.Limit, input.Offset)
	nodes, err := db.ListNodes(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeUnavailable, err.Error(), "db error"), ListNodesOutput{}, nil
	}
	end := offset + limit
	if end > len(nodes) {
		end = len(nodes)
	}
	if offset > len(nodes) {
		offset = len(nodes)
	}
	return nil, ListNodesOutput{Nodes: nodes[offset:end], Meta: Meta{Limit: limit, Offset: offset, Total: len(nodes)}}, nil
}

// ListDistributedTables tool
type ListDistributedTablesInput struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}
type ListDistributedTablesOutput struct {
	Tables []citus.DistributedTable `json:"tables"`
	Meta   Meta                     `json:"meta"`
}

// ListShards tool
type ListShardsInput struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}
type ListShardsOutput struct {
	Shards []citus.Shard `json:"shards"`
	Meta   Meta          `json:"meta"`
}

func ListShards(ctx context.Context, deps Dependencies, input ListShardsInput) (*mcp.CallToolResult, ListShardsOutput, error) {
	limit, offset := normalizeLimitOffset(deps.Config, input.Limit, input.Offset)
	shards, err := citus.ListShards(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeUnavailable, err.Error(), "db error"), ListShardsOutput{}, nil
	}
	end := offset + limit
	if end > len(shards) {
		end = len(shards)
	}
	if offset > len(shards) {
		offset = len(shards)
	}
	return nil, ListShardsOutput{Shards: shards[offset:end], Meta: Meta{Limit: limit, Offset: offset, Total: len(shards)}}, nil
}

func ListDistributedTables(ctx context.Context, deps Dependencies, input ListDistributedTablesInput) (*mcp.CallToolResult, ListDistributedTablesOutput, error) {
	limit, offset := normalizeLimitOffset(deps.Config, input.Limit, input.Offset)
	tables, err := citus.ListDistributedTables(ctx, deps.Pool)
	if err != nil {
		return callError(serr.CodeUnavailable, err.Error(), "db error"), ListDistributedTablesOutput{}, nil
	}
	end := offset + limit
	if end > len(tables) {
		end = len(tables)
	}
	if offset > len(tables) {
		offset = len(tables)
	}
	return nil, ListDistributedTablesOutput{Tables: tables[offset:end], Meta: Meta{Limit: limit, Offset: offset, Total: len(tables)}}, nil
}

// Rebalance tools
type RebalanceTableInput struct {
	Table string `json:"table" jsonschema:"required"`
}
type RebalanceTablePlanOutput struct {
	Plan *citus.RebalancePlan `json:"plan"`
}

func RebalanceTablePlan(ctx context.Context, deps Dependencies, input RebalanceTableInput) (*mcp.CallToolResult, RebalanceTablePlanOutput, error) {
	if input.Table == "" {
		return callError(serr.CodeInvalidInput, "table required", "provide table name"), RebalanceTablePlanOutput{}, nil
	}
	plan, err := citus.PlanRebalanceTable(ctx, deps.Pool, input.Table)
	if err != nil {
		return callError(serr.CodeUnavailable, err.Error(), "citus error"), RebalanceTablePlanOutput{}, nil
	}
	return nil, RebalanceTablePlanOutput{Plan: plan}, nil
}

type RebalanceTableExecuteInput struct {
	Table         string `json:"table" jsonschema:"required"`
	ApprovalToken string `json:"approval_token" jsonschema:"required"`
}
type RebalanceTableExecuteOutput struct {
	Status string `json:"status"`
}

func RebalanceTableExecute(ctx context.Context, deps Dependencies, input RebalanceTableExecuteInput) (*mcp.CallToolResult, RebalanceTableExecuteOutput, error) {
	if input.Table == "" {
		return callError(serr.CodeInvalidInput, "table required", "provide table name"), RebalanceTableExecuteOutput{}, nil
	}
	if err := deps.Guardrails.RequireExecuteAllowed(input.ApprovalToken, "rebalance_table:"+input.Table); err != nil {
		if me, ok := err.(*serr.MCPError); ok {
			return callError(me.Code, me.Message, me.Hint), RebalanceTableExecuteOutput{}, nil
		}
		return callError(serr.CodeUnauthorized, err.Error(), "approval required"), RebalanceTableExecuteOutput{}, nil
	}
	if err := citus.ExecuteRebalanceTable(ctx, deps.Pool, input.Table); err != nil {
		return callError(serr.CodeUnavailable, err.Error(), "citus error"), RebalanceTableExecuteOutput{}, nil
	}
	return nil, RebalanceTableExecuteOutput{Status: "ok"}, nil
}

// Helper error creation
func callError(code serr.Code, msg, hint string) *mcp.CallToolResult {
	errObj := map[string]any{"code": code, "message": msg}
	if hint != "" {
		errObj["hint"] = hint
	}
	return &mcp.CallToolResult{
		IsError:           true,
		StructuredContent: errObj,
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf("%s: %s", code, msg)},
		},
	}
}

func normalizeLimitOffset(cfg config.Config, limit, offset int) (int, int) {
	if limit <= 0 {
		limit = cfg.MaxRows
	}
	if limit > cfg.MaxRows {
		limit = cfg.MaxRows
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
