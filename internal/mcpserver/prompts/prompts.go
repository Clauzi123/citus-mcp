package prompts

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"citus-mcp/internal/mcpserver/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RegisterAll registers all prompts with the MCP server.
func RegisterAll(server *mcp.Server, deps tools.Dependencies) {
	server.AddPrompt(&mcp.Prompt{Name: "/citus.health_check", Title: "Citus health check", Description: "Checklist with cluster summary and worker health"}, promptHealthCheck(deps))
	server.AddPrompt(&mcp.Prompt{Name: "/citus.rebalance_workflow", Title: "Citus rebalance workflow", Description: "Step-by-step rebalance guidance"}, promptRebalanceWorkflow(deps))
	server.AddPrompt(&mcp.Prompt{Name: "/citus.skew_investigation", Title: "Citus skew investigation", Description: "Investigate shard/table skew"}, promptSkewInvestigation(deps))
}

func promptHealthCheck(deps tools.Dependencies) mcp.PromptHandler {
	return func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		var summaryText string
		var checklist strings.Builder
		checklist.WriteString("### 🩺 Citus Health Check\n")
		checklist.WriteString("- [ ] Run `citus.cluster_summary`\n")
		checklist.WriteString("- [ ] Verify workers active vs expected\n")
		checklist.WriteString("- [ ] Review warnings\n")
		checklist.WriteString("- [ ] Inspect distributed/reference table counts\n\n")

		_, out, err := tools.ClusterSummary(ctx, deps, tools.ClusterSummaryInput{})
		if err == nil {
			active := 0
			for _, w := range out.Workers {
				if w.IsActive {
					active++
				}
			}
			inactive := len(out.Workers) - active
			checklist.WriteString(fmt.Sprintf("**Workers active**: %d/%d\n\n", active, len(out.Workers)))
			if inactive > 0 {
				checklist.WriteString("Inactive workers:\n")
				for _, w := range out.Workers {
					if !w.IsActive {
						checklist.WriteString(fmt.Sprintf("- %s:%d (should_have_shards=%v)\n", w.Host, w.Port, w.ShouldHaveShards))
					}
				}
				checklist.WriteString("\n")
			}
			if len(out.Warnings) > 0 {
				checklist.WriteString("Warnings:\n")
				for _, w := range out.Warnings {
					checklist.WriteString(fmt.Sprintf("- %s\n", w))
				}
				checklist.WriteString("\n")
			}
			b, _ := json.MarshalIndent(out, "", "  ")
			summaryText = fmt.Sprintf("```json\n%s\n```", string(b))
		} else {
			summaryText = fmt.Sprintf("⚠️ Unable to fetch cluster summary: %v", err)
		}

		messages := []*mcp.PromptMessage{
			{Role: mcp.Role("system"), Content: &mcp.TextContent{Text: "You are a concise Citus SRE assistant. Provide checklists and actionable next steps."}},
			{Role: mcp.Role("assistant"), Content: &mcp.TextContent{Text: checklist.String() + summaryText}},
		}
		return &mcp.GetPromptResult{Description: "Citus health check", Messages: messages}, nil
	}
}

func promptRebalanceWorkflow(deps tools.Dependencies) mcp.PromptHandler {
	return func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		var b strings.Builder
		b.WriteString("### 🔄 Citus Rebalance Workflow\n")
		b.WriteString("1) Validate prerequisites\n")
		b.WriteString("```json\n{")
		b.WriteString("\n  \"table\": \"public.my_table\"")
		b.WriteString("\n}\n```\nRun: `citus.validate_rebalance_prereqs`\n\n")
		b.WriteString("2) Plan rebalance\n")
		b.WriteString("```json\n{\n  \"table\": \"public.my_table\"\n}\n```\nRun: `citus.rebalance_plan`\n\n")
		b.WriteString("3) Execute rebalance (approval required)\n")
		b.WriteString("```json\n{\n  \"plan_id\": \"<from plan>\",\n  \"approval_token\": \"<token>\"\n}\n```\nRun: `citus.rebalance_execute`\n\n")
		b.WriteString("4) Monitor status\n")
		b.WriteString("```json\n{\n  \"plan_id\": \"<from plan>\"\n}\n```\nRun: `citus.rebalance_status`\n\n")
		b.WriteString("Notes:\n- Obtain approval token with `citus.request_approval_token` (admin mode).\n- Re-run status until complete; expect `Completed` or `InProgress`.\n")
		messages := []*mcp.PromptMessage{
			{Role: mcp.Role("system"), Content: &mcp.TextContent{Text: "You are a concise Citus operations assistant. Provide step-by-step guidance."}},
			{Role: mcp.Role("assistant"), Content: &mcp.TextContent{Text: b.String()}},
		}
		return &mcp.GetPromptResult{Description: "Citus rebalance workflow", Messages: messages}, nil
	}
}

func promptSkewInvestigation(deps tools.Dependencies) mcp.PromptHandler {
	return func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		table := ""
		if req != nil && req.Params != nil && req.Params.Arguments != nil {
			table = strings.TrimSpace(req.Params.Arguments["table"])
		}

		if table == "" {
			msg := "### 📊 Citus Skew Investigation\n- Provide `table` argument (schema.table).\n- Example: get_prompt /citus.skew_investigation arguments:{\"table\":\"public.orders\"}\n"
			messages := []*mcp.PromptMessage{
				{Role: mcp.Role("assistant"), Content: &mcp.TextContent{Text: msg}},
			}
			return &mcp.GetPromptResult{Description: "Provide table argument", Messages: messages}, nil
		}

		var b strings.Builder
		b.WriteString("### 📊 Citus Skew Investigation\n")
		b.WriteString(fmt.Sprintf("**Target table**: %s\n\n", table))
		b.WriteString("1) Run shard skew report\n")
		b.WriteString(fmt.Sprintf("Run: `citus.shard_skew_report` with `{\"table\":\"%s\"}`\n", table))
		b.WriteString(fmt.Sprintf("Resource: `citus://shards/skew?table=%s`\n\n", table))
		b.WriteString("2) If skew detected, consider:\n")
		b.WriteString("- `citus.rebalance_plan` (optionally with table)\n")
		b.WriteString("- `citus.move_shard_plan` / `citus.move_shard_execute` for targeted moves\n")
		b.WriteString("- `citus.rebalance_execute` (approval required)\n\n")
		b.WriteString("3) Monitor:\n- `citus.rebalance_status` for ongoing rebalance\n- `citus.cluster_summary` to confirm worker balance\n")

		messages := []*mcp.PromptMessage{
			{Role: mcp.Role("system"), Content: &mcp.TextContent{Text: "You are a concise Citus operations assistant. Suggest next tools to run."}},
			{Role: mcp.Role("assistant"), Content: &mcp.TextContent{Text: b.String()}},
		}
		return &mcp.GetPromptResult{Description: "Citus skew investigation", Messages: messages}, nil
	}
}
