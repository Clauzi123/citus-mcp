package tools

import (
	"context"
	"fmt"
	"strings"

	serr "citus-mcp/internal/errors"
	"citus-mcp/internal/safety"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ValidateRebalancePrereqsInput input for citus.validate_rebalance_prereqs.
type ValidateRebalancePrereqsInput struct {
	Table string `json:"table" jsonschema:"required"`
}

// ValidateRebalancePrereqsOutput output.
type ValidateRebalancePrereqsOutput struct {
	Ready    bool          `json:"ready"`
	Issues   []Issue       `json:"issues,omitempty"`
	Detected DetectedProps `json:"detected"`
}

type Issue struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	SuggestedFixSQL string `json:"suggested_fix_sql,omitempty"`
}

type DetectedProps struct {
	HasPrimaryKey       bool     `json:"has_primary_key"`
	ReplicaIdentity     string   `json:"replica_identity_setting"`
	CandidateUniqueIdxs []string `json:"candidate_unique_indexes_that_include_distribution_column"`
}

func validateRebalancePrereqsTool(ctx context.Context, deps Dependencies, input ValidateRebalancePrereqsInput) (*mcp.CallToolResult, ValidateRebalancePrereqsOutput, error) {
	table := strings.TrimSpace(input.Table)
	if table == "" {
		return callError(serr.CodeInvalidInput, "table is required", ""), ValidateRebalancePrereqsOutput{}, nil
	}
	if err := deps.Guardrails.RequireReadOnlySQL("SELECT 1"); err != nil {
		return callError(serr.CodePermissionDenied, err.Error(), ""), ValidateRebalancePrereqsOutput{}, nil
	}

	schema, rel, err := parseSchemaTable(table)
	if err != nil {
		return callError(serr.CodeInvalidInput, "invalid table format", "use schema.table"), ValidateRebalancePrereqsOutput{}, nil
	}

	distributionCol, err := fetchDistributionColumn(ctx, deps, schema, rel)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "fetch distribution column"), ValidateRebalancePrereqsOutput{}, nil
	}

	hasPK, replicaIdent, candidateIdxs, err := fetchIndexInfo(ctx, deps, schema, rel, distributionCol)
	if err != nil {
		return callError(serr.CodeInternalError, err.Error(), "fetch index info"), ValidateRebalancePrereqsOutput{}, nil
	}

	ready := true
	issues := []Issue{}

	if !hasPK {
		ready = false
		issues = append(issues, Issue{Code: "NO_PRIMARY_KEY", Message: "Table missing primary key", SuggestedFixSQL: fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY (%s);", safety.QuoteIdent(schema), safety.QuoteIdent(rel), safety.QuoteIdent(distributionCol))})
	}
	if replicaIdent == "n" { // DEFAULT
		ready = false
		// suggest replica identity using a candidate unique index if exists
		if len(candidateIdxs) > 0 {
			issues = append(issues, Issue{Code: "REPLICA_IDENTITY_DEFAULT", Message: "Replica identity DEFAULT (none)", SuggestedFixSQL: fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY USING INDEX %s;", safety.QuoteIdent(schema), safety.QuoteIdent(rel), safety.QuoteIdent(candidateIdxs[0]))})
		} else {
			issues = append(issues, Issue{Code: "REPLICA_IDENTITY_DEFAULT", Message: "Replica identity DEFAULT (none); set using suitable unique index", SuggestedFixSQL: fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY FULL; -- Not recommended; prefer USING INDEX", safety.QuoteIdent(schema), safety.QuoteIdent(rel))})
		}
	}

	return nil, ValidateRebalancePrereqsOutput{
		Ready:  ready,
		Issues: issues,
		Detected: DetectedProps{
			HasPrimaryKey:       hasPK,
			ReplicaIdentity:     replicaIdent,
			CandidateUniqueIdxs: candidateIdxs,
		},
	}, nil
}

// ValidateRebalancePrereqs exported for integration/tests.
func ValidateRebalancePrereqs(ctx context.Context, deps Dependencies, input ValidateRebalancePrereqsInput) (*mcp.CallToolResult, ValidateRebalancePrereqsOutput, error) {
	return validateRebalancePrereqsTool(ctx, deps, input)
}

func fetchDistributionColumn(ctx context.Context, deps Dependencies, schema, rel string) (string, error) {
	const q = `
	SELECT column_to_column_name(p.logicalrelid, p.partkey)
	FROM pg_dist_partition p
	JOIN pg_class c ON c.oid = p.logicalrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1::name AND c.relname = $2::name`
	var col string
	// Citus 14: partkey is text; use column_to_column_name helper via citus_tables for robustness
	if err := deps.Pool.QueryRow(ctx, q, schema, rel).Scan(&col); err != nil {
		return "", err
	}
	return col, nil
}

func fetchIndexInfo(ctx context.Context, deps Dependencies, schema, rel, distCol string) (hasPK bool, replicaIdent string, candidateIdxs []string, err error) {
	// primary key
	const pkq = `
SELECT EXISTS (
  SELECT 1
  FROM pg_index i
  JOIN pg_class c ON c.oid = i.indrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1::name AND c.relname = $2::name AND i.indisprimary
)`
	if err = deps.Pool.QueryRow(ctx, pkq, schema, rel).Scan(&hasPK); err != nil {
		return
	}
	const replq = `
SELECT c.relreplident
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1::name AND c.relname = $2::name`
	if err = deps.Pool.QueryRow(ctx, replq, schema, rel).Scan(&replicaIdent); err != nil {
		return
	}
	// candidate unique indexes containing distCol
	const uq = `
SELECT ci.relname AS index_name
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class ci ON ci.oid = i.indexrelid
JOIN pg_attribute a ON a.attrelid = c.oid
WHERE n.nspname = $1::name AND c.relname = $2::name AND i.indisunique
  AND a.attname = $3 AND a.attnum = ANY(i.indkey)
ORDER BY ci.relname`
	rows, err2 := deps.Pool.Query(ctx, uq, schema, rel, distCol)
	if err2 != nil {
		err = err2
		return
	}
	defer rows.Close()
	for rows.Next() {
		var idx string
		if err = rows.Scan(&idx); err != nil {
			return
		}
		candidateIdxs = append(candidateIdxs, idx)
	}
	err = rows.Err()
	return
}
