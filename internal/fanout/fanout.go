// citus-mcp: AI-powered MCP server for Citus distributed PostgreSQL
// SPDX-License-Identifier: MIT
//
// Parallel query fanout to multiple worker nodes.

package fanout

import (
	"context"

	"citus-mcp/internal/db"
	"golang.org/x/sync/errgroup"
)

// Fanout runs fn concurrently across nodes and returns results in same order.
func Fanout[T any](ctx context.Context, nodes []db.Node, fn func(context.Context, db.Node) (T, error)) ([]T, error) {
	g, ctx := errgroup.WithContext(ctx)
	results := make([]T, len(nodes))
	for i, node := range nodes {
		i, node := i, node
		g.Go(func() error {
			r, err := fn(ctx, node)
			if err != nil {
				var zero T
				results[i] = zero
				return err
			}
			results[i] = r
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
