package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"sampling-svc/internal/plan"
)

// ctxQuerier is satisfied by both *pgxpool.Pool and pgx.Tx.
type ctxQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func loadPlan(ctx context.Context, q ctxQuerier, id string) (*plan.Plan, error) {
	row := q.QueryRow(ctx, `SELECT `+planColumns+` FROM plans WHERE id=$1`, id)
	p, err := scanPlan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}
