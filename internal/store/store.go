// Package store is the PostgreSQL persistence layer. It owns schema
// migration, plan/stream/event CRUD and the transactional replay-rebuild
// that keeps derived inspection state consistent with event history.
package store

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"sampling-svc/internal/plan"
)

//go:embed schema.sql
var schemaSQL string

// ErrNotFound is returned for missing rows.
var ErrNotFound = errors.New("not found")

// ErrConflict wraps unique-violation and optimistic concurrency errors.
type ErrConflict struct{ Msg string }

func (e ErrConflict) Error() string { return e.Msg }

// Store wraps the connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open creates a pool and verifies connectivity.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Migrate applies the embedded schema (idempotent DDL).
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

// Pool exposes the underlying pool (health checks etc.).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// lockStream takes a transaction-scoped advisory lock derived from the
// stream id. Advisory locks never conflict between different streams and
// are released automatically at COMMIT/ROLLBACK.
func lockStream(ctx context.Context, tx pgx.Tx, streamID string) error {
	h := fnv.New64a()
	_, _ = h.Write([]byte(streamID))
	key := int64(h.Sum64())
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", key)
	return err
}

// runTx executes fn in a transaction.
func (s *Store) runTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	if err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// isUniqueViolation reports whether err is a PG unique_violation.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// foreignKeyViolation / checkViolation codes.
func isFKViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}

// rowScanner abstracts pgx.Row / pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanPlan(row rowScanner) (*plan.Plan, error) {
	var (
		p                        plan.Plan
		id, name, kind, distName string
		nLot                     *int
		n, c, n1, c1, r1, n2, c2 int
	)
	if err := row.Scan(&id, &name, &kind, &distName, &nLot,
		&n, &c, &n1, &c1, &r1, &n2, &c2); err != nil {
		return nil, err
	}
	p.ID, p.Name = id, name
	p.Kind = plan.Kind(kind)
	p.Distribution = plan.Distribution(distName)
	p.Approximate = p.Distribution == plan.DistPoisson
	p.N = nLot
	if p.Kind == plan.KindSingle {
		p.SampleSize, p.AcceptNumber = n, c
	} else {
		p.N1, p.C1, p.R1, p.N2, p.C2 = n1, c1, r1, n2, c2
	}
	return &p, nil
}

const planColumns = `id, name, kind, distribution, n_lot,
    n, c, n1, c1, r1, n2, c2`

func planInsertArgs(p *plan.Plan) []any {
	nLot := p.N
	singleN, singleC := 0, 0
	if p.Kind == plan.KindSingle {
		singleN, singleC = p.SampleSize, p.AcceptNumber
	}
	return []any{
		p.ID, p.Name, string(p.Kind), string(p.Distribution), nLot,
		singleN, singleC, p.N1, p.C1, p.R1, p.N2, p.C2,
	}
}
