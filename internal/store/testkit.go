package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewIsolatedTestStore is a test-only helper (kept out of _test.go so the
// httpapi package's tests can use it) that creates an isolated schema,
// runs the migration into it and drops the schema on test cleanup.
//
// Separate test binaries therefore never see each other's data and can
// run in parallel against the same PostgreSQL instance.
func NewIsolatedTestStore(t *testing.T, dsn, schema string) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for schema setup: %v", err)
	}
	if _, err := admin.Exec(ctx,
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE; CREATE SCHEMA %s`,
			schema, schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	_ = admin.Close(ctx)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := &Store{pool: pool}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		cCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_, _ = pool.Exec(cCtx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		st.Close()
	})
	return st
}
