package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"sampling-svc/internal/plan"
)

// CreatePlan inserts a new plan together with its initial revision
// (revision 1), effective at plan creation so it covers every point in
// history. The plan header and the revision row are written in one
// transaction.
func (s *Store) CreatePlan(ctx context.Context, p *plan.Plan) error {
	return s.runTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
INSERT INTO plans (id, name, kind, distribution, n_lot, n, c, n1, c1, r1, n2, c2)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			planInsertArgs(p)...)
		if isUniqueViolation(err) {
			return ErrConflict{Msg: "plan name already exists: " + p.Name}
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
INSERT INTO plan_revisions
(plan_id, revision_no, effective_at, distribution, n_lot, n, c, n1, c1, r1, n2, c2)
VALUES ($1,1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			p.ID, historyEpoch(), string(p.Distribution), p.N,
			singleN(p), singleC(p), p.N1, p.C1, p.R1, p.N2, p.C2)
		return err
	})
}

// historyEpoch is the inclusive effective time of every plan's revision
// 1: the earliest representable instant, so the initial revision covers
// the ENTIRE timeline (including back-dated lots older than plan
// creation). Back-dated revisions from explicit clients are not
// constrained, but the system's own baseline never lands in the middle.
func historyEpoch() time.Time { return time.Time{} }

// planColumnsEscaped is the same list as planColumns but safe inside the
// INSERT column list (identical here; kept separate for readability).
func planColumnsEscaped() string {
	return planColumns
}

// UpdatePlan implements the legacy update entry point as "append an
// immediately-effective revision carrying the new header name". It never
// overwrites history and never reaches before now. Old clients keep
// working unchanged.
func (s *Store) UpdatePlan(ctx context.Context, p *plan.Plan) error {
	now := timeNow()
	_, err := s.AppendRevision(ctx, p.ID, p, nil, &now)
	return err
}

// DeletePlan removes a plan; RESTRICT on streams prevents deleting one in
// use by a stream.
func (s *Store) DeletePlan(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM plans WHERE id=$1`, id)
	if isFKViolation(err) {
		return ErrConflict{Msg: "plan is referenced by an inspection stream and cannot be deleted"}
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// GetPlan fetches by id. The returned plan reflects the currently
// effective revision (the plans header mirror).
func (s *Store) GetPlan(ctx context.Context, id string) (*plan.Plan, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+planColumns+` FROM plans WHERE id=$1`, id)
	p, err := scanPlan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}

// GetPlanByName fetches by unique name.
func (s *Store) GetPlanByName(ctx context.Context, name string) (*plan.Plan, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+planColumns+` FROM plans WHERE name=$1`, name)
	p, err := scanPlan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}

// ListPlans returns all plans ordered by name.
func (s *Store) ListPlans(ctx context.Context) ([]*plan.Plan, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+planColumns+` FROM plans ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*plan.Plan
	for rows.Next() {
		p, err := scanPlan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// PlanInUse reports whether a plan id is bound to any stream.
func (s *Store) PlanInUse(ctx context.Context, id string) (bool, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
SELECT count(*) FROM streams
WHERE normal_id=$1 OR tightened_id=$1 OR reduced_id=$1`, id).Scan(&n)
	return n > 0, err
}
