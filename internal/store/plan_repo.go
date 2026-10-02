package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"sampling-svc/internal/plan"
)

// CreatePlan inserts a new plan. p.ID must be pre-assigned.
func (s *Store) CreatePlan(ctx context.Context, p *plan.Plan) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO plans (`+planColumnsEscaped()+`)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		planInsertArgs(p)...)
	if isUniqueViolation(err) {
		return ErrConflict{Msg: "plan name already exists: " + p.Name}
	}
	return err
}

// planColumnsEscaped is the same list as planColumns but safe inside the
// INSERT column list (identical here; kept separate for readability).
func planColumnsEscaped() string {
	return planColumns
}

// UpdatePlan overwrites all numeric fields of an existing plan.
func (s *Store) UpdatePlan(ctx context.Context, p *plan.Plan) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE plans SET name=$2, kind=$3, distribution=$4, n_lot=$5,
    n=$6, c=$7, n1=$8, c1=$9, r1=$10, n2=$11, c2=$12,
    updated_at=now()
WHERE id=$1`, planInsertArgs(p)...)
	if isUniqueViolation(err) {
		return ErrConflict{Msg: "plan name already exists: " + p.Name}
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
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

// GetPlan fetches by id.
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
