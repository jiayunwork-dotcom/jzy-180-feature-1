package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"sampling-svc/internal/plan"
)

// planColumnsEscaped is the same list as planColumns but named for use in
// an INSERT column list.
func planColumnsEscaped() string { return planColumns }

// CreatePlan inserts a new plan together with its initial revision,
// which covers all of history (effective at the plan epoch).
func (s *Store) CreatePlan(ctx context.Context, p *plan.Plan) error {
	return s.runTx(ctx, func(tx pgx.Tx) error {
		return createPlanTx(ctx, tx, p, plan.Epoch)
	})
}

// createPlanTx inserts the plan row and its initial revision effective at
// eff. Used directly for tests that simulate pre-revision data.
func createPlanTx(ctx context.Context, tx pgx.Tx, p *plan.Plan, eff time.Time) error {
	_, err := tx.Exec(ctx, `
INSERT INTO plans (`+planColumnsEscaped()+`)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		planInsertArgs(p)...)
	if isUniqueViolation(err) {
		return ErrConflict{Msg: "plan name already exists: " + p.Name}
	}
	if err != nil {
		return err
	}
	rv := plan.Revision{
		PlanID: p.ID, Number: 1, EffectiveAt: eff,
		CreatedAt: time.Now().UTC(), Plan: p,
	}
	_, err = tx.Exec(ctx, `
INSERT INTO plan_revisions
(plan_id, revision_no, effective_at, created_at,
 name, distribution, n_lot, n, c, n1, c1, r1, n2, c2)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		revisionInsertArgs(rv)...)
	return err
}

// UpdatePlan implements the legacy modify endpoint: it appends a new
// revision effective immediately and brings the convenience columns up
// to date. Earlier history is no longer overwritten. Same sampling kind
// as the existing plan is required.
func (s *Store) UpdatePlan(ctx context.Context, p *plan.Plan) error {
	_, err := s.AppendRevision(ctx, p.ID, plan.RevisionRequest{
		Request: plan.Request{
			Name:   p.Name,
			Kind:   string(p.Kind),
			Single: singleParamOf(p),
			Double: doubleParamOf(p),
		},
	}, time.Now().UTC())
	return err
}

func singleParamOf(p *plan.Plan) *plan.SingleParam {
	if p.Kind != plan.KindSingle {
		return nil
	}
	return &plan.SingleParam{
		N: p.N, SampleSize: p.SampleSize, AcceptNumber: p.AcceptNumber,
		Distribution: string(p.Distribution),
	}
}

func doubleParamOf(p *plan.Plan) *plan.DoubleParam {
	if p.Kind != plan.KindDouble {
		return nil
	}
	return &plan.DoubleParam{
		N: p.N, N1: p.N1, C1: p.C1, R1: p.R1, N2: p.N2, C2: p.C2,
		Distribution: string(p.Distribution),
	}
}

// DeletePlan removes a plan; RESTRICT on streams prevents deleting one in
// use by a stream. Its revision history cascades away. The plan advisory
// lock is taken so deletion cannot interleave with a concurrent revision
// append (a referenced plan is rejected by RESTRICT anyway).
func (s *Store) DeletePlan(ctx context.Context, id string) error {
	var tag pgconn.CommandTag
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		if err := lockKey(ctx, tx, advisoryKey(lockNSPlan, id)); err != nil {
			return err
		}
		var e error
		tag, e = tx.Exec(ctx, `DELETE FROM plans WHERE id=$1`, id)
		return e
	})
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

// currentPlanFrom resolves the table projection for the revision that is
// in force RIGHT NOW (greatest effective_at <= now, tie by revision
// number), so future-dated revisions do not show up as the current plan
// until their time comes. Kind lives on the plan row (immutable).
const currentPlanCols = `p.id, r.name, p.kind, r.distribution, r.n_lot,
    r.n, r.c, r.n1, r.c1, r.r1, r.n2, r.c2`

const currentPlanFrom = `
FROM plans p
JOIN LATERAL (
    SELECT name, distribution, n_lot, n, c, n1, c1, r1, n2, c2
    FROM plan_revisions
    WHERE plan_id = p.id AND effective_at <= now()
    ORDER BY effective_at DESC, revision_no DESC
    LIMIT 1
) r ON true`

// GetPlan fetches the currently-effective plan by id.
func (s *Store) GetPlan(ctx context.Context, id string) (*plan.Plan, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+currentPlanCols+currentPlanFrom+` WHERE p.id=$1`, id)
	p, err := scanPlan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}

// GetPlanByName fetches the currently-effective plan by unique name.
func (s *Store) GetPlanByName(ctx context.Context, name string) (*plan.Plan, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+currentPlanCols+currentPlanFrom+` WHERE r.name=$1`, name)
	p, err := scanPlan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}

// ListPlans returns all currently-effective plans ordered by name.
func (s *Store) ListPlans(ctx context.Context) ([]*plan.Plan, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+currentPlanCols+currentPlanFrom+` ORDER BY r.name`)
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

// ListRevisions returns the full revision history of a plan in
// chronological (effective_at, revision_no) order.
func (s *Store) ListRevisions(ctx context.Context, planID string) ([]plan.Revision, error) {
	kind, err := planKind(ctx, s.pool, planID)
	if err != nil {
		return nil, err
	}
	revs, err := loadRevisions(ctx, s.pool, planID)
	if err != nil {
		return nil, err
	}
	if len(revs) == 0 {
		return nil, ErrNotFound
	}
	for i := range revs {
		revs[i].Plan.Kind = kind
	}
	return revs, nil
}

// GetRevision fetches one revision by number.
func (s *Store) GetRevision(ctx context.Context, planID string, number int) (plan.Revision, error) {
	kind, err := planKind(ctx, s.pool, planID)
	if err != nil {
		return plan.Revision{}, err
	}
	row := s.pool.QueryRow(ctx, `
SELECT `+revisionColumns+`
FROM plan_revisions WHERE plan_id=$1 AND revision_no=$2`, planID, number)
	rv, err := scanRevision(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return plan.Revision{}, ErrNotFound
	}
	if err != nil {
		return plan.Revision{}, err
	}
	rv.Plan.Kind = kind
	return rv, nil
}
