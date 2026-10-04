package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"sampling-svc/internal/plan"
)

// lockPlan takes a transaction-scoped advisory lock for a plan id. It
// hashes into the same single-argument key space as stream locks but
// mixes the key with a fixed salt; plan keys and stream keys are never
// allocated at runtime in the other's context (a revision append takes
// the plan lock then stream locks), and hash collisions merely mean an
// unrelated resource waits briefly — never a correctness problem.
func lockPlan(ctx context.Context, tx pgx.Tx, planID string) error {
	h := fnvHash("plan-lock:" + planID)
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", h)
	return err
}

// revisionColumns lists the parameter columns of a revision row.
const revisionColumns = `plan_id, revision_no, effective_at, distribution,
    n_lot, n, c, n1, c1, r1, n2, c2`

func scanRevision(row rowScanner, headerName string) (plan.Revision, error) {
	var (
		pid, distName string
		no            int
		at            time.Time
		nLot          *int
		n, c          int
		n1, c1, r1    int
		n2, c2        int
	)
	if err := row.Scan(&pid, &no, &at, &distName, &nLot,
		&n, &c, &n1, &c1, &r1, &n2, &c2); err != nil {
		return plan.Revision{}, err
	}
	pl := revisionToPlan(pid, headerName, distName, nLot, n, c, n1, c1, r1, n2, c2)
	pl.RevisionNo = no
	pl.RevisionEffectiveAt = at
	return plan.Revision{PlanID: pid, No: no, EffectiveAt: at, Plan: pl}, nil
}

func revisionToPlan(id, name, distName string, nLot *int,
	n, c, n1, c1, r1, n2, c2 int) *plan.Plan {
	p := &plan.Plan{
		ID: id, Name: name,
		Distribution: plan.Distribution(distName),
		N:            nLot,
	}
	p.Approximate = p.Distribution == plan.DistPoisson
	// Kind is resolved from the populated parameters; the loader sets it
	// from the header afterwards. Single plans store n/c; doubles store
	// n1..c2. The loader always knows the header kind, so prefer context.
	_ = n
	p.SampleSize, p.AcceptNumber = n, c
	p.N1, p.C1, p.R1, p.N2, p.C2 = n1, c1, r1, n2, c2
	return p
}

// loadRevisions reads one plan's full revision history ordered by
// (effective_at, revision_no), the resolution order of Ref.At.
func loadRevisions(ctx context.Context, q ctxQuerier, planID string, headerName, kind string) ([]plan.Revision, error) {
	rows, err := q.Query(ctx, `
SELECT `+revisionColumns+`
FROM plan_revisions WHERE plan_id=$1
ORDER BY effective_at ASC, revision_no ASC`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []plan.Revision
	for rows.Next() {
		rv, err := scanRevision(rows, headerName)
		if err != nil {
			return nil, err
		}
		rv.Plan.Kind = plan.Kind(kind)
		out = append(out, rv)
	}
	return out, rows.Err()
}

// ListRevisions returns the public revision history of a plan.
func (s *Store) ListRevisions(ctx context.Context, planID string) ([]plan.Revision, error) {
	var name, kind string
	err := s.pool.QueryRow(ctx,
		`SELECT name, kind FROM plans WHERE id=$1`, planID).Scan(&name, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return loadRevisions(ctx, s.pool, planID, name, kind)
}

// GetRevision fetches one revision of a plan by revision number.
func (s *Store) GetRevision(ctx context.Context, planID string, no int) (*plan.Plan, error) {
	var name, kind string
	err := s.pool.QueryRow(ctx,
		`SELECT name, kind FROM plans WHERE id=$1`, planID).Scan(&name, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	row := s.pool.QueryRow(ctx, `
SELECT `+revisionColumns+`
FROM plan_revisions WHERE plan_id=$1 AND revision_no=$2`, planID, no)
	rv, err := scanRevision(row, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rv.Plan.Kind = plan.Kind(kind)
	return rv.Plan, nil
}

// nextRevisionNo allocates the gap-free next number under the plan lock.
func nextRevisionNo(ctx context.Context, tx pgx.Tx, planID string) (int, error) {
	var no int
	err := tx.QueryRow(ctx, `
SELECT COALESCE(MAX(revision_no),0)+1 FROM plan_revisions WHERE plan_id=$1`,
		planID).Scan(&no)
	return no, err
}

// revisionInsertArgs renders the bound args for an INSERT into
// plan_revisions.
func revisionInsertArgs(planID string, no int, at time.Time, p *plan.Plan) []any {
	return []any{
		planID, no, at.UTC(), string(p.Distribution), p.N,
		singleN(p), singleC(p), p.N1, p.C1, p.R1, p.N2, p.C2,
	}
}

func singleN(p *plan.Plan) int {
	if p.Kind == plan.KindSingle {
		return p.SampleSize
	}
	return 0
}

func singleC(p *plan.Plan) int {
	if p.Kind == plan.KindSingle {
		return p.AcceptNumber
	}
	return 0
}
