package store

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"sampling-svc/internal/plan"
)

// revisionColumns is the stored content of one plan revision.
const revisionColumns = `plan_id, revision_no, effective_at, created_at,
    name, distribution, n_lot, n, c, n1, c1, r1, n2, c2`

func scanRevision(row rowScanner) (plan.Revision, error) {
	var (
		rv                       plan.Revision
		pid, name, distName      string
		nLot                     *int
		no                       int
		n, c, n1, c1, r1, n2, c2 int
		eff, created             time.Time
	)
	if err := row.Scan(&pid, &no, &eff, &created,
		&name, &distName, &nLot, &n, &c, &n1, &c1, &r1, &n2, &c2); err != nil {
		return plan.Revision{}, err
	}
	rv.PlanID, rv.Number, rv.EffectiveAt, rv.CreatedAt = pid, no, eff, created
	p := &plan.Plan{
		ID: pid, Name: name, Kind: "", Distribution: plan.Distribution(distName),
		Approximate: distName == string(plan.DistPoisson),
		N:           nLot,
	}
	// Kind is stored on plans, not per revision; the caller fills it in
	// when a full domain plan is required. setRevisionPlanKind patches it
	// after a second query in loadSlot.
	p.SampleSize, p.AcceptNumber = n, c
	p.N1, p.C1, p.R1, p.N2, p.C2 = n1, c1, r1, n2, c2
	rv.Plan = p
	return rv, nil
}

// revisionInsertArgs renders the INSERT argument list.
func revisionInsertArgs(rv plan.Revision) []any {
	p := rv.Plan
	nLot := p.N
	singleN, singleC := 0, 0
	if p.Kind == plan.KindSingle {
		singleN, singleC = p.SampleSize, p.AcceptNumber
	}
	return []any{
		rv.PlanID, rv.Number, rv.EffectiveAt.UTC(), rv.CreatedAt.UTC(),
		p.Name, string(p.Distribution), nLot,
		singleN, singleC, p.N1, p.C1, p.R1, p.N2, p.C2,
	}
}

// loadRevisions returns every revision of one plan ordered by
// (effective_at, revision_no).
func loadRevisions(ctx context.Context, q ctxQuerier, planID string) ([]plan.Revision, error) {
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
		rv, err := scanRevision(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rv)
	}
	return out, rows.Err()
}

// planKind fetches the immutable sampling kind of a plan.
func planKind(ctx context.Context, q ctxQuerier, planID string) (plan.Kind, error) {
	var k string
	err := q.QueryRow(ctx, `SELECT kind FROM plans WHERE id=$1`, planID).Scan(&k)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return plan.Kind(k), err
}

// advisoryKey derives a 64-bit advisory-lock key from a namespace byte
// and an id, so stream locks and plan locks never collide even if their
// id hashes do.
func advisoryKey(ns byte, id string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte{ns})
	_, _ = h.Write([]byte(id))
	return int64(h.Sum64())
}

// lockKey takes one transaction-scoped advisory lock by numeric key.
func lockKey(ctx context.Context, tx pgx.Tx, key int64) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", key)
	return err
}

// lockMany takes transaction-scoped advisory locks for the given keys in
// strictly ascending order. Revision-append transactions need the plan
// lock plus the locks of every affected stream; acquiring ALL locks of a
// transaction in one global order prevents deadlock cycles against batch
// writers (which take a single stream lock).
func lockMany(ctx context.Context, tx pgx.Tx, keys []int64) error {
	sorted := append([]int64(nil), keys...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, k := range sorted {
		if err := lockKey(ctx, tx, k); err != nil {
			return err
		}
	}
	return nil
}

// streamsReferencingPlan returns ids of streams that bind the plan in any
// slot.
func streamsReferencingPlan(ctx context.Context, q ctxQuerier, planID string) ([]string, error) {
	rows, err := q.Query(ctx, `
SELECT id FROM streams
WHERE normal_id=$1 OR tightened_id=$1 OR reduced_id=$1
ORDER BY id`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
