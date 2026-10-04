package store

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"sampling-svc/internal/inspection"
	"sampling-svc/internal/plan"
)

// ErrRevisionKind marks a revision that tries to change a plan's sampling
// kind (single<->double), which is never allowed: severity slots and
// recorded first/second-stage counts are interpreted per kind.
type ErrRevisionKind struct{ Msg string }

func (e ErrRevisionKind) Error() string { return e.Msg }

// AppendRevision atomically appends one immutable revision to a plan and
// rebuilds every stream bound to that plan so the stored derived state
// equals a fresh replay of all events under the full revision history.
//
// effectiveAt:
//   - nil  -> the revision takes effect immediately (now);
//   - set  -> it may be dated in the future or back-dated into the past,
//     potentially between two existing revisions.
//
// Before the history is written, every affected stream is replayed with
// the proposed history; if any recorded lot can no longer be judged (its
// counts exceed the effective revision's sample size, ...), the whole
// revision is rejected and the offending lots are returned, one by one.
//
// Concurrency: the transaction takes the plan lock and then the advisory
// lock of EVERY affected stream in ascending id order. Batch writers take
// only their stream lock. The ordering plan-lock-before-stream-lock is
// fixed, so no lock cycle (deadlock) is possible; unrelated streams are
// never blocked.
func (s *Store) AppendRevision(ctx context.Context, planID string, p *plan.Plan,
	rename *string, effectiveAt *time.Time) (RevisionResult, error) {

	var res RevisionResult
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		if err := lockPlan(ctx, tx, planID); err != nil {
			return err
		}
		// Header row.
		var oldName, oldKind string
		err := tx.QueryRow(ctx,
			`SELECT name, kind FROM plans WHERE id=$1 FOR UPDATE`, planID).
			Scan(&oldName, &oldKind)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if string(p.Kind) != oldKind {
			return ErrRevisionKind{Msg: "a revision must keep the plan's kind (" + oldKind + ")"}
		}
		newName := oldName
		if rename != nil && *rename != "" {
			newName = *rename
		}
		p.ID = planID
		p.Name = newName
		p.Kind = plan.Kind(oldKind)

		no, err := nextRevisionNo(ctx, tx, planID)
		if err != nil {
			return err
		}
		at := time.Now().UTC()
		if effectiveAt != nil {
			at = effectiveAt.UTC()
		}

		// Find affected streams before touching anything, sorted for a
		// deterministic diff and lock order.
		streamIDs, err := streamsUsingPlan(ctx, tx, planID)
		if err != nil {
			return err
		}
		sort.Strings(streamIDs)
		// Lock every affected stream NOW (already ascending) while
		// holding the plan lock. This serializes the revision append
		// against concurrent batch writers for the whole
		// validate -> snapshot -> rebuild sequence, so the diff's
		// before-image and the rebuilt after-image bracket no other
		// write. Lock order is always plan -> stream(asc); a batch
		// writer takes only a stream lock, so no wait cycle exists.
		for _, sid := range streamIDs {
			if err := lockStream(ctx, tx, sid); err != nil {
				return err
			}
		}

		// Provisional histories: validate (unjudgeable lots) against the
		// revision history as it would be AFTER this append, before any
		// write.
		type candidate struct {
			id, name string
			dm       *inspection.Stream
			events   []inspection.Event
		}
		cands := make([]candidate, 0, len(streamIDs))
		var conflicts []inspection.BatchConflict
		for _, sid := range streamIDs {
			dm, header, err := loadDomainStreamWithPending(ctx, tx, sid, pendingRevision{
				planID: planID, no: no, at: at, plan: p,
			})
			if err != nil {
				return err
			}
			events, err := loadEvents(ctx, tx, sid)
			if err != nil {
				return err
			}
			// STRICT validation over the provisional history.
			if _, cs, err := inspection.ReplayChecked(dm, events); err != nil {
				return err
			} else if len(cs) > 0 {
				conflicts = append(conflicts, cs...)
			}
			cands = append(cands, candidate{id: sid, name: header.Name, dm: dm, events: events})
		}
		if len(conflicts) > 0 {
			return ErrRevisionConflicts{Conflicts: conflicts}
		}

		// Capture stored (pre-append) per-batch results for the diff,
		// BEFORE overwriting them.
		storedBefore := make(map[string]inspection.Snapshot, len(cands))
		for _, c := range cands {
			snap, err := readSnapshotTx(ctx, tx, c.id)
			if err != nil {
				return err
			}
			storedBefore[c.id] = snap
		}

		// Commit the history + header mirror.
		if _, err := tx.Exec(ctx, `
INSERT INTO plan_revisions
(plan_id, revision_no, effective_at, distribution, n_lot, n, c, n1, c1, r1, n2, c2)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			revisionInsertArgs(planID, no, at, p)...); err != nil {
			if isUniqueViolation(err) {
				return ErrConflict{Msg: "revision already exists"}
			}
			return err
		}
		if _, err := tx.Exec(ctx, `
UPDATE plans SET name=$2, distribution=$3, n_lot=$4, n=$5, c=$6,
    n1=$7, c1=$8, r1=$9, n2=$10, c2=$11, updated_at=now()
WHERE id=$1`,
			planID, newName, string(p.Distribution), p.N,
			singleN(p), singleC(p), p.N1, p.C1, p.R1, p.N2, p.C2); err != nil {
			return err
		}

		// Rebuild every affected stream (locks already held).
		res = RevisionResult{RevisionNo: no, EffectiveAt: at}
		for _, c := range cands {
			after, err := rebuildStream(ctx, tx, c.id)
			if err != nil {
				return err
			}
			diff := diffSnapshots(c.name, storedBefore[c.id], after)
			res.Streams = append(res.Streams, diff)
		}
		return nil
	})
	if err != nil {
		return RevisionResult{}, mapRevisionErr(err)
	}
	return res, nil
}

// RevisionResult reports the appended revision and the per-stream change
// report (possibly all-empty — a change with no derived effect is still
// stated explicitly).
type RevisionResult struct {
	RevisionNo  int          `json:"revision_no"`
	EffectiveAt time.Time    `json:"effective_at"`
	Streams     []StreamDiff `json:"streams"`
}

// StreamDiff is one affected stream's before/after report.
type StreamDiff struct {
	StreamID        string       `json:"stream_id"`
	Name            string       `json:"name"`
	Changed         bool         `json:"changed"`
	SeverityChanged *FieldChange `json:"severity,omitempty"`
	ScoreChanged    *FieldChange `json:"score,omitempty"`
	Batches         []BatchDiff  `json:"batches,omitempty"`
}

// FieldChange is a scalar before/after pair.
type FieldChange struct {
	From any `json:"from"`
	To   any `json:"to"`
}

// BatchDiff is one batch whose derived fields changed.
type BatchDiff struct {
	BatchID     string       `json:"batch_id"`
	LotNo       string       `json:"lot_no"`
	InspectedAt time.Time    `json:"inspected_at"`
	Severity    *FieldChange `json:"severity,omitempty"`
	Decision    *FieldChange `json:"decision,omitempty"`
	Accepted    *FieldChange `json:"accepted,omitempty"`
	Score       *FieldChange `json:"score,omitempty"`
	PlanID      *FieldChange `json:"plan_id,omitempty"`
	RevisionNo  *FieldChange `json:"revision_no,omitempty"`
}

// ErrRevisionConflicts is the rejection of a revision that leaves
// recorded lots unjudgeable.
type ErrRevisionConflicts struct {
	Conflicts []inspection.BatchConflict
}

func (e ErrRevisionConflicts) Error() string {
	return "revision rejected: recorded batch(es) cannot be judged under it"
}

func mapRevisionErr(err error) error {
	if err == nil {
		return nil
	}
	var rc ErrRevisionConflicts
	if errors.As(err, &rc) {
		return rc
	}
	var rk ErrRevisionKind
	if errors.As(err, &rk) {
		return plan.FieldError{Field: "kind", Message: "revisions cannot change the sampling kind"}
	}
	return err
}

// streamsUsingPlan lists stream ids bound to any of the three plan slots.
func streamsUsingPlan(ctx context.Context, tx pgx.Tx, planID string) ([]string, error) {
	rows, err := tx.Query(ctx, `
SELECT id FROM streams
WHERE normal_id=$1 OR tightened_id=$1 OR reduced_id=$1`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// pendingRevision is one not-yet-written revision to overlay while
// validating/replaying inside the append transaction.
type pendingRevision struct {
	planID string
	no     int
	at     time.Time
	plan   *plan.Plan
}

func timeNow() time.Time { return time.Now().UTC() }
