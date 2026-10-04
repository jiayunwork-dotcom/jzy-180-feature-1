package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"sampling-svc/internal/inspection"
	"sampling-svc/internal/plan"
)

// Advisory-lock namespaces keep plan locks and stream locks disjoint.
const (
	lockNSStream byte = 1
	lockNSPlan   byte = 2
)

// BatchDiff is one per-batch change produced by appending a revision.
type BatchDiff struct {
	BatchID  string               `json:"batch_id"`
	LotNo    string               `json:"lot_no"`
	At       time.Time            `json:"inspected_at"`
	Severity *fieldChange[string] `json:"severity,omitempty"`
	Decision *fieldChange[string] `json:"decision,omitempty"`
	Score    *fieldChange[int]    `json:"score,omitempty"`
	Plan     *planChange          `json:"plan,omitempty"`
}

type fieldChange[T comparable] struct {
	From T `json:"from"`
	To   T `json:"to"`
}

type planChange struct {
	PlanID       string `json:"plan_id"`
	FromRevision int    `json:"from_revision"`
	ToRevision   int    `json:"to_revision"`
}

// StreamDiff summarizes the recomputation of one affected stream.
type StreamDiff struct {
	StreamID string      `json:"stream_id"`
	Name     string      `json:"name"`
	Batches  []BatchDiff `json:"batches"`
}

// RevisionResult is returned by AppendRevision: the new revision and the
// complete diff over every bound stream.
type RevisionResult struct {
	Revision plan.Revision `json:"revision"`
	// Streams is empty when no stream binds the plan; each bound stream
	// appears even when no batch changed.
	Streams []StreamDiff `json:"streams"`
}

// RevisionConflictError aggregates every lot the candidate revision
// history could no longer judge.
type RevisionConflictError struct {
	PlanID    string
	Revision  int
	Conflicts []inspection.BatchConflict
}

func (e *RevisionConflictError) Error() string {
	return "revision cannot judge one or more recorded batches"
}

// AppendRevision validates the candidate content, inserts an immutable
// revision effective at the given time (future or backdated), then
// synchronously replays every stream bound to the plan in the SAME
// transaction. The whole operation is atomic: until it commits, readers
// see neither the new revision nor any partially rebuilt stream; on
// conflict the transaction rolls back and nothing changes.
func (s *Store) AppendRevision(ctx context.Context, planID string, req plan.RevisionRequest, now time.Time) (RevisionResult, error) {
	p, err := plan.Build(req.Request)
	if err != nil {
		return RevisionResult{}, err
	}
	eff := plan.EffectiveAtOf(&req, now)

	var result RevisionResult
	err = s.runTx(ctx, func(tx pgx.Tx) error {
		// 1. Current plan: must exist and keep its sampling kind.
		var curName, curKind string
		err := tx.QueryRow(ctx,
			`SELECT name, kind FROM plans WHERE id=$1`, planID).
			Scan(&curName, &curKind)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if string(p.Kind) != curKind {
			return plan.FieldError{Field: "kind",
				Message: "a revision must keep the plan's sampling kind ('" + curKind + "')"}
		}

		// 2. Lock the plan and every currently bound stream in one global
		//    key order, so this transaction can never deadlock against a
		//    single-stream batch writer (which holds exactly one stream
		//    lock). Stream creation/deletion takes the same plan lock.
		boundBefore, err := streamsReferencingPlan(ctx, tx, planID)
		if err != nil {
			return err
		}
		keys := make([]int64, 0, len(boundBefore)+1)
		keys = append(keys, advisoryKey(lockNSPlan, planID))
		for _, sid := range boundBefore {
			keys = append(keys, advisoryKey(lockNSStream, sid))
		}
		if err := lockMany(ctx, tx, keys); err != nil {
			return err
		}

		// Under the plan lock the bound set is stable against concurrent
		// CreateStream (which acquires the same plan lock before binding).
		affected, err := streamsReferencingPlan(ctx, tx, planID)
		if err != nil {
			return err
		}

		// Allocate the revision number only now that concurrent appenders
		// are serialized on the plan lock.
		var nextNo int
		if err := tx.QueryRow(ctx, `
SELECT COALESCE(MAX(revision_no),0)+1 FROM plan_revisions WHERE plan_id=$1`,
			planID).Scan(&nextNo); err != nil {
			return err
		}

		// 3. Build the candidate revision history and, for each bound
		//    stream, dry-run the replay BEFORE writing anything.
		candidate := *p
		candidate.ID = planID
		candRev := plan.Revision{
			PlanID: planID, Number: nextNo, EffectiveAt: eff,
			CreatedAt: now.UTC(), Plan: &candidate,
		}
		for _, sid := range affected {
			dm, _, err := loadDomainStreamWithCandidate(ctx, tx, sid, candRev)
			if err != nil {
				return err
			}
			events, err := loadEvents(ctx, tx, sid)
			if err != nil {
				return err
			}
			_, conflicts, err := inspection.ReplayDiagnose(dm, events)
			if err != nil {
				return err
			}
			if len(conflicts) > 0 {
				return &RevisionConflictError{
					PlanID: planID, Revision: nextNo, Conflicts: conflicts,
				}
			}
		}

		// 4. Persist the revision. Duplicate effective_at is rejected by
		//    the unique constraint (clients must pick another instant).
		if _, err := tx.Exec(ctx, `
INSERT INTO plan_revisions
(plan_id, revision_no, effective_at, created_at,
 name, distribution, n_lot, n, c, n1, c1, r1, n2, c2)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
			revisionInsertArgs(candRev)...); err != nil {
			if isUniqueViolation(err) {
				return ErrConflict{Msg: "a revision with this effective_at already exists for this plan"}
			}
			return err
		}

		// 5. The convenience "current plan" columns describe the revision
		//    that is effective latest (greatest effective_at, ties broken
		//    by revision number). A backdated revision inserted between
		//    two existing ones therefore does not change the current view.
		var curContent struct {
			name, dist               string
			nLot                     *int
			n, c, n1, c1, r1, n2, c2 int
		}
		row := tx.QueryRow(ctx, `
SELECT name, distribution, n_lot, n, c, n1, c1, r1, n2, c2
FROM plan_revisions
WHERE plan_id=$1
ORDER BY effective_at DESC, revision_no DESC
LIMIT 1`, planID)
		if err := row.Scan(&curContent.name, &curContent.dist, &curContent.nLot,
			&curContent.n, &curContent.c, &curContent.n1, &curContent.c1,
			&curContent.r1, &curContent.n2, &curContent.c2); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
UPDATE plans SET name=$2, distribution=$3, n_lot=$4, n=$5, c=$6,
    n1=$7, c1=$8, r1=$9, n2=$10, c2=$11, updated_at=now()
WHERE id=$1`,
			planID, curContent.name, curContent.dist, curContent.nLot,
			curContent.n, curContent.c, curContent.n1, curContent.c1,
			curContent.r1, curContent.n2, curContent.c2); err != nil {
			return err
		}

		result.Revision = candRev
		result.Revision.Plan = &candidate

		// 6. Rebuild every bound stream and collect the diff.
		result.Streams = []StreamDiff{}
		for _, sid := range affected {
			diff, err := rebuildStreamWithDiff(ctx, tx, sid)
			if err != nil {
				return err
			}
			result.Streams = append(result.Streams, diff)
		}
		return nil
	})
	if err != nil {
		return RevisionResult{}, mapWriteErr(err)
	}
	return result, nil
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

// rebuildStreamWithDiff rebuilds derived rows (like rebuildStream) but
// first reads the existing outcomes and returns a per-batch diff.
func rebuildStreamWithDiff(ctx context.Context, tx pgx.Tx, streamID string) (StreamDiff, error) {
	before, err := loadOutcomes(ctx, tx, streamID)
	if err != nil {
		return StreamDiff{}, err
	}
	snap, err := rebuildStream(ctx, tx, streamID)
	if err != nil {
		return StreamDiff{}, err
	}
	var name string
	if err := tx.QueryRow(ctx, `SELECT name FROM streams WHERE id=$1`, streamID).
		Scan(&name); err != nil {
		return StreamDiff{}, err
	}
	diff := StreamDiff{StreamID: streamID, Name: name, Batches: []BatchDiff{}}
	after := snap.Batches

	oldByID := make(map[string]inspection.BatchOutcome, len(before))
	for _, o := range before {
		oldByID[o.BatchID] = o
	}
	newByID := make(map[string]inspection.BatchOutcome, len(after))
	for _, o := range after {
		newByID[o.BatchID] = o
	}
	// Iterate in post-rebuild order for a stable diff.
	for _, nw := range after {
		old, ok := oldByID[nw.BatchID]
		if !ok {
			continue // newly added batches are handled by batch mutations
		}
		bd := BatchDiff{BatchID: nw.BatchID, LotNo: nw.LotNo, At: nw.At}
		changed := false
		if old.Severity != nw.Severity {
			bd.Severity = &fieldChange[string]{From: string(old.Severity), To: string(nw.Severity)}
			changed = true
		}
		if old.Decision != nw.Decision {
			bd.Decision = &fieldChange[string]{From: string(old.Decision), To: string(nw.Decision)}
			changed = true
		}
		if old.Score != nw.Score {
			bd.Score = &fieldChange[int]{From: old.Score, To: nw.Score}
			changed = true
		}
		if old.PlanID != nw.PlanID || old.PlanRevision != nw.PlanRevision {
			bd.Plan = &planChange{
				PlanID:       nw.PlanID,
				FromRevision: old.PlanRevision,
				ToRevision:   nw.PlanRevision,
			}
			changed = true
		}
		if changed {
			diff.Batches = append(diff.Batches, bd)
		}
	}
	return diff, nil
}
