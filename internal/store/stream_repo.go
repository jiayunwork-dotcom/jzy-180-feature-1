package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"sampling-svc/internal/inspection"
	"sampling-svc/internal/plan"
)

// StreamRecord is the persisted stream header.
type StreamRecord struct {
	ID              string
	Name            string
	NormalID        string
	TightenedID     string
	ReducedID       string
	CurrentSeverity inspection.Severity
	CurrentScore    int
}

// BatchRecord is a batch event payload.
type BatchRecord struct {
	ID    string
	LotNo string
	At    time.Time
	D1    int
	HasD2 bool
	D2    int
}

// CreateStream inserts a stream after verifying the three bound plans
// exist and share one sampling kind (so every recorded lot is judged by
// structurally identical rules across severity tracks).
//
// The transaction takes the plan advisory lock of every bound plan: this
// serializes stream creation against a revision append, so an append that
// read the bound set before the new stream existed cannot commit before
// the binding (or vice versa) — every bound stream is always rebuilt.
func (s *Store) CreateStream(ctx context.Context, rec StreamRecord) error {
	return s.runTx(ctx, func(tx pgx.Tx) error {
		keys := []int64{
			advisoryKey(lockNSPlan, rec.NormalID),
			advisoryKey(lockNSPlan, rec.TightenedID),
			advisoryKey(lockNSPlan, rec.ReducedID),
		}
		if err := lockMany(ctx, tx, keys); err != nil {
			return err
		}
		plans := make(map[string]*plan.Plan, 3)
		for _, pid := range []string{rec.NormalID, rec.TightenedID, rec.ReducedID} {
			p, err := loadPlan(ctx, tx, pid)
			if err != nil {
				return err
			}
			plans[pid] = p
		}
		kinds := map[plan.Kind]bool{
			plans[rec.NormalID].Kind: true,
		}
		if !kinds[plans[rec.TightenedID].Kind] || !kinds[plans[rec.ReducedID].Kind] {
			return ErrConflict{Msg: "normal/tightened/reduced plans must all be single or all be double sampling"}
		}
		_, err := tx.Exec(ctx, `
INSERT INTO streams (id, name, normal_id, tightened_id, reduced_id)
VALUES ($1,$2,$3,$4,$5)`,
			rec.ID, rec.Name, rec.NormalID, rec.TightenedID, rec.ReducedID)
		if isUniqueViolation(err) {
			return ErrConflict{Msg: "stream name already exists: " + rec.Name}
		}
		if isFKViolation(err) {
			return ErrNotFound
		}
		return err
	})
}

// DeleteStream removes a stream and (cascade) all its events/results. It
// takes the stream advisory lock so it cannot run interleaved with a
// revision append that is rebuilding this very stream.
func (s *Store) DeleteStream(ctx context.Context, id string) error {
	var tag pgconn.CommandTag
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		if err := lockStream(ctx, tx, id); err != nil {
			return err
		}
		var e error
		tag, e = tx.Exec(ctx, `DELETE FROM streams WHERE id=$1`, id)
		return e
	})
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListStreams returns all stream headers.
func (s *Store) ListStreams(ctx context.Context) ([]StreamRecord, error) {
	rows, err := s.pool.Query(ctx, `
SELECT id, name, normal_id, tightened_id, reduced_id,
       current_severity, current_score
FROM streams ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StreamRecord
	for rows.Next() {
		var r StreamRecord
		var sev string
		if err := rows.Scan(&r.ID, &r.Name, &r.NormalID, &r.TightenedID,
			&r.ReducedID, &sev, &r.CurrentScore); err != nil {
			return nil, err
		}
		r.CurrentSeverity = inspection.Severity(sev)
		out = append(out, r)
	}
	return out, rows.Err()
}

// loadDomainStream loads a stream header plus the complete revision
// histories of its three plans and builds the replay domain object.
func loadDomainStream(ctx context.Context, q ctxQuerier, id string) (*inspection.Stream, *StreamRecord, error) {
	return loadDomainStreamWithCandidate(ctx, q, id, plan.Revision{})
}

// loadDomainStreamWithCandidate is like loadDomainStream but, when extra
// is a non-zero revision for one of the bound plans, injects it into that
// slot's history so the replay sees the candidate (dry-run conflict
// check before the revision row exists).
func loadDomainStreamWithCandidate(ctx context.Context, q ctxQuerier, id string, extra plan.Revision) (*inspection.Stream, *StreamRecord, error) {
	var rec StreamRecord
	var sev string
	err := q.QueryRow(ctx, `
SELECT id, name, normal_id, tightened_id, reduced_id,
       current_severity, current_score
FROM streams WHERE id=$1`, id).Scan(
		&rec.ID, &rec.Name, &rec.NormalID, &rec.TightenedID, &rec.ReducedID,
		&sev, &rec.CurrentScore)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	rec.CurrentSeverity = inspection.Severity(sev)

	loadSlot := func(pid string) (inspection.Slot, error) {
		slot, err := loadSlotTx(ctx, q, pid)
		if err != nil {
			return inspection.Slot{}, err
		}
		if extra.Plan != nil && extra.PlanID == pid {
			p := *extra.Plan
			p.ID = pid
			slot.Revisions = append(slot.Revisions, inspection.RevisionRef{
				Number: extra.Number, EffectiveAt: extra.EffectiveAt, Plan: &p,
			})
		}
		return slot, nil
	}
	normal, err := loadSlot(rec.NormalID)
	if err != nil {
		return nil, nil, err
	}
	tight, err := loadSlot(rec.TightenedID)
	if err != nil {
		return nil, nil, err
	}
	reduced, err := loadSlot(rec.ReducedID)
	if err != nil {
		return nil, nil, err
	}
	return &inspection.Stream{
		ID: id, Name: rec.Name,
		Normal: normal, Tightened: tight, Reduced: reduced,
	}, &rec, nil
}

// loadSlotTx loads a slot and stamps the plan kind onto every revision.
func loadSlotTx(ctx context.Context, q ctxQuerier, pid string) (inspection.Slot, error) {
	kind, err := planKind(ctx, q, pid)
	if err != nil {
		return inspection.Slot{}, err
	}
	revs, err := loadRevisions(ctx, q, pid)
	if err != nil {
		return inspection.Slot{}, err
	}
	refs := make([]inspection.RevisionRef, 0, len(revs))
	for _, rv := range revs {
		rv.Plan.Kind = kind
		refs = append(refs, inspection.RevisionRef{
			Number: rv.Number, EffectiveAt: rv.EffectiveAt, Plan: rv.Plan,
		})
	}
	return inspection.Slot{ID: pid, Revisions: refs}, nil
}

// loadEvents reads every event in deterministic (at, seq) order.
func loadEvents(ctx context.Context, q ctxQuerier, streamID string) ([]inspection.Event, error) {
	rows, err := q.Query(ctx, `
SELECT seq, at, kind, batch_id, lot_no, d1, has_d2, d2, flag, flag_value
FROM stream_events WHERE stream_id=$1
ORDER BY at ASC, seq ASC`, streamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []inspection.Event
	for rows.Next() {
		var (
			seq       int64
			at        time.Time
			kind      string
			batchID   *string
			lotNo     *string
			d1        *int
			hasD2     bool
			d2        *int
			flag      *string
			flagValue *bool
		)
		if err := rows.Scan(&seq, &at, &kind, &batchID, &lotNo,
			&d1, &hasD2, &d2, &flag, &flagValue); err != nil {
			return nil, err
		}
		ev := inspection.Event{Seq: seq, At: at}
		switch kind {
		case "batch":
			ev.Batch = &inspection.BatchInput{
				ID:    deref(batchID),
				LotNo: deref(lotNo),
				At:    at,
				D1:    derefInt(d1),
				HasD2: hasD2,
				D2:    derefInt(d2),
				Seq:   seq,
			}
		case "flag":
			ev.Flag = inspection.FlagName(deref(flag))
			ev.FlagValue = derefBool(flagValue)
		case "resume":
			ev.Resume = true
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
func derefInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}
func derefBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

// rebuildStream replays the full event history inside tx and atomically
// replaces the derived batch_results and current-state columns.
func rebuildStream(ctx context.Context, tx pgx.Tx, streamID string) (inspection.Snapshot, error) {
	snap, _, err := rebuildStreamDiagnose(ctx, tx, streamID, false)
	return snap, err
}

// rebuildChecked additionally fails the transaction when the resulting
// timeline contains lots the current revisions cannot judge. Used by
// batch add/update so a never-judgeable lot is rejected rather than
// silently rendered accepted/rejected.
func rebuildChecked(ctx context.Context, tx pgx.Tx, streamID string) (inspection.Snapshot, error) {
	snap, conflicts, err := rebuildStreamDiagnose(ctx, tx, streamID, true)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	if len(conflicts) > 0 {
		cf := conflicts[0]
		return inspection.Snapshot{}, plan.FieldError{
			Field: cf.Field, Message: cf.Message,
		}
	}
	return snap, nil
}

// rebuildStreamDiagnose is the shared rebuild core. When diagnose is set
// the diagnostic replay is used and the conflicts are returned (the
// derived rows are still written, letting the caller roll back after
// examining them).
func rebuildStreamDiagnose(ctx context.Context, tx pgx.Tx, streamID string, diagnose bool) (inspection.Snapshot, []inspection.BatchConflict, error) {
	dm, _, err := loadDomainStream(ctx, tx, streamID)
	if err != nil {
		return inspection.Snapshot{}, nil, err
	}
	events, err := loadEvents(ctx, tx, streamID)
	if err != nil {
		return inspection.Snapshot{}, nil, err
	}
	var snap inspection.Snapshot
	var conflicts []inspection.BatchConflict
	if diagnose {
		snap, conflicts, err = inspection.ReplayDiagnose(dm, events)
	} else {
		snap, err = inspection.Replay(dm, events)
	}
	if err != nil {
		return inspection.Snapshot{}, nil, err
	}
	if err := writeOutcomes(ctx, tx, streamID, events, snap); err != nil {
		return inspection.Snapshot{}, nil, err
	}
	return snap, conflicts, nil
}

// writeOutcomes replaces batch_results and updates the current-state
// columns from a freshly replayed snapshot.
func writeOutcomes(ctx context.Context, tx pgx.Tx, streamID string, events []inspection.Event, snap inspection.Snapshot) error {
	if _, err := tx.Exec(ctx,
		`DELETE FROM batch_results WHERE stream_id=$1`, streamID); err != nil {
		return err
	}
	if len(snap.Batches) > 0 {
		batch := &pgx.Batch{}
		seqByID := make(map[string]int64, len(events))
		for _, ev := range events {
			if ev.Batch != nil {
				seqByID[ev.Batch.ID] = ev.Seq
			}
		}
		for i := range snap.Batches {
			o := snap.Batches[i]
			var d2 any
			if o.D2 != nil {
				d2 = *o.D2
			}
			batch.Queue(`INSERT INTO batch_results
(stream_id, seq, batch_id, lot_no, at, severity, plan_id, plan_revision,
 plan_name, decision, accepted, d1, d2, score, note)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
				streamID, seqByID[o.BatchID], o.BatchID, o.LotNo, o.At,
				string(o.Severity), o.PlanID, o.PlanRevision, o.PlanName,
				string(o.Decision), o.Accepted, o.D1, d2, o.Score, o.Note)
		}
		br := tx.SendBatch(ctx, batch)
		for range snap.Batches {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return err
			}
		}
		br.Close()
	}
	_, err := tx.Exec(ctx, `
UPDATE streams SET current_severity=$2, current_score=$3, updated_at=now()
WHERE id=$1`, streamID, string(snap.Current.Severity), snap.Current.ScoreValue())
	return err
}

// GetSnapshot returns current state and ordered per-batch outcomes from
// the derived tables.
func (s *Store) GetSnapshot(ctx context.Context, streamID string) (inspection.Snapshot, error) {
	rec, err := s.streamHeader(ctx, streamID)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	outcomes, err := loadOutcomes(ctx, s.pool, streamID)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	return inspection.Snapshot{
		StreamID: streamID,
		Current:  inspection.StateFrom(rec.CurrentSeverity, rec.CurrentScore),
		Batches:  outcomes,
	}, nil
}

// outcomeColumns is the per-batch derived projection shared by reads.
const outcomeColumns = `batch_id, lot_no, at, severity, plan_id, plan_revision,
       plan_name, decision, accepted, d1, d2, score, note`

// loadOutcomes reads derived batch outcomes in (at, seq) order.
func loadOutcomes(ctx context.Context, q ctxQuerier, streamID string) ([]inspection.BatchOutcome, error) {
	rows, err := q.Query(ctx, `
SELECT `+outcomeColumns+`
FROM batch_results WHERE stream_id=$1
ORDER BY at ASC, seq ASC`, streamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []inspection.BatchOutcome
	for rows.Next() {
		var (
			o                                   inspection.BatchOutcome
			bid, lot, sev, pid, pname, decision string
			at                                  time.Time
			d2                                  *int
			note                                string
		)
		if err := rows.Scan(&bid, &lot, &at, &sev, &pid, &o.PlanRevision,
			&pname, &decision, &o.Accepted, &o.D1, &d2, &o.Score, &note); err != nil {
			return nil, err
		}
		o.BatchID, o.LotNo, o.At = bid, lot, at
		o.Severity = inspection.Severity(sev)
		o.PlanID, o.PlanName = pid, pname
		o.Decision = inspection.Decision(decision)
		o.D2 = d2
		o.Note = note
		out = append(out, o)
	}
	return out, rows.Err()
}

func (s *Store) loadStream(ctx context.Context, id string) (*inspection.Stream, *StreamRecord, error) {
	return loadDomainStream(ctx, s.pool, id)
}

// streamHeader fetches just the persisted stream header.
func (s *Store) streamHeader(ctx context.Context, id string) (StreamRecord, error) {
	var rec StreamRecord
	var sev string
	err := s.pool.QueryRow(ctx, `
SELECT id, name, normal_id, tightened_id, reduced_id,
       current_severity, current_score
FROM streams WHERE id=$1`, id).Scan(
		&rec.ID, &rec.Name, &rec.NormalID, &rec.TightenedID, &rec.ReducedID,
		&sev, &rec.CurrentScore)
	if errors.Is(err, pgx.ErrNoRows) {
		return StreamRecord{}, ErrNotFound
	}
	if err != nil {
		return StreamRecord{}, err
	}
	rec.CurrentSeverity = inspection.Severity(sev)
	return rec, nil
}
