package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

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
func (s *Store) CreateStream(ctx context.Context, rec StreamRecord) error {
	plans := make(map[string]*plan.Plan, 3)
	for _, pid := range []string{rec.NormalID, rec.TightenedID, rec.ReducedID} {
		p, err := s.GetPlan(ctx, pid)
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
	_, err := s.pool.Exec(ctx, `
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
}

// DeleteStream removes a stream and (cascade) all its events/results.
func (s *Store) DeleteStream(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM streams WHERE id=$1`, id)
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

// loadDomainStream loads a stream header plus the full revision history
// of its three bound plans and builds the replay domain object.
func loadDomainStream(ctx context.Context, q ctxQuerier, id string) (*inspection.Stream, *StreamRecord, error) {
	return loadDomainStreamWithPending(ctx, q, id)
}

// loadDomainStreamWithPending is loadDomainStream with an optional
// not-yet-inserted revision overlaid (used inside AppendRevision to
// validate streams against the history AS IT WILL BE once committed).
func loadDomainStreamWithPending(ctx context.Context, q ctxQuerier, id string,
	pending ...pendingRevision) (*inspection.Stream, *StreamRecord, error) {

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

	ref := func(pid string) (inspection.Ref, error) {
		r, err := loadRef(ctx, q, pid)
		if err != nil {
			return inspection.Ref{}, err
		}
		return r, nil
	}
	normal, err := ref(rec.NormalID)
	if err != nil {
		return nil, nil, err
	}
	for i := range pending {
		if pending[i].planID == rec.NormalID {
			normal = overlayPending(normal, pending[i])
		}
	}
	tight, err := ref(rec.TightenedID)
	if err != nil {
		return nil, nil, err
	}
	for _, pn := range pending {
		if pn.planID == rec.TightenedID {
			tight = overlayPending(tight, pn)
		}
	}
	reduced, err := ref(rec.ReducedID)
	if err != nil {
		return nil, nil, err
	}
	for _, pn := range pending {
		if pn.planID == rec.ReducedID {
			reduced = overlayPending(reduced, pn)
		}
	}
	return &inspection.Stream{
		ID: id, Name: rec.Name,
		Normal: normal, Tightened: tight, Reduced: reduced,
	}, &rec, nil
}

// loadRef loads one plan's header and ordered revision history.
func loadRef(ctx context.Context, q ctxQuerier, pid string) (inspection.Ref, error) {
	var name, kind string
	err := q.QueryRow(ctx, `SELECT name, kind FROM plans WHERE id=$1`, pid).
		Scan(&name, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return inspection.Ref{}, ErrNotFound
	}
	if err != nil {
		return inspection.Ref{}, err
	}
	revs, err := loadRevisions(ctx, q, pid, name, kind)
	if err != nil {
		return inspection.Ref{}, err
	}
	// Current effective plan for the degenerate Plan field.
	cur, err := loadPlan(ctx, q, pid)
	if err != nil {
		return inspection.Ref{}, err
	}
	return inspection.Ref{ID: pid, Plan: cur, Revisions: revs}, nil
}

// overlayPending inserts a provisional revision into a ref's history at
// its ordered position (it may sit between existing revisions).
func overlayPending(ref inspection.Ref, p pendingRevision) inspection.Ref {
	pl := *p.plan
	pl.ID = p.planID
	rv := plan.Revision{
		PlanID: p.planID, No: p.no, EffectiveAt: p.at, Plan: &pl,
	}
	out := append(append([]plan.Revision(nil), ref.Revisions...), rv)
	// Resolution order: effective_at asc, revision_no asc.
	insertionSortRevs(out)
	ref.Revisions = out
	return ref
}

func insertionSortRevs(a []plan.Revision) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0; j-- {
			if !revLess(a[j-1], a[j]) {
				a[j-1], a[j] = a[j], a[j-1]
			} else {
				break
			}
		}
	}
}

func revLess(a, b plan.Revision) bool {
	if !a.EffectiveAt.Equal(b.EffectiveAt) {
		return a.EffectiveAt.Before(b.EffectiveAt)
	}
	return a.No < b.No
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
	dm, _, err := loadDomainStream(ctx, tx, streamID)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	events, err := loadEvents(ctx, tx, streamID)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	snap, err := inspection.Replay(dm, events)
	if err != nil {
		return inspection.Snapshot{}, err
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM batch_results WHERE stream_id=$1`, streamID); err != nil {
		return inspection.Snapshot{}, err
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
			var d2, revNo any
			if o.D2 != nil {
				d2 = *o.D2
			}
			if o.RevisionNo != nil {
				revNo = *o.RevisionNo
			}
			batch.Queue(`INSERT INTO batch_results
(stream_id, seq, batch_id, lot_no, at, severity, plan_id, plan_name,
 revision_no, decision, accepted, d1, d2, score, note)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
				streamID, seqByID[o.BatchID], o.BatchID, o.LotNo, o.At,
				string(o.Severity), o.PlanID, o.PlanName, revNo,
				string(o.Decision), o.Accepted, o.D1, d2, o.Score, o.Note)
		}
		br := tx.SendBatch(ctx, batch)
		for range snap.Batches {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return inspection.Snapshot{}, err
			}
		}
		br.Close()
	}
	if _, err := tx.Exec(ctx, `
UPDATE streams SET current_severity=$2, current_score=$3, updated_at=now()
WHERE id=$1`, streamID, string(snap.Current.Severity), snap.Current.ScoreValue()); err != nil {
		return inspection.Snapshot{}, err
	}
	return snap, nil
}

// readSnapshotTx reads the derived snapshot using an existing tx.
func readSnapshotTx(ctx context.Context, tx pgx.Tx, streamID string) (inspection.Snapshot, error) {
	var sev string
	var score int
	if err := tx.QueryRow(ctx, `
SELECT current_severity, current_score FROM streams WHERE id=$1`, streamID).
		Scan(&sev, &score); err != nil {
		return inspection.Snapshot{}, err
	}
	rows, err := tx.Query(ctx, `
SELECT batch_id, lot_no, at, severity, plan_id, plan_name, revision_no,
       decision, accepted, d1, d2, score, note
FROM batch_results WHERE stream_id=$1
ORDER BY at ASC, seq ASC`, streamID)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	defer rows.Close()
	snap, err := scanSnapshotRows(streamID, rows)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	snap.Current = inspection.StateFrom(inspection.Severity(sev), score)
	return snap, nil
}

func scanSnapshotRows(streamID string, rows pgx.Rows) (inspection.Snapshot, error) {
	snap := inspection.Snapshot{StreamID: streamID}
	for rows.Next() {
		var (
			o                                   inspection.BatchOutcome
			bid, lot, sev, pid, pname, decision string
			at                                  time.Time
			d2, revNo                           *int
			note                                string
		)
		if err := rows.Scan(&bid, &lot, &at, &sev, &pid, &pname, &revNo,
			&decision, &o.Accepted, &o.D1, &d2, &o.Score, &note); err != nil {
			return inspection.Snapshot{}, err
		}
		o.BatchID, o.LotNo, o.At = bid, lot, at
		o.Severity = inspection.Severity(sev)
		o.PlanID, o.PlanName = pid, pname
		o.RevisionNo = revNo
		o.Decision = inspection.Decision(decision)
		o.D2 = d2
		o.Note = note
		snap.Batches = append(snap.Batches, o)
	}
	if err := rows.Err(); err != nil {
		return inspection.Snapshot{}, err
	}
	return snap, nil
}

// GetSnapshot returns current state and ordered per-batch outcomes from
// the derived tables.
func (s *Store) GetSnapshot(ctx context.Context, streamID string) (inspection.Snapshot, error) {
	rec, err := s.streamHeader(ctx, streamID)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT batch_id, lot_no, at, severity, plan_id, plan_name, revision_no,
       decision, accepted, d1, d2, score, note
FROM batch_results WHERE stream_id=$1
ORDER BY at ASC, seq ASC`, streamID)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	defer rows.Close()
	snap, err := scanSnapshotRows(streamID, rows)
	if err != nil {
		return inspection.Snapshot{}, err
	}
	snap.Current = inspection.StateFrom(rec.CurrentSeverity, rec.CurrentScore)
	return snap, nil
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
