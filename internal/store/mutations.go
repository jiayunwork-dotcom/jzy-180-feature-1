package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"sampling-svc/internal/inspection"
)

// AddBatch inserts one lot, allocating the stream's next sequence number,
// then rebuilds derived state. Concurrent calls for the same stream
// serialize on its advisory lock; other streams proceed independently.
func (s *Store) AddBatch(ctx context.Context, streamID string, b BatchRecord) (inspection.Snapshot, error) {
	if b.LotNo == "" {
		return inspection.Snapshot{}, errors.New("lot_no must not be empty")
	}
	var snap inspection.Snapshot
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		if err := lockStream(ctx, tx, streamID); err != nil {
			return err
		}
		dm, _, err := loadDomainStream(ctx, tx, streamID)
		if err != nil {
			return err
		}
		in := &inspection.BatchInput{
			ID: b.ID, LotNo: b.LotNo, At: b.At.UTC(),
			D1: b.D1, HasD2: b.HasD2, D2: b.D2,
		}
		if err := inspection.ValidateBatch(dm, in); err != nil {
			return validationErr(err)
		}
		seq, err := nextSeq(ctx, tx, streamID)
		if err != nil {
			return err
		}
		if err := insertBatchEvent(ctx, tx, streamID, seq, in); err != nil {
			return err
		}
		snap, err = rebuildStream(ctx, tx, streamID)
		return err
	})
	return snap, mapWriteErr(err)
}

// UpdateBatch modifies an existing lot (lot_no / time / counts), keeping
// its original sequence number so arrival order is unchanged. After the
// edit the whole stream is rebuilt.
func (s *Store) UpdateBatch(ctx context.Context, streamID, batchID string, b BatchRecord) (inspection.Snapshot, error) {
	var snap inspection.Snapshot
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		if err := lockStream(ctx, tx, streamID); err != nil {
			return err
		}
		dm, _, err := loadDomainStream(ctx, tx, streamID)
		if err != nil {
			return err
		}
		in := &inspection.BatchInput{
			ID: batchID, LotNo: b.LotNo, At: b.At.UTC(),
			D1: b.D1, HasD2: b.HasD2, D2: b.D2,
		}
		if err := inspection.ValidateBatch(dm, in); err != nil {
			return validationErr(err)
		}
		tag, err := tx.Exec(ctx, `
UPDATE stream_events
SET lot_no=$3, at=$4, d1=$5, has_d2=$6, d2=$7
WHERE stream_id=$1 AND batch_id=$2 AND kind='batch'`,
			streamID, batchID, b.LotNo, b.At.UTC(), b.D1, b.HasD2, nullableD2(b))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		snap, err = rebuildStream(ctx, tx, streamID)
		return err
	})
	return snap, mapWriteErr(err)
}

// DeleteBatch removes one lot event and rebuilds.
func (s *Store) DeleteBatch(ctx context.Context, streamID, batchID string) (inspection.Snapshot, error) {
	var snap inspection.Snapshot
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		if err := lockStream(ctx, tx, streamID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
DELETE FROM stream_events WHERE stream_id=$1 AND batch_id=$2 AND kind='batch'`,
			streamID, batchID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		snap, err = rebuildStream(ctx, tx, streamID)
		return err
	})
	return snap, mapWriteErr(err)
}

// SetFlag records a stable/approved flag change (ordered at that moment)
// and rebuilds. Revoking "stable" while reduced immediately returns the
// stream to normal per the transition rules.
func (s *Store) SetFlag(ctx context.Context, streamID string, name inspection.FlagName, value bool, at time.Time) (inspection.Snapshot, error) {
	var snap inspection.Snapshot
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		if err := lockStream(ctx, tx, streamID); err != nil {
			return err
		}
		if _, _, err := loadDomainStream(ctx, tx, streamID); err != nil {
			return err
		}
		seq, err := nextSeq(ctx, tx, streamID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO stream_events (stream_id, seq, kind, at, flag, flag_value)
VALUES ($1,$2,'flag',$3,$4,$5)`,
			streamID, seq, at.UTC(), string(name), value); err != nil {
			return err
		}
		snap, err = rebuildStream(ctx, tx, streamID)
		return err
	})
	return snap, mapWriteErr(err)
}

// Resume records a manual resume from suspension.
func (s *Store) Resume(ctx context.Context, streamID string, at time.Time) (inspection.Snapshot, error) {
	var snap inspection.Snapshot
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		if err := lockStream(ctx, tx, streamID); err != nil {
			return err
		}
		if _, _, err := loadDomainStream(ctx, tx, streamID); err != nil {
			return err
		}
		seq, err := nextSeq(ctx, tx, streamID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO stream_events (stream_id, seq, kind, at)
VALUES ($1,$2,'resume',$3)`, streamID, seq, at.UTC()); err != nil {
			return err
		}
		snap, err = rebuildStream(ctx, tx, streamID)
		return err
	})
	return snap, mapWriteErr(err)
}

func nextSeq(ctx context.Context, tx pgx.Tx, streamID string) (int64, error) {
	var seq int64
	err := tx.QueryRow(ctx, `
SELECT COALESCE(MAX(seq),0)+1 FROM stream_events WHERE stream_id=$1`,
		streamID).Scan(&seq)
	return seq, err
}

func insertBatchEvent(ctx context.Context, tx pgx.Tx, streamID string, seq int64, b *inspection.BatchInput) error {
	_, err := tx.Exec(ctx, `
INSERT INTO stream_events
(stream_id, seq, kind, at, batch_id, lot_no, d1, has_d2, d2)
VALUES ($1,$2,'batch',$3,$4,$5,$6,$7,$8)`,
		streamID, seq, b.At.UTC(), b.ID, b.LotNo, b.D1, b.HasD2, nullableD2Record(b.HasD2, b.D2))
	if isUniqueViolation(err) {
		// PRIMARY (stream,seq) or UNIQUE (stream,batch_id)
		return ErrConflict{Msg: "batch already exists in this stream"}
	}
	return err
}

func nullableD2(b BatchRecord) any {
	if b.HasD2 {
		return b.D2
	}
	return nil
}

func nullableD2Record(has bool, d2 int) any {
	if has {
		return d2
	}
	return nil
}
