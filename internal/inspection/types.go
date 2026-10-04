// Package inspection implements the GB/T 2828.1 switching state machine
// (normal / tightened / reduced / suspended), deterministic history
// replay, and validation of stream mutations.
package inspection

import (
	"time"

	"sampling-svc/internal/plan"
)

// Severity is the inspection strictness in force for a lot.
type Severity string

const (
	SeverityNormal    Severity = "normal"
	SeverityTightened Severity = "tightened"
	SeverityReduced   Severity = "reduced"
	// SeveritySuspended means tightened inspection has been discontinued;
	// later lots cannot be inspected until a manual resume.
	SeveritySuspended Severity = "suspended"
)

// Decision is the disposition of one recorded lot.
type Decision string

const (
	DecisionAccepted     Decision = "accepted"
	DecisionRejected     Decision = "rejected"
	DecisionNotInspected Decision = "not_inspected" // while suspended
)

// FlagName identifies one of the two reduced-inspection prerequisites.
type FlagName string

const (
	FlagStable   FlagName = "stable"   // 生产稳定
	FlagApproved FlagName = "approved" // 主管同意
)

// BatchInput is one lot inspection result.
type BatchInput struct {
	ID    string
	LotNo string
	At    time.Time
	D1    int  // defects in first (or only) sample
	HasD2 bool // D2 was supplied
	D2    int  // defects in second sample (double plans)
	Seq   int64
}

// Event is a single ordered item in a stream's replay timeline.
type Event struct {
	Seq int64
	At  time.Time

	Batch *BatchInput

	Flag      FlagName
	FlagValue bool

	Resume bool
}

// Ref binds a plan slot to a stored plan.
type Ref struct {
	ID   string
	Plan *plan.Plan
}

// RevisionRef is one stored revision of the plan in a slot. The rows are
// immutable; the slot carries the full revision history so replay can
// pick, for each lot, the revision effective at that lot's inspection
// time.
type RevisionRef struct {
	Number      int
	EffectiveAt time.Time
	Plan        *plan.Plan
}

// Slot is one severity track (normal/tightened/reduced) with the complete
// revision history of the plan bound to it. Rows are expected in any
// order; resolution sorts as needed.
type Slot struct {
	ID        string // bound plan id
	Revisions []RevisionRef
}

// At returns the plan revision effective at time t: the revision with the
// greatest EffectiveAt <= t; on an exact EffectiveAt tie the greater
// revision number wins. There is always at least one revision (the
// initial one covers all history), so At never returns nil for a valid
// slot.
func (s Slot) At(t time.Time) RevisionRef {
	best := RevisionRef{Number: -1}
	found := false
	for _, r := range s.Revisions {
		if r.EffectiveAt.After(t) {
			continue
		}
		if !found ||
			r.EffectiveAt.After(best.EffectiveAt) ||
			(r.EffectiveAt.Equal(best.EffectiveAt) && r.Number > best.Number) {
			best = r
			found = true
		}
	}
	return best
}

// Plan returns the latest (newest effective) revision's plan. It keeps
// callers that need "the plan in this slot" independent of revision
// ordering details.
func (s Slot) Plan() *plan.Plan {
	var best RevisionRef
	found := false
	for _, r := range s.Revisions {
		if !found ||
			r.EffectiveAt.After(best.EffectiveAt) ||
			(r.EffectiveAt.Equal(best.EffectiveAt) && r.Number > best.Number) {
			best, found = r, true
		}
	}
	if !found {
		return nil
	}
	return best.Plan
}

// SingleSlot builds a slot containing exactly one revision, covering all
// history (effective at the Go zero time). Used by pure-logic tests and
// as a convenience where revision history is irrelevant.
func SingleSlot(id string, p *plan.Plan) Slot {
	return Slot{ID: id, Revisions: []RevisionRef{{
		Number: 1, EffectiveAt: plan.Epoch, Plan: p,
	}}}
}

// Stream binds the three severity plans.
type Stream struct {
	ID        string
	Name      string
	Normal    Slot
	Tightened Slot
	Reduced   Slot
}

// State is the stream state after replaying through some point.
type State struct {
	Severity Severity `json:"severity"`
	Score    int      `json:"score"`

	// Normal -> tightened tracking: accept/reject results of the last
	// up to 5 lots processed under normal inspection (true=accepted).
	normalWindow []bool

	// Tightened counters, cumulative since entering/resuming tightened.
	tightenedAcceptedRun int // consecutive accepted lots
	tightenedRejectTotal int // cumulative rejected lots

	stable   bool
	approved bool
}

// BatchOutcome is the computed disposition attached to one batch record.
type BatchOutcome struct {
	BatchID      string    `json:"batch_id"`
	LotNo        string    `json:"lot_no"`
	At           time.Time `json:"inspected_at"`
	Severity     Severity  `json:"severity"`
	PlanID       string    `json:"plan_id"`
	PlanRevision int       `json:"plan_revision"`
	PlanName     string    `json:"plan_name"`
	Decision     Decision  `json:"decision"`
	Accepted     bool      `json:"accepted"`
	D1           int       `json:"d1"`
	D2           *int      `json:"d2,omitempty"`
	Score        int       `json:"score"`
	Note         string    `json:"note,omitempty"`
}

// Snapshot is the current stream state plus every per-batch outcome.
type Snapshot struct {
	StreamID string         `json:"stream_id"`
	Current  State          `json:"current"`
	Batches  []BatchOutcome `json:"batches"`
}

// InitialState is the starting state of every new stream.
func InitialState() State {
	return State{Severity: SeverityNormal}
}

// StateFrom rebuilds the externally visible part of a state from the
// persisted severity and score. Internal transition counters are not
// stored separately: they are reconstructed only during full replay.
func StateFrom(sev Severity, score int) State {
	return State{Severity: sev, Score: score}
}

// ScoreValue exposes the persisted switching score.
func (s State) ScoreValue() int { return s.Score }
