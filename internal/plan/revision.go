package plan

import "time"

// Revision is one immutable, time-bounded version of a sampling plan.
//
// Revisions are append-only: once stored they never change; a correction
// is another revision (which may take effect in the future or be backdated
// into the past, possibly between two existing revisions).
//
// At inspection time t the effective revision is the one with the
// greatest EffectiveAt <= t; an exact tie at EffectiveAt == t is resolved
// by the greater revision number ("effective exactly at the inspection
// time counts as the new revision").
type Revision struct {
	PlanID      string
	Number      int
	EffectiveAt time.Time
	CreatedAt   time.Time

	// Revisioned plan content. Kind never changes within a plan: streams
	// bind plans of one fixed sampling kind, and a revision must keep that
	// kind so every recorded lot remains judgeable structurally.
	Plan *Plan
}

// RevisionRequest is the wire payload for appending a revision. It reuses
// the same validated parameter shape as plan create/update; EffectiveAt
// is parsed by the HTTP layer (RFC3339) and defaults to "now" when empty.
type RevisionRequest struct {
	EffectiveAt *time.Time `json:"effective_at,omitempty"`
	Request
}

// Epoch is the EffectiveAt of every plan's initial revision: far enough
// in the past to cover all real inspection data (Go zero time, year 1).
var Epoch = time.Unix(0, 0).UTC().AddDate(-1969, 0, 0) // 0001-01-01 UTC

// BuildRevision validates a revision payload and resolves the plan
// content. The revision number is assigned by the store.
func BuildRevision(req RevisionRequest) (*Plan, error) {
	return Build(req.Request)
}

// EffectiveAtOf returns the requested effective time or now when omitted.
func EffectiveAtOf(req *RevisionRequest, now time.Time) time.Time {
	if req.EffectiveAt != nil {
		return req.EffectiveAt.UTC()
	}
	return now.UTC()
}
