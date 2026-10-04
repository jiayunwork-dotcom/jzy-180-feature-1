package plan

import "time"

// RevisionNo is the per-plan, gap-free, 1-based revision number. A plan
// always has at least one revision; appending a change allocates the next
// number. Existing revisions are immutable — corrections are appended as
// newer revisions, never overwritten.
type RevisionNo = int

// Revision is one immutable parameter set of a plan in force from
// EffectiveAt until the next revision. The earliest revision also covers
// every earlier point in time, so a freshly created plan's revision 1
// applies to back-dated data.
type Revision struct {
	PlanID string
	No     int
	// EffectiveAt is the inclusive start of the revision's validity
	// window: a lot inspected exactly at EffectiveAt is judged under
	// this revision. Multiple revisions may share an effective time;
	// the highest revision number wins.
	EffectiveAt time.Time
	// Plan carries the full parameter set (kind, distribution, n/c,
	// n1/c1/r1/n2/c2, lot size). ID and Name come from the plan header.
	Plan *Plan
}

// RevisionRequest is the wire payload for appending one revision. It is a
// plan Request plus an optional RFC3339 effective time; when the time is
// omitted the revision takes effect immediately.
type RevisionRequest struct {
	Name        string       `json:"name,omitempty"`
	Kind        string       `json:"kind,omitempty"`
	Single      *SingleParam `json:"single,omitempty"`
	Double      *DoubleParam `json:"double,omitempty"`
	EffectiveAt string       `json:"effective_at,omitempty"`
}

// HasParams reports whether the request carries a parameter body.
func (r RevisionRequest) HasParams() bool {
	return r.Single != nil || r.Double != nil
}

// AsRequest converts the revision payload into a validated plan request.
// fallbackName supplies the current header name when the revision does
// not itself rename the plan (old clients omit the name on updates).
func (r RevisionRequest) AsRequest(fallbackName string) Request {
	name := r.Name
	if name == "" {
		name = fallbackName
	}
	return Request{
		Name: name, Kind: r.Kind,
		Single: r.Single, Double: r.Double,
	}
}
