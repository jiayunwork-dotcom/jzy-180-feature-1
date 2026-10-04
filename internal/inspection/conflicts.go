package inspection

import "time"

// BatchConflict names a recorded lot that a proposed revision history
// can no longer judge: the lot's defect counts do not fit the sample
// sizes of the revision in force for its (severity, inspection time).
// Such a revision is rejected; the conflict is reported rather than the
// lot being silently accepted or rejected.
type BatchConflict struct {
	BatchID    string    `json:"batch_id"`
	LotNo      string    `json:"lot_no"`
	At         time.Time `json:"inspected_at"`
	Severity   Severity  `json:"severity"`
	PlanID     string    `json:"plan_id"`
	RevisionNo int       `json:"revision_no"`
	Reason     string    `json:"reason"`
}

func (c BatchConflict) Error() string {
	return "batch " + c.BatchID + " cannot be judged under revision " +
		itoa(c.RevisionNo) + ": " + c.Reason
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
