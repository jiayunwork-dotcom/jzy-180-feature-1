package inspection

import (
	"sampling-svc/internal/plan"
)

// switchScoreAward implements the GB/T 2828.1 (ISO 2859-1) switching-score
// table for a lot accepted under NORMAL inspection:
//
//	single, c=0          : d=0 -> 2, else reset
//	single, c=1          : d=0 -> 2, d=1 -> 1, else reset
//	single, c>=2         : d<=c-2 -> 3, d=c-1 -> 2, d=c -> 1, else reset
//
// Double sampling uses the same single-sampling table with acceptance
// number c1 for first-stage accepted lots, and c2 with the combined
// count for second-stage accepted lots. Any rejected lot resets the
// score to zero. The score only accumulates on the normal track.
func switchScoreAward(pl *plan.Plan, accepted bool, d1 int, hasD2 bool, d2 int) int {
	if !accepted {
		return 0
	}
	var c, d int
	if pl.Kind == plan.KindSingle {
		c, d = pl.AcceptNumber, d1
	} else {
		if needsSecond(pl, d1) && hasD2 {
			c, d = pl.C2, d1+d2
		} else {
			c, d = pl.C1, d1
		}
	}
	switch {
	case c == 0:
		if d == 0 {
			return 2
		}
		return 0
	case c == 1:
		switch {
		case d == 0:
			return 2
		case d == 1:
			return 1
		default:
			return 0
		}
	default: // c >= 2
		switch {
		case d <= c-2:
			return 3
		case d == c-1:
			return 2
		case d == c:
			return 1
		default:
			return 0
		}
	}
}
