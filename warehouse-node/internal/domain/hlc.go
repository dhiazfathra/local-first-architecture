package domain

import "math"

// HLC is a hybrid logical clock reading. Wall is Unix milliseconds, Counter
// disambiguates events sharing a wall reading, and Node breaks remaining ties so
// that ordering is total across nodes without requiring synchronised clocks.
//
// Ordering events on HLC rather than on wall time is what lets two nodes that
// have never spoken produce a log that replays identically everywhere.
type HLC struct {
	Wall    int64  `json:"wall"`
	Counter uint32 `json:"counter"`
	Node    NodeID `json:"node"`
}

// Compare returns -1, 0 or 1 ordering a before, equal to, or after other.
// Fields are compared relationally rather than by subtraction: a remote reading
// is untrusted input, and `h.Wall - other.Wall` overflows for extreme values,
// which would invert the very ordering this type promises.
func (h HLC) Compare(other HLC) int {
	switch {
	case h.Wall != other.Wall:
		return less(h.Wall < other.Wall)
	case h.Counter != other.Counter:
		return less(h.Counter < other.Counter)
	case h.Node != other.Node:
		return less(h.Node < other.Node)
	default:
		return 0
	}
}

func less(isLess bool) int {
	if isLess {
		return -1
	}
	return 1
}

// bump returns the reading one logical step after (wall, counter). A saturated
// counter carries into the wall component instead of wrapping to zero, which
// would silently send the clock backwards.
func bump(wall int64, counter uint32, node NodeID) HLC {
	if counter == math.MaxUint32 {
		return HLC{Wall: wall + 1, Counter: 0, Node: node}
	}
	return HLC{Wall: wall, Counter: counter + 1, Node: node}
}

// Tick produces the next local HLC for an event emitted now. If the wall clock
// has advanced past the previous reading it is adopted and the counter resets;
// otherwise (equal, or the clock jumped backwards) the previous wall reading is
// kept and the counter bumps, so the clock never goes backwards.
func Tick(prev HLC, nowMillis int64, node NodeID) HLC {
	if nowMillis > prev.Wall {
		return HLC{Wall: nowMillis, Counter: 0, Node: node}
	}
	return bump(prev.Wall, prev.Counter, node)
}

// Merge produces the next local HLC after observing a remote reading, which is
// how causality crosses the sync stream: anything this node emits after seeing
// remote sorts after remote.
func Merge(local, remote HLC, nowMillis int64, node NodeID) HLC {
	maxWall := max64(max64(local.Wall, remote.Wall), nowMillis)
	switch {
	case maxWall == nowMillis && nowMillis > local.Wall && nowMillis > remote.Wall:
		return HLC{Wall: nowMillis, Counter: 0, Node: node}
	case maxWall == local.Wall && maxWall == remote.Wall: //nolint:gocritic // both readings tie for max; distinct from the single-winner cases below
		return bump(maxWall, maxU32(local.Counter, remote.Counter), node)
	case local.Wall == maxWall:
		return bump(maxWall, local.Counter, node)
	default:
		return bump(maxWall, remote.Counter, node)
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxU32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
