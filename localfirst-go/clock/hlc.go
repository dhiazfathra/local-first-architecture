// Package clock implements a hybrid logical clock (HLC).
//
// An HLC timestamp is (wall millis, logical counter, node id). It stays close
// to wall time so humans can read it, but never regresses: when wall time does
// not advance — including when the host clock jumps backwards — the logical
// counter increments instead. The node id is the final tiebreak, which makes
// any two timestamps totally ordered.
//
// In this architecture HLCs are used for ordering and audit ONLY. Because each
// node exclusively owns its keys there is no conflict to resolve, so nothing
// here implements last-writer-wins. See docs/adr/0003-hlc-for-ordering-only.md.
package clock

import (
	"strconv"
	"sync"
	"time"
)

// HLC is a hybrid logical clock timestamp.
type HLC struct {
	Wall    int64  // Unix milliseconds.
	Logical uint32 // Ticks within the same wall millisecond.
	NodeID  string // Final tiebreak; also records who issued the timestamp.
}

// Compare orders h against o: -1 if h sorts first, 0 if identical, 1 otherwise.
func (h HLC) Compare(o HLC) int {
	switch {
	case h.Wall != o.Wall:
		return sign(h.Wall - o.Wall)
	case h.Logical != o.Logical:
		return sign(int64(h.Logical) - int64(o.Logical))
	case h.NodeID != o.NodeID:
		if h.NodeID < o.NodeID {
			return -1
		}
		return 1
	}
	return 0
}

// Before reports whether h sorts strictly before o.
func (h HLC) Before(o HLC) bool { return h.Compare(o) < 0 }

// String renders the timestamp as wall.logical@node.
func (h HLC) String() string {
	return strconv.FormatInt(h.Wall, 10) + "." + strconv.FormatUint(uint64(h.Logical), 10) + "@" + h.NodeID
}

func sign(d int64) int {
	if d < 0 {
		return -1
	}
	return 1
}

// Clock issues strictly increasing HLC timestamps for one node.
// All methods are safe for concurrent use.
type Clock struct {
	nodeID string
	now    func() time.Time

	mu   sync.Mutex
	last HLC
}

// New returns a Clock for nodeID. now may be nil, in which case time.Now is
// used; tests inject a fake to exercise wall-clock jumps.
func New(nodeID string, now func() time.Time) *Clock {
	if now == nil {
		now = time.Now
	}
	return &Clock{nodeID: nodeID, now: now}
}

// Now returns the next timestamp, strictly greater than every timestamp this
// Clock has previously returned or observed.
func (c *Clock) Now() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.advance(c.now().UnixMilli())
}

// Observe folds a peer timestamp in, then returns the next local timestamp.
// It guarantees the result sorts after remote, so a node's clock can never lag
// a record it has already accepted.
func (c *Clock) Observe(remote HLC) HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.advance(c.now().UnixMilli(), remote)
}

// advance implements the HLC update rule against the highest of: physical
// time, the last issued timestamp, and any observed peer timestamps.
func (c *Clock) advance(physical int64, peers ...HLC) HLC {
	next := HLC{Wall: physical, Logical: 0, NodeID: c.nodeID}
	for _, p := range append(peers, c.last) {
		switch {
		case p.Wall > next.Wall:
			next.Wall, next.Logical = p.Wall, p.Logical+1
		case p.Wall == next.Wall && p.Logical >= next.Logical:
			next.Logical = p.Logical + 1
		}
	}
	c.last = next
	return next
}
