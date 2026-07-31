// Package clock implements hybrid logical clocks (HLCs).
//
// An HLC timestamp combines a wall-clock reading with a logical counter and a
// node identifier. The triple has a strict total order, which is what makes
// last-writer-wins registers deterministic across replicas: every replica
// reaches the same verdict on which of two writes is later, with no ties.
package clock

import (
	"cmp"
	"math"
	"sync"
	"time"
)

// NodeID identifies a replica. It is the final tiebreak in HLC ordering, so it
// must be stable for the lifetime of a node's data.
type NodeID string

// HLC is a hybrid logical clock timestamp.
type HLC struct {
	Wall    int64  // unix milliseconds
	Logical uint32 // tiebreak counter within the same Wall value
	NodeID  NodeID // final tiebreak, yields a total order
}

// Compare orders a against b by Wall, then Logical, then NodeID.
// It returns -1, 0, or +1.
func (a HLC) Compare(b HLC) int {
	if c := cmp.Compare(a.Wall, b.Wall); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Logical, b.Logical); c != 0 {
		return c
	}
	return cmp.Compare(a.NodeID, b.NodeID)
}

// Before reports whether a strictly precedes b in the total order.
func (a HLC) Before(b HLC) bool { return a.Compare(b) < 0 }

// WallFunc reads a wall clock in unix milliseconds. Injected so tests and the
// fault harness can skew, stall, or reverse time.
type WallFunc func() int64

// Clock issues monotonically increasing HLC timestamps for one node.
type Clock struct {
	mu   sync.Mutex
	id   NodeID
	wall WallFunc
	last HLC
}

// New returns a Clock for id. A nil wall means real system time.
func New(id NodeID, wall WallFunc) *Clock {
	if wall == nil {
		wall = func() int64 { return time.Now().UnixMilli() }
	}
	return &Clock{id: id, wall: wall, last: HLC{NodeID: id}}
}

// Now returns the next local timestamp. It never regresses: if the wall clock
// has not advanced past the last issued timestamp -- including a jump
// backwards -- the wall value is clamped and Logical is bumped instead.
func (c *Clock) Now() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tick(HLC{NodeID: c.id})
}

// Observe merges a remote timestamp into the local clock and returns the new
// local timestamp, which is guaranteed to sort after remote. Calling it on
// every received event is what preserves causality across drifting clocks.
func (c *Clock) Observe(remote HLC) HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tick(remote)
}

// Last returns the most recently issued timestamp.
func (c *Clock) Last() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// tick advances the clock past both c.last and floor, using the wall reading
// when it is ahead of both. Caller holds c.mu.
//
// bumpLogical advances past prior's Logical counter. If prior.Logical is
// already math.MaxUint32, incrementing would wrap to 0 and sort *before*
// prior, breaking monotonicity -- so instead it advances Wall by one
// millisecond and resets Logical to 0, which still sorts after prior under
// the total order (Wall, Logical, NodeID).
func bumpLogical(next HLC, prior HLC) HLC {
	if prior.Logical == math.MaxUint32 {
		next.Wall = prior.Wall + 1
		next.Logical = 0
		return next
	}
	next.Wall = prior.Wall
	next.Logical = prior.Logical + 1
	return next
}

func (c *Clock) tick(floor HLC) HLC {
	next := HLC{Wall: c.wall(), NodeID: c.id}
	for _, prior := range [...]HLC{c.last, floor} {
		if next.Wall < prior.Wall {
			next = bumpLogical(next, prior)
		} else if next.Wall == prior.Wall && next.Logical <= prior.Logical {
			next = bumpLogical(next, prior)
		}
	}
	c.last = next
	return next
}
