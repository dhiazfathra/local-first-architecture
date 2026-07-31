// Package clock implements hybrid logical clocks (HLCs).
//
// An HLC timestamp combines a wall-clock reading with a logical counter and a
// node identifier. The triple has a strict total order, which is what makes
// last-writer-wins registers deterministic across replicas: every replica
// reaches the same verdict on which of two writes is later, with no ties.
package clock

import "cmp"

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
