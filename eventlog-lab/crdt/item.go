package crdt

import (
	"encoding/json"
	"fmt"
	"iter"
	"maps"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

// ItemState is the merged state of one StockItem.
//
// Quantity is a PN-counter: two maps keyed by node, holding everything that
// node ever added (Pos) and subtracted (Neg). Each node writes only its own
// slot and the value is a sum, so the result is independent of the order events
// arrive in. The metadata fields are LWW-registers.
type ItemState struct {
	Pos          map[clock.NodeID]int64 `json:"pos"`
	Neg          map[clock.NodeID]int64 `json:"neg"`
	Name         LWW[string]            `json:"name"`
	ReorderPoint LWW[int64]             `json:"reorder_point"`
	Deleted      LWW[bool]              `json:"deleted"`
}

// NewItemState returns the identity state: quantity zero, no metadata.
func NewItemState() *ItemState {
	return &ItemState{Pos: map[clock.NodeID]int64{}, Neg: map[clock.NodeID]int64{}}
}

// Apply folds one event into s. It is commutative and associative, so any
// order of the same event set yields the same state.
//
// Apply is deliberately NOT idempotent: applying a KindQuantityDelta event
// twice double-counts it. Idempotence is provided by eventlog.Append's primary
// key on (NodeID, Seq), which makes duplicate delivery a storage no-op. Never
// drive Apply from anything but a projection over the log; driving it straight
// from a network frame reintroduces double-counting.
//
// Unapplicable events (unknown kind, missing payload) are ignored rather than
// panicking, but they can never reach here in practice: eventlog.Event.Validate
// rejects them before storage.
func (s *ItemState) Apply(e eventlog.Event) {
	switch e.Kind {
	case eventlog.KindQuantityDelta:
		// -e.Delta below would overflow if e.Delta == math.MinInt64 (its
		// magnitude does not fit in int64). eventlog.Event.Validate rejects
		// that value at the trust boundary before any event reaches here, on
		// both the local-write path and the remote-frame path, so this
		// negation is safe by construction rather than by luck.
		if e.Delta >= 0 {
			s.Pos[e.ID.NodeID] += e.Delta
		} else {
			s.Neg[e.ID.NodeID] += -e.Delta
		}
	case eventlog.KindMetaSet:
		if e.Meta == nil {
			return
		}
		if e.Meta.Name != nil {
			s.Name.Set(*e.Meta.Name, e.HLC)
		}
		if e.Meta.ReorderPoint != nil {
			s.ReorderPoint.Set(*e.Meta.ReorderPoint, e.HLC)
		}
	case eventlog.KindDeleteSet:
		if e.DeletedTo != nil {
			s.Deleted.Set(*e.DeletedTo, e.HLC)
		}
	}
}

// Fold applies every event the iterator yields, stopping at the first error.
func (s *ItemState) Fold(events iter.Seq2[eventlog.Event, error]) error {
	for e, err := range events {
		if err != nil {
			return fmt.Errorf("fold events: %w", err)
		}
		s.Apply(e)
	}
	return nil
}

// Quantity is sum(Pos) - sum(Neg). It may legitimately be negative when two
// nodes overdraw concurrently while partitioned; see README.
func (s ItemState) Quantity() int64 {
	var total int64
	for _, v := range s.Pos {
		total += v
	}
	for _, v := range s.Neg {
		total -= v
	}
	return total
}

// Equal reports whether two states are identical. This is the convergence
// predicate the harness asserts with.
func (s *ItemState) Equal(other *ItemState) bool {
	if other == nil {
		return false
	}
	return maps.Equal(s.Pos, other.Pos) &&
		maps.Equal(s.Neg, other.Neg) &&
		s.Name == other.Name &&
		s.ReorderPoint == other.ReorderPoint &&
		s.Deleted == other.Deleted
}

// Marshal serialises s for a snapshot row, per-node counters included.
//
// The error return is kept for API stability (callers already expect
// ([]byte, error)), but it can never actually trigger for an *ItemState:
// every field is a plain string/int64/bool map or LWW register over those
// types, none of which json.Marshal can fail on (no channels, funcs,
// NaN/Inf floats, or cycles).
func Marshal(s *ItemState) ([]byte, error) {
	b, _ := json.Marshal(s) // unreachable: ItemState has no funcs/chans/NaN/cycles
	return b, nil
}

// Unmarshal restores a snapshotted state, normalising nil maps so Apply can
// write into them.
func Unmarshal(b []byte) (*ItemState, error) {
	s := NewItemState()
	if err := json.Unmarshal(b, s); err != nil {
		return nil, fmt.Errorf("unmarshal item state: %w", err)
	}
	if s.Pos == nil {
		s.Pos = map[clock.NodeID]int64{}
	}
	if s.Neg == nil {
		s.Neg = map[clock.NodeID]int64{}
	}
	return s, nil
}
