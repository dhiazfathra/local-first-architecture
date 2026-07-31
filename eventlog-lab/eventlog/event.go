// Package eventlog defines the append-only event log that is a node's source
// of truth, plus the event types replicated between nodes.
package eventlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// ErrMalformedEvent marks an event that must never be persisted: the local
// crdt package could not apply it, and silently storing it would break
// convergence. Callers reject the frame, log loudly, and continue.
var ErrMalformedEvent = errors.New("malformed event")

// Seq is a per-node monotonic sequence number starting at 1. Gap-free
// numbering is what lets a version vector summarise a node with one integer.
type Seq uint64

// EventID globally identifies an event without a UUID: a node only ever mints
// IDs bearing its own NodeID, so no coordination is needed.
type EventID struct {
	NodeID clock.NodeID
	Seq    Seq
}

// Kind discriminates the event payload.
type Kind int

const (
	// KindQuantityDelta event carries a signed Delta, commutative.
	KindQuantityDelta Kind = 1
	// KindMetaSet event carries LWW Name and/or ReorderPoint.
	KindMetaSet Kind = 2
	// KindDeleteSet event carries LWW tombstone flag.
	KindDeleteSet Kind = 3
)

// Valid reports whether k is a kind this build can apply.
func (k Kind) Valid() bool {
	return k == KindQuantityDelta || k == KindMetaSet || k == KindDeleteSet
}

// MetaSet carries the last-writer-wins metadata fields. Nil fields are not
// written, which lets one event set Name without clobbering ReorderPoint.
type MetaSet struct {
	Name         *string `json:"name,omitempty"`
	ReorderPoint *int64  `json:"reorder_point,omitempty"`
}

// Empty reports whether the MetaSet would write nothing.
func (m *MetaSet) Empty() bool { return m == nil || (m.Name == nil && m.ReorderPoint == nil) }

// Event is one immutable fact about a StockItem.
type Event struct {
	ID        EventID
	HLC       clock.HLC
	SKU       string
	Kind      Kind
	Delta     int64    // KindQuantityDelta only; sign carries increment/decrement
	Meta      *MetaSet // KindMetaSet only
	DeletedTo *bool    // KindDeleteSet only
}

// payload is the kind-specific JSON blob stored in the events table. Keeping
// the identity and ordering columns out of it means indexes work and the blob
// stays a pure payload.
type payload struct {
	Delta     int64    `json:"delta,omitempty"`
	Meta      *MetaSet `json:"meta,omitempty"`
	DeletedTo *bool    `json:"deleted_to,omitempty"`
}

// MarshalPayload encodes the kind-specific fields.
func (e Event) MarshalPayload() ([]byte, error) {
	b, err := json.Marshal(payload{Delta: e.Delta, Meta: e.Meta, DeletedTo: e.DeletedTo})
	if err != nil {
		return nil, fmt.Errorf("marshal payload for %v: %w", e.ID, err)
	}
	return b, nil
}

// UnmarshalPayload decodes the kind-specific fields into e.
func (e *Event) UnmarshalPayload(b []byte) error {
	var p payload
	if err := json.Unmarshal(b, &p); err != nil {
		return fmt.Errorf("%w: unparseable payload for %v: %w", ErrMalformedEvent, e.ID, err)
	}
	e.Delta, e.Meta, e.DeletedTo = p.Delta, p.Meta, p.DeletedTo
	return nil
}

// Validate is the trust boundary for remote events: anything it rejects must
// never reach storage.
func (e Event) Validate() error {
	switch {
	case e.ID.NodeID == "":
		return fmt.Errorf("%w: empty node id", ErrMalformedEvent)
	case e.ID.Seq == 0:
		return fmt.Errorf("%w: seq must start at 1", ErrMalformedEvent)
	case e.SKU == "":
		return fmt.Errorf("%w: empty sku", ErrMalformedEvent)
	case e.HLC.NodeID != e.ID.NodeID:
		return fmt.Errorf("%w: hlc node %q != event node %q", ErrMalformedEvent, e.HLC.NodeID, e.ID.NodeID)
	case !e.Kind.Valid():
		return fmt.Errorf("%w: unknown kind %d", ErrMalformedEvent, e.Kind)
	}
	// Every payload field belongs to exactly one kind. Reject any field set on
	// a kind it doesn't belong to, not just Delta -- Apply silently ignores
	// fields that don't match e.Kind, so an accepted-but-mismatched event
	// would persist data that never takes effect anywhere.
	if e.Kind != KindQuantityDelta && e.Delta != 0 {
		return fmt.Errorf("%w: delta set on kind %d", ErrMalformedEvent, e.Kind)
	}
	if e.Kind != KindMetaSet && e.Meta != nil {
		return fmt.Errorf("%w: meta set on kind %d", ErrMalformedEvent, e.Kind)
	}
	if e.Kind != KindDeleteSet && e.DeletedTo != nil {
		return fmt.Errorf("%w: deletedTo set on kind %d", ErrMalformedEvent, e.Kind)
	}
	switch e.Kind {
	case KindQuantityDelta:
		if e.Delta == 0 {
			return fmt.Errorf("%w: zero quantity delta", ErrMalformedEvent)
		}
		if e.Delta == math.MinInt64 {
			return fmt.Errorf("%w: delta %d has no representable magnitude", ErrMalformedEvent, e.Delta)
		}
	case KindMetaSet:
		if e.Meta.Empty() {
			return fmt.Errorf("%w: meta event writes nothing", ErrMalformedEvent)
		}
	case KindDeleteSet:
		if e.DeletedTo == nil {
			return fmt.Errorf("%w: delete event without value", ErrMalformedEvent)
		}
	}
	return nil
}

// VersionVector maps each node to the highest Seq of its events we hold.
type VersionVector map[clock.NodeID]Seq

// Clone returns an independent copy, never nil.
func (vv VersionVector) Clone() VersionVector {
	out := make(VersionVector, len(vv))
	for k, v := range vv {
		out[k] = v
	}
	return out
}

// Observe records that we now hold id.
func (vv VersionVector) Observe(id EventID) {
	if vv[id.NodeID] < id.Seq {
		vv[id.NodeID] = id.Seq
	}
}

// Contains reports whether id is already covered.
func (vv VersionVector) Contains(id EventID) bool { return vv[id.NodeID] >= id.Seq }

// Dominates reports whether vv holds everything other holds.
func (vv VersionVector) Dominates(other VersionVector) bool {
	for k, v := range other {
		if vv[k] < v {
			return false
		}
	}
	return true
}

// Merge returns the pointwise maximum of vv and other.
func (vv VersionVector) Merge(other VersionVector) VersionVector {
	out := vv.Clone()
	for k, v := range other {
		if out[k] < v {
			out[k] = v
		}
	}
	return out
}
