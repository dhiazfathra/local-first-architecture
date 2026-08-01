// Package eventlog is the domain-agnostic append-only log.
//
// It stores Records — opaque (Type, Payload) pairs stamped with the authoring
// node, a per-node sequence number and an HLC timestamp — and knows nothing
// about what they mean. A domain plugs in through two interfaces defined here,
// Validator and Projector, plus projection.Reducer for replay.
package eventlog

import (
	"context"
	"errors"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
)

// ErrUnknownType is returned when a domain is asked to interpret a record type
// it does not know. Projection MUST fail hard on it rather than skip the
// record: silently ignoring unknown events is how replicas silently diverge.
var ErrUnknownType = errors.New("eventlog: unknown record type")

// Record is one immutable fact. (NodeID, Seq) is the primary key: the authoring
// node assigns Seq densely from 1, so re-delivering a record is a no-op insert.
type Record struct {
	NodeID  string    // Who authored it.
	Seq     uint64    // Author's own dense sequence, starting at 1.
	Clock   clock.HLC // Ordering and audit only.
	Type    string    // Domain-defined discriminator.
	Payload []byte    // Domain-defined encoding.
}

// Less orders records for replay: HLC first, then the author's sequence so the
// order is total and identical on every replica.
func (r Record) Less(o Record) bool {
	if c := r.Clock.Compare(o.Clock); c != 0 {
		return c < 0
	}
	return r.Seq < o.Seq
}

// VersionVector maps node id to the highest contiguous Seq held for that node.
// It is the entire "what do you already have?" question in the sync protocol.
type VersionVector map[string]uint64

// Get returns the highest Seq held for node, or 0 if none. Safe on a nil map.
func (v VersionVector) Get(node string) uint64 { return v[node] }

// Observe raises the mark for node to seq. Marks never decrease, which is what
// makes a peer's stale or regressed vector harmless.
func (v VersionVector) Observe(node string, seq uint64) {
	if seq > v[node] {
		v[node] = seq
	}
}

// Clone returns an independent copy.
func (v VersionVector) Clone() VersionVector {
	out := make(VersionVector, len(v))
	for k, s := range v {
		out[k] = s
	}
	return out
}

// Equal reports whether the two vectors hold the same marks.
func (v VersionVector) Equal(o VersionVector) bool {
	if len(v) != len(o) {
		return false
	}
	for k, s := range v {
		if o[k] != s {
			return false
		}
	}
	return true
}

// Reader is the read half of a log. There is deliberately no query language:
// everything downstream is built by folding records in order.
type Reader interface {
	// Since returns every held record the caller lacks according to vv,
	// in replay order. A nil vv means "everything".
	Since(ctx context.Context, vv VersionVector) ([]Record, error)
}

// Validator decides whether a locally authored record may be appended.
//
// Check is called INSIDE the append transaction, against the current projected
// state, before the row is visible. Returning an error rolls the whole
// transaction back, so a rejected command leaves no trace in the log.
type Validator interface {
	Check(r Record) error
}

// Projector folds a record into derived in-memory state.
//
// Project is called inside the append transaction and must NOT mutate anything
// observable; it returns a commit closure that the store calls only after the
// transaction commits. That two-phase shape is why a projection can never
// disagree with the log: either both happen or neither does.
type Projector interface {
	Project(r Record) (commit func(), err error)
}
