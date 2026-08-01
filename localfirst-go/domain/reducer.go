package domain

import (
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// Key identifies one stock balance.
type Key struct {
	SKU      string
	Location string
}

// State is the projected balance per key. Derived data only: it can always be
// rebuilt by replaying the log.
type State map[Key]int64

// Clone returns an independent copy, so a validator can try an event against a
// hypothetical future state without touching the live one.
func (s State) Clone() State {
	out := make(State, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

// BalanceReducer folds inventory records into balances.
// It is the read-side half of the plug-in seam: projection.Reducer[State].
type BalanceReducer struct{}

// Zero returns an empty balance set.
func (BalanceReducer) Zero() State { return State{} }

// Apply folds one record. It mutates and returns the same map — safe because
// FoldRecords threads a single state value through the replay.
func (BalanceReducer) Apply(s State, r eventlog.Record) (State, error) {
	switch r.Type {
	case TypeReceived:
		ev, err := decode[Received](r)
		if err != nil {
			return s, err
		}
		s[Key{ev.SKU, ev.Location}] += ev.Qty
	case TypeIssued:
		ev, err := decode[Issued](r)
		if err != nil {
			return s, err
		}
		s[Key{ev.SKU, ev.Location}] -= ev.Qty
	case TypeMoved:
		ev, err := decode[Moved](r)
		if err != nil {
			return s, err
		}
		s[Key{ev.SKU, ev.From}] -= ev.Qty
		s[Key{ev.SKU, ev.To}] += ev.Qty
	default:
		return s, fmt.Errorf("%w: %q", eventlog.ErrUnknownType, r.Type)
	}
	return s, nil
}
