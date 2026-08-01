// Package projection replays a log into derived state.
//
// It is domain-agnostic: Fold is generic over the state type S, so nothing here
// names an inventory concept. A new domain implements one Reducer and is done.
package projection

import (
	"context"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// Reducer interprets records. It is the entire read-side extension point.
//
// Apply must be pure with respect to the log: replaying the same records in the
// same order must always produce the same state, on any machine, forever.
type Reducer[S any] interface {
	// Zero returns the state before any record has been applied.
	Zero() S
	// Apply folds one record into state. An unrecognised r.Type must return an
	// error wrapping eventlog.ErrUnknownType — never silently ignore it.
	Apply(state S, r eventlog.Record) (S, error)
}

// Fold reads the whole log and replays it through r.
func Fold[S any](ctx context.Context, log eventlog.Reader, r Reducer[S]) (S, error) {
	recs, err := log.Since(ctx, nil)
	if err != nil {
		var zero S
		return zero, fmt.Errorf("projection: read log: %w", err)
	}
	return FoldRecords(r, recs)
}

// FoldRecords replays an already-read slice. Useful for catch-up after a sync
// batch, and for tests that do not want a log at all.
func FoldRecords[S any](r Reducer[S], recs []eventlog.Record) (S, error) {
	state := r.Zero()
	for _, rec := range recs {
		next, err := r.Apply(state, rec)
		if err != nil {
			var zero S
			return zero, fmt.Errorf("projection: record %s/%d type %q: %w",
				rec.NodeID, rec.Seq, rec.Type, err)
		}
		state = next
	}
	return state, nil
}
