package crdt

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

// SQLLog is the slice of a log the projector needs. Both eventlog.SQLiteLog and
// eventlog.PostgresLog satisfy it, which is how central runs this identical
// projection code.
type SQLLog interface {
	eventlog.Log
	EventsForSKU(ctx context.Context, sku string, after eventlog.VersionVector) iter.Seq2[eventlog.Event, error]
	CountForSKU(ctx context.Context, sku string) (int, error)
}

// Projector computes ItemState for a SKU as fold(Apply, events), using
// snapshots to avoid replaying history on every read.
type Projector struct {
	Log SQLLog
	// SnapshotEvery is the per-SKU event count at or above which
	// MaybeSnapshot writes a snapshot. Zero disables snapshotting.
	SnapshotEvery int
}

// Project returns the merged state for sku: load the snapshot if one exists,
// then apply only the events its version vector does not already fold in.
// Because Apply is commutative, this equals folding the whole log.
func (p *Projector) Project(ctx context.Context, sku string) (*ItemState, error) {
	state := NewItemState()
	covered := eventlog.VersionVector{}

	blob, snapVV, err := p.Log.LoadSnapshot(ctx, sku)
	switch {
	case err == nil:
		if state, err = Unmarshal(blob); err != nil {
			return nil, fmt.Errorf("project %q: %w", sku, err)
		}
		covered = snapVV
	case !errors.Is(err, eventlog.ErrNoSnapshot):
		return nil, fmt.Errorf("project %q: %w", sku, err)
	}

	if err := state.Fold(p.Log.EventsForSKU(ctx, sku, covered)); err != nil {
		return nil, fmt.Errorf("project %q: %w", sku, err)
	}
	return state, nil
}

// MaybeSnapshot writes a snapshot for sku once its event count reaches
// SnapshotEvery, recording the version vector the snapshot folds in. Compaction
// later uses that vector as one of its two safety gates.
func (p *Projector) MaybeSnapshot(ctx context.Context, sku string) error {
	if p.SnapshotEvery <= 0 {
		return nil
	}
	n, err := p.Log.CountForSKU(ctx, sku)
	if err != nil {
		return err
	}
	if n < p.SnapshotEvery {
		return nil
	}

	state := NewItemState()
	covers := eventlog.VersionVector{}
	for e, err := range p.Log.EventsForSKU(ctx, sku, eventlog.VersionVector{}) {
		if err != nil {
			return fmt.Errorf("snapshot %q: %w", sku, err)
		}
		state.Apply(e)
		covers.Observe(e.ID)
	}
	// Marshal's error return is unreachable for an *ItemState (see its doc
	// comment): discard rather than leave a permanent coverage gap.
	blob, _ := Marshal(state)
	return p.Log.SaveSnapshot(ctx, sku, blob, covers)
}
