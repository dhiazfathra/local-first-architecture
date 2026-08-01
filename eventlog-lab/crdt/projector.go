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

	// Fold from the existing snapshot forward, not from identity: Compact
	// deletes rows once a snapshot covers them, so after the first
	// compaction the surviving rows are no longer this SKU's whole history.
	// Refolding from identity over only the surviving rows would silently
	// replace a correct snapshot with one missing every compacted delta,
	// and Project could never recover them because the rows are gone.
	//
	// Seq is allocated per node across the whole log, not per SKU, so a gap
	// in this SKU's own seq sequence is normal (another SKU's event used
	// that number) and is NOT evidence of a missing event. The only source
	// of truth for "is a node's frontier actually gap-free" is the full
	// log's own contiguity check (the same one VersionVector uses): compute
	// it BEFORE folding, and use it to decide what gets folded at all.
	//
	// A node's events past its frontier must be skipped here, not folded
	// and then merely left out of covers: state.Apply is deliberately non-
	// idempotent (see its doc comment), and Project's read path re-applies
	// anything with seq > covers. Folding an event now but excluding it from
	// covers would make Project fold it again later -- double-counting a
	// quantity delta. Skipping it here instead defers it to that same
	// ordinary incremental fold, exactly once, whenever its node's frontier
	// catches up.
	trueFrontier, err := p.Log.VersionVector(ctx)
	if err != nil {
		return fmt.Errorf("snapshot %q: %w", sku, err)
	}
	state := NewItemState()
	covers := eventlog.VersionVector{}
	blob, snapVV, err := p.Log.LoadSnapshot(ctx, sku)
	switch {
	case err == nil:
		if state, err = Unmarshal(blob); err != nil {
			return fmt.Errorf("snapshot %q: %w", sku, err)
		}
		covers = snapVV.Clone()
	case !errors.Is(err, eventlog.ErrNoSnapshot):
		return fmt.Errorf("snapshot %q: %w", sku, err)
	}
	for e, err := range p.Log.EventsForSKU(ctx, sku, covers) {
		if err != nil {
			return fmt.Errorf("snapshot %q: %w", sku, err)
		}
		if e.ID.Seq > trueFrontier[e.ID.NodeID] {
			continue
		}
		state.Apply(e)
		covers.Observe(e.ID)
	}
	// Marshal's error return is unreachable for an *ItemState (see its doc
	// comment): discard rather than leave a permanent coverage gap.
	blob, _ = Marshal(state)
	return p.Log.SaveSnapshot(ctx, sku, blob, covers)
}
