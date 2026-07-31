// Package node wires one replica together: local log, hybrid logical clock, and
// a projection over the log. It exposes an in-process op API that both the CLI
// and the fault harness drive directly. There is deliberately no HTTP layer.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

// Config constructs a Node.
type Config struct {
	ID   clock.NodeID
	Log  crdt.SQLLog
	Wall clock.WallFunc // nil means real time
	// SnapshotEvery is the per-SKU event count at which a snapshot is
	// written. Zero disables snapshotting.
	SnapshotEvery int
}

// Node is one replica.
type Node struct {
	id        clock.NodeID
	log       crdt.SQLLog
	clk       *clock.Clock
	projector *crdt.Projector
}

// New validates cfg and returns a ready Node.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("node: empty node id")
	}
	if cfg.Log == nil {
		return nil, errors.New("node: nil log")
	}
	n := &Node{
		id:        cfg.ID,
		log:       cfg.Log,
		clk:       clock.New(cfg.ID, cfg.Wall),
		projector: &crdt.Projector{Log: cfg.Log, SnapshotEvery: cfg.SnapshotEvery},
	}
	if err := n.recoverClock(context.Background()); err != nil {
		return nil, err
	}
	return n, nil
}

// recoverClock seeds the HLC from the log's highest stamp. Every `lab op`
// invocation is a fresh process, and a fresh Clock trusts only the wall
// reading -- so a wall clock that has been skewed or jumped backwards since
// this node last wrote (exactly what the ClockSkew fault simulates) would let
// a restarted node emit a stamp sorting BEFORE its own earlier events. LWW
// then silently rejects every metadata write it makes until wall time catches
// up. Observing the stored maximum once, before the node is used, removes
// that window.
//
// ponytail: full scan of the log, no new Log method or index -- Since already
// orders by HLC and both backends implement it. If startup ever shows up in a
// profile, add a `SELECT MAX(hlc_wall), ...` to the interface.
func (n *Node) recoverClock(ctx context.Context) error {
	var maxHLC clock.HLC
	for e, err := range n.log.Since(ctx, eventlog.VersionVector{}) {
		if err != nil {
			return fmt.Errorf("node %q: recover clock: %w", n.id, err)
		}
		if maxHLC.Before(e.HLC) {
			maxHLC = e.HLC
		}
	}
	// Empty log: nothing to recover, and Observing the zero value would
	// wrongly pin NodeID-less state into the clock.
	if maxHLC != (clock.HLC{}) {
		n.clk.Observe(maxHLC)
	}
	return nil
}

// ID reports this replica's identity.
func (n *Node) ID() clock.NodeID { return n.id }

// Log exposes the underlying log for the sync layer and the harness.
func (n *Node) Log() crdt.SQLLog { return n.log }

// Clock exposes the HLC so the sync layer can Observe remote stamps.
func (n *Node) Clock() *clock.Clock { return n.clk }

// Receive records an inbound quantity as a positive delta.
func (n *Node) Receive(ctx context.Context, sku string, qty int64) (eventlog.EventID, error) {
	if qty <= 0 {
		return eventlog.EventID{}, fmt.Errorf("node: receive quantity must be positive, got %d", qty)
	}
	return n.emit(ctx, sku, func(e *eventlog.Event) {
		e.Kind, e.Delta = eventlog.KindQuantityDelta, qty
	})
}

// Pick records an outbound quantity as a negative delta. Picking more than is
// on hand is accepted: quantity is a CRDT counter, and a negative result is a
// reportable anomaly at central, not a rejected write. See README.
func (n *Node) Pick(ctx context.Context, sku string, qty int64) (eventlog.EventID, error) {
	if qty <= 0 {
		return eventlog.EventID{}, fmt.Errorf("node: pick quantity must be positive, got %d", qty)
	}
	return n.emit(ctx, sku, func(e *eventlog.Event) {
		e.Kind, e.Delta = eventlog.KindQuantityDelta, -qty
	})
}

// SetMeta writes the last-writer-wins metadata fields. Nil arguments are left
// untouched rather than cleared.
func (n *Node) SetMeta(ctx context.Context, sku string, name *string, reorderPoint *int64) (eventlog.EventID, error) {
	meta := &eventlog.MetaSet{Name: name, ReorderPoint: reorderPoint}
	if meta.Empty() {
		return eventlog.EventID{}, errors.New("node: set-meta writes nothing")
	}
	return n.emit(ctx, sku, func(e *eventlog.Event) {
		e.Kind, e.Meta = eventlog.KindMetaSet, meta
	})
}

// Delete sets or clears the tombstone. It is an LWW-register, so a later
// Delete(false) genuinely resurrects the item.
func (n *Node) Delete(ctx context.Context, sku string, deleted bool) (eventlog.EventID, error) {
	return n.emit(ctx, sku, func(e *eventlog.Event) {
		e.Kind, e.DeletedTo = eventlog.KindDeleteSet, &deleted
	})
}

// emit stamps, appends, and snapshots one locally-originated event. An append
// failure is returned to the caller and the op is NOT acknowledged -- the
// "no lost event" property only covers acknowledged ops.
func (n *Node) emit(ctx context.Context, sku string, fill func(*eventlog.Event)) (eventlog.EventID, error) {
	if sku == "" {
		return eventlog.EventID{}, errors.New("node: empty sku")
	}
	stamp := n.clk.Now()
	e, err := n.log.AppendLocal(ctx, func(s eventlog.Seq) eventlog.Event {
		ev := eventlog.Event{ID: eventlog.EventID{NodeID: n.id, Seq: s}, HLC: stamp, SKU: sku}
		fill(&ev)
		return ev
	})
	if err != nil {
		return eventlog.EventID{}, fmt.Errorf("node %q: %w", n.id, err)
	}
	if err := n.projector.MaybeSnapshot(ctx, sku); err != nil {
		return eventlog.EventID{}, fmt.Errorf("node %q: %w", n.id, err)
	}
	return e.ID, nil
}

// Merge appends remote events and returns how many were accepted. It calls
// Observe on every accepted event so a subsequent local write sorts after
// anything we have already seen -- without that, LWW resolves backwards.
//
// A malformed event is rejected, logged loudly, and skipped; the session
// continues. Storing an event the crdt package cannot apply would break
// convergence silently, which is far worse than dropping a frame.
func (n *Node) Merge(ctx context.Context, events []eventlog.Event) (int, error) {
	accepted := 0
	for _, e := range events {
		if err := n.log.Append(ctx, e); err != nil {
			if errors.Is(err, eventlog.ErrMalformedEvent) {
				slog.Error("rejecting malformed remote event",
					"node", n.id, "event", e.ID, "kind", e.Kind, "err", err)
				continue
			}
			return accepted, fmt.Errorf("node %q merge: %w", n.id, err)
		}
		n.clk.Observe(e.HLC)
		accepted++
		if err := n.projector.MaybeSnapshot(ctx, e.SKU); err != nil {
			return accepted, fmt.Errorf("node %q merge: %w", n.id, err)
		}
		// The reporting projection (Postgres central only) must stay
		// synchronized with every merge, not just local writes -- otherwise
		// Anomalies() reads stale or empty data after replication, which is
		// exactly the gap a merge-only central store must not have. n.log is
		// an ordinary eventlog.Log everywhere except central, so this is an
		// optional capability check, not a Postgres import here.
		if sink, ok := n.log.(projectionSink); ok {
			state, err := n.projector.Project(ctx, e.SKU)
			if err != nil {
				return accepted, fmt.Errorf("node %q merge: project %q: %w", n.id, e.SKU, err)
			}
			if err := sink.UpsertProjection(ctx, e.SKU, state.Quantity(),
				state.Name.Value, state.ReorderPoint.Value, state.Deleted.Value); err != nil {
				return accepted, fmt.Errorf("node %q merge: upsert projection %q: %w", n.id, e.SKU, err)
			}
		}
	}
	return accepted, nil
}

// projectionSink is satisfied by eventlog.PostgresLog and nothing else in
// this codebase; it lets Merge keep the central reporting projection current
// without node importing eventlog.PostgresLog or Postgres-specific types.
type projectionSink interface {
	UpsertProjection(ctx context.Context, sku string, quantity int64, name string, reorderPoint int64, deleted bool) error
}

// Get returns the merged state for sku.
func (n *Node) Get(ctx context.Context, sku string) (*crdt.ItemState, error) {
	if sku == "" {
		return nil, errors.New("node: empty sku")
	}
	return n.projector.Project(ctx, sku)
}

// VersionVector reports what this node holds.
func (n *Node) VersionVector(ctx context.Context) (eventlog.VersionVector, error) {
	return n.log.VersionVector(ctx)
}
