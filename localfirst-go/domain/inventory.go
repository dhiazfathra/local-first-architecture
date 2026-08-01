package domain

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// Command errors. Each names the invariant it protects.
var (
	// ErrNegativeBalance is the one business invariant: stock never goes below
	// zero. A node owns its locations, so it can always decide this alone,
	// locally, offline — which is why this design needs no coordination.
	ErrNegativeBalance = errors.New("domain: balance would go negative")
	// ErrNotOwned means the command targets a location owned by another node.
	ErrNotOwned = errors.New("domain: location not owned by this node")
	// ErrBadQty means a non-positive quantity.
	ErrBadQty = errors.New("domain: quantity must be positive")
)

// Inventory is the node's aggregate: commands in, records out, balances kept.
// It implements both halves of the write-side seam, eventlog.Validator and
// eventlog.Projector, so the log can validate and project inside one
// transaction without knowing what stock is.
type Inventory struct {
	store *eventlog.Store
	clk   *clock.Clock
	owned map[string]bool // empty means "own everything" (central, tests)

	mu    sync.RWMutex
	state State
}

// NewInventory rebuilds balances by replaying the whole log, then serves
// commands. owned lists the locations this node exclusively writes to; an empty
// list disables ownership checks.
func NewInventory(
	ctx context.Context, store *eventlog.Store, clk *clock.Clock, owned []string,
) (*Inventory, error) {
	state, err := projection.Fold[State](ctx, store, BalanceReducer{})
	if err != nil {
		return nil, fmt.Errorf("domain: replay: %w", err)
	}
	set := make(map[string]bool, len(owned))
	for _, loc := range owned {
		set[loc] = true
	}
	return &Inventory{store: store, clk: clk, owned: set, state: state}, nil
}

// Receive adds qty of sku at location.
func (i *Inventory) Receive(ctx context.Context, sku, location string, qty int64) error {
	if err := i.guard(qty, location); err != nil {
		return err
	}
	return i.append(ctx, TypeReceived, Received{SKU: sku, Location: location, Qty: qty})
}

// Issue removes qty of sku from location.
func (i *Inventory) Issue(ctx context.Context, sku, location string, qty int64) error {
	if err := i.guard(qty, location); err != nil {
		return err
	}
	return i.append(ctx, TypeIssued, Issued{SKU: sku, Location: location, Qty: qty})
}

// Move transfers qty of sku from a location this node owns to any location,
// including one owned by another node. There is no handshake with the
// destination: it will apply the same record when it syncs.
func (i *Inventory) Move(ctx context.Context, sku, from, to string, qty int64) error {
	if err := i.guard(qty, from); err != nil {
		return err
	}
	return i.append(ctx, TypeMoved, Moved{SKU: sku, From: from, To: to, Qty: qty})
}

func (i *Inventory) guard(qty int64, location string) error {
	if qty <= 0 {
		return fmt.Errorf("%w: got %d", ErrBadQty, qty)
	}
	if len(i.owned) > 0 && !i.owned[location] {
		return fmt.Errorf("%w: %s", ErrNotOwned, location)
	}
	return nil
}

func (i *Inventory) append(ctx context.Context, typ string, ev any) error {
	// The store calls i.Check then i.Project inside one transaction.
	_, err := i.store.Append(ctx, typ, encode(ev), i.clk.Now(), i, i)
	return err
}

// Check implements eventlog.Validator: it replays r against a copy of the
// current state and refuses if any balance it touches would go negative.
// Called inside the append transaction, before the row is visible.
func (i *Inventory) Check(r eventlog.Record) error {
	i.mu.RLock()
	trial := i.state.Clone()
	i.mu.RUnlock()

	next, err := BalanceReducer{}.Apply(trial, r)
	if err != nil {
		return err
	}
	// Only the keys this record touches are validated. A node's state can
	// legitimately carry a negative balance at a location it does not own —
	// the shadow of a Moved authored and validated elsewhere (see
	// docs/limitations.md) — and that must never block a local command
	// against an unrelated key.
	for _, k := range touchedKeys(r) {
		if v := next[k]; v < 0 {
			return fmt.Errorf("%w: %s at %s would be %d", ErrNegativeBalance, k.SKU, k.Location, v)
		}
	}
	return nil
}

// touchedKeys returns the balance keys r's Apply call can change. Errors are
// ignored here because Apply above already decoded r successfully; an
// unrecognized type would have failed there first.
func touchedKeys(r eventlog.Record) []Key {
	switch r.Type {
	case TypeReceived:
		ev, _ := decode[Received](r)
		return []Key{{ev.SKU, ev.Location}}
	case TypeIssued:
		ev, _ := decode[Issued](r)
		return []Key{{ev.SKU, ev.Location}}
	case TypeMoved:
		ev, _ := decode[Moved](r)
		return []Key{{ev.SKU, ev.From}, {ev.SKU, ev.To}}
	default:
		return nil
	}
}

// Project implements eventlog.Projector: it computes the next state but does
// not publish it until the store's transaction has committed.
//
// It also observes r's clock. Project runs for every record this node ever
// projects, local or synced in from a peer, which makes it the one place
// guaranteed to see every timestamp the node learns about. Folding a peer's
// HLC in here is what makes ADR 0003's promise true: after merging a
// record with a future timestamp, this node's own next Now() sorts after it.
func (i *Inventory) Project(r eventlog.Record) (func(), error) {
	i.mu.RLock()
	staged := i.state.Clone()
	i.mu.RUnlock()

	next, err := BalanceReducer{}.Apply(staged, r)
	if err != nil {
		return nil, err
	}
	i.clk.Observe(r.Clock)
	return func() {
		i.mu.Lock()
		i.state = next
		i.mu.Unlock()
	}, nil
}

// Balance returns the projected balance for one key.
func (i *Inventory) Balance(sku, location string) int64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.state[Key{SKU: sku, Location: location}]
}

// Balances returns a snapshot copy of all balances.
func (i *Inventory) Balances() State {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.state.Clone()
}
