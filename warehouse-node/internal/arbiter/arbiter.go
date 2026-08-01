// Package arbiter is central's authority. It runs each event a node pushed up through
// the validators for the invariants no single node can check, and when one rejects, it
// emits a compensating event: a normal event that undoes the effect, attributed to
// central, carrying CausationID pointing at the event it answers.
//
// Rejection never deletes anything. The original event stays in the log because the
// log is immutable history including mistakes, and compensation is deliberately not
// cascaded through downstream events — that way lies distributed rollback, which real
// ERPs do not do either.
package arbiter

import (
	"context"
	"fmt"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// Rejection is what a validator returns when it refuses an event: the reason code that
// goes on the compensation and into the decisions table, the compensating events to
// emit in order, and the quantity being taken back, which is what lets the accepted
// portion of a partly valid receipt still count.
type Rejection struct {
	Reason string
	Events []domain.Event
	Excess float64
}

// Validator checks one central-enforced invariant. It returns nil to accept.
type Validator interface {
	Name() string
	Validate(ctx context.Context, s central.Store, env domain.Envelope, payload any) (*Rejection, error)
}

// Arbiter applies the validator chain to incoming node events.
type Arbiter struct {
	store      central.Store
	now        func() time.Time
	validators []Validator
}

// New builds an arbiter over a store. now supplies the instant compensating events
// are stamped with; it is injected so tests are deterministic.
func New(store central.Store, now func() time.Time) *Arbiter {
	return &Arbiter{store: store, now: now, validators: []Validator{
		unknownSKU{}, duplicateDeliveryNote{}, poOverReceipt{}, transferDestination{}, transferOverReceipt{},
	}}
}

// Validators returns the chain in the order it runs. The order is load-bearing: an
// event breaking two rules yields exactly one compensation, from the first to fire.
func (a *Arbiter) Validators() []Validator { return a.validators }

// Arbitrate persists an event, decides on it, records the consequences, and — on
// rejection — emits and enqueues the compensating events for the node that produced
// it. It returns the decision and the compensations.
//
// It is idempotent and safe to retry: before doing anything else it checks for a
// decision already recorded against this exact event ID and, if one exists, returns
// it and its compensations without re-running validators or re-emitting anything.
// Without this check, a retry after a crash between EmitCentral, Enqueue, and
// RecordDecision would re-run every side effect — duplicating a compensating event
// and its queue entry, or (since Append is itself idempotent and silently reports no
// new event on a retry) silently skipping arbitration on the retry while a caller
// still waits on a decision that was never recorded for it. Checking first avoids
// both failure modes uniformly.
func (a *Arbiter) Arbitrate(ctx context.Context, env domain.Envelope) (central.Decision, []domain.Envelope, error) {
	if existing, ok, err := a.store.Decision(ctx, env.ID); err != nil {
		return central.Decision{}, nil, fmt.Errorf("check existing decision for %s: %w", env.ID, err)
	} else if ok {
		comps, err := a.compensationsOf(ctx, env.ID)
		if err != nil {
			return central.Decision{}, nil, err
		}
		return existing, comps, nil
	}

	if _, err := a.store.Append(ctx, []domain.Envelope{env}); err != nil {
		return central.Decision{}, nil, err
	}
	// An event that cannot be decoded fails loudly. Skipping it would leave central
	// and the node with different histories, which is the one failure this system
	// must not have.
	payload, err := domain.DecodePayload(env)
	if err != nil {
		return central.Decision{}, nil, fmt.Errorf("arbitrate %s: %w", env.ID, err)
	}

	var rejection *Rejection
	for _, v := range a.validators {
		rejection, err = v.Validate(ctx, a.store, env, payload)
		if err != nil {
			return central.Decision{}, nil, fmt.Errorf("validator %s on %s: %w", v.Name(), env.ID, err)
		}
		if rejection != nil {
			break
		}
	}

	if err := a.record(ctx, env, payload, rejection); err != nil {
		return central.Decision{}, nil, err
	}
	if err := a.fanOut(ctx, env, payload, rejection); err != nil {
		return central.Decision{}, nil, err
	}

	decision := central.Decision{EventID: env.ID, Verdict: central.VerdictAccepted}
	if rejection == nil {
		return decision, nil, a.store.RecordDecision(ctx, decision)
	}

	// EmitCentral is not idempotent: it allocates a fresh sequence, HLC and event ID
	// on every call, so a retry after a crash between here and RecordDecision would
	// mint a second, distinct compensation for the same rejection. The natural-key
	// dedup on Enqueue cannot catch that — the two events have different IDs. The
	// compensations already in the log, keyed by causation, are the idempotency
	// marker: if any exist for this event, the earlier attempt got this far and they
	// are replayed rather than re-emitted.
	comps, err := a.compensationsOf(ctx, env.ID)
	if err != nil {
		return central.Decision{}, nil, err
	}
	if len(comps) == 0 {
		if comps, err = a.store.EmitCentral(ctx, rejection.Events, &env.ID, a.now()); err != nil {
			return central.Decision{}, nil, err
		}
	}
	if err := a.store.Enqueue(ctx, env.ID.NodeID, comps); err != nil {
		return central.Decision{}, nil, err
	}
	decision.Verdict, decision.Reason = central.VerdictRejected, rejection.Reason
	if len(comps) > 0 {
		decision.CompensatingID = &comps[0].ID
	}
	return decision, comps, a.store.RecordDecision(ctx, decision)
}

// compensationsOf returns the envelopes already emitted with causation pointing at
// id, for replaying the result of an earlier decision back to a caller that retried
// Arbitrate. Most decisions have none (accepted) or one (rejected); it is a small
// filter over the log rather than a dedicated index because retries are the
// exception, not the hot path.
func (a *Arbiter) compensationsOf(ctx context.Context, id domain.EventID) ([]domain.Envelope, error) {
	all, err := a.store.Events(ctx)
	if err != nil {
		return nil, fmt.Errorf("read compensations of %s: %w", id, err)
	}
	var comps []domain.Envelope
	for _, e := range all {
		if e.CausationID != nil && *e.CausationID == id {
			comps = append(comps, e)
		}
	}
	return comps, nil
}

// record folds the event's consequences into central's cross-node bookkeeping. For a
// rejected receipt only the accepted portion is counted, so the purchase order is not
// left permanently over-received by an event that was compensated away.
func (a *Arbiter) record(ctx context.Context, env domain.Envelope, payload any, rej *Rejection) error {
	switch p := payload.(type) {
	case domain.GoodsReceived:
		if rej != nil && rej.Reason != domain.ReasonPOOverReceipt {
			// A duplicate or an unknown SKU contributes nothing at all; the note's
			// first sighting is already recorded against the original receipt.
			return nil
		}
		qty := p.Move.Qty
		if rej != nil {
			qty -= rej.Excess
		}
		return a.store.RecordReceipt(ctx, central.ReceiptFact{EventID: env.ID, Node: env.ID.NodeID,
			PORef: p.PORef, DeliveryNote: p.DeliveryNote, SKU: p.Move.SKU, QtyBase: qty})
	case domain.TransferDispatched:
		for _, m := range p.Lines {
			if err := a.store.RecordDispatch(ctx, central.InTransitRow{TransferID: p.TransferID,
				Key: domain.StockKey{SKU: m.SKU, LotID: m.LotID}, FromNode: p.FromNode, ToNode: p.ToNode,
				Dispatched: m.Qty, DispatchedAt: env.RecordedAt}); err != nil {
				return err
			}
		}
		if rej == nil {
			return nil
		}
		// The transfer is dead: the stock is going back to the source, so its
		// in-transit rows must stop counting as goods on a truck.
		return a.store.FailTransfer(ctx, p.TransferID)
	case domain.TransferReceived:
		for _, m := range p.Lines {
			key := domain.StockKey{SKU: m.SKU, LotID: m.LotID}
			row, ok, err := a.store.InTransit(ctx, p.TransferID, key)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			accepted := m.Qty
			if remaining := row.Dispatched - row.Received; accepted > remaining {
				accepted = remaining
			}
			if accepted <= 0 {
				continue
			}
			if err := a.store.AddReceived(ctx, env.ID, p.TransferID, key, accepted); err != nil {
				return err
			}
		}
	}
	return nil
}

// fanOut forwards an event to whichever node needs it for its own view of a
// transfer. Transfers are the only cross-node flow: the destination must learn of a
// dispatch before it can record the truck arriving, and the source must learn the
// goods landed. A rejected TransferDispatched suppresses fan-out entirely — the
// transfer is dead and the destination has nothing useful to learn. A rejected
// TransferReceived (an over-receipt) does not: central still records the accepted
// portion and emits the destination's excess compensation separately, so the
// source must still see the receipt event or its transfer projection stays stuck
// showing the goods as in-transit forever.
func (a *Arbiter) fanOut(ctx context.Context, env domain.Envelope, payload any, rej *Rejection) error {
	switch p := payload.(type) {
	case domain.TransferDispatched:
		if rej != nil {
			return nil
		}
		return a.store.Enqueue(ctx, p.ToNode, []domain.Envelope{env})
	case domain.TransferReceived:
		for _, m := range p.Lines {
			row, ok, err := a.store.InTransit(ctx, p.TransferID, domain.StockKey{SKU: m.SKU, LotID: m.LotID})
			if err != nil {
				return err
			}
			if ok {
				return a.store.Enqueue(ctx, row.FromNode, []domain.Envelope{env})
			}
		}
	}
	return nil
}

// Discrepancies reports transfers dispatched before olderThan that are still carrying
// stock. It is a report and nothing else: an unmatched dispatch past the window is a
// lost truck, which is a human problem, and auto-compensating it would silently
// invent stock at the source that may well be sitting in a lay-by.
func Discrepancies(ctx context.Context, s central.Store, olderThan time.Time) ([]central.InTransitRow, error) {
	return s.OpenTransfers(ctx, olderThan)
}
