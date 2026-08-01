package arbiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// crash arbitrates e against a store whose RecordDecision fails, leaving exactly the
// state a process crash between record's side effects and the decision write would
// leave: the receipt folded, the compensation possibly emitted, no decision stored.
// It then returns an arbiter over the same underlying store for the retry.
func crash(t *testing.T, store central.Store, e domain.Envelope) *Arbiter {
	t.Helper()
	now := func() time.Time { return at }
	broken := New(newErrFake(store, "RecordDecision", 1), now)
	if _, _, err := broken.Arbitrate(context.Background(), e); !errors.Is(err, errBoom) {
		t.Fatalf("crashing Arbitrate: err = %v, want errBoom", err)
	}
	if _, ok, err := store.Decision(context.Background(), e.ID); err != nil || ok {
		t.Fatalf("decision after crash = ok %v, err %v; want none recorded", ok, err)
	}
	return New(store, now)
}

// TestRetryAfterCrashDoesNotRejectItsOwnRecordedReceipt covers finding #42 on the PO
// side: RecordReceipt runs before RecordDecision, so a retry re-reaching
// poOverReceipt finds its own quantity already summed into ReceivedAgainstPO. Without
// excluding it the retry rejects an event that was correctly accepted and emits a
// bogus compensation.
func TestRetryAfterCrashDoesNotRejectItsOwnRecordedReceipt(t *testing.T) {
	ctx := context.Background()
	store := seededMemory(t) // PO-1 ordered 100
	e := mustEnvelope("wh-a", 1, domain.TypeGoodsReceived, "R1",
		goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 60, "RECV-01"))

	a := crash(t, store, e)
	decision, comps, err := a.Arbitrate(ctx, e)
	if err != nil {
		t.Fatalf("retry Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictAccepted || len(comps) != 0 {
		t.Fatalf("retry decision = %+v, comps %+v; want an acceptance with no compensation", decision, comps)
	}
	if total, err := store.ReceivedAgainstPO(ctx, "PO-1", "WIDGET"); err != nil || total != 60 {
		t.Fatalf("ReceivedAgainstPO = %v, %v; want 60, nil", total, err)
	}
}

// TestRetryAfterCrashDoesNotRejectItsOwnTransferReceipt is the same window on the
// transfer side (finding #42, transferOverReceipt): AddReceived has already moved the
// in-transit balance, so a retry that does not add its own folded quantity back sees
// no remaining balance and rejects itself as an over-receipt.
func TestRetryAfterCrashDoesNotRejectItsOwnTransferReceipt(t *testing.T) {
	ctx := context.Background()
	store := seededMemory(t)
	a := New(store, func() time.Time { return at })
	mustArbitrate(t, a, dispatch()) // 6 units in transit on T1

	r := receipt(6)
	a = crash(t, store, r)
	decision, comps, err := a.Arbitrate(ctx, r)
	if err != nil {
		t.Fatalf("retry Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictAccepted || len(comps) != 0 {
		t.Fatalf("retry decision = %+v, comps %+v; want an acceptance with no compensation", decision, comps)
	}
	row, ok, err := store.InTransit(ctx, "T1", domain.StockKey{SKU: "WIDGET", LotID: "L1"})
	if err != nil || !ok || row.Received != 6 {
		t.Fatalf("in-transit row = %+v, ok %v, err %v; want 6 received exactly once", row, ok, err)
	}
}

// TestRetryAfterCrashEmitsOneCompensation covers finding #8: EmitCentral mints a new
// event ID on every call, so a retry after a crash between it and RecordDecision
// would leave two distinct compensations in the log for one rejection. The
// compensations already carrying this event's causation are the marker that says the
// earlier attempt got that far.
func TestRetryAfterCrashEmitsOneCompensation(t *testing.T) {
	ctx := context.Background()
	store := seededMemory(t)
	e := unknownSKUReceipt()

	a := crash(t, store, e)
	decision, comps, err := a.Arbitrate(ctx, e)
	if err != nil {
		t.Fatalf("retry Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictRejected || len(comps) != 1 {
		t.Fatalf("retry decision = %+v, comps %+v; want one rejection compensation", decision, comps)
	}
	all, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	var emitted []domain.Envelope
	for _, env := range all {
		if env.CausationID != nil && *env.CausationID == e.ID {
			emitted = append(emitted, env)
		}
	}
	if len(emitted) != 1 {
		t.Fatalf("compensations in the log = %d, want exactly 1: EmitCentral must not re-mint on retry", len(emitted))
	}
	if decision.CompensatingID == nil || *decision.CompensatingID != emitted[0].ID {
		t.Fatalf("decision.CompensatingID = %v, want the replayed compensation %v",
			decision.CompensatingID, emitted[0].ID)
	}
	// The queue must not carry the compensation twice either.
	out, err := store.Outbound(ctx, "wh-a", 0, 10)
	if err != nil {
		t.Fatalf("Outbound: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("outbound entries = %d, want 1", len(out))
	}
}
