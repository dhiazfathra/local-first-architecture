package arbiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// errFake wraps a central.Store and forces one named method to fail on its Nth call
// (1-indexed; 0 means every call), delegating everything else — and every other
// call to that same method — to the underlying store. It exercises the error
// branches inside the arbiter and its validators without standing up a real,
// closeable store for every one of them.
type errFake struct {
	central.Store
	method string
	failOn int
	calls  *int
}

var errBoom = errors.New("boom")

func newErrFake(s central.Store, method string, failOn int) errFake {
	n := 0
	return errFake{Store: s, method: method, failOn: failOn, calls: &n}
}

func (f errFake) fail(method string) error {
	if f.method != method {
		return nil
	}
	*f.calls++
	if f.failOn == 0 || *f.calls == f.failOn {
		return errBoom
	}
	return nil
}

func (f errFake) Decision(ctx context.Context, id domain.EventID) (central.Decision, bool, error) {
	if err := f.fail("Decision"); err != nil {
		return central.Decision{}, false, err
	}
	return f.Store.Decision(ctx, id)
}

func (f errFake) Append(ctx context.Context, envs []domain.Envelope) ([]domain.Envelope, error) {
	if err := f.fail("Append"); err != nil {
		return nil, err
	}
	return f.Store.Append(ctx, envs)
}

func (f errFake) Events(ctx context.Context) ([]domain.Envelope, error) {
	if err := f.fail("Events"); err != nil {
		return nil, err
	}
	return f.Store.Events(ctx)
}

func (f errFake) Item(ctx context.Context, sku string) (domain.Item, bool, error) {
	if err := f.fail("Item"); err != nil {
		return domain.Item{}, false, err
	}
	return f.Store.Item(ctx, sku)
}

func (f errFake) DeliveryNoteFirstSeen(ctx context.Context, note, sku string) (domain.EventID, bool, error) {
	if err := f.fail("DeliveryNoteFirstSeen"); err != nil {
		return domain.EventID{}, false, err
	}
	return f.Store.DeliveryNoteFirstSeen(ctx, note, sku)
}

func (f errFake) PurchaseOrder(ctx context.Context, poRef, sku string) (float64, bool, error) {
	if err := f.fail("PurchaseOrder"); err != nil {
		return 0, false, err
	}
	return f.Store.PurchaseOrder(ctx, poRef, sku)
}

func (f errFake) ReceivedAgainstPO(ctx context.Context, poRef, sku string) (float64, error) {
	if err := f.fail("ReceivedAgainstPO"); err != nil {
		return 0, err
	}
	return f.Store.ReceivedAgainstPO(ctx, poRef, sku)
}

func (f errFake) Receipt(ctx context.Context, id domain.EventID) (central.ReceiptFact, bool, error) {
	if err := f.fail("Receipt"); err != nil {
		return central.ReceiptFact{}, false, err
	}
	return f.Store.Receipt(ctx, id)
}

func (f errFake) ReceivedFromEvent(ctx context.Context, id domain.EventID, transferID string,
	k domain.StockKey) (float64, error) {
	if err := f.fail("ReceivedFromEvent"); err != nil {
		return 0, err
	}
	return f.Store.ReceivedFromEvent(ctx, id, transferID, k)
}

func (f errFake) NodeConfig(ctx context.Context, node domain.NodeID) (map[string]bool, bool, error) {
	if err := f.fail("NodeConfig"); err != nil {
		return nil, false, err
	}
	return f.Store.NodeConfig(ctx, node)
}

func (f errFake) InTransit(ctx context.Context, transferID string, k domain.StockKey) (central.InTransitRow, bool, error) {
	if err := f.fail("InTransit"); err != nil {
		return central.InTransitRow{}, false, err
	}
	return f.Store.InTransit(ctx, transferID, k)
}

func (f errFake) RecordReceipt(ctx context.Context, fact central.ReceiptFact) error {
	if err := f.fail("RecordReceipt"); err != nil {
		return err
	}
	return f.Store.RecordReceipt(ctx, fact)
}

func (f errFake) RecordDispatch(ctx context.Context, row central.InTransitRow) error {
	if err := f.fail("RecordDispatch"); err != nil {
		return err
	}
	return f.Store.RecordDispatch(ctx, row)
}

func (f errFake) AddReceived(ctx context.Context, eventID domain.EventID, transferID string, k domain.StockKey, qty float64) error {
	if err := f.fail("AddReceived"); err != nil {
		return err
	}
	return f.Store.AddReceived(ctx, eventID, transferID, k, qty)
}

func (f errFake) FailTransfer(ctx context.Context, transferID string) error {
	if err := f.fail("FailTransfer"); err != nil {
		return err
	}
	return f.Store.FailTransfer(ctx, transferID)
}

func (f errFake) EmitCentral(ctx context.Context, events []domain.Event, causation *domain.EventID,
	now time.Time,
) ([]domain.Envelope, error) {
	if err := f.fail("EmitCentral"); err != nil {
		return nil, err
	}
	return f.Store.EmitCentral(ctx, events, causation, now)
}

func (f errFake) Enqueue(ctx context.Context, target domain.NodeID, envs []domain.Envelope) error {
	if err := f.fail("Enqueue"); err != nil {
		return err
	}
	return f.Store.Enqueue(ctx, target, envs)
}

func (f errFake) RecordDecision(ctx context.Context, d central.Decision) error {
	if err := f.fail("RecordDecision"); err != nil {
		return err
	}
	return f.Store.RecordDecision(ctx, d)
}

// seededMemory returns a memory store seeded exactly like newArbiter's, so an
// errFake wrapping it exercises the arbiter's real validator logic up to the one
// forced failure.
func seededMemory(t *testing.T) central.Store {
	t.Helper()
	ctx := context.Background()
	store := central.NewMemory()
	if err := store.UpsertItem(ctx, domain.Item{SKU: "WIDGET", Description: "Blue widget",
		BaseUoM: "EA", LotTracked: true, ShelfLifeDays: 30}); err != nil {
		t.Fatalf("UpsertItem: %v", err)
	}
	if err := store.UpsertPurchaseOrder(ctx, "PO-1", "WIDGET", 100); err != nil {
		t.Fatalf("UpsertPurchaseOrder: %v", err)
	}
	for _, node := range []domain.NodeID{"wh-a", "wh-b"} {
		if err := store.RegisterNode(ctx, node, nil); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
	}
	return store
}

// mustEnvelope seals a payload the same way env (in arbiter_test.go) does, but
// without needing a *testing.T, for envelope literals shared across the table in
// TestArbitratePropagatesStoreErrors.
func mustEnvelope(node domain.NodeID, seq uint64, typ, agg string, payload any) domain.Envelope {
	e, err := domain.NewEnvelope(
		domain.EventID{NodeID: node, Seq: seq},
		domain.HLC{Wall: at.UnixMilli() + int64(seq), Node: node},
		at.Add(time.Duration(seq)*time.Second), nil,
		domain.Event{Type: typ, AggregateID: agg, Payload: payload})
	if err != nil {
		panic(err)
	}
	return e
}

func goodReceipt() domain.Envelope {
	return mustEnvelope("wh-a", 1, domain.TypeGoodsReceived, "R1",
		goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 10, "RECV-01"))
}

func unknownSKUReceipt() domain.Envelope {
	return mustEnvelope("wh-a", 1, domain.TypeGoodsReceived, "R1",
		goodsReceived("R1", "DN-1", "PO-1", "GHOST", "", 5, "RECV-01"))
}

func dispatch() domain.Envelope {
	return mustEnvelope("wh-a", 1, domain.TypeTransferDispatched, "T1", domain.TransferDispatched{
		TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
		Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})
}

func receipt(qty float64) domain.Envelope {
	return mustEnvelope("wh-b", 1, domain.TypeTransferReceived, "T1", domain.TransferReceived{
		TransferID: "T1",
		Lines:      []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-09", Qty: qty}}})
}

func mustArbitrate(t *testing.T, a *Arbiter, e domain.Envelope) {
	t.Helper()
	if _, _, err := a.Arbitrate(context.Background(), e); err != nil {
		t.Fatalf("seed Arbitrate: %v", err)
	}
}

// TestArbitratePropagatesStoreErrors drives every store-failure branch in Arbitrate,
// record, fanOut and the five validators by forcing one store method to fail on a
// specific call, against an otherwise fully seeded and working memory store.
func TestArbitratePropagatesStoreErrors(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name   string
		method string
		failOn int
		seed   func(t *testing.T, a *Arbiter)
		event  domain.Envelope
	}{
		{name: "Decision before anything else", method: "Decision", failOn: 1, event: goodReceipt()},
		{name: "Append after a fresh decision check", method: "Append", failOn: 1, event: goodReceipt()},
		{name: "unknownSKU validator's Item call", method: "Item", failOn: 1, event: goodReceipt()},
		{name: "duplicateDeliveryNote validator", method: "DeliveryNoteFirstSeen", failOn: 1, event: goodReceipt()},
		{name: "poOverReceipt validator's PurchaseOrder call", method: "PurchaseOrder", failOn: 1, event: goodReceipt()},
		{name: "poOverReceipt validator's ReceivedAgainstPO call", method: "ReceivedAgainstPO", failOn: 1, event: goodReceipt()},
		{name: "poOverReceipt validator's own-contribution Receipt call", method: "Receipt", failOn: 1, event: goodReceipt()},
		{name: "transferDestination validator", method: "NodeConfig", failOn: 1, event: dispatch()},
		{name: "transferOverReceipt validator", method: "InTransit", failOn: 1, event: receipt(4)},
		{
			name: "transferOverReceipt validator's ReceivedFromEvent call", method: "ReceivedFromEvent", failOn: 1,
			seed: func(t *testing.T, a *Arbiter) { mustArbitrate(t, a, dispatch()) }, event: receipt(6),
		},
		{name: "record's RecordReceipt on acceptance", method: "RecordReceipt", failOn: 1, event: goodReceipt()},
		{name: "record's RecordDispatch on acceptance", method: "RecordDispatch", failOn: 1, event: dispatch()},
		{
			name: "record's AddReceived on an accepted transfer receipt", method: "AddReceived", failOn: 1,
			seed: func(t *testing.T, a *Arbiter) { mustArbitrate(t, a, dispatch()) }, event: receipt(6),
		},
		{
			name: "record's second InTransit call on an accepted transfer receipt", method: "InTransit", failOn: 2,
			seed: func(t *testing.T, a *Arbiter) { mustArbitrate(t, a, dispatch()) }, event: receipt(6),
		},
		{
			name: "fanOut's InTransit call on an accepted transfer receipt", method: "InTransit", failOn: 3,
			seed: func(t *testing.T, a *Arbiter) { mustArbitrate(t, a, dispatch()) }, event: receipt(6),
		},
		{name: "fanOut's Enqueue on an accepted dispatch", method: "Enqueue", failOn: 1, event: dispatch()},
		{name: "compensation replay check on a fresh rejection", method: "Events", failOn: 1, event: unknownSKUReceipt()},
		{name: "EmitCentral on rejection", method: "EmitCentral", failOn: 1, event: unknownSKUReceipt()},
		{name: "Enqueue of the compensation on rejection", method: "Enqueue", failOn: 1, event: unknownSKUReceipt()},
		{name: "RecordDecision on acceptance", method: "RecordDecision", failOn: 1, event: goodReceipt()},
		{name: "RecordDecision on rejection", method: "RecordDecision", failOn: 1, event: unknownSKUReceipt()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := seededMemory(t)
			seedArbiter := New(base, func() time.Time { return at })
			if tc.seed != nil {
				tc.seed(t, seedArbiter)
			}
			store := newErrFake(base, tc.method, tc.failOn)
			a := New(store, func() time.Time { return at })
			if _, _, err := a.Arbitrate(ctx, tc.event); err == nil {
				t.Fatalf("Arbitrate with failing %s (call %d): expected an error", tc.method, tc.failOn)
			}
		})
	}

	t.Run("compensationsOf on a retried rejection", func(t *testing.T) {
		base := seededMemory(t)
		bad := unknownSKUReceipt()
		mustArbitrate(t, New(base, func() time.Time { return at }), bad)

		store := newErrFake(base, "Events", 1)
		a := New(store, func() time.Time { return at })
		if _, _, err := a.Arbitrate(ctx, bad); err == nil {
			t.Fatal("retried Arbitrate with failing Events: expected an error")
		}
	})
}

// TestPOOverReceiptClampsExcessToTheReceivedQuantity covers the defensive clamp:
// if the purchase order's ordered quantity is lowered after receipts were already
// recorded against it, the computed excess can exceed the new receipt's own
// quantity, and must be clamped to it rather than compensating more than arrived.
func TestPOOverReceiptClampsExcessToTheReceivedQuantity(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	if _, _, err := a.Arbitrate(ctx, env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
		goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 90, "RECV-01"))); err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if err := store.UpsertPurchaseOrder(ctx, "PO-1", "WIDGET", 5); err != nil {
		t.Fatalf("UpsertPurchaseOrder: %v", err)
	}
	_, comps, err := a.Arbitrate(ctx, env(t, "wh-b", 1, domain.TypeGoodsReceived, "R2",
		goodsReceived("R2", "DN-2", "PO-1", "WIDGET", "L1", 3, "RECV-09")))
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if got := decodeAdjusted(t, comps[0]).Move.Qty; got != 3 {
		t.Errorf("compensated quantity = %v, want the clamped 3 (the whole receipt), not the raw excess", got)
	}
}

// TestRecordSkipsAZeroRemainingTransferLine proves that record's per-line loop over
// a TransferReceived skips a line that has nothing left to accept — e.g. a second,
// wholly over-received receipt against a transfer already fully received — rather
// than recording a negative or zero addition.
func TestRecordSkipsAZeroRemainingTransferLine(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
	if _, _, err := a.Arbitrate(ctx, dispatch()); err != nil {
		t.Fatalf("Arbitrate dispatch: %v", err)
	}
	if _, _, err := a.Arbitrate(ctx, receipt(6)); err != nil {
		t.Fatalf("Arbitrate full receipt: %v", err)
	}
	row, ok, err := store.InTransit(ctx, "T1", key)
	if err != nil || !ok || row.Dispatched-row.Received != 0 {
		t.Fatalf("in transit after full receipt = %+v, ok %v, err %v; want fully closed", row, ok, err)
	}
	// Nothing remains, so this second receipt is entirely over-received: record's
	// loop must compute a non-positive accepted quantity and skip it.
	decision, _, err := a.Arbitrate(ctx, env(t, "wh-b", 2, domain.TypeTransferReceived, "T1",
		domain.TransferReceived{TransferID: "T1",
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-09", Qty: 3}}}))
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictRejected {
		t.Fatalf("decision = %+v, want rejected", decision)
	}
	if row, _, err = store.InTransit(ctx, "T1", key); err != nil || row.Received != 6 {
		t.Fatalf("in transit after the over-received retry = %+v, %v; want Received unchanged at 6", row, err)
	}
}

// TestArbitrateAcceptsAnEventWithNoStockMovement proves that movementsOf's default
// case — an event type that moves no stock, like an internal PutAway — flows
// straight through every validator (each of which finds nothing to object to) to
// an unqualified acceptance.
func TestArbitrateAcceptsAnEventWithNoStockMovement(t *testing.T) {
	a, _ := newArbiter(t)
	e := env(t, "wh-a", 1, domain.TypePutAway, "R1", domain.PutAway{
		Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "DOCK", To: "PICK-01", Qty: 4}})
	decision, comps, err := a.Arbitrate(context.Background(), e)
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictAccepted || len(comps) != 0 {
		t.Fatalf("decision = %+v, comps = %+v; want an unqualified acceptance", decision, comps)
	}
}
