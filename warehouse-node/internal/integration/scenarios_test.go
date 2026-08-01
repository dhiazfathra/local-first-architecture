package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/projection"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

// TestBothNodesReceiveAgainstOnePurchaseOrder is the invariant no node can enforce:
// neither warehouse can see the other's receipts, so only central knows the order has
// been over-received. Exactly one compensation must result — one per rejected event,
// not one per earlier receipt.
func TestBothNodesReceiveAgainstOnePurchaseOrder(t *testing.T) {
	c := newCluster(t, 100, nil)
	ctx := context.Background()

	if _, err := c.node("wh-a").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA",
		DeliveryNote: "DN-A", PoRef: "PO-1", Line: line(80), To: "RECV-wh-a"}); err != nil {
		t.Fatalf("Receive at wh-a: %v", err)
	}
	if _, err := c.node("wh-b").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RB",
		DeliveryNote: "DN-B", PoRef: "PO-1", Line: line(40), To: "RECV-wh-b"}); err != nil {
		t.Fatalf("Receive at wh-b: %v", err)
	}
	// wh-a reaches central first, so its 80 units are accepted in full.
	c.syncNode("wh-a")
	c.syncNode("wh-b")

	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 80 {
		t.Errorf("wh-a dock = %v, want 80 untouched", got)
	}
	if got := c.node("wh-b").balance("WIDGET", "RECV-wh-b"); got != 20 {
		t.Errorf("wh-b dock = %v, want 20: the 20 over the order are compensated away", got)
	}
	if exceptions, err := c.node("wh-a").svc.Exceptions(); err != nil || len(exceptions) != 0 {
		t.Errorf("wh-a exceptions = %+v, %v; want none", exceptions, err)
	}
	exceptions, err := c.node("wh-b").svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 {
		t.Fatalf("wh-b exceptions = %+v, want exactly one over-receipt compensation", exceptions)
	}
	if exceptions[0].Reason != domain.ReasonPOOverReceipt || exceptions[0].Qty != 20 {
		t.Errorf("exception = %+v, want po_overreceipt for 20", exceptions[0])
	}
	for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
		assertDeterministic(t, c.node(id))
	}
}

// TestSameDeliveryNoteAtTwoNodes is cross-node duplicate detection: the same supplier
// paperwork keyed twice, once at each warehouse.
func TestSameDeliveryNoteAtTwoNodes(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()

	for _, spec := range []struct {
		id      domain.NodeID
		receipt string
	}{{"wh-a", "RA"}, {"wh-b", "RB"}} {
		if _, err := c.node(spec.id).api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: spec.receipt,
			DeliveryNote: "DN-DUPE", PoRef: "PO-1", Line: line(12),
			To: "RECV-" + string(spec.id)}); err != nil {
			t.Fatalf("Receive at %s: %v", spec.id, err)
		}
	}
	c.syncNode("wh-a")
	c.syncNode("wh-b")

	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 12 {
		t.Errorf("wh-a dock = %v, want 12: the first sighting of a note is the real one", got)
	}
	if got := c.node("wh-b").balance("WIDGET", "RECV-wh-b"); got != 0 {
		t.Errorf("wh-b dock = %v, want 0: the whole duplicated line is removed", got)
	}
	exceptions, err := c.node("wh-b").svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 || exceptions[0].Reason != domain.ReasonDuplicateReceipt {
		t.Fatalf("wh-b exceptions = %+v, want one duplicate_receipt row", exceptions)
	}
	for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
		assertDeterministic(t, c.node(id))
	}
}

// TestReceiptAgainstASKUCentralHasDeleted covers the stale-master case: a node
// validates SKUs against its replicated copy of the item master, which may be out of
// date. Central re-checks against the live one and rejects.
func TestReceiptAgainstASKUCentralHasDeleted(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()

	deleted := widget()
	deleted.Deleted = true
	if err := c.store.UpsertItem(ctx, deleted); err != nil {
		t.Fatalf("UpsertItem: %v", err)
	}

	// The node's copy still says the SKU is live, so the command succeeds locally.
	if _, err := c.node("wh-a").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA",
		DeliveryNote: "DN-A", PoRef: "PO-1", Line: line(5), To: "RECV-wh-a"}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	c.syncAll()

	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 0 {
		t.Errorf("dock = %v, want 0: the sku is zeroed", got)
	}
	exceptions, err := c.node("wh-a").svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 || exceptions[0].Reason != domain.ReasonUnknownSKU {
		t.Fatalf("exceptions = %+v, want one unknown_sku row for manual cleanup", exceptions)
	}
	assertDeterministic(t, c.node("wh-a"))
}

// TestTwoHundredOperationsOfflineThenOneCompensation is the week-offline case: a long
// backlog syncs in bounded chunks, exactly one event in it is rejected, and the
// operator can see which one and why.
func TestTwoHundredOperationsOfflineThenOneCompensation(t *testing.T) {
	c := newCluster(t, 100000, nil)
	ctx := context.Background()

	// wh-b keys delivery note DN-003 first and syncs, so it owns that note.
	if _, err := c.node("wh-b").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RB",
		DeliveryNote: "DN-003", PoRef: "PO-1", Line: line(1), To: "RECV-wh-b"}); err != nil {
		t.Fatalf("Receive at wh-b: %v", err)
	}
	c.syncNode("wh-b")

	// wh-a now works offline for 200 operations. Operation 3 reuses DN-003.
	var rejected string
	for i := 1; i <= 200; i++ {
		note := fmt.Sprintf("DN-%03d", i)
		resp, err := c.node("wh-a").api.Receive(ctx, &nodeapi.ReceiveRequest{
			ReceiptId: fmt.Sprintf("RA-%03d", i), DeliveryNote: note, PoRef: "PO-1",
			Line: line(1), To: "RECV-wh-a"})
		if err != nil {
			t.Fatalf("Receive %d: %v", i, err)
		}
		if i == 3 {
			// The GoodsReceived event is the last of the three a new receipt emits,
			// and it is the one central rejects.
			rejected = resp.GetEventIds()[len(resp.GetEventIds())-1]
		}
	}
	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 200 {
		t.Fatalf("dock = %v, want 200: every offline operation stood", got)
	}

	c.syncNode("wh-a")

	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 199 {
		t.Errorf("dock = %v, want 199: exactly one unit was compensated", got)
	}
	exceptions, err := c.node("wh-a").svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 {
		t.Fatalf("exceptions = %+v, want exactly one", exceptions)
	}
	if exceptions[0].Kind != projection.ExceptionCompensation {
		t.Errorf("Kind = %q, want %q", exceptions[0].Kind, projection.ExceptionCompensation)
	}
	if exceptions[0].Reason != domain.ReasonDuplicateReceipt {
		t.Errorf("Reason = %q, want %q", exceptions[0].Reason, domain.ReasonDuplicateReceipt)
	}
	if exceptions[0].CausedBy != rejected {
		t.Errorf("CausedBy = %q, want the operation-3 event %q", exceptions[0].CausedBy, rejected)
	}
	assertDeterministic(t, c.node("wh-a"))
}

// TestCompensationDrivesALocationNegative is the spec's accepted limitation. The stock
// was picked and shipped before central's rejection arrived, so undoing the receipt
// cannot undo the pick — cascading compensation through downstream events is
// explicitly out of scope. The resulting negative balance is real, is flagged, and
// waits for a human with a clipboard.
func TestCompensationDrivesALocationNegative(t *testing.T) {
	c := newCluster(t, 6, nil) // the order allows six; the operator receives ten
	ctx := context.Background()
	w := c.node("wh-a")
	key := domain.StockKey{SKU: "WIDGET", Location: "RECV-wh-a", LotID: "L1"}

	if _, err := w.api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA", DeliveryNote: "DN-A",
		PoRef: "PO-1", Line: line(10), To: "RECV-wh-a"}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	// All ten are picked and shipped while the line is still down.
	if _, err := w.api.Pick(ctx, &nodeapi.PickRequest{Line: line(10), From: "RECV-wh-a",
		OrderRef: "SO-1"}); err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got := w.balance("WIDGET", "RECV-wh-a"); got != 0 {
		t.Fatalf("dock = %v, want 0 before the compensation lands", got)
	}

	c.syncAll()

	balance, err := w.svc.StockOnHand("WIDGET", "RECV-wh-a")
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(balance) != 1 || balance[0].Qty != -4 {
		t.Fatalf("balance = %+v, want a single row of -4: a negative balance must be shown, not hidden",
			balance)
	}

	exceptions, err := w.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	var negative, compensation *projection.ExceptionRow
	for i := range exceptions {
		switch exceptions[i].Kind {
		case projection.ExceptionNegativeBalance:
			negative = &exceptions[i]
		case projection.ExceptionCompensation:
			compensation = &exceptions[i]
		}
	}
	if compensation == nil || compensation.Reason != domain.ReasonPOOverReceipt {
		t.Fatalf("exceptions = %+v, want a po_overreceipt compensation row", exceptions)
	}
	if negative == nil {
		t.Fatalf("exceptions = %+v, want a negative_balance row", exceptions)
	}
	if negative.Resolved {
		t.Error("the negative balance is marked resolved; it needs a human, not a shrug")
	}
	if negative.Qty != -4 || negative.Key != key {
		t.Errorf("negative row = %+v, want -4 at %+v", negative, key)
	}

	// The sanctioned human repair: count the shelf. Counting zero books a +4 variance,
	// the balance returns to zero and the exception resolves.
	if _, err := w.api.StartCount(ctx, &nodeapi.StartCountRequest{CountId: "C1",
		Location: "RECV-wh-a"}); err != nil {
		t.Fatalf("StartCount: %v", err)
	}
	if _, err := w.api.CountLine(ctx, &nodeapi.CountLineRequest{CountId: "C1",
		Line: &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: 0, Uom: "EA"}}); err != nil {
		t.Fatalf("CountLine: %v", err)
	}
	if _, err := w.api.CloseCount(ctx, &nodeapi.CloseCountRequest{CountId: "C1"}); err != nil {
		t.Fatalf("CloseCount: %v", err)
	}

	if got, err := w.svc.StockOnHand("WIDGET", "RECV-wh-a"); err != nil || len(got) != 0 {
		t.Fatalf("balance after the count = %+v, %v; want the row gone at zero", got, err)
	}
	exceptions, err = w.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	for _, row := range exceptions {
		if row.Kind == projection.ExceptionNegativeBalance && !row.Resolved {
			t.Errorf("negative row = %+v, want it resolved after the stock count", row)
		}
	}
	// The compensation itself is never resolved away: it is a permanent audit record.
	if compensation.Resolved {
		t.Error("the compensation row was resolved; it is history, not a task")
	}
	assertDeterministic(t, w)
}
