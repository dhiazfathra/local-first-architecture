package integration

import (
	"context"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/arbiter"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

func line(qty float64) *nodeapi.Line {
	return &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: qty, Uom: "EA"}
}

// stockAt receives qty widgets at a node's dock and puts them on its pick face, so a
// transfer has something to dispatch.
func stockAt(t *testing.T, c *cluster, id domain.NodeID, receipt, note string, qty float64) {
	t.Helper()
	ctx := context.Background()
	api := c.node(id).api
	if _, err := api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: receipt, DeliveryNote: note,
		PoRef: "PO-1", Line: line(qty), To: "RECV-" + string(id)}); err != nil {
		t.Fatalf("Receive at %s: %v", id, err)
	}
	if _, err := api.PutAway(ctx, &nodeapi.PutAwayRequest{Line: line(qty),
		From: "RECV-" + string(id), To: "PICK-" + string(id)}); err != nil {
		t.Fatalf("PutAway at %s: %v", id, err)
	}
}

// TestTransferHappyPath is the in-transit sandwich: zero, then six, then zero.
func TestTransferHappyPath(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()
	key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
	source, dest := c.node("wh-a"), c.node("wh-b")

	stockAt(t, c, "wh-a", "R1", "DN-1", 10)
	c.syncAll()
	if got := c.inTransit("T1", key); got != 0 {
		t.Fatalf("in transit before the dispatch = %v, want 0", got)
	}

	if _, err := source.api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
		ToNode: "wh-b", From: "PICK-wh-a", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("DispatchTransfer: %v", err)
	}
	// The source's stock left the moment the truck did, before central knows anything.
	if got := source.balance("WIDGET", "PICK-wh-a"); got != 4 {
		t.Fatalf("source pick face = %v, want 4 immediately after dispatch", got)
	}
	c.syncAll()

	if got := c.inTransit("T1", key); got != 6 {
		t.Fatalf("in transit between dispatch and receipt = %v, want 6", got)
	}
	if got := dest.balance("WIDGET", "RECV-wh-b"); got != 0 {
		t.Fatalf("destination dock = %v, want 0: the truck has not arrived", got)
	}

	if _, err := dest.api.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{TransferId: "T1",
		To: "RECV-wh-b", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("ReceiveTransfer: %v", err)
	}
	c.syncAll()

	if got := c.inTransit("T1", key); got != 0 {
		t.Fatalf("in transit after the receipt = %v, want 0", got)
	}
	if got := dest.balance("WIDGET", "RECV-wh-b"); got != 6 {
		t.Fatalf("destination dock = %v, want 6", got)
	}
	if got := source.balance("WIDGET", "PICK-wh-a"); got != 4 {
		t.Fatalf("source pick face = %v, want 4", got)
	}

	// Both nodes' transfer views close, and nobody was compensated.
	for _, w := range []*warehouse{source, dest} {
		rows, err := w.svc.Transfers()
		if err != nil {
			t.Fatalf("Transfers: %v", err)
		}
		if len(rows) != 1 || rows[0].Status != domain.TransferComplete {
			t.Errorf("%s transfers = %+v, want one complete row", w.svc.NodeID(), rows)
		}
		exceptions, err := w.svc.Exceptions()
		if err != nil {
			t.Fatalf("Exceptions: %v", err)
		}
		if len(exceptions) != 0 {
			t.Errorf("%s exceptions = %+v, want none", w.svc.NodeID(), exceptions)
		}
		assertDeterministic(t, w)
	}
}

// TestTransferToARefusingDestinationRestoresSourceStock covers the invariant no node
// can check: the destination's configuration is not the source's business.
func TestTransferToARefusingDestinationRestoresSourceStock(t *testing.T) {
	c := newCluster(t, 1000, map[domain.NodeID][]string{"wh-b": {"WIDGET"}})
	ctx := context.Background()
	source := c.node("wh-a")

	stockAt(t, c, "wh-a", "R1", "DN-1", 10)
	c.syncAll()

	if _, err := source.api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
		ToNode: "wh-b", From: "PICK-wh-a", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("DispatchTransfer: %v", err)
	}
	if got := source.balance("WIDGET", "PICK-wh-a"); got != 4 {
		t.Fatalf("source pick face = %v, want 4 before central weighs in", got)
	}
	c.syncAll()

	if got := source.balance("WIDGET", "PICK-wh-a"); got != 10 {
		t.Fatalf("source pick face = %v, want 10: the compensation restores the stock", got)
	}
	rows, err := source.svc.Transfers()
	if err != nil {
		t.Fatalf("Transfers: %v", err)
	}
	if len(rows) != 1 || rows[0].Status != domain.TransferFailed {
		t.Fatalf("transfers = %+v, want one failed row", rows)
	}
	exceptions, err := source.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 || exceptions[0].Reason != domain.ReasonTransferRejected {
		t.Fatalf("exceptions = %+v, want one transfer_rejected row", exceptions)
	}
	// The destination never learns of a transfer that was refused.
	destRows, err := c.node("wh-b").svc.Transfers()
	if err != nil {
		t.Fatalf("destination Transfers: %v", err)
	}
	if len(destRows) != 0 {
		t.Errorf("destination transfers = %+v, want none", destRows)
	}
	assertDeterministic(t, source)
}

func TestTransferOverReceiptIsCompensatedAtTheDestination(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()
	source, dest := c.node("wh-a"), c.node("wh-b")

	stockAt(t, c, "wh-a", "R1", "DN-1", 10)
	c.syncAll()
	if _, err := source.api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
		ToNode: "wh-b", From: "PICK-wh-a", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("DispatchTransfer: %v", err)
	}
	c.syncAll()

	// The operator books ten off a truck that carried six. Only central can see that.
	if _, err := dest.api.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{TransferId: "T1",
		To: "RECV-wh-b", Lines: []*nodeapi.Line{line(10)}}); err != nil {
		t.Fatalf("ReceiveTransfer: %v", err)
	}
	if got := dest.balance("WIDGET", "RECV-wh-b"); got != 10 {
		t.Fatalf("destination dock = %v, want the operator's 10 before arbitration", got)
	}
	c.syncAll()

	if got := dest.balance("WIDGET", "RECV-wh-b"); got != 6 {
		t.Fatalf("destination dock = %v, want 6 after the over-receipt is compensated", got)
	}
	if got := c.inTransit("T1", domain.StockKey{SKU: "WIDGET", LotID: "L1"}); got != 0 {
		t.Errorf("in transit = %v, want 0", got)
	}
	exceptions, err := dest.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 || exceptions[0].Reason != domain.ReasonTransferOverReceipt {
		t.Fatalf("exceptions = %+v, want one transfer_overreceipt row", exceptions)
	}
	assertDeterministic(t, dest)
}

// TestUnmatchedDispatchIsReportedNotCompensated is the lost-truck case. The window is
// configurable; past it, central reports and does nothing else.
func TestUnmatchedDispatchIsReportedNotCompensated(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()
	source := c.node("wh-a")

	stockAt(t, c, "wh-a", "R1", "DN-1", 10)
	c.syncAll()
	if _, err := source.api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
		ToNode: "wh-b", From: "PICK-wh-a", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("DispatchTransfer: %v", err)
	}
	c.syncAll()

	tests := []struct {
		name   string
		window time.Duration
		want   int
	}{
		{name: "inside a 48h window nothing is reported", window: 48 * time.Hour},
		{name: "past a 1h window the transfer is reported", window: time.Hour, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := arbiter.Discrepancies(ctx, c.store, at.Add(24*time.Hour).Add(-tt.window))
			if err != nil {
				t.Fatalf("Discrepancies: %v", err)
			}
			if len(rows) != tt.want {
				t.Fatalf("reported %d rows, want %d: %+v", len(rows), tt.want, rows)
			}
		})
	}

	c.syncAll()
	if got := source.balance("WIDGET", "PICK-wh-a"); got != 4 {
		t.Errorf("source pick face = %v, want 4: a late truck must not restore stock", got)
	}
	exceptions, err := source.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 0 {
		t.Errorf("exceptions = %+v, want none: a lost truck is reported, never compensated", exceptions)
	}
	if got := c.inTransit("T1", domain.StockKey{SKU: "WIDGET", LotID: "L1"}); got != 6 {
		t.Errorf("in transit = %v, want it still 6: the goods are somewhere", got)
	}
	assertDeterministic(t, source)
}
