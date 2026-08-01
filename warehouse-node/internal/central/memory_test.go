package central

import (
	"context"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

func TestMemorySatisfiesTheStoreContract(t *testing.T) {
	runStoreContract(t, func(*testing.T) Store { return NewMemory() })
}

func TestMemoryCloseIsANoOp(t *testing.T) {
	if err := NewMemory().Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSortedEventsBreaksTiesBySeqWhenHLCIsEqual(t *testing.T) {
	m, ctx := NewMemory(), context.Background()
	hlc := domain.HLC{Wall: 1, Node: "wh-a"}
	at := time.Now()
	env2, err := domain.NewEnvelope(domain.EventID{NodeID: "wh-a", Seq: 2}, hlc, at, nil,
		domain.Event{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.PutAway{
			Move: domain.Movement{SKU: "WIDGET", From: "RECV-01", To: "PICK-01", Qty: 2}}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	env1, err := domain.NewEnvelope(domain.EventID{NodeID: "wh-a", Seq: 1}, hlc, at, nil,
		domain.Event{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.PutAway{
			Move: domain.Movement{SKU: "WIDGET", From: "RECV-01", To: "PICK-01", Qty: 1}}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if _, err := m.Append(ctx, []domain.Envelope{env2, env1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	all, err := m.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(all) != 2 || all[0].ID.Seq != 1 || all[1].ID.Seq != 2 {
		t.Fatalf("Events = %+v, want seq 1 then 2 when HLC ties", all)
	}
}

func TestEmitCentralPropagatesAnInvalidPayloadError(t *testing.T) {
	m := NewMemory()
	_, err := m.EmitCentral(context.Background(), []domain.Event{
		{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.Picked{}},
	}, nil, time.Now())
	if err == nil {
		t.Fatal("EmitCentral with a mismatched payload: expected an error")
	}
}

func TestDeliveryNoteFirstSeenPrefersTheEarlierEventAcrossNodes(t *testing.T) {
	m, ctx := NewMemory(), context.Background()
	later := ReceiptFact{EventID: domain.EventID{NodeID: "wh-b", Seq: 1}, Node: "wh-b",
		PORef: "PO-1", DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 5}
	earlier := ReceiptFact{EventID: domain.EventID{NodeID: "wh-a", Seq: 1}, Node: "wh-a",
		PORef: "PO-1", DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 5}
	if err := m.RecordReceipt(ctx, later); err != nil {
		t.Fatalf("RecordReceipt: %v", err)
	}
	if err := m.RecordReceipt(ctx, earlier); err != nil {
		t.Fatalf("RecordReceipt: %v", err)
	}
	got, ok, err := m.DeliveryNoteFirstSeen(ctx, "DN-1", "WIDGET")
	if err != nil || !ok || got != earlier.EventID {
		t.Fatalf("DeliveryNoteFirstSeen = %v, ok %v, err %v; want %v, true, nil",
			got, ok, err, earlier.EventID)
	}
}

func TestDeliveryNoteFirstSeenIgnoresOtherNotesAndSKUsAndTiesBySeq(t *testing.T) {
	m, ctx := NewMemory(), context.Background()
	// A receipt for a different note/SKU must be skipped (the continue branch).
	if err := m.RecordReceipt(ctx, ReceiptFact{EventID: domain.EventID{NodeID: "wh-a", Seq: 1},
		Node: "wh-a", PORef: "PO-1", DeliveryNote: "DN-OTHER", SKU: "WIDGET", QtyBase: 1}); err != nil {
		t.Fatalf("RecordReceipt: %v", err)
	}
	// Two receipts from the same node let less() compare by Seq rather than NodeID.
	later := ReceiptFact{EventID: domain.EventID{NodeID: "wh-a", Seq: 5}, Node: "wh-a",
		PORef: "PO-1", DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 5}
	earlier := ReceiptFact{EventID: domain.EventID{NodeID: "wh-a", Seq: 2}, Node: "wh-a",
		PORef: "PO-1", DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 5}
	if err := m.RecordReceipt(ctx, later); err != nil {
		t.Fatalf("RecordReceipt: %v", err)
	}
	if err := m.RecordReceipt(ctx, earlier); err != nil {
		t.Fatalf("RecordReceipt: %v", err)
	}
	got, ok, err := m.DeliveryNoteFirstSeen(ctx, "DN-1", "WIDGET")
	if err != nil || !ok || got != earlier.EventID {
		t.Fatalf("DeliveryNoteFirstSeen = %v, ok %v, err %v; want %v, true, nil",
			got, ok, err, earlier.EventID)
	}
}

func TestRecordDispatchUpdatesAnExistingRowsDispatchedQuantity(t *testing.T) {
	m, ctx := NewMemory(), context.Background()
	key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
	row := InTransitRow{TransferID: "T1", Key: key, FromNode: "wh-a", ToNode: "wh-b",
		Dispatched: 6, DispatchedAt: time.Now()}
	if err := m.RecordDispatch(ctx, row); err != nil {
		t.Fatalf("RecordDispatch: %v", err)
	}
	if err := m.AddReceived(ctx, domain.EventID{NodeID: "wh-b", Seq: 1}, "T1", key, 2); err != nil {
		t.Fatalf("AddReceived: %v", err)
	}
	row.Dispatched = 10
	if err := m.RecordDispatch(ctx, row); err != nil {
		t.Fatalf("re-RecordDispatch: %v", err)
	}
	got, ok, err := m.InTransit(ctx, "T1", key)
	if err != nil || !ok {
		t.Fatalf("InTransit = ok %v, err %v; want true, nil", ok, err)
	}
	if got.Dispatched != 10 || got.Received != 2 {
		t.Fatalf("InTransit = %+v, want Dispatched 10 and Received still 2 preserved", got)
	}
}

func TestOpenTransfersExcludesEachDisqualifyingCondition(t *testing.T) {
	m, ctx := NewMemory(), context.Background()
	key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
	at := time.Now()

	// Fully received: Dispatched - Received <= 0.
	if err := m.RecordDispatch(ctx, InTransitRow{TransferID: "T1", Key: key, FromNode: "wh-a",
		ToNode: "wh-b", Dispatched: 5, DispatchedAt: at.Add(-time.Hour)}); err != nil {
		t.Fatalf("RecordDispatch: %v", err)
	}
	if err := m.AddReceived(ctx, domain.EventID{NodeID: "wh-b", Seq: 1}, "T1", key, 5); err != nil {
		t.Fatalf("AddReceived: %v", err)
	}

	// Failed, still carrying stock.
	if err := m.RecordDispatch(ctx, InTransitRow{TransferID: "T2", Key: key, FromNode: "wh-a",
		ToNode: "wh-b", Dispatched: 5, DispatchedAt: at.Add(-time.Hour)}); err != nil {
		t.Fatalf("RecordDispatch: %v", err)
	}
	if err := m.FailTransfer(ctx, "T2"); err != nil {
		t.Fatalf("FailTransfer: %v", err)
	}

	// Dispatched after the window: not before dispatchedBefore.
	if err := m.RecordDispatch(ctx, InTransitRow{TransferID: "T3", Key: key, FromNode: "wh-a",
		ToNode: "wh-b", Dispatched: 5, DispatchedAt: at.Add(time.Hour)}); err != nil {
		t.Fatalf("RecordDispatch: %v", err)
	}

	open, err := m.OpenTransfers(ctx, at)
	if err != nil {
		t.Fatalf("OpenTransfers: %v", err)
	}
	if len(open) != 0 {
		t.Fatalf("OpenTransfers = %+v, want empty: none qualify", open)
	}
}

func TestOpenTransfersSortsMultipleQualifyingRowsByTransferID(t *testing.T) {
	m, ctx := NewMemory(), context.Background()
	key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
	at := time.Now()
	for _, id := range []string{"T2", "T1"} {
		if err := m.RecordDispatch(ctx, InTransitRow{TransferID: id, Key: key, FromNode: "wh-a",
			ToNode: "wh-b", Dispatched: 5, DispatchedAt: at.Add(-time.Hour)}); err != nil {
			t.Fatalf("RecordDispatch: %v", err)
		}
	}
	open, err := m.OpenTransfers(ctx, at)
	if err != nil {
		t.Fatalf("OpenTransfers: %v", err)
	}
	if len(open) != 2 || open[0].TransferID != "T1" || open[1].TransferID != "T2" {
		t.Fatalf("OpenTransfers = %+v, want [T1, T2] in order", open)
	}
}
