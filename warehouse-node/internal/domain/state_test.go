package domain

import (
	"testing"
	"time"
)

// env is a test helper sealing a payload into an envelope with sequential
// identity. It keeps the table-driven tests free of clock and identity noise.
func env(t *testing.T, seq uint64, typ, agg string, payload any) Envelope {
	t.Helper()
	e, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: seq}, HLC{Wall: int64(seq), Node: "wh-a"},
		time.Unix(int64(seq), 0).UTC(), nil, Event{Type: typ, AggregateID: agg, Payload: payload})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	return e
}

// applyAll folds a sequence of envelopes into a fresh state, failing on error.
func applyAll(t *testing.T, envs ...Envelope) *State {
	t.Helper()
	s := NewState()
	for _, e := range envs {
		if err := s.Apply(e); err != nil {
			t.Fatalf("Apply(%s): %v", e.Type, err)
		}
	}
	return s
}

func TestApplyMovementsAreBalanced(t *testing.T) {
	mv := Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: 10}
	s := applyAll(t,
		env(t, 1, TypeGoodsReceived, "R1", GoodsReceived{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1", Move: mv}),
		env(t, 2, TypePutAway, "WIDGET", PutAway{Move: Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: "PICK-01", Qty: 4}}),
		env(t, 3, TypePicked, "WIDGET", Picked{Move: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 1}, OrderRef: "SO-1"}),
		env(t, 4, TypeStockAdjusted, "WIDGET", StockAdjusted{Move: Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "PICK-01", Qty: 2}, Reason: ReasonCountVariance}),
	)
	want := map[StockKey]float64{
		{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}: 6,
		{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}: 5,
		{SKU: "WIDGET", Location: External, LotID: "L1"}:  -11,
	}
	for k, v := range want {
		if got := s.OnHand(k); got != v {
			t.Errorf("OnHand(%+v) = %v, want %v", k, got, v)
		}
	}
	// Every movement is balanced, so all balances including the external
	// sentinel must sum to zero. This is the reconciliation property.
	var total float64
	for _, v := range s.Stock {
		total += v
	}
	if total != 0 {
		t.Fatalf("balances sum to %v, want 0", total)
	}
}

func TestApplyAllowsNegativeBalanceFromCompensation(t *testing.T) {
	// Received 5, picked all 5, then central compensates the receipt. The node
	// kept working, so compensation drives the location negative. That must be
	// recorded, not clamped.
	cause := EventID{NodeID: "wh-a", Seq: 1}
	comp, err := NewEnvelope(EventID{NodeID: "central", Seq: 1}, HLC{Wall: 99, Node: "central"}, time.Unix(99, 0).UTC(), &cause,
		Event{Type: TypeStockAdjusted, AggregateID: "WIDGET", Payload: StockAdjusted{
			Move:   Movement{SKU: "WIDGET", From: "RECV-01", To: External, Qty: 5},
			Reason: ReasonDuplicateReceipt,
		}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	s := applyAll(t,
		env(t, 1, TypeGoodsReceived, "R1", GoodsReceived{ReceiptID: "R1", Move: Movement{SKU: "WIDGET", From: External, To: "RECV-01", Qty: 5}}),
		env(t, 2, TypePicked, "WIDGET", Picked{Move: Movement{SKU: "WIDGET", From: "RECV-01", To: External, Qty: 5}}),
		comp,
	)
	if got := s.OnHand(StockKey{SKU: "WIDGET", Location: "RECV-01"}); got != -5 {
		t.Fatalf("OnHand = %v, want -5", got)
	}
	if len(s.Compensations) != 1 || s.Compensations[0].ID != comp.ID {
		t.Fatalf("Compensations = %+v, want the one compensating envelope", s.Compensations)
	}
}

func TestApplyReservationLifecycleAffectsAvailable(t *testing.T) {
	key := StockKey{SKU: "WIDGET", Location: "PICK-01"}
	base := []Envelope{
		env(t, 1, TypeGoodsReceived, "R1", GoodsReceived{ReceiptID: "R1", Move: Movement{SKU: "WIDGET", From: External, To: "PICK-01", Qty: 10}}),
		env(t, 2, TypeStockReserved, "RS1", StockReserved{ReservationID: "RS1", Key: key, Qty: 4}),
	}
	tests := []struct {
		name          string
		extra         []Envelope
		wantAvailable float64
		wantStatus    ReservationStatus
	}{
		{name: "active reservation reduces available", wantAvailable: 6, wantStatus: ResActive},
		{
			name:          "released reservation returns to available",
			extra:         []Envelope{env(t, 3, TypeReservationReleased, "RS1", ReservationReleased{ReservationID: "RS1"})},
			wantAvailable: 10,
			wantStatus:    ResReleased,
		},
		{
			name: "consumed reservation stops holding but stock left with the pick",
			extra: []Envelope{
				env(t, 3, TypePicked, "WIDGET", Picked{Move: Movement{SKU: "WIDGET", From: "PICK-01", To: External, Qty: 4}}),
				env(t, 4, TypeReservationConsumed, "RS1", ReservationConsumed{ReservationID: "RS1"}),
			},
			wantAvailable: 6,
			wantStatus:    ResConsumed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := applyAll(t, append(append([]Envelope{}, base...), tt.extra...)...)
			if got := s.Available(key); got != tt.wantAvailable {
				t.Fatalf("Available() = %v, want %v", got, tt.wantAvailable)
			}
			if got := s.Reservations["RS1"].Status; got != tt.wantStatus {
				t.Fatalf("status = %q, want %q", got, tt.wantStatus)
			}
		})
	}
}

func TestApplyAggregateLifecycles(t *testing.T) {
	s := applyAll(t,
		env(t, 1, TypeItemUpserted, "WIDGET", ItemUpserted{Item: widget()}),
		env(t, 2, TypeLocationRegistered, "PICK-01", LocationRegistered{Code: "PICK-01", Type: LocPick}),
		env(t, 3, TypeReceiptOpened, "R1", ReceiptOpened{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1"}),
		env(t, 4, TypeReceiptLineRecorded, "R1", ReceiptLineRecorded{ReceiptID: "R1", LineNo: 1, SKU: "WIDGET", LotID: "L1", QtyBase: 10}),
		env(t, 5, TypeReceiptClosed, "R1", ReceiptClosed{ReceiptID: "R1"}),
		env(t, 6, TypeTransferDispatched, "T1", TransferDispatched{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
			Lines: []Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 3}}}),
		env(t, 7, TypeTransferReceived, "T1", TransferReceived{TransferID: "T1",
			Lines: []Movement{{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: 3}}}),
		env(t, 8, TypeCountStarted, "C1", CountStarted{CountID: "C1", Location: "PICK-01"}),
		env(t, 9, TypeCountLineCounted, "C1", CountLineCounted{CountID: "C1", Key: StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}, CountedQty: 7}),
		env(t, 10, TypeCountClosed, "C1", CountClosed{CountID: "C1"}),
	)

	if _, err := s.Item("WIDGET"); err != nil {
		t.Fatalf("Item: %v", err)
	}
	if s.Locations["PICK-01"] != LocPick {
		t.Fatalf("location type = %q", s.Locations["PICK-01"])
	}
	r := s.Receipts["R1"]
	if r.Status != ReceiptClosedStatus || r.NextLineNo != 2 || r.Lines[1].QtyBase != 10 || r.DeliveryNote != "DN-1" || r.PORef != "PO-1" {
		t.Fatalf("receipt state = %+v", r)
	}
	// Receiving a lot-tracked SKU creates the lot with expiry = receipt day plus
	// shelf life, so the node can enforce expiry at pick time with no help.
	lot, ok := s.Lots["L1"]
	if !ok || !lot.ExpiresOn.Equal(time.Unix(4, 0).UTC().AddDate(0, 0, 30)) {
		t.Fatalf("lot = %+v ok=%v", lot, ok)
	}
	tr := s.Transfers["T1"]
	if tr.Status != TransferComplete || tr.FromNode != "wh-a" || tr.ToNode != "wh-b" {
		t.Fatalf("transfer state = %+v", tr)
	}
	c := s.Counts["C1"]
	if c.Status != CountClosedStatus || c.Counted[StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}] != 7 {
		t.Fatalf("count state = %+v", c)
	}
}

func TestTransferStaysInFlightUntilReceived(t *testing.T) {
	s := applyAll(t, env(t, 1, TypeTransferDispatched, "T1", TransferDispatched{
		TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
		Lines: []Movement{{SKU: "WIDGET", From: "PICK-01", To: External, Qty: 3}},
	}))
	if s.Transfers["T1"].Status != TransferInFlight {
		t.Fatalf("status = %q, want %q", s.Transfers["T1"].Status, TransferInFlight)
	}
}

func TestSetHomeGatesTransferStockMovement(t *testing.T) {
	// wh-b is the destination. Central relays the dispatch to wh-b purely for
	// metadata; wh-b must not move stock at wh-a's PICK-01, since it does not
	// own that location. It also buffers a TransferReceived that arrives before
	// the dispatch, and must move stock for that receipt since wh-b is the
	// receiving node.
	s := NewState()
	s.SetHome("wh-b")

	if err := s.Apply(env(t, 1, TypeTransferReceived, "T1", TransferReceived{
		TransferID: "T1", Lines: []Movement{{SKU: "WIDGET", From: External, To: "RECV-01", Qty: 3}},
	})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(s.PendingReceipts["T1"]) != 1 {
		t.Fatalf("PendingReceipts[T1] = %+v, want 1 buffered line", s.PendingReceipts["T1"])
	}

	if err := s.Apply(env(t, 2, TypeTransferDispatched, "T1", TransferDispatched{
		TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
		Lines: []Movement{{SKU: "WIDGET", From: "PICK-01", To: External, Qty: 3}},
	})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := s.OnHand(StockKey{SKU: "WIDGET", Location: "PICK-01"}); got != 0 {
		t.Fatalf("OnHand(PICK-01) = %v, want 0 (not this node's stock)", got)
	}
	if got := s.OnHand(StockKey{SKU: "WIDGET", Location: "RECV-01"}); got != 3 {
		t.Fatalf("OnHand(RECV-01) = %v, want 3 (this node received it)", got)
	}
	if _, buffered := s.PendingReceipts["T1"]; buffered {
		t.Fatalf("PendingReceipts[T1] still buffered after dispatch arrived")
	}
	if s.Transfers["T1"].Status != TransferComplete {
		t.Fatalf("status = %q, want %q", s.Transfers["T1"].Status, TransferComplete)
	}
}

func TestApplyIgnoresEventsForUnknownAggregates(t *testing.T) {
	// Events can arrive out of order across a sync boundary; Apply must not panic
	// or error on a line for a receipt/count/transfer it has never seen. Later
	// events are folded in when they arrive.
	tests := []struct {
		name string
		e    Envelope
	}{
		{"line for unknown receipt", env(t, 1, TypeReceiptLineRecorded, "R9", ReceiptLineRecorded{ReceiptID: "R9", LineNo: 1, SKU: "WIDGET", QtyBase: 1})},
		{"close of unknown receipt", env(t, 2, TypeReceiptClosed, "R9", ReceiptClosed{ReceiptID: "R9"})},
		{"line for unknown count", env(t, 3, TypeCountLineCounted, "C9", CountLineCounted{CountID: "C9", CountedQty: 1})},
		{"close of unknown count", env(t, 4, TypeCountClosed, "C9", CountClosed{CountID: "C9"})},
		{"receive of unknown transfer", env(t, 5, TypeTransferReceived, "T9", TransferReceived{TransferID: "T9"})},
		{"release of unknown reservation", env(t, 6, TypeReservationReleased, "RS9", ReservationReleased{ReservationID: "RS9"})},
		{"consume of unknown reservation", env(t, 7, TypeReservationConsumed, "RS9", ReservationConsumed{ReservationID: "RS9"})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewState().Apply(tt.e); err != nil {
				t.Fatalf("Apply: %v", err)
			}
		})
	}
}

func TestApplyRejectsUndecodableEvent(t *testing.T) {
	if err := NewState().Apply(Envelope{Type: "NopeHappened", Payload: []byte(`{}`)}); err == nil {
		t.Fatal("expected an error for an unknown event type")
	}
}

func TestStateItem(t *testing.T) {
	s := NewState()
	deleted := widget()
	deleted.SKU = "GONE"
	deleted.Deleted = true
	if err := s.Apply(env(t, 1, TypeItemUpserted, "WIDGET", ItemUpserted{Item: widget()})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := s.Apply(env(t, 2, TypeItemUpserted, "GONE", ItemUpserted{Item: deleted})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	tests := []struct {
		name    string
		sku     string
		wantErr bool
	}{
		{"known sku", "WIDGET", false},
		{"absent from stale master", "NEVERHEARDOFIT", true},
		{"deleted at central", "GONE", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Item(tt.sku)
			if tt.wantErr != IsViolation(err, RuleSKUExists) {
				t.Fatalf("Item(%q) err = %v, wantErr = %v", tt.sku, err, tt.wantErr)
			}
		})
	}
}

// applyEvents seals command output into envelopes and folds it into s, which is
// what a real caller does after a command succeeds.
func applyEvents(t *testing.T, s *State, startSeq uint64, at time.Time, events ...Event) {
	t.Helper()
	for i, e := range events {
		env, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: startSeq + uint64(i)},
			HLC{Wall: int64(startSeq) + int64(i), Node: "wh-a"}, at, nil, e)
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		if err := s.Apply(env); err != nil {
			t.Fatalf("Apply(%s): %v", e.Type, err)
		}
	}
}
