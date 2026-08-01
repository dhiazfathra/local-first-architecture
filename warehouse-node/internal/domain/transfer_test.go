package domain

import (
	"testing"
	"time"
)

func TestDoDispatchTransfer(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		cmd       DispatchTransferCmd
		wantRule  string
		wantLines []Movement
	}{
		{
			name: "dispatches all lines out to external",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 3, UoM: "EA"}}},
			wantLines: []Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 3}},
		},
		{
			name: "converts alternate uom per line",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 0.5, UoM: "CASE"}}},
			wantLines: []Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 6}},
		},
		{
			name: "rejects when the sum of lines exceeds on hand even though each line fits",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
				Lines: []Line{
					{SKU: "WIDGET", LotID: "L1", Qty: 6, UoM: "EA"},
					{SKU: "WIDGET", LotID: "L1", Qty: 6, UoM: "EA"},
				}},
			wantRule: RuleStockNonNegative,
		},
		{
			name: "rejects an expired lot: it must not leave the building",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01",
				At:    time.Date(2026, 8, 1, 0, 1, 0, 0, time.UTC),
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}}},
			wantRule: RuleLotNotExpired,
		},
		{
			name: "rejects an unknown sku",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
				Lines: []Line{{SKU: "GHOST", Qty: 1, UoM: "EA"}}},
			wantRule: RuleSKUExists,
		},
		{
			name: "rejects an unknown source location",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "NOWHERE", At: day,
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}}},
			wantRule: RuleLocationExists,
		},
		{
			name:     "rejects an empty transfer",
			cmd:      DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day},
			wantRule: RuleQtyPositive,
		},
		{
			name: "rejects a transfer to this same node",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-a", From: "PICK-01", At: day,
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}}},
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			events, err := DoDispatchTransfer(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			p := events[0].Payload.(TransferDispatched)
			if p.TransferID != "T1" || p.FromNode != "wh-a" || p.ToNode != "wh-b" {
				t.Fatalf("header = %+v", p)
			}
			if len(p.Lines) != len(tt.wantLines) {
				t.Fatalf("got %d lines, want %d", len(p.Lines), len(tt.wantLines))
			}
			for i := range p.Lines {
				if p.Lines[i] != tt.wantLines[i] {
					t.Fatalf("line %d = %+v, want %+v", i, p.Lines[i], tt.wantLines[i])
				}
			}
		})
	}
}

func TestDoDispatchTransferRejectsDuplicateID(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := stocked(t, day)
	cmd := DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
		Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}}}
	events, err := DoDispatchTransfer(s, cmd)
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	applyEvents(t, s, 700, day, events...)
	if _, err := DoDispatchTransfer(s, cmd); !IsViolation(err, RuleAggregateState) {
		t.Fatalf("err = %v, want violation of %s", err, RuleAggregateState)
	}
}

// dispatchedInto seeds a destination node's state with the TransferDispatched
// event it learned about from central, without any of the source node's stock.
func dispatchedInto(t *testing.T, day time.Time, qty float64) *State {
	t.Helper()
	s := NewState()
	s.SetHome("wh-b")
	applyEvents(t, s, 1, day,
		Event{Type: TypeItemUpserted, AggregateID: "WIDGET", Payload: ItemUpserted{Item: widget()}},
		Event{Type: TypeLocationRegistered, AggregateID: "RECV-01", Payload: LocationRegistered{Code: "RECV-01", Type: LocReceiving}},
		Event{Type: TypeTransferDispatched, AggregateID: "T1", Payload: TransferDispatched{
			TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
			Lines: []Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: qty}},
		}},
	)
	return s
}

// TestForwardedDispatchDoesNotMoveDestinationStock proves that folding in a
// TransferDispatched relayed by central for metadata only updates the destination's
// transfer bookkeeping, never its Stock map — the source's own From/To locations
// (here PICK-01 and external) must not appear as balances on a node that never
// physically held that stock.
func TestForwardedDispatchDoesNotMoveDestinationStock(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := dispatchedInto(t, day, 5)
	if got := s.OnHand(StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}); got != 0 {
		t.Fatalf("destination on-hand at the source's own location = %v, want 0", got)
	}
	if got := s.OnHand(StockKey{SKU: "WIDGET", Location: External, LotID: "L1"}); got != 0 {
		t.Fatalf("destination on-hand at the external sentinel = %v, want 0", got)
	}
	key := StockKey{SKU: "WIDGET", LotID: "L1"}
	if got := s.Transfers["T1"].Dispatched[key]; got != 5 {
		t.Fatalf("transfer metadata still tracks dispatched qty = %v, want 5", got)
	}
}

// TestTransferReceivedBeforeDispatchIsNotLost proves a TransferReceived that
// arrives before its matching TransferDispatched metadata is buffered rather than
// discarded, and is correctly folded in once the dispatch shows up.
func TestTransferReceivedBeforeDispatchIsNotLost(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := NewState()
	s.SetHome("wh-b")
	applyEvents(t, s, 1, day,
		Event{Type: TypeItemUpserted, AggregateID: "WIDGET", Payload: ItemUpserted{Item: widget()}},
		Event{Type: TypeLocationRegistered, AggregateID: "RECV-01", Payload: LocationRegistered{Code: "RECV-01", Type: LocReceiving}},
		Event{Type: TypeTransferReceived, AggregateID: "T1", Payload: TransferReceived{
			TransferID: "T1", Lines: []Movement{{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: 5}},
		}},
	)
	if _, ok := s.Transfers["T1"]; ok {
		t.Fatalf("transfer T1 should not exist yet: only the receipt has arrived")
	}
	if got := len(s.PendingReceipts["T1"]); got != 1 {
		t.Fatalf("pending receipts for T1 = %d, want 1 (buffered, not discarded)", got)
	}

	applyEvents(t, s, 2, day,
		Event{Type: TypeTransferDispatched, AggregateID: "T1", Payload: TransferDispatched{
			TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
			Lines: []Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 5}},
		}},
	)
	key := StockKey{SKU: "WIDGET", LotID: "L1"}
	if got := s.Transfers["T1"].Received[key]; got != 5 {
		t.Fatalf("received qty after dispatch arrives = %v, want 5 (folded in from the buffer)", got)
	}
	if s.Transfers["T1"].Status != TransferComplete {
		t.Fatalf("status = %q, want %q", s.Transfers["T1"].Status, TransferComplete)
	}
	if _, buffered := s.PendingReceipts["T1"]; buffered {
		t.Fatalf("pending receipt for T1 should be cleared once folded in")
	}
	if got := s.OnHand(StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}); got != 5 {
		t.Fatalf("destination on-hand = %v, want 5", got)
	}
}

func TestTransferStateInTransit(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := dispatchedInto(t, day, 5)
	key := StockKey{SKU: "WIDGET", LotID: "L1"}
	if got := s.Transfers["T1"].InTransit(key); got != 5 {
		t.Fatalf("in transit after dispatch = %v, want 5", got)
	}
	events, err := DoReceiveTransfer(s, ReceiveTransferCmd{TransferID: "T1", To: "RECV-01",
		Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	applyEvents(t, s, 800, day, events...)
	if got := s.Transfers["T1"].InTransit(key); got != 0 {
		t.Fatalf("in transit after receipt = %v, want 0", got)
	}
	if s.Transfers["T1"].Status != TransferComplete {
		t.Fatalf("status = %q, want %q", s.Transfers["T1"].Status, TransferComplete)
	}
	if got := s.OnHand(StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}); got != 5 {
		t.Fatalf("destination on hand = %v, want 5", got)
	}
}

func TestDoReceiveTransfer(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		known    bool // has the dispatch reached this node yet?
		cmd      ReceiveTransferCmd
		wantRule string
		wantQty  float64
	}{
		{
			name:    "receives the dispatched quantity",
			known:   true,
			cmd:     ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}},
			wantQty: 5,
		},
		{
			name:    "receives a short shipment",
			known:   true,
			cmd:     ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 4, UoM: "EA"}}},
			wantQty: 4,
		},
		{
			name: "an over-receipt is accepted locally and left for central to arbitrate",
			// The node cannot know the true dispatched quantity is authoritative
			// -- it may hold a stale copy of the dispatch -- so it records what
			// the operator counted off the truck. Central compares both halves
			// and compensates the excess with reason transfer_overreceipt.
			known:   true,
			cmd:     ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 9, UoM: "EA"}}},
			wantQty: 9,
		},
		{
			name:     "rejects a receipt with no matching dispatch",
			known:    false,
			cmd:      ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}},
			wantRule: RuleAggregateState,
		},
		{
			name:     "rejects an unknown destination location",
			known:    true,
			cmd:      ReceiveTransferCmd{TransferID: "T1", To: "NOWHERE", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}},
			wantRule: RuleLocationExists,
		},
		{
			name:     "rejects an unknown sku",
			known:    true,
			cmd:      ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "GHOST", Qty: 1, UoM: "EA"}}},
			wantRule: RuleSKUExists,
		},
		{
			name:     "rejects an empty receipt",
			known:    true,
			cmd:      ReceiveTransferCmd{TransferID: "T1", To: "RECV-01"},
			wantRule: RuleQtyPositive,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewState()
			if tt.known {
				s = dispatchedInto(t, day, 5)
			} else {
				applyEvents(t, s, 1, day,
					Event{Type: TypeItemUpserted, AggregateID: "WIDGET", Payload: ItemUpserted{Item: widget()}},
					Event{Type: TypeLocationRegistered, AggregateID: "RECV-01", Payload: LocationRegistered{Code: "RECV-01", Type: LocReceiving}},
				)
			}
			events, err := DoReceiveTransfer(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			p := events[0].Payload.(TransferReceived)
			want := Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: tt.wantQty}
			if p.TransferID != "T1" || len(p.Lines) != 1 || p.Lines[0] != want {
				t.Fatalf("payload = %+v, want line %+v", p, want)
			}
		})
	}
}

func TestDoReceiveTransferRejectsSecondReceipt(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := dispatchedInto(t, day, 5)
	cmd := ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}}
	events, err := DoReceiveTransfer(s, cmd)
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}
	applyEvents(t, s, 900, day, events...)
	if _, err := DoReceiveTransfer(s, cmd); !IsViolation(err, RuleAggregateState) {
		t.Fatalf("err = %v, want violation of %s", err, RuleAggregateState)
	}
}
