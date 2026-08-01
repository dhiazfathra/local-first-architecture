package domain

import (
	"testing"
	"time"
)

// stocked builds a state with the widget item master, two locations, and 10 units
// of lot L1 in PICK-01 received on the given day. It is the only fixture the pure
// domain tests need.
func stocked(t *testing.T, receivedOn time.Time) *State {
	t.Helper()
	s := NewState()
	seq := uint64(1)
	push := func(typ, agg string, payload any) {
		e, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: seq}, HLC{Wall: int64(seq), Node: "wh-a"},
			receivedOn, nil, Event{Type: typ, AggregateID: agg, Payload: payload})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		if err := s.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		seq++
	}
	push(TypeItemUpserted, "WIDGET", ItemUpserted{Item: widget()})
	push(TypeItemUpserted, "BOLT", ItemUpserted{Item: Item{SKU: "BOLT", BaseUoM: "EA", AltUoM: map[UoM]float64{"BOX": 100}}})
	push(TypeLocationRegistered, "RECV-01", LocationRegistered{Code: "RECV-01", Type: LocReceiving})
	push(TypeLocationRegistered, "PICK-01", LocationRegistered{Code: "PICK-01", Type: LocPick})
	push(TypeLocationRegistered, "BULK-01", LocationRegistered{Code: "BULK-01", Type: LocBulk})
	push(TypeGoodsReceived, "R0", GoodsReceived{ReceiptID: "R0", Move: Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "PICK-01", Qty: 10}})
	return s
}

func TestDoPutAway(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		cmd      PutAwayCmd
		wantRule string // "" means success
		wantMove Movement
	}{
		{
			name:     "moves stock in base units",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 4, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantMove: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: "BULK-01", Qty: 4},
		},
		{
			name:     "converts an alternate uom before checking stock",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0.5, UoM: "CASE"}, From: "PICK-01", To: "BULK-01"},
			wantMove: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: "BULK-01", Qty: 6},
		},
		{
			name:     "would drive the source location negative",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 11, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleStockNonNegative,
		},
		{
			name:     "alternate uom conversion pushes it over the balance",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "CASE"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleStockNonNegative,
		},
		{
			name:     "sku missing from the stale replicated master",
			cmd:      PutAwayCmd{Line: Line{SKU: "GHOST", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleSKUExists,
		},
		{
			name:     "uom not declared for this item",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "BOX"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleUoMValid,
		},
		{
			name:     "lot-tracked sku with no lot",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", Qty: 1, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleLotRequired,
		},
		{
			name:     "unknown source location",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "NOWHERE", To: "BULK-01"},
			wantRule: RuleLocationExists,
		},
		{
			name:     "unknown destination location",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", To: "NOWHERE"},
			wantRule: RuleLocationExists,
		},
		{
			name:     "external is never a putaway endpoint",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", To: External},
			wantRule: RuleLocationExists,
		},
		{
			name:     "non-positive quantity",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleQtyPositive,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			before := s.OnHand(StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"})
			events, err := DoPutAway(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				if s.OnHand(StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}) != before {
					t.Fatal("rejected command mutated state")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(events) != 1 || events[0].Type != TypePutAway || events[0].AggregateID != "WIDGET" {
				t.Fatalf("events = %+v", events)
			}
			if got := events[0].Payload.(PutAway).Move; got != tt.wantMove {
				t.Fatalf("move = %+v, want %+v", got, tt.wantMove)
			}
		})
	}
}

func TestDoPick(t *testing.T) {
	// widget() has ShelfLifeDays 30, so lot L1 received 2026-07-01 expires
	// 2026-07-31 and is usable through that whole day.
	received := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		cmd      PickCmd
		wantRule string
		wantQty  float64
	}{
		{
			name:    "picks to external",
			cmd:     PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 3, UoM: "EA"}, From: "PICK-01", OrderRef: "SO-1", At: received.AddDate(0, 0, 5)},
			wantQty: 3,
		},
		{
			name:    "picks on the expiry day itself",
			cmd:     PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", At: time.Date(2026, 7, 31, 23, 0, 0, 0, time.UTC)},
			wantQty: 1,
		},
		{
			name:     "refuses an expired lot",
			cmd:      PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", At: time.Date(2026, 8, 1, 0, 1, 0, 0, time.UTC)},
			wantRule: RuleLotNotExpired,
		},
		{
			name:     "refuses to drive the location negative",
			cmd:      PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 11, UoM: "EA"}, From: "PICK-01", At: received},
			wantRule: RuleStockNonNegative,
		},
		{
			name:     "refuses an unknown sku",
			cmd:      PickCmd{Line: Line{SKU: "GHOST", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", At: received},
			wantRule: RuleSKUExists,
		},
		{
			name:     "refuses an unknown location",
			cmd:      PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "NOWHERE", At: received},
			wantRule: RuleLocationExists,
		},
		{
			name:     "lot-tracked sku with no lot",
			cmd:      PickCmd{Line: Line{SKU: "WIDGET", Qty: 1, UoM: "EA"}, From: "PICK-01", At: received},
			wantRule: RuleLotRequired,
		},
		{
			name:     "non-positive quantity",
			cmd:      PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0, UoM: "EA"}, From: "PICK-01", At: received},
			wantRule: RuleQtyPositive,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, received)
			events, err := DoPick(s, tt.cmd)
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
			p := events[0].Payload.(Picked)
			want := Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: tt.wantQty}
			if p.Move != want || p.OrderRef != tt.cmd.OrderRef {
				t.Fatalf("payload = %+v, want move %+v", p, want)
			}
		})
	}
}

func TestDoPickAllowsUntrackedLotForNonLotItem(t *testing.T) {
	s := stocked(t, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC))
	// Seed BOLT stock, which is not lot-tracked, so no lot is required and no
	// expiry check applies.
	e, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: 99}, HLC{Wall: 99, Node: "wh-a"}, time.Unix(99, 0).UTC(), nil,
		Event{Type: TypeGoodsReceived, AggregateID: "R0", Payload: GoodsReceived{ReceiptID: "R0",
			Move: Movement{SKU: "BOLT", From: External, To: "PICK-01", Qty: 500}}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if err := s.Apply(e); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	events, err := DoPick(s, PickCmd{Line: Line{SKU: "BOLT", Qty: 2, UoM: "BOX"}, From: "PICK-01", At: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := events[0].Payload.(Picked).Move.Qty; got != 200 {
		t.Fatalf("qty = %v, want 200 (2 boxes of 100)", got)
	}
}
