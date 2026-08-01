package domain

import (
	"testing"
	"time"
)

func TestDoStartCount(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		existing []Event
		cmd      StartCountCmd
		wantRule string
	}{
		{name: "starts a count on a known location", cmd: StartCountCmd{CountID: "C1", Location: "PICK-01"}},
		{name: "rejects an unknown location", cmd: StartCountCmd{CountID: "C1", Location: "NOWHERE"}, wantRule: RuleLocationExists},
		{
			name:     "rejects a duplicate count id",
			existing: []Event{{Type: TypeCountStarted, AggregateID: "C1", Payload: CountStarted{CountID: "C1", Location: "PICK-01"}}},
			cmd:      StartCountCmd{CountID: "C1", Location: "PICK-01"},
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 1000, day, tt.existing...)
			events, err := DoStartCount(s, tt.cmd)
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
			want := CountStarted{CountID: "C1", Location: "PICK-01"}
			if len(events) != 1 || events[0].Payload != want || events[0].AggregateID != "C1" {
				t.Fatalf("events = %+v, want %+v", events, want)
			}
		})
	}
}

func TestDoCountLine(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	open := []Event{{Type: TypeCountStarted, AggregateID: "C1", Payload: CountStarted{CountID: "C1", Location: "PICK-01"}}}
	closed := append(append([]Event{}, open...),
		Event{Type: TypeCountClosed, AggregateID: "C1", Payload: CountClosed{CountID: "C1"}})

	tests := []struct {
		name     string
		existing []Event
		cmd      CountLineCmd
		wantRule string
		wantQty  float64
	}{
		{
			name:     "records a counted quantity in base units",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 7, UoM: "EA"}},
			wantQty:  7,
		},
		{
			name:     "converts an alternate uom",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "CASE"}},
			wantQty:  12,
		},
		{
			name:     "rejects a line on an unknown count",
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}},
			wantRule: RuleAggregateState,
		},
		{
			name:     "rejects a line on a closed count",
			existing: closed,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}},
			wantRule: RuleAggregateState,
		},
		{
			name:     "rejects an unknown sku",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "GHOST", Qty: 1, UoM: "EA"}},
			wantRule: RuleSKUExists,
		},
		{
			name:     "rejects a bad uom",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "TONNE"}},
			wantRule: RuleUoMValid,
		},
		{
			name:     "rejects a bad uom even when the counted quantity is zero",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0, UoM: "TONNE"}},
			wantRule: RuleUoMValid,
		},
		{
			name:     "rejects a lot-tracked sku counted without a lot id",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", Qty: 1, UoM: "EA"}},
			wantRule: RuleLotRequired,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 1100, day, tt.existing...)
			events, err := DoCountLine(s, tt.cmd)
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
			want := CountLineCounted{CountID: "C1",
				Key: StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}, CountedQty: tt.wantQty}
			if len(events) != 1 || events[0].Payload != want {
				t.Fatalf("events = %+v, want %+v", events, want)
			}
		})
	}
}

func TestDoCountLineCountingZeroIsAllowed(t *testing.T) {
	// Counting zero is how an operator says "the shelf is empty", so unlike every
	// other command a count line accepts a zero quantity.
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := stocked(t, day)
	applyEvents(t, s, 1200, day, Event{Type: TypeCountStarted, AggregateID: "C1",
		Payload: CountStarted{CountID: "C1", Location: "PICK-01"}})
	events, err := DoCountLine(s, CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0, UoM: "EA"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := events[0].Payload.(CountLineCounted).CountedQty; got != 0 {
		t.Fatalf("counted qty = %v, want 0", got)
	}
}

func TestDoCloseCountEmitsVarianceAdjustments(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	pickL1 := StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}
	tests := []struct {
		name     string
		counted  map[StockKey]float64
		seed     []Event
		wantAdj  []StockAdjusted
		wantRule string
	}{
		{
			name:    "shortfall moves stock out to external",
			counted: map[StockKey]float64{pickL1: 7}, // book is 10
			wantAdj: []StockAdjusted{{Move: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 3}, Reason: ReasonCountVariance}},
		},
		{
			name:    "surplus moves stock in from external",
			counted: map[StockKey]float64{pickL1: 12},
			wantAdj: []StockAdjusted{{Move: Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "PICK-01", Qty: 2}, Reason: ReasonCountVariance}},
		},
		{
			name:    "no variance emits no adjustment",
			counted: map[StockKey]float64{pickL1: 10},
			wantAdj: nil,
		},
		{
			name:    "counting zero writes the whole balance off",
			counted: map[StockKey]float64{pickL1: 0},
			wantAdj: []StockAdjusted{{Move: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 10}, Reason: ReasonCountVariance}},
		},
		{
			name: "a count repairs a location driven negative by a compensation",
			seed: []Event{{Type: TypeStockAdjusted, AggregateID: "WIDGET", Payload: StockAdjusted{
				Move: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 14}, Reason: ReasonDuplicateReceipt}}},
			counted: map[StockKey]float64{pickL1: 2}, // book is now -4
			wantAdj: []StockAdjusted{{Move: Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "PICK-01", Qty: 6}, Reason: ReasonCountVariance}},
		},
		{
			name: "multiple variance lines are emitted in deterministic order",
			counted: map[StockKey]float64{
				pickL1: 7, // book 10, shortfall 3
				{SKU: "WIDGET", Location: "PICK-01", LotID: "L2"}: 1, // book 0, surplus 1
				{SKU: "GADGET", Location: "PICK-01", LotID: "L1"}: 1, // book 0, surplus 1
			},
			wantAdj: []StockAdjusted{
				{Move: Movement{SKU: "GADGET", LotID: "L1", From: External, To: "PICK-01", Qty: 1}, Reason: ReasonCountVariance},
				{Move: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 3}, Reason: ReasonCountVariance},
				{Move: Movement{SKU: "WIDGET", LotID: "L2", From: External, To: "PICK-01", Qty: 1}, Reason: ReasonCountVariance},
			},
		},
		{
			name:     "closing an unknown count is rejected",
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 1300, day, tt.seed...)
			if tt.wantRule == "" {
				seedEvents := []Event{{Type: TypeCountStarted, AggregateID: "C1",
					Payload: CountStarted{CountID: "C1", Location: "PICK-01"}}}
				for k, qty := range tt.counted {
					seedEvents = append(seedEvents, Event{Type: TypeCountLineCounted, AggregateID: "C1",
						Payload: CountLineCounted{CountID: "C1", Key: k, CountedQty: qty}})
				}
				applyEvents(t, s, 1400, day, seedEvents...)
			}

			events, err := DoCloseCount(s, CloseCountCmd{CountID: "C1"})
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
			// Adjustments come first so the balance is corrected before the count
			// closes; CountClosed is always last.
			if last := events[len(events)-1]; last.Type != TypeCountClosed {
				t.Fatalf("last event = %s, want %s", last.Type, TypeCountClosed)
			}
			var got []StockAdjusted
			for _, e := range events[:len(events)-1] {
				if e.Type != TypeStockAdjusted {
					t.Fatalf("unexpected event type %s", e.Type)
				}
				got = append(got, e.Payload.(StockAdjusted))
			}
			// Compared in emitted order, not sorted: tt.wantAdj is already written
			// in the order DoCloseCount must emit it, so this also proves the
			// ordering guarantee, not just set equality.
			if len(got) != len(tt.wantAdj) {
				t.Fatalf("got %d adjustments, want %d: %+v", len(got), len(tt.wantAdj), got)
			}
			for i := range got {
				if got[i] != tt.wantAdj[i] {
					t.Fatalf("adjustment %d = %+v, want %+v", i, got[i], tt.wantAdj[i])
				}
			}
			// Applying the adjustments must make the book agree with the count.
			applyEvents(t, s, 1500, day, events...)
			for k, qty := range tt.counted {
				if s.OnHand(k) != qty {
					t.Fatalf("after close OnHand(%+v) = %v, want counted %v", k, s.OnHand(k), qty)
				}
			}
		})
	}
}

func TestDoCloseCountSortsByLocationWhenSKUAndLotTie(t *testing.T) {
	// A single count's lines all share one Location in practice (DoCountLine
	// always sets it from count.Location), so the sort comparator's Location
	// tiebreak never fires through the command path. Seeding Counted directly
	// exercises it anyway, proving the comparator is a total order rather than
	// one that happens to work only because of that external invariant.
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := stocked(t, day)
	applyEvents(t, s, 1650, day, Event{Type: TypeCountStarted, AggregateID: "C1",
		Payload: CountStarted{CountID: "C1", Location: "PICK-01"}})
	s.Counts["C1"].Counted[StockKey{SKU: "WIDGET", Location: "BULK-01", LotID: "L1"}] = 1
	s.Counts["C1"].Counted[StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}] = 1

	events, err := DoCloseCount(s, CloseCountCmd{CountID: "C1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got []StockAdjusted
	for _, e := range events[:len(events)-1] {
		got = append(got, e.Payload.(StockAdjusted))
	}
	if len(got) != 2 || got[0].Move.To != "BULK-01" || got[1].Move.From != "PICK-01" {
		t.Fatalf("adjustments = %+v, want BULK-01 before PICK-01", got)
	}
}

func TestDoCloseCountRejectsAlreadyClosed(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := stocked(t, day)
	applyEvents(t, s, 1600, day,
		Event{Type: TypeCountStarted, AggregateID: "C1", Payload: CountStarted{CountID: "C1", Location: "PICK-01"}},
		Event{Type: TypeCountClosed, AggregateID: "C1", Payload: CountClosed{CountID: "C1"}},
	)
	if _, err := DoCloseCount(s, CloseCountCmd{CountID: "C1"}); !IsViolation(err, RuleAggregateState) {
		t.Fatalf("err = %v, want violation of %s", err, RuleAggregateState)
	}
}
