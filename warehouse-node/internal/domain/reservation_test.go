package domain

import (
	"testing"
	"time"
)

func TestDoReserve(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	key := StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}
	tests := []struct {
		name     string
		existing []Event // reservations already in place
		cmd      ReserveCmd
		wantRule string
		wantQty  float64
	}{
		{
			name:    "reserves within available",
			cmd:     ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 4, UoM: "EA"}, Location: "PICK-01"},
			wantQty: 4,
		},
		{
			name:    "reserves exactly all of available",
			cmd:     ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 10, UoM: "EA"}, Location: "PICK-01"},
			wantQty: 10,
		},
		{
			name:    "converts alternate uom before checking",
			cmd:     ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0.5, UoM: "CASE"}, Location: "PICK-01"},
			wantQty: 6,
		},
		{
			name:     "exceeds available with nothing reserved",
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 11, UoM: "EA"}, Location: "PICK-01"},
			wantRule: RuleReservationAvailable,
		},
		{
			name: "exceeds available because of an existing active hold",
			existing: []Event{{Type: TypeStockReserved, AggregateID: "RS0",
				Payload: StockReserved{ReservationID: "RS0", Key: key, Qty: 7}}},
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 4, UoM: "EA"}, Location: "PICK-01"},
			wantRule: RuleReservationAvailable,
		},
		{
			name: "a released hold no longer blocks",
			existing: []Event{
				{Type: TypeStockReserved, AggregateID: "RS0", Payload: StockReserved{ReservationID: "RS0", Key: key, Qty: 7}},
				{Type: TypeReservationReleased, AggregateID: "RS0", Payload: ReservationReleased{ReservationID: "RS0"}},
			},
			cmd:     ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 10, UoM: "EA"}, Location: "PICK-01"},
			wantQty: 10,
		},
		{
			name: "duplicate reservation id",
			existing: []Event{{Type: TypeStockReserved, AggregateID: "RS1",
				Payload: StockReserved{ReservationID: "RS1", Key: key, Qty: 1}}},
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, Location: "PICK-01"},
			wantRule: RuleAggregateState,
		},
		{
			name:     "unknown sku",
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "GHOST", Qty: 1, UoM: "EA"}, Location: "PICK-01"},
			wantRule: RuleSKUExists,
		},
		{
			name:     "bad uom",
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "TONNE"}, Location: "PICK-01"},
			wantRule: RuleUoMValid,
		},
		{
			name:     "unknown location",
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, Location: "NOWHERE"},
			wantRule: RuleLocationExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 500, day, tt.existing...)
			events, err := DoReserve(s, tt.cmd)
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
			want := StockReserved{ReservationID: "RS1", Key: key, Qty: tt.wantQty}
			if len(events) != 1 || events[0].Payload != want || events[0].AggregateID != "RS1" {
				t.Fatalf("events = %+v, want payload %+v", events, want)
			}
		})
	}
}

func TestDoReleaseAndConsumeReservation(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	key := StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}
	active := []Event{{Type: TypeStockReserved, AggregateID: "RS1",
		Payload: StockReserved{ReservationID: "RS1", Key: key, Qty: 3}}}
	released := append(append([]Event{}, active...),
		Event{Type: TypeReservationReleased, AggregateID: "RS1", Payload: ReservationReleased{ReservationID: "RS1"}})

	tests := []struct {
		name     string
		existing []Event
		release  bool // true: DoReleaseReservation, false: DoConsumeReservation
		wantType string
		wantRule string
	}{
		{name: "release an active hold", existing: active, release: true, wantType: TypeReservationReleased},
		{name: "consume an active hold", existing: active, wantType: TypeReservationConsumed},
		{name: "release an unknown hold", release: true, wantRule: RuleAggregateState},
		{name: "consume an unknown hold", wantRule: RuleAggregateState},
		{name: "release an already-released hold", existing: released, release: true, wantRule: RuleAggregateState},
		{name: "consume an already-released hold", existing: released, wantRule: RuleAggregateState},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 600, day, tt.existing...)
			var (
				events []Event
				err    error
			)
			if tt.release {
				events, err = DoReleaseReservation(s, ReleaseReservationCmd{ReservationID: "RS1"})
			} else {
				events, err = DoConsumeReservation(s, ConsumeReservationCmd{ReservationID: "RS1"})
			}
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
			if len(events) != 1 || events[0].Type != tt.wantType || events[0].AggregateID != "RS1" {
				t.Fatalf("events = %+v, want one %s", events, tt.wantType)
			}
		})
	}
}
