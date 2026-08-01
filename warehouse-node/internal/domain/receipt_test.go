package domain

import (
	"testing"
	"time"
)

func TestDoReceiveOpensTheReceiptOnFirstLine(t *testing.T) {
	s := stocked(t, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC))
	events, err := DoReceive(s, ReceiveCmd{
		ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
		Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 2, UoM: "CASE"}, To: "RECV-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3 (opened, line, goods received)", len(events))
	}
	if got, want := events[0].Payload, (ReceiptOpened{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1"}); got != want {
		t.Fatalf("event 0 = %+v, want %+v", got, want)
	}
	if got, want := events[1].Payload, (ReceiptLineRecorded{ReceiptID: "R1", LineNo: 1, SKU: "WIDGET", LotID: "L9", QtyBase: 24}); got != want {
		t.Fatalf("event 1 = %+v, want %+v", got, want)
	}
	if got, want := events[2].Payload, (GoodsReceived{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
		Move: Movement{SKU: "WIDGET", LotID: "L9", From: External, To: "RECV-01", Qty: 24}}); got != want {
		t.Fatalf("event 2 = %+v, want %+v", got, want)
	}
	for i, e := range events {
		if e.AggregateID != "R1" {
			t.Fatalf("event %d aggregate = %q, want R1", i, e.AggregateID)
		}
	}
}

func TestDoReceiveSecondLineDoesNotReopen(t *testing.T) {
	s := stocked(t, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC))
	first, err := DoReceive(s, ReceiveCmd{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
		Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 1, UoM: "EA"}, To: "RECV-01"})
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}
	applyEvents(t, s, 100, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), first...)

	second, err := DoReceive(s, ReceiveCmd{ReceiptID: "R1",
		Line: Line{SKU: "BOLT", Qty: 1, UoM: "BOX"}, To: "RECV-01"})
	if err != nil {
		t.Fatalf("second receive: %v", err)
	}
	if len(second) != 2 {
		t.Fatalf("got %d events, want 2 (line, goods received)", len(second))
	}
	line := second[0].Payload.(ReceiptLineRecorded)
	if line.LineNo != 2 || line.QtyBase != 100 {
		t.Fatalf("line = %+v, want LineNo 2 and QtyBase 100", line)
	}
	// The delivery note and PO come from the already-open receipt, not the command,
	// so central sees one consistent note per receipt no matter what the operator
	// retypes on later lines.
	if gr := second[1].Payload.(GoodsReceived); gr.DeliveryNote != "DN-1" || gr.PORef != "PO-1" {
		t.Fatalf("goods received = %+v, want DN-1/PO-1 carried from the open receipt", gr)
	}
}

func TestDoReceiveRejections(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		setup    func(t *testing.T, s *State)
		cmd      ReceiveCmd
		wantRule string
	}{
		{
			name:     "unknown sku against a stale master",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "GHOST", Qty: 1, UoM: "EA"}, To: "RECV-01"},
			wantRule: RuleSKUExists,
		},
		{
			name:     "uom not valid for the item",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 1, UoM: "TONNE"}, To: "RECV-01"},
			wantRule: RuleUoMValid,
		},
		{
			name:     "lot-tracked item with no lot",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", Qty: 1, UoM: "EA"}, To: "RECV-01"},
			wantRule: RuleLotRequired,
		},
		{
			name:     "unregistered destination",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 1, UoM: "EA"}, To: "NOWHERE"},
			wantRule: RuleLocationExists,
		},
		{
			name:     "zero quantity",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 0, UoM: "EA"}, To: "RECV-01"},
			wantRule: RuleQtyPositive,
		},
		{
			name: "line on a closed receipt",
			setup: func(t *testing.T, s *State) {
				t.Helper()
				applyEvents(t, s, 200, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
					Event{Type: TypeReceiptOpened, AggregateID: "R1", Payload: ReceiptOpened{ReceiptID: "R1", DeliveryNote: "DN-1"}},
					Event{Type: TypeReceiptClosed, AggregateID: "R1", Payload: ReceiptClosed{ReceiptID: "R1"}},
				)
			},
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 1, UoM: "EA"}, To: "RECV-01"},
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			if tt.setup != nil {
				tt.setup(t, s)
			}
			events, err := DoReceive(s, tt.cmd)
			if !IsViolation(err, tt.wantRule) {
				t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
			}
			if len(events) != 0 {
				t.Fatalf("rejected command produced %d events, want 0", len(events))
			}
		})
	}
}

func TestDoCloseReceipt(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		setup    func(t *testing.T, s *State)
		wantRule string
	}{
		{
			name: "closes an open receipt",
			setup: func(t *testing.T, s *State) {
				t.Helper()
				applyEvents(t, s, 300, day, Event{Type: TypeReceiptOpened, AggregateID: "R1", Payload: ReceiptOpened{ReceiptID: "R1"}})
			},
		},
		{
			name:     "unknown receipt",
			wantRule: RuleAggregateState,
		},
		{
			name: "already closed receipt",
			setup: func(t *testing.T, s *State) {
				t.Helper()
				applyEvents(t, s, 400, day,
					Event{Type: TypeReceiptOpened, AggregateID: "R1", Payload: ReceiptOpened{ReceiptID: "R1"}},
					Event{Type: TypeReceiptClosed, AggregateID: "R1", Payload: ReceiptClosed{ReceiptID: "R1"}},
				)
			},
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			if tt.setup != nil {
				tt.setup(t, s)
			}
			events, err := DoCloseReceipt(s, CloseReceiptCmd{ReceiptID: "R1"})
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(events) != 1 || events[0].Payload != (ReceiptClosed{ReceiptID: "R1"}) {
				t.Fatalf("events = %+v", events)
			}
		})
	}
}
