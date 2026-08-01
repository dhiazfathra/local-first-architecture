package domain

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestMovementKeys(t *testing.T) {
	m := Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: 10}
	if got, want := m.FromKey(), (StockKey{SKU: "WIDGET", Location: External, LotID: "L1"}); got != want {
		t.Fatalf("FromKey() = %+v, want %+v", got, want)
	}
	if got, want := m.ToKey(), (StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}); got != want {
		t.Fatalf("ToKey() = %+v, want %+v", got, want)
	}
}

func TestNewEnvelopeRoundTripsEveryPayloadType(t *testing.T) {
	at := time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)
	mv := Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: 10}
	tests := []struct {
		typ     string
		payload any
	}{
		{TypeGoodsReceived, GoodsReceived{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1", Move: mv}},
		{TypePutAway, PutAway{Move: mv}},
		{TypePicked, Picked{Move: mv, OrderRef: "SO-1"}},
		{TypeStockAdjusted, StockAdjusted{Move: mv, Reason: ReasonCountVariance}},
		{TypeStockReserved, StockReserved{ReservationID: "RS1", Key: mv.ToKey(), Qty: 4}},
		{TypeReservationReleased, ReservationReleased{ReservationID: "RS1"}},
		{TypeReservationConsumed, ReservationConsumed{ReservationID: "RS1"}},
		{TypeTransferDispatched, TransferDispatched{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", Lines: []Movement{mv}}},
		{TypeTransferReceived, TransferReceived{TransferID: "T1", Lines: []Movement{mv}}},
		{TypeReceiptOpened, ReceiptOpened{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1"}},
		{TypeReceiptLineRecorded, ReceiptLineRecorded{ReceiptID: "R1", LineNo: 1, SKU: "WIDGET", LotID: "L1", QtyBase: 10}},
		{TypeReceiptClosed, ReceiptClosed{ReceiptID: "R1"}},
		{TypeCountStarted, CountStarted{CountID: "C1", Location: "PICK-01"}},
		{TypeCountLineCounted, CountLineCounted{CountID: "C1", Key: mv.ToKey(), CountedQty: 9}},
		{TypeCountClosed, CountClosed{CountID: "C1"}},
		{TypeItemUpserted, ItemUpserted{Item: widget()}},
		{TypeLocationRegistered, LocationRegistered{Code: "PICK-01", Type: LocPick}},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			env, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: 1}, HLC{Wall: 1, Node: "wh-a"}, at, nil,
				Event{Type: tt.typ, AggregateID: "agg", Payload: tt.payload})
			if err != nil {
				t.Fatalf("NewEnvelope: %v", err)
			}
			if env.Type != tt.typ || env.AggregateID != "agg" || !env.RecordedAt.Equal(at) {
				t.Fatalf("envelope header wrong: %+v", env)
			}
			got, err := DecodePayload(env)
			if err != nil {
				t.Fatalf("DecodePayload: %v", err)
			}
			if !reflect.DeepEqual(got, tt.payload) {
				t.Fatalf("round trip mismatch:\n got %#v\nwant %#v", got, tt.payload)
			}
		})
	}
}

func TestNewEnvelopeCarriesCausationID(t *testing.T) {
	cause := EventID{NodeID: "wh-a", Seq: 3}
	env, err := NewEnvelope(EventID{NodeID: "central", Seq: 1}, HLC{Wall: 9, Node: "central"}, time.Unix(0, 0), &cause,
		Event{Type: TypeStockAdjusted, AggregateID: "WIDGET", Payload: StockAdjusted{Reason: ReasonPOOverReceipt}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if env.CausationID == nil || *env.CausationID != cause {
		t.Fatalf("CausationID = %v, want %v", env.CausationID, cause)
	}
}

func TestNewEnvelopeRejectsUnmarshalablePayload(t *testing.T) {
	_, err := NewEnvelope(EventID{NodeID: "a", Seq: 1}, HLC{}, time.Unix(0, 0), nil,
		Event{Type: TypePutAway, AggregateID: "x", Payload: math.Inf(1)})
	if err == nil {
		t.Fatal("expected an error encoding a non-JSON payload")
	}
}

func TestNewEnvelopeRejectsUnknownType(t *testing.T) {
	_, err := NewEnvelope(EventID{NodeID: "a", Seq: 1}, HLC{}, time.Unix(0, 0), nil,
		Event{Type: "NopeHappened", AggregateID: "x", Payload: PutAway{}})
	if err == nil {
		t.Fatal("expected an error for an unregistered event type")
	}
}

func TestNewEnvelopeRejectsPayloadThatFailsToMarshal(t *testing.T) {
	// The payload's Go type matches TypePutAway, so the type check passes, but a
	// NaN quantity cannot be represented in JSON, so json.Marshal itself fails.
	_, err := NewEnvelope(EventID{NodeID: "a", Seq: 1}, HLC{}, time.Unix(0, 0), nil,
		Event{Type: TypePutAway, AggregateID: "x", Payload: PutAway{Move: Movement{Qty: math.NaN()}}})
	if err == nil {
		t.Fatal("expected an error encoding a NaN quantity")
	}
}

func TestDecodePayloadErrors(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
	}{
		{"unknown type", Envelope{Type: "NopeHappened", Payload: []byte(`{}`)}},
		{"malformed json for known type", Envelope{Type: TypePutAway, Payload: []byte(`{"move":`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodePayload(tt.env); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
