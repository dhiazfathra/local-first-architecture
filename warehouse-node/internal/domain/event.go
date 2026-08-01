package domain

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Event is what a command returns: a type name, the aggregate it belongs to, and
// a payload struct. Identity, HLC and timestamps are assigned by the event log,
// keeping the domain free of clocks and sequence state.
type Event struct {
	Type        string
	AggregateID string
	Payload     any
}

// Envelope is the stored and replicated form of an event.
type Envelope struct {
	ID          EventID `json:"id"`
	AggregateID string  `json:"aggregate_id"`
	Type        string  `json:"type"`
	HLC         HLC     `json:"hlc"`
	// RecordedAt is the emitting wall clock, kept for audit only. Ordering uses
	// HLC, never this.
	RecordedAt time.Time `json:"recorded_at"`
	// CausationID is set only on compensating events emitted by central, and
	// points at the event being compensated.
	CausationID *EventID        `json:"causation_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

// Event type names, used as the discriminator in Envelope.Type.
const (
	TypeGoodsReceived       = "GoodsReceived"
	TypePutAway             = "PutAway"
	TypePicked              = "Picked"
	TypeStockAdjusted       = "StockAdjusted"
	TypeStockReserved       = "StockReserved"
	TypeReservationReleased = "ReservationReleased"
	TypeReservationConsumed = "ReservationConsumed"
	TypeTransferDispatched  = "TransferDispatched"
	TypeTransferReceived    = "TransferReceived"
	TypeReceiptOpened       = "ReceiptOpened"
	TypeReceiptLineRecorded = "ReceiptLineRecorded"
	TypeReceiptClosed       = "ReceiptClosed"
	TypeCountStarted        = "CountStarted"
	TypeCountLineCounted    = "CountLineCounted"
	TypeCountClosed         = "CountClosed"
	TypeItemUpserted        = "ItemUpserted"
	TypeLocationRegistered  = "LocationRegistered"
)

// Movement is a balanced stock move: Qty leaves From and arrives at To. Using
// External as one side models the outside world, so receiving, picking,
// putaway and transfers are all the same shape and the stock projection is one
// code path whose totals must sum to zero.
type Movement struct {
	SKU   string       `json:"sku"`
	LotID string       `json:"lot_id,omitempty"`
	From  LocationCode `json:"from"`
	To    LocationCode `json:"to"`
	Qty   float64      `json:"qty"`
}

// FromKey is the stock key the movement debits.
func (m Movement) FromKey() StockKey {
	return StockKey{SKU: m.SKU, Location: m.From, LotID: m.LotID}
}

// ToKey is the stock key the movement credits.
func (m Movement) ToKey() StockKey {
	return StockKey{SKU: m.SKU, Location: m.To, LotID: m.LotID}
}

// Payload types.
type (
	// GoodsReceived records a supplier delivery line arriving: external -> receiving.
	GoodsReceived struct {
		ReceiptID    string   `json:"receipt_id"`
		DeliveryNote string   `json:"delivery_note"`
		PORef        string   `json:"po_ref"`
		Move         Movement `json:"move"`
	}
	// PutAway records an internal move from the dock into storage.
	PutAway struct {
		Move Movement `json:"move"`
	}
	// Picked records stock leaving for a customer: pick -> external.
	Picked struct {
		Move     Movement `json:"move"`
		OrderRef string   `json:"order_ref,omitempty"`
	}
	// StockAdjusted corrects a balance, either from a count variance on the node
	// or from a compensation emitted by central. Reason is one of the Reason*
	// constants.
	StockAdjusted struct {
		Move   Movement `json:"move"`
		Reason string   `json:"reason"`
	}
	// StockReserved places a soft hold on stock at one key.
	StockReserved struct {
		ReservationID string   `json:"reservation_id"`
		Key           StockKey `json:"key"`
		Qty           float64  `json:"qty"`
	}
	// ReservationReleased cancels a hold, returning the quantity to available.
	ReservationReleased struct {
		ReservationID string `json:"reservation_id"`
	}
	// ReservationConsumed closes a hold because the stock was picked against it.
	ReservationConsumed struct {
		ReservationID string `json:"reservation_id"`
	}
	// TransferDispatched is the first half of an inter-node transfer. Source
	// stock decrements immediately; central holds the quantity in transit.
	TransferDispatched struct {
		TransferID string     `json:"transfer_id"`
		FromNode   NodeID     `json:"from_node"`
		ToNode     NodeID     `json:"to_node"`
		Lines      []Movement `json:"lines"`
	}
	// TransferReceived is the second half, emitted by the destination node.
	TransferReceived struct {
		TransferID string     `json:"transfer_id"`
		Lines      []Movement `json:"lines"`
	}
	// ReceiptOpened starts a receipt against a supplier delivery note and PO.
	ReceiptOpened struct {
		ReceiptID    string `json:"receipt_id"`
		DeliveryNote string `json:"delivery_note"`
		PORef        string `json:"po_ref"`
	}
	// ReceiptLineRecorded records one line of a receipt in base units. A negative
	// QtyBase is a reversal emitted by central when it compensates a receipt.
	ReceiptLineRecorded struct {
		ReceiptID string  `json:"receipt_id"`
		LineNo    int     `json:"line_no"`
		SKU       string  `json:"sku"`
		LotID     string  `json:"lot_id,omitempty"`
		QtyBase   float64 `json:"qty_base"`
	}
	// ReceiptClosed ends the receipt; no further lines may be recorded.
	ReceiptClosed struct {
		ReceiptID string `json:"receipt_id"`
	}
	// CountStarted opens a physical recount of one location.
	CountStarted struct {
		CountID  string       `json:"count_id"`
		Location LocationCode `json:"location"`
	}
	// CountLineCounted records what the operator physically counted at one key.
	CountLineCounted struct {
		CountID    string   `json:"count_id"`
		Key        StockKey `json:"key"`
		CountedQty float64  `json:"counted_qty"`
	}
	// CountClosed ends the count. Variance adjustments are emitted alongside it.
	CountClosed struct {
		CountID string `json:"count_id"`
	}
	// ItemUpserted replicates one item-master row down from central.
	ItemUpserted struct {
		Item Item `json:"item"`
	}
	// LocationRegistered declares a node-owned stock location.
	LocationRegistered struct {
		Code LocationCode `json:"code"`
		Type LocationType `json:"type"`
	}
)

// payloadFactories maps each event type to a constructor for its payload, so
// decoding is a table lookup rather than a switch repeated per call site.
var payloadFactories = map[string]func() any{
	TypeGoodsReceived:       func() any { return new(GoodsReceived) },
	TypePutAway:             func() any { return new(PutAway) },
	TypePicked:              func() any { return new(Picked) },
	TypeStockAdjusted:       func() any { return new(StockAdjusted) },
	TypeStockReserved:       func() any { return new(StockReserved) },
	TypeReservationReleased: func() any { return new(ReservationReleased) },
	TypeReservationConsumed: func() any { return new(ReservationConsumed) },
	TypeTransferDispatched:  func() any { return new(TransferDispatched) },
	TypeTransferReceived:    func() any { return new(TransferReceived) },
	TypeReceiptOpened:       func() any { return new(ReceiptOpened) },
	TypeReceiptLineRecorded: func() any { return new(ReceiptLineRecorded) },
	TypeReceiptClosed:       func() any { return new(ReceiptClosed) },
	TypeCountStarted:        func() any { return new(CountStarted) },
	TypeCountLineCounted:    func() any { return new(CountLineCounted) },
	TypeCountClosed:         func() any { return new(CountClosed) },
	TypeItemUpserted:        func() any { return new(ItemUpserted) },
	TypeLocationRegistered:  func() any { return new(LocationRegistered) },
}

// NewEnvelope seals an Event into its stored form with the given identity, clock
// reading and optional causation link.
//
// The payload's concrete type must be the one registered for e.Type. Without
// that check any struct marshals under any type name and the log accepts an
// envelope whose payload decodes to a zero value later — a `PutAway` that moves
// nothing. The log is the source of truth, so a wrong shape must never enter it.
func NewEnvelope(id EventID, hlc HLC, recordedAt time.Time, causation *EventID, e Event) (Envelope, error) {
	factory, ok := payloadFactories[e.Type]
	if !ok {
		return Envelope{}, fmt.Errorf("unknown event type %q", e.Type)
	}
	if want, got := reflect.TypeOf(factory()).Elem(), reflect.TypeOf(e.Payload); want != got {
		return Envelope{}, fmt.Errorf("event type %s requires payload %s, got %s", e.Type, want, got)
	}
	raw, err := json.Marshal(e.Payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode payload of %s: %w", e.Type, err)
	}
	return Envelope{
		ID:          id,
		AggregateID: e.AggregateID,
		Type:        e.Type,
		HLC:         hlc,
		RecordedAt:  recordedAt,
		CausationID: causation,
		Payload:     raw,
	}, nil
}

// DecodePayload returns the concrete payload value for an envelope. An unknown
// type is an error, never a skip: silently ignoring an event from the authority
// would fork state.
func DecodePayload(env Envelope) (any, error) {
	factory, ok := payloadFactories[env.Type]
	if !ok {
		return nil, fmt.Errorf("unknown event type %q", env.Type)
	}
	target := factory()
	if err := json.Unmarshal(env.Payload, target); err != nil {
		return nil, fmt.Errorf("decode %s payload: %w", env.Type, err)
	}
	// Return the value, not the pointer, so callers type-switch on payload
	// structs and comparisons in tests are by value.
	return derefPayload(target), nil
}

func derefPayload(p any) any {
	switch v := p.(type) {
	case *GoodsReceived:
		return *v
	case *PutAway:
		return *v
	case *Picked:
		return *v
	case *StockAdjusted:
		return *v
	case *StockReserved:
		return *v
	case *ReservationReleased:
		return *v
	case *ReservationConsumed:
		return *v
	case *TransferDispatched:
		return *v
	case *TransferReceived:
		return *v
	case *ReceiptOpened:
		return *v
	case *ReceiptLineRecorded:
		return *v
	case *ReceiptClosed:
		return *v
	case *CountStarted:
		return *v
	case *CountLineCounted:
		return *v
	case *CountClosed:
		return *v
	case *ItemUpserted:
		return *v
	default:
		return *(p.(*LocationRegistered))
	}
}
