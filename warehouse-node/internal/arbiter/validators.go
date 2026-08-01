package arbiter

import (
	"context"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// movementsOf returns the stock movements a payload carries. Only these payloads move
// stock, and only they can need a stock compensation.
func movementsOf(payload any) []domain.Movement {
	switch p := payload.(type) {
	case domain.GoodsReceived:
		return []domain.Movement{p.Move}
	case domain.TransferDispatched:
		return p.Lines
	case domain.TransferReceived:
		return p.Lines
	default:
		return nil
	}
}

// reverse turns a movement into the movement that undoes it.
func reverse(m domain.Movement, qty float64) domain.Movement {
	return domain.Movement{SKU: m.SKU, LotID: m.LotID, From: m.To, To: m.From, Qty: qty}
}

// adjustment builds the compensating StockAdjusted for one movement.
func adjustment(aggregateID string, move domain.Movement, reason string) domain.Event {
	return domain.Event{Type: domain.TypeStockAdjusted, AggregateID: aggregateID,
		Payload: domain.StockAdjusted{Move: move, Reason: reason}}
}

// unknownSKU rejects an event naming a SKU central's item master does not have, or has
// but has deleted. A node validates SKUs against its replicated copy of the master,
// which may be stale; central re-checks against the live one. The compensation zeroes
// that SKU at the location it landed in, and the exceptions projection on the node
// flags it for manual cleanup.
type unknownSKU struct{}

func (unknownSKU) Name() string { return "unknown_sku" }

func (unknownSKU) Validate(ctx context.Context, s central.Store, _ domain.Envelope, payload any) (*Rejection, error) {
	moves := movementsOf(payload)
	events := make([]domain.Event, 0, len(moves))
	var excess float64
	for _, m := range moves {
		item, ok, err := s.Item(ctx, m.SKU)
		if err != nil {
			return nil, err
		}
		if ok && !item.Deleted {
			continue
		}
		events = append(events, adjustment(m.SKU, reverse(m, m.Qty), domain.ReasonUnknownSKU))
		excess += m.Qty
	}
	if len(events) == 0 {
		return nil, nil
	}
	return &Rejection{Reason: domain.ReasonUnknownSKU, Events: events, Excess: excess}, nil
}

// duplicateDeliveryNote rejects a receipt keying a supplier delivery note some other
// receipt already keyed. Only central can see this: the two receipts may be at
// different warehouses, and neither node can see the other's log.
type duplicateDeliveryNote struct{}

func (duplicateDeliveryNote) Name() string { return "duplicate_delivery_note" }

func (duplicateDeliveryNote) Validate(ctx context.Context, s central.Store, env domain.Envelope, payload any) (*Rejection, error) {
	p, ok := payload.(domain.GoodsReceived)
	if !ok {
		return nil, nil
	}
	first, found, err := s.DeliveryNoteFirstSeen(ctx, p.DeliveryNote, p.Move.SKU)
	if err != nil {
		return nil, err
	}
	if !found || first == env.ID {
		return nil, nil
	}
	return &Rejection{
		Reason: domain.ReasonDuplicateReceipt,
		Events: []domain.Event{adjustment(p.Move.SKU, reverse(p.Move, p.Move.Qty), domain.ReasonDuplicateReceipt)},
		Excess: p.Move.Qty,
	}, nil
}

// poOverReceipt rejects the portion of a receipt that takes a purchase order past its
// ordered quantity. Two warehouses can receive against the same order and neither sees
// the other's receipts, so this is structurally central's call.
//
// A PORef central has no order for is accepted: purchase-order lifecycle is out of
// scope in this project, so there is no ordered quantity to exceed.
type poOverReceipt struct{}

func (poOverReceipt) Name() string { return "po_over_receipt" }

func (poOverReceipt) Validate(ctx context.Context, s central.Store, _ domain.Envelope, payload any) (*Rejection, error) {
	p, ok := payload.(domain.GoodsReceived)
	if !ok {
		return nil, nil
	}
	ordered, known, err := s.PurchaseOrder(ctx, p.PORef, p.Move.SKU)
	if err != nil || !known {
		return nil, err
	}
	already, err := s.ReceivedAgainstPO(ctx, p.PORef, p.Move.SKU)
	if err != nil {
		return nil, err
	}
	excess := already + p.Move.Qty - ordered
	if excess <= 0 {
		return nil, nil
	}
	if excess > p.Move.Qty {
		excess = p.Move.Qty
	}
	return &Rejection{
		Reason: domain.ReasonPOOverReceipt,
		Events: []domain.Event{
			adjustment(p.Move.SKU, reverse(p.Move, excess), domain.ReasonPOOverReceipt),
			// The paperwork is reversed too, by a negative line: the receipt as
			// recorded claimed more than the order allowed.
			{Type: domain.TypeReceiptLineRecorded, AggregateID: p.ReceiptID,
				Payload: domain.ReceiptLineRecorded{ReceiptID: p.ReceiptID, LineNo: 0,
					SKU: p.Move.SKU, LotID: p.Move.LotID, QtyBase: -excess}},
		},
		Excess: excess,
	}, nil
}

// transferDestination rejects a dispatch to a node central does not know, or to a node
// configured to refuse that item. No node has authority over another node's
// configuration, so the source cannot check this before dispatching.
type transferDestination struct{}

func (transferDestination) Name() string { return "transfer_destination" }

func (transferDestination) Validate(ctx context.Context, s central.Store, _ domain.Envelope, payload any) (*Rejection, error) {
	p, ok := payload.(domain.TransferDispatched)
	if !ok {
		return nil, nil
	}
	rejects, known, err := s.NodeConfig(ctx, p.ToNode)
	if err != nil {
		return nil, err
	}
	refused := !known
	for _, m := range p.Lines {
		refused = refused || rejects[m.SKU]
	}
	if !refused {
		return nil, nil
	}
	events := make([]domain.Event, 0, len(p.Lines))
	var excess float64
	for _, m := range p.Lines {
		// Restore the source stock: the goods never left, as far as the books go.
		// The aggregate is the transfer, so the node's transfers projection learns
		// the transfer failed.
		events = append(events, adjustment(p.TransferID, reverse(m, m.Qty), domain.ReasonTransferRejected))
		excess += m.Qty
	}
	return &Rejection{Reason: domain.ReasonTransferRejected, Events: events, Excess: excess}, nil
}

// transferOverReceipt rejects the portion of a transfer receipt beyond what was
// dispatched, including the whole of a receipt for a transfer that was never
// dispatched at all. The two halves live on different nodes, so only central holds
// both.
type transferOverReceipt struct{}

func (transferOverReceipt) Name() string { return "transfer_over_receipt" }

func (transferOverReceipt) Validate(ctx context.Context, s central.Store, _ domain.Envelope, payload any) (*Rejection, error) {
	p, ok := payload.(domain.TransferReceived)
	if !ok {
		return nil, nil
	}
	events := make([]domain.Event, 0, len(p.Lines))
	var excess float64
	for _, m := range p.Lines {
		var remaining float64
		row, found, err := s.InTransit(ctx, p.TransferID, domain.StockKey{SKU: m.SKU, LotID: m.LotID})
		if err != nil {
			return nil, err
		}
		if found {
			remaining = row.Dispatched - row.Received
		}
		over := m.Qty - remaining
		if over <= 0 {
			continue
		}
		events = append(events, adjustment(m.SKU, reverse(m, over), domain.ReasonTransferOverReceipt))
		excess += over
	}
	if len(events) == 0 {
		return nil, nil
	}
	return &Rejection{Reason: domain.ReasonTransferOverReceipt, Events: events, Excess: excess}, nil
}
