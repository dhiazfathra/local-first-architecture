package domain

// ReceiveCmd records one line of a supplier delivery. DeliveryNote and PORef are
// used only when the command opens the receipt; later lines inherit them from the
// open receipt so central always sees one note per receipt.
//
// The node cannot check either of the two invariants that make receipts
// interesting — "does this exceed the open PO quantity" and "have we seen this
// delivery note elsewhere" — because both need to see other nodes' receipts.
// Central arbitrates them after the fact.
type ReceiveCmd struct {
	ReceiptID    string
	DeliveryNote string
	PORef        string
	Line         Line
	To           LocationCode
}

// DoReceive validates the line and returns ReceiptOpened (first line only),
// ReceiptLineRecorded and GoodsReceived.
func DoReceive(s *State, c ReceiveCmd) ([]Event, error) {
	move, _, err := s.ResolveLine(c.Line)
	if err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.To); err != nil {
		return nil, err
	}
	move.From, move.To = External, c.To

	var events []Event
	receipt, open := s.Receipts[c.ReceiptID]
	note, po, lineNo := c.DeliveryNote, c.PORef, 1
	switch {
	case !open:
		events = append(events, Event{Type: TypeReceiptOpened, AggregateID: c.ReceiptID,
			Payload: ReceiptOpened{ReceiptID: c.ReceiptID, DeliveryNote: note, PORef: po}})
	case receipt.Status != ReceiptOpen:
		return nil, Violation(RuleAggregateState, "receipt %s is %s; cannot record more lines", c.ReceiptID, receipt.Status)
	default:
		note, po, lineNo = receipt.DeliveryNote, receipt.PORef, receipt.NextLineNo
	}

	return append(events,
		Event{Type: TypeReceiptLineRecorded, AggregateID: c.ReceiptID, Payload: ReceiptLineRecorded{
			ReceiptID: c.ReceiptID, LineNo: lineNo, SKU: c.Line.SKU, LotID: c.Line.LotID, QtyBase: move.Qty,
		}},
		Event{Type: TypeGoodsReceived, AggregateID: c.ReceiptID, Payload: GoodsReceived{
			ReceiptID: c.ReceiptID, DeliveryNote: note, PORef: po, Move: move,
		}},
	), nil
}

// CloseReceiptCmd ends a receipt so no further lines can be recorded.
type CloseReceiptCmd struct {
	ReceiptID string
}

// DoCloseReceipt validates and returns the ReceiptClosed event.
func DoCloseReceipt(s *State, c CloseReceiptCmd) ([]Event, error) {
	receipt, ok := s.Receipts[c.ReceiptID]
	if !ok {
		return nil, Violation(RuleAggregateState, "receipt %s does not exist", c.ReceiptID)
	}
	if receipt.Status != ReceiptOpen {
		return nil, Violation(RuleAggregateState, "receipt %s is already %s", c.ReceiptID, receipt.Status)
	}
	return []Event{{Type: TypeReceiptClosed, AggregateID: c.ReceiptID, Payload: ReceiptClosed(c)}}, nil
}
