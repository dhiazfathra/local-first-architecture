package domain

import "time"

// InTransit is the quantity of one SKU/lot dispatched but not yet received. The
// key's Location is empty because in-transit stock is at no location: it is on a
// truck, owned by neither node, and held as a first-class balance at central.
func (t *TransferState) InTransit(k StockKey) float64 {
	bare := StockKey{SKU: k.SKU, LotID: k.LotID}
	return t.Dispatched[bare] - t.Received[bare]
}

// DispatchTransferCmd sends stock from this node to another. Only the source half
// is emitted here; the destination emits its own TransferReceived when the truck
// arrives.
//
// Two invariants are deliberately absent: whether ToNode exists and accepts the
// item, and whether the eventual receipt exceeds this dispatch. Neither is
// knowable here, so central arbitrates both and compensates what it rejects.
type DispatchTransferCmd struct {
	TransferID string
	FromNode   NodeID
	ToNode     NodeID
	From       LocationCode
	Lines      []Line
	At         time.Time
}

// DoDispatchTransfer validates every line against this node's own stock and
// returns a single TransferDispatched event.
func DoDispatchTransfer(s *State, c DispatchTransferCmd) ([]Event, error) {
	if len(c.Lines) == 0 {
		return nil, Violation(RuleQtyPositive, "transfer %s has no lines", c.TransferID)
	}
	if c.ToNode == c.FromNode {
		return nil, Violation(RuleAggregateState, "transfer %s targets its own node %s", c.TransferID, c.ToNode)
	}
	if _, exists := s.Transfers[c.TransferID]; exists {
		return nil, Violation(RuleAggregateState, "transfer %s already exists", c.TransferID)
	}
	if err := s.RequireLocation(c.From); err != nil {
		return nil, err
	}
	// Accumulate per key so several lines drawing on the same key are checked
	// against the balance in aggregate, not one at a time.
	running := map[StockKey]float64{}
	moves := make([]Movement, 0, len(c.Lines))
	for _, l := range c.Lines {
		move, _, err := s.ResolveLine(l)
		if err != nil {
			return nil, err
		}
		if err := s.RequireLotUsable(l.LotID, c.At); err != nil {
			return nil, err
		}
		move.From, move.To = c.From, External
		running[move.FromKey()] += move.Qty
		if err := s.RequireOnHand(move.FromKey(), running[move.FromKey()]); err != nil {
			return nil, err
		}
		moves = append(moves, move)
	}
	return []Event{{Type: TypeTransferDispatched, AggregateID: c.TransferID, Payload: TransferDispatched{
		TransferID: c.TransferID, FromNode: c.FromNode, ToNode: c.ToNode, Lines: moves,
	}}}, nil
}

// ReceiveTransferCmd records the truck arriving at this node.
type ReceiveTransferCmd struct {
	TransferID string
	To         LocationCode
	Lines      []Line
}

// DoReceiveTransfer requires that this node has already learned of the dispatch —
// a TransferReceived with no matching TransferDispatched is rejected outright,
// because it is either a typo or a transfer this node was never sent. It does not
// compare quantities against the dispatch: the operator records what came off the
// truck, and central, which holds both halves authoritatively, compensates any
// excess with reason transfer_overreceipt.
func DoReceiveTransfer(s *State, c ReceiveTransferCmd) ([]Event, error) {
	if len(c.Lines) == 0 {
		return nil, Violation(RuleQtyPositive, "transfer receipt %s has no lines", c.TransferID)
	}
	transfer, ok := s.Transfers[c.TransferID]
	if !ok {
		return nil, Violation(RuleAggregateState, "transfer %s has no matching dispatch on this node", c.TransferID)
	}
	if transfer.Status != TransferInFlight {
		return nil, Violation(RuleAggregateState, "transfer %s is %s; cannot receive it again", c.TransferID, transfer.Status)
	}
	if err := s.RequireLocation(c.To); err != nil {
		return nil, err
	}
	moves := make([]Movement, 0, len(c.Lines))
	for _, l := range c.Lines {
		move, _, err := s.ResolveLine(l)
		if err != nil {
			return nil, err
		}
		move.From, move.To = External, c.To
		moves = append(moves, move)
	}
	return []Event{{Type: TypeTransferReceived, AggregateID: c.TransferID,
		Payload: TransferReceived{TransferID: c.TransferID, Lines: moves}}}, nil
}
