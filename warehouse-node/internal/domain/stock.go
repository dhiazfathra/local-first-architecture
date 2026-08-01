package domain

import "time"

// Line is a quantity as the operator entered it: a SKU, an optional lot, an
// amount and the unit that amount is in. Conversion to base units happens in
// ResolveLine, so no command duplicates it.
type Line struct {
	SKU   string  `json:"sku"`
	LotID string  `json:"lot_id,omitempty"`
	Qty   float64 `json:"qty"`
	UoM   UoM     `json:"uom"`
}

// ResolveLine enforces the three node-side line invariants — SKU exists in the
// replicated master, the UoM is valid for that item, and a lot-tracked item names
// a lot — and returns the movement in base units with From/To left for the caller
// to fill in.
func (s *State) ResolveLine(l Line) (Movement, Item, error) {
	item, err := s.Item(l.SKU)
	if err != nil {
		return Movement{}, Item{}, err
	}
	if item.LotTracked && l.LotID == "" {
		return Movement{}, Item{}, Violation(RuleLotRequired, "sku %s is lot-tracked; a lot id is required", l.SKU)
	}
	base, err := item.ToBase(l.Qty, l.UoM)
	if err != nil {
		return Movement{}, Item{}, err
	}
	return Movement{SKU: l.SKU, LotID: l.LotID, Qty: base}, item, nil
}

// RequireLocation enforces that a location is one this node owns. External is
// rejected: it is a sentinel for the outside world, valid only where a command
// explicitly puts it (receiving, picking, transfers), never as operator input.
func (s *State) RequireLocation(code LocationCode) error {
	if _, ok := s.Locations[code]; !ok {
		return Violation(RuleLocationExists, "location %q is not registered on this node", code)
	}
	return nil
}

// RequireOnHand enforces the "stock at a location never negative" invariant for a
// command about to remove qty from k. The node owns its own locations outright, so
// it can decide this alone, synchronously, before appending anything.
func (s *State) RequireOnHand(k StockKey, qty float64) error {
	if onHand := s.OnHand(k); onHand < qty {
		return Violation(RuleStockNonNegative,
			"location %s holds %v of sku %s lot %q; cannot remove %v", k.Location, onHand, k.SKU, k.LotID, qty)
	}
	return nil
}

// RequireLotUsable enforces "lot not expired" for a lot-tracked item at instant at.
func (s *State) RequireLotUsable(lotID string, at time.Time) error {
	lot, ok := s.Lots[lotID]
	if !ok {
		return nil // Unknown lots carry no expiry information to check.
	}
	if lot.ExpiredAt(at) {
		return Violation(RuleLotNotExpired, "lot %s of sku %s expired on %s", lot.ID, lot.SKU, lot.ExpiresOn.Format(time.DateOnly))
	}
	return nil
}

// PutAwayCmd moves stock between two locations inside this warehouse.
type PutAwayCmd struct {
	Line Line
	From LocationCode
	To   LocationCode
}

// DoPutAway validates and returns the PutAway event, or a named rule violation
// with no events at all.
func DoPutAway(s *State, c PutAwayCmd) ([]Event, error) {
	move, _, err := s.ResolveLine(c.Line)
	if err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.From); err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.To); err != nil {
		return nil, err
	}
	move.From, move.To = c.From, c.To
	if err := s.RequireOnHand(move.FromKey(), move.Qty); err != nil {
		return nil, err
	}
	return []Event{{Type: TypePutAway, AggregateID: c.Line.SKU, Payload: PutAway{Move: move}}}, nil
}

// PickCmd takes stock off a shelf for an outbound order: From -> external.
type PickCmd struct {
	Line     Line
	From     LocationCode
	OrderRef string
	At       time.Time
}

// DoPick validates and returns the Picked event. It is the only command enforcing
// lot expiry, because expiry exists to stop expired goods being shipped.
func DoPick(s *State, c PickCmd) ([]Event, error) {
	move, _, err := s.ResolveLine(c.Line)
	if err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.From); err != nil {
		return nil, err
	}
	if err := s.RequireLotUsable(c.Line.LotID, c.At); err != nil {
		return nil, err
	}
	move.From, move.To = c.From, External
	if err := s.RequireOnHand(move.FromKey(), move.Qty); err != nil {
		return nil, err
	}
	return []Event{{Type: TypePicked, AggregateID: c.Line.SKU, Payload: Picked{Move: move, OrderRef: c.OrderRef}}}, nil
}
