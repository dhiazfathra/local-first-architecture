package domain

import "sort"

// StartCountCmd opens a physical recount of one location.
type StartCountCmd struct {
	CountID  string
	Location LocationCode
}

// DoStartCount validates and returns the CountStarted event.
func DoStartCount(s *State, c StartCountCmd) ([]Event, error) {
	if err := s.RequireLocation(c.Location); err != nil {
		return nil, err
	}
	if _, exists := s.Counts[c.CountID]; exists {
		return nil, Violation(RuleAggregateState, "count %s already exists", c.CountID)
	}
	return []Event{{Type: TypeCountStarted, AggregateID: c.CountID,
		Payload: CountStarted(c)}}, nil
}

// CountLineCmd records what the operator physically counted for one SKU/lot at the
// count's location.
type CountLineCmd struct {
	CountID string
	Line    Line
}

// DoCountLine validates and returns the CountLineCounted event. Unlike other
// commands a zero quantity is legitimate: it is how the operator reports an empty
// shelf, which is the most consequential count result there is.
func DoCountLine(s *State, c CountLineCmd) ([]Event, error) {
	count, err := s.requireOpenCount(c.CountID)
	if err != nil {
		return nil, err
	}
	item, err := s.Item(c.Line.SKU)
	if err != nil {
		return nil, err
	}
	if item.LotTracked && c.Line.LotID == "" {
		return nil, Violation(RuleLotRequired, "sku %s is lot-tracked; a lot id is required", c.Line.SKU)
	}
	qty := c.Line.Qty
	if qty != 0 {
		if qty, err = item.ToBase(qty, c.Line.UoM); err != nil {
			return nil, err
		}
	} else if _, err := item.ToBase(1, c.Line.UoM); err != nil {
		// Still validate the unit itself even when the quantity is zero.
		return nil, err
	}
	return []Event{{Type: TypeCountLineCounted, AggregateID: c.CountID, Payload: CountLineCounted{
		CountID:    c.CountID,
		Key:        StockKey{SKU: c.Line.SKU, Location: count.Location, LotID: c.Line.LotID},
		CountedQty: qty,
	}}}, nil
}

// CloseCountCmd ends a count and books its variances.
type CloseCountCmd struct {
	CountID string
}

// DoCloseCount emits one StockAdjusted per variance line, then CountClosed.
// Variances are modelled as movements against external so the stock projection
// stays a single balanced-pair code path: a shortfall leaves the location, a
// surplus arrives from outside. A count can raise a negative balance, which is how
// a human repairs a location that a compensation drove below zero.
func DoCloseCount(s *State, c CloseCountCmd) ([]Event, error) {
	count, err := s.requireOpenCount(c.CountID)
	if err != nil {
		return nil, err
	}
	// Sort keys so the emitted event order is deterministic and replays identically.
	keys := make([]StockKey, 0, len(count.Counted))
	for k := range count.Counted {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].SKU != keys[j].SKU {
			return keys[i].SKU < keys[j].SKU
		}
		return keys[i].LotID < keys[j].LotID
	})

	events := make([]Event, 0, len(keys)+1)
	for _, k := range keys {
		variance := count.Counted[k] - s.OnHand(k)
		if variance == 0 {
			continue
		}
		move := Movement{SKU: k.SKU, LotID: k.LotID, Qty: variance, From: External, To: k.Location}
		if variance < 0 {
			move.Qty, move.From, move.To = -variance, k.Location, External
		}
		events = append(events, Event{Type: TypeStockAdjusted, AggregateID: k.SKU,
			Payload: StockAdjusted{Move: move, Reason: ReasonCountVariance}})
	}
	return append(events, Event{Type: TypeCountClosed, AggregateID: c.CountID,
		Payload: CountClosed(c)}), nil
}

func (s *State) requireOpenCount(id string) (*CountState, error) {
	count, ok := s.Counts[id]
	if !ok {
		return nil, Violation(RuleAggregateState, "count %s does not exist", id)
	}
	if count.Status != CountOpen {
		return nil, Violation(RuleAggregateState, "count %s is already %s", id, count.Status)
	}
	return count, nil
}
