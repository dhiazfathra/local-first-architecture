package domain

// ReserveCmd places a soft hold on stock at one location so a second order cannot
// promise the same units.
type ReserveCmd struct {
	ReservationID string
	Line          Line
	Location      LocationCode
}

// DoReserve enforces "reservation cannot exceed available", where available is
// on-hand minus every other active hold on the same key. The node owns its own
// locations, so this needs no coordination with central.
func DoReserve(s *State, c ReserveCmd) ([]Event, error) {
	move, _, err := s.ResolveLine(c.Line)
	if err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.Location); err != nil {
		return nil, err
	}
	if _, exists := s.Reservations[c.ReservationID]; exists {
		return nil, Violation(RuleAggregateState, "reservation %s already exists", c.ReservationID)
	}
	key := StockKey{SKU: c.Line.SKU, Location: c.Location, LotID: c.Line.LotID}
	if avail := s.Available(key); avail < move.Qty {
		return nil, Violation(RuleReservationAvailable,
			"only %v of sku %s lot %q available at %s; cannot reserve %v", avail, key.SKU, key.LotID, key.Location, move.Qty)
	}
	return []Event{{Type: TypeStockReserved, AggregateID: c.ReservationID,
		Payload: StockReserved{ReservationID: c.ReservationID, Key: key, Qty: move.Qty}}}, nil
}

// ReleaseReservationCmd cancels a hold, returning its quantity to available.
type ReleaseReservationCmd struct {
	ReservationID string
}

// DoReleaseReservation validates and returns the ReservationReleased event.
func DoReleaseReservation(s *State, c ReleaseReservationCmd) ([]Event, error) {
	if err := s.requireActiveReservation(c.ReservationID); err != nil {
		return nil, err
	}
	return []Event{{Type: TypeReservationReleased, AggregateID: c.ReservationID,
		Payload: ReservationReleased(c)}}, nil
}

// ConsumeReservationCmd closes a hold because the stock was picked against it.
type ConsumeReservationCmd struct {
	ReservationID string
}

// DoConsumeReservation validates and returns the ReservationConsumed event.
func DoConsumeReservation(s *State, c ConsumeReservationCmd) ([]Event, error) {
	if err := s.requireActiveReservation(c.ReservationID); err != nil {
		return nil, err
	}
	return []Event{{Type: TypeReservationConsumed, AggregateID: c.ReservationID,
		Payload: ReservationConsumed(c)}}, nil
}

func (s *State) requireActiveReservation(id string) error {
	r, ok := s.Reservations[id]
	if !ok {
		return Violation(RuleAggregateState, "reservation %s does not exist", id)
	}
	if r.Status != ResActive {
		return Violation(RuleAggregateState, "reservation %s is already %s", id, r.Status)
	}
	return nil
}
