package domain

import "time"

// ReservationStatus tracks whether a soft hold is still holding stock.
type ReservationStatus string

// Reservation statuses.
const (
	ResActive   ReservationStatus = "active"
	ResReleased ReservationStatus = "released"
	ResConsumed ReservationStatus = "consumed"
)

// Reservation is a soft hold on stock at one key. Only active reservations
// subtract from available.
type Reservation struct {
	ID     string
	Key    StockKey
	Qty    float64
	Status ReservationStatus
}

// ReceiptStatus is the lifecycle position of a receipt.
type ReceiptStatus string

// Receipt statuses.
const (
	ReceiptOpen         ReceiptStatus = "open"
	ReceiptClosedStatus ReceiptStatus = "closed"
)

// ReceiptState folds one receipt's events: the supplier delivery note it records,
// the PO it is against, and its lines by line number.
type ReceiptState struct {
	ID           string
	DeliveryNote string
	PORef        string
	Status       ReceiptStatus
	Lines        map[int]ReceiptLineRecorded
	NextLineNo   int
}

// TransferStatus is the lifecycle position of an inter-node transfer as this node
// understands it.
type TransferStatus string

// Transfer statuses. InFlight means dispatched but not yet received: the goods are
// in transit and belong to neither node.
const (
	TransferInFlight TransferStatus = "in_flight"
	TransferComplete TransferStatus = "complete"
	TransferFailed   TransferStatus = "failed"
)

// TransferState folds both halves of a transfer keyed by stock key, so the
// in-transit quantity for any key is Dispatched minus Received.
type TransferState struct {
	ID         string
	FromNode   NodeID
	ToNode     NodeID
	Dispatched map[StockKey]float64
	Received   map[StockKey]float64
	Status     TransferStatus
}

// CountStatus is the lifecycle position of a stock count.
type CountStatus string

// Count statuses.
const (
	CountOpen         CountStatus = "open"
	CountClosedStatus CountStatus = "closed"
)

// CountState folds one physical count: which location, and what the operator
// counted at each key.
type CountState struct {
	ID       string
	Location LocationCode
	Status   CountStatus
	Counted  map[StockKey]float64
}

// State is the in-memory fold of a node's log. Commands validate against it and
// never mutate it; only Apply mutates.
type State struct {
	// Items is the replicated read-only item master. It may be stale.
	Items     map[string]Item
	Locations map[LocationCode]LocationType
	Lots      map[string]Lot
	// Stock may hold negative values: a compensation from central can drive a
	// location negative after the node has already shipped the goods. That is
	// recorded and flagged, never clamped.
	Stock        map[StockKey]float64
	Reservations map[string]Reservation
	Receipts     map[string]*ReceiptState
	Transfers    map[string]*TransferState
	Counts       map[string]*CountState
	// Compensations collects every envelope carrying a CausationID, i.e. every
	// event central emitted to undo one of ours. The exceptions projection is
	// built from these.
	Compensations []Envelope
	// PendingReceipts holds TransferReceived lines that arrived before this node
	// learned the matching TransferDispatched metadata, keyed by TransferID. Sync
	// delivery across different aggregates is not guaranteed to preserve the
	// causal dispatch-before-receipt order, and a receipt arriving first must not
	// be silently discarded — the corresponding TransferDispatched folds any
	// pending lines in once it arrives.
	PendingReceipts map[string][]Movement
	// Home is this node's own identity. It is empty by default (used by pure
	// domain tests that only ever apply a single node's own events, where every
	// TransferDispatched/TransferReceived folded in is unconditionally this
	// node's own movement). A node.Service sets it via SetHome so that a
	// TransferDispatched or TransferReceived forwarded by central purely for
	// metadata — because it did not originate here and was not addressed here —
	// updates transfer bookkeeping only and never moves stock. Without this gate,
	// the destination of a transfer folds the source's own From/To locations into
	// its own Stock map the moment central relays the dispatch for visibility,
	// corrupting the destination's projection with balances at locations it does
	// not own.
	Home NodeID
}

// SetHome fixes this state's own node identity after construction. Call it once,
// before applying any events, on any State backing a running node.Service.
func (s *State) SetHome(id NodeID) { s.Home = id }

// NewState returns an empty state with every map ready to use.
func NewState() *State {
	return &State{
		Items:           map[string]Item{},
		Locations:       map[LocationCode]LocationType{},
		Lots:            map[string]Lot{},
		Stock:           map[StockKey]float64{},
		Reservations:    map[string]Reservation{},
		Receipts:        map[string]*ReceiptState{},
		Transfers:       map[string]*TransferState{},
		Counts:          map[string]*CountState{},
		PendingReceipts: map[string][]Movement{},
	}
}

// OnHand is the book quantity at one key.
func (s *State) OnHand(k StockKey) float64 { return s.Stock[k] }

// Available is on-hand minus every active reservation against the same key. This
// is the quantity a new command may promise.
func (s *State) Available(k StockKey) float64 {
	avail := s.Stock[k]
	for _, r := range s.Reservations {
		if r.Status == ResActive && r.Key == k {
			avail -= r.Qty
		}
	}
	return avail
}

// Item looks a SKU up in the replicated master, enforcing the node-side
// "SKU exists in item master" invariant. The node's master may be stale, so
// central re-checks this after the fact and compensates what it disagrees with.
func (s *State) Item(sku string) (Item, error) {
	it, ok := s.Items[sku]
	if !ok {
		return Item{}, Violation(RuleSKUExists, "sku %q is not in the replicated item master", sku)
	}
	if it.Deleted {
		return Item{}, Violation(RuleSKUExists, "sku %q was deleted at central", sku)
	}
	return it, nil
}

// Apply folds one envelope into the state. It is the only mutator, and every
// movement goes through move(), so stock arithmetic exists exactly once.
func (s *State) Apply(envelope Envelope) error {
	payload, err := DecodePayload(envelope)
	if err != nil {
		return err
	}
	if envelope.CausationID != nil {
		s.Compensations = append(s.Compensations, envelope)
	}
	switch p := payload.(type) {
	case ItemUpserted:
		s.Items[p.Item.SKU] = p.Item
	case LocationRegistered:
		s.Locations[p.Code] = p.Type
	case GoodsReceived:
		s.registerLot(p.Move, envelope.RecordedAt)
		s.move(p.Move)
	case PutAway:
		s.move(p.Move)
	case Picked:
		s.move(p.Move)
	case StockAdjusted:
		s.move(p.Move)
	case StockReserved:
		s.Reservations[p.ReservationID] = Reservation{ID: p.ReservationID, Key: p.Key, Qty: p.Qty, Status: ResActive}
	case ReservationReleased:
		s.setReservationStatus(p.ReservationID, ResReleased)
	case ReservationConsumed:
		s.setReservationStatus(p.ReservationID, ResConsumed)
	case ReceiptOpened:
		s.Receipts[p.ReceiptID] = &ReceiptState{
			ID: p.ReceiptID, DeliveryNote: p.DeliveryNote, PORef: p.PORef,
			Status: ReceiptOpen, Lines: map[int]ReceiptLineRecorded{}, NextLineNo: 1,
		}
	case ReceiptLineRecorded:
		if r, ok := s.Receipts[p.ReceiptID]; ok {
			r.Lines[p.LineNo] = p
			if p.LineNo >= r.NextLineNo {
				r.NextLineNo = p.LineNo + 1
			}
		}
		s.registerLot(Movement{SKU: p.SKU, LotID: p.LotID}, envelope.RecordedAt)
	case ReceiptClosed:
		if r, ok := s.Receipts[p.ReceiptID]; ok {
			r.Status = ReceiptClosedStatus
		}
	case TransferDispatched:
		t, existing := s.Transfers[p.TransferID]
		if !existing {
			t = &TransferState{
				ID: p.TransferID, FromNode: p.FromNode, ToNode: p.ToNode,
				Dispatched: map[StockKey]float64{}, Received: map[StockKey]float64{},
				Status: TransferInFlight,
			}
		}
		// Only the originating node's own stock actually left a shelf. Central
		// also relays this same event to the destination purely so it can learn
		// transfer metadata ahead of the truck; folding it into the destination's
		// own Stock map there would apply the source's From/To locations against
		// the destination's projection, corrupting it with balances at locations
		// the destination does not own. Home == "" (a pure domain test applying
		// only its own node's events) always applies the move, matching prior
		// behavior.
		mine := s.Home == "" || s.Home == p.FromNode
		for _, line := range p.Lines {
			t.Dispatched[StockKey{SKU: line.SKU, LotID: line.LotID}] += line.Qty
			if mine {
				s.move(line)
			}
		}
		// A TransferReceived for this transfer may have arrived first — sync
		// delivery does not guarantee dispatch-before-receipt across aggregates —
		// and been buffered in PendingReceipts instead of discarded. Fold it in now
		// that the dispatch has finally arrived.
		if pending, buffered := s.PendingReceipts[p.TransferID]; buffered {
			mineRecv := s.Home == "" || s.Home == p.ToNode
			for _, line := range pending {
				t.Received[StockKey{SKU: line.SKU, LotID: line.LotID}] += line.Qty
				if mineRecv {
					s.move(line)
				}
			}
			if t.Status == TransferInFlight {
				t.Status = TransferComplete
			}
			delete(s.PendingReceipts, p.TransferID)
		}
		s.Transfers[p.TransferID] = t
	case TransferReceived:
		t, ok := s.Transfers[p.TransferID]
		if !ok {
			// This node has not learned the matching TransferDispatched metadata
			// yet. The receipt is not discarded: it is buffered so the dispatch,
			// whenever it arrives, can fold it in and the transfer never gets
			// stuck showing zero received despite the receipt already being in
			// the log.
			s.PendingReceipts[p.TransferID] = append(s.PendingReceipts[p.TransferID], p.Lines...)
			return nil
		}
		// Symmetric with the dispatch case above: this event moved stock only at
		// the destination that actually received it. Central also relays it back
		// to the source purely for transfer-projection visibility (so the
		// source's view of the transfer does not stay stuck in-flight forever),
		// and that relay must not move stock at the source.
		mine := s.Home == "" || s.Home == t.ToNode
		for _, line := range p.Lines {
			t.Received[StockKey{SKU: line.SKU, LotID: line.LotID}] += line.Qty
			if mine {
				s.move(line)
			}
		}
		if t.Status == TransferInFlight {
			t.Status = TransferComplete
		}
	case CountStarted:
		s.Counts[p.CountID] = &CountState{ID: p.CountID, Location: p.Location, Status: CountOpen, Counted: map[StockKey]float64{}}
	case CountLineCounted:
		if c, ok := s.Counts[p.CountID]; ok {
			c.Counted[p.Key] = p.CountedQty
		}
	case CountClosed:
		if c, ok := s.Counts[p.CountID]; ok {
			c.Status = CountClosedStatus
		}
	}
	return nil
}

// move applies one balanced movement: the quantity leaves From and arrives at To.
// Zero-valued keys are pruned so replays produce byte-identical maps regardless of
// the path taken to get there, which is what makes determinism testable.
func (s *State) move(m Movement) {
	s.add(m.FromKey(), -m.Qty)
	s.add(m.ToKey(), m.Qty)
}

func (s *State) add(k StockKey, delta float64) {
	v := s.Stock[k] + delta
	if v == 0 {
		delete(s.Stock, k)
		return
	}
	s.Stock[k] = v
}

// registerLot creates the lot record on first receipt of a lot-tracked SKU,
// dating expiry from the receipt instant plus the item's shelf life.
func (s *State) registerLot(m Movement, at time.Time) {
	if _, exists := s.Lots[m.LotID]; m.LotID == "" || exists {
		return
	}
	lot := Lot{ID: m.LotID, SKU: m.SKU}
	if it, ok := s.Items[m.SKU]; ok && it.ShelfLifeDays > 0 {
		lot.ExpiresOn = at.UTC().AddDate(0, 0, it.ShelfLifeDays)
	}
	s.Lots[m.LotID] = lot
}

func (s *State) setReservationStatus(id string, status ReservationStatus) {
	if r, ok := s.Reservations[id]; ok {
		r.Status = status
		s.Reservations[id] = r
	}
}
