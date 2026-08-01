package central

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// Memory is an in-memory Store. It is not a test double: the arbiter and the
// integration suite run against it because arbitration logic must be testable
// without a database, and it passes the same contract test as Postgres.
type Memory struct {
	mu        sync.Mutex
	events    map[domain.EventID]domain.Envelope
	centralHL domain.HLC
	centralN  uint64
	pushed    map[domain.NodeID]uint64
	delivered map[domain.NodeID]uint64
	queue     map[domain.NodeID][]Outbound
	nextOrd   uint64
	items     map[string]domain.Item
	orders    map[string]float64
	receipts  map[domain.EventID]ReceiptFact
	nodes     map[domain.NodeID]map[string]bool
	transit   map[string]InTransitRow
	decisions map[domain.EventID]Decision
	enqueued  map[string]bool
	// folded records the quantity AddReceived already applied for one (event,
	// SKU/lot), both as the idempotency marker and as the amount a retried
	// validator must exclude from the balance.
	folded map[string]float64
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{
		events:    map[domain.EventID]domain.Envelope{},
		pushed:    map[domain.NodeID]uint64{},
		delivered: map[domain.NodeID]uint64{},
		queue:     map[domain.NodeID][]Outbound{},
		items:     map[string]domain.Item{},
		orders:    map[string]float64{},
		receipts:  map[domain.EventID]ReceiptFact{},
		nodes:     map[domain.NodeID]map[string]bool{},
		transit:   map[string]InTransitRow{},
		decisions: map[domain.EventID]Decision{},
		enqueued:  map[string]bool{},
		folded:    map[string]float64{},
	}
}

// Close is a no-op; nothing outlives the process.
func (m *Memory) Close() error { return nil }

// poKey and transitKey compose the composite map keys, in one place so the two
// implementations of Store cannot drift on what "the same purchase order line" means.
func poKey(poRef, sku string) string { return poRef + "\x00" + sku }

func transitKey(transferID string, k domain.StockKey) string {
	return transferID + "\x00" + k.SKU + "\x00" + k.LotID
}

// eventKey composes the natural key of one (event, SKU/lot) fold or enqueue, so a
// retry recognizes it already ran.
func eventKey(id domain.EventID, k domain.StockKey) string {
	return string(id.NodeID) + "\x00" + strconv.FormatUint(id.Seq, 10) + "\x00" + k.SKU + "\x00" + k.LotID
}

// enqueueKey composes the natural key of one (target, event) queue entry.
func enqueueKey(target domain.NodeID, id domain.EventID) string {
	return string(target) + "\x00" + string(id.NodeID) + "\x00" + strconv.FormatUint(id.Seq, 10)
}

// Append stores node events idempotently by (node, seq) and returns only those
// that were new.
func (m *Memory) Append(_ context.Context, envs []domain.Envelope) ([]domain.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fresh := make([]domain.Envelope, 0, len(envs))
	for _, env := range envs {
		if _, ok := m.events[env.ID]; ok {
			continue
		}
		m.events[env.ID] = env
		fresh = append(fresh, env)
	}
	return fresh, nil
}

// Events returns the whole log in total HLC order.
func (m *Memory) Events(_ context.Context) ([]domain.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sortedEvents(), nil
}

func (m *Memory) sortedEvents() []domain.Envelope {
	out := make([]domain.Envelope, 0, len(m.events))
	for _, env := range m.events {
		out = append(out, env)
	}
	sort.Slice(out, func(i, j int) bool {
		if c := out[i].HLC.Compare(out[j].HLC); c != 0 {
			return c < 0
		}
		return out[i].ID.Seq < out[j].ID.Seq
	})
	return out
}

// EmitCentral seals central's own events, assigning central's sequence numbers
// and HLC readings, and appends them. When the emission is compensating a
// specific event (causation set), the HLC is merged with that event's own HLC
// so the compensation is guaranteed to sort after its cause in every replay,
// not just in the order it happened to be applied live.
func (m *Memory) EmitCentral(_ context.Context, events []domain.Event, causation *domain.EventID,
	now time.Time) ([]domain.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var causing domain.HLC
	if causation != nil {
		if env, ok := m.events[*causation]; ok {
			causing = env.HLC
		}
	}

	out := make([]domain.Envelope, 0, len(events))
	for _, e := range events {
		m.centralN++
		if causation != nil {
			m.centralHL = domain.Merge(m.centralHL, causing, now.UnixMilli(), CentralNode)
		} else {
			m.centralHL = domain.Tick(m.centralHL, now.UnixMilli(), CentralNode)
		}
		env, err := domain.NewEnvelope(
			domain.EventID{NodeID: CentralNode, Seq: m.centralN}, m.centralHL, now, causation, e)
		if err != nil {
			return nil, err
		}
		m.events[env.ID] = env
		out = append(out, env)
	}
	return out, nil
}

// PushedSeq is the highest sequence of a node's own events central has stored.
func (m *Memory) PushedSeq(_ context.Context, node domain.NodeID) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pushed[node], nil
}

// SetPushedSeq records the highest sequence of a node's own events central has stored.
func (m *Memory) SetPushedSeq(_ context.Context, node domain.NodeID, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pushed[node] = seq
	return nil
}

// DeliveredOrd is the highest outbound position a node has acknowledged.
func (m *Memory) DeliveredOrd(_ context.Context, node domain.NodeID) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delivered[node], nil
}

// SetDeliveredOrd records the highest outbound position a node has acknowledged.
func (m *Memory) SetDeliveredOrd(_ context.Context, node domain.NodeID, ord uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered[node] = ord
	return nil
}

// Enqueue adds events to a node's downstream queue. It is idempotent per (target,
// event): a retried arbitration that already queued an event for this target is a
// no-op the second time, rather than duplicating the outbound entry.
func (m *Memory) Enqueue(_ context.Context, target domain.NodeID, envs []domain.Envelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, env := range envs {
		key := enqueueKey(target, env.ID)
		if m.enqueued[key] {
			continue
		}
		m.enqueued[key] = true
		m.nextOrd++
		m.queue[target] = append(m.queue[target], Outbound{Ord: m.nextOrd, Env: env})
	}
	return nil
}

// Outbound reads up to limit queued events for a node above afterOrd.
func (m *Memory) Outbound(_ context.Context, target domain.NodeID, afterOrd uint64, limit int) ([]Outbound, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := []Outbound{}
	if limit <= 0 {
		return out, nil
	}
	for _, row := range m.queue[target] {
		if row.Ord <= afterOrd {
			continue
		}
		out = append(out, row)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// UpsertItem inserts or replaces an item master row.
func (m *Memory) UpsertItem(_ context.Context, item domain.Item) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[item.SKU] = item
	return nil
}

// Item looks up an item master row by SKU.
func (m *Memory) Item(_ context.Context, sku string) (domain.Item, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item, ok := m.items[sku]
	return item, ok, nil
}

// UpsertPurchaseOrder inserts or replaces the ordered quantity for a PO/SKU line.
func (m *Memory) UpsertPurchaseOrder(_ context.Context, poRef, sku string, ordered float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.orders[poKey(poRef, sku)] = ordered
	return nil
}

// PurchaseOrder looks up the ordered quantity for a PO/SKU line.
func (m *Memory) PurchaseOrder(_ context.Context, poRef, sku string) (float64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	qty, ok := m.orders[poKey(poRef, sku)]
	return qty, ok, nil
}

// RegisterNode records that a node exists and which SKUs it refuses to stock.
func (m *Memory) RegisterNode(_ context.Context, node domain.NodeID, rejects []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := map[string]bool{}
	for _, sku := range rejects {
		set[sku] = true
	}
	m.nodes[node] = set
	return nil
}

// NodeConfig looks up a node's registration and its rejected SKUs.
func (m *Memory) NodeConfig(_ context.Context, node domain.NodeID) (map[string]bool, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set, ok := m.nodes[node]
	if !ok {
		return nil, false, nil
	}
	out := make(map[string]bool, len(set))
	for sku := range set {
		out[sku] = true
	}
	return out, true, nil
}

// RecordReceipt records a receipt fact idempotently per event id.
func (m *Memory) RecordReceipt(_ context.Context, f ReceiptFact) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.receipts[f.EventID]; ok {
		return nil
	}
	m.receipts[f.EventID] = f
	return nil
}

// Receipt reads back the fact recorded for one event.
func (m *Memory) Receipt(_ context.Context, id domain.EventID) (ReceiptFact, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.receipts[id]
	return f, ok, nil
}

// ReceivedAgainstPO sums every recorded receipt against a PO/SKU line, across all nodes.
func (m *Memory) ReceivedAgainstPO(_ context.Context, poRef, sku string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total float64
	for _, f := range m.receipts {
		if f.PORef == poRef && f.SKU == sku {
			total += f.QtyBase
		}
	}
	return total, nil
}

// DeliveryNoteFirstSeen returns the event that first keyed a delivery note for a SKU.
func (m *Memory) DeliveryNoteFirstSeen(_ context.Context, note, sku string) (domain.EventID, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var first domain.EventID
	found := false
	for _, f := range m.receipts {
		if f.DeliveryNote != note || f.SKU != sku {
			continue
		}
		if !found || less(f.EventID, first) {
			first, found = f.EventID, true
		}
	}
	return first, found, nil
}

// less orders event ids so "first seen" is deterministic regardless of arrival order.
func less(a, b domain.EventID) bool {
	if a.NodeID != b.NodeID {
		return a.NodeID < b.NodeID
	}
	return a.Seq < b.Seq
}

// RecordDispatch stores or updates the dispatched quantity for a transfer/key.
func (m *Memory) RecordDispatch(_ context.Context, row InTransitRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := transitKey(row.TransferID, row.Key)
	if existing, ok := m.transit[key]; ok {
		existing.Dispatched = row.Dispatched
		m.transit[key] = existing
		return nil
	}
	m.transit[key] = row
	return nil
}

// AddReceived adds to the received quantity of an existing transfer/key. It is
// idempotent per (eventID, SKU, lot): a retried arbitration that already folded this
// event's line into the balance returns nil without adding qty again.
func (m *Memory) AddReceived(_ context.Context, eventID domain.EventID, transferID string, k domain.StockKey, qty float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := transitKey(transferID, k)
	row, ok := m.transit[key]
	if !ok {
		return fmt.Errorf("transfer %s has no dispatched line for %s/%s", transferID, k.SKU, k.LotID)
	}
	fk := eventKey(eventID, k)
	if _, done := m.folded[fk]; done {
		return nil // already folded into the balance by an earlier attempt
	}
	m.folded[fk] = qty
	row.Received += qty
	m.transit[key] = row
	return nil
}

// ReceivedFromEvent is the quantity AddReceived already folded for one event and line.
func (m *Memory) ReceivedFromEvent(_ context.Context, eventID domain.EventID, _ string, k domain.StockKey) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.folded[eventKey(eventID, k)], nil
}

// InTransit looks up the current in-transit balance for a transfer/key.
func (m *Memory) InTransit(_ context.Context, transferID string, k domain.StockKey) (InTransitRow, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.transit[transitKey(transferID, k)]
	return row, ok, nil
}

// OpenTransfers returns transfers dispatched before the given instant that are still carrying stock.
func (m *Memory) OpenTransfers(_ context.Context, dispatchedBefore time.Time) ([]InTransitRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := []InTransitRow{}
	for _, row := range m.transit {
		if row.Failed || row.Dispatched-row.Received <= 0 || !row.DispatchedAt.Before(dispatchedBefore) {
			continue
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TransferID != out[j].TransferID {
			return out[i].TransferID < out[j].TransferID
		}
		if out[i].Key.SKU != out[j].Key.SKU {
			return out[i].Key.SKU < out[j].Key.SKU
		}
		return out[i].Key.LotID < out[j].Key.LotID
	})
	return out, nil
}

// FailTransfer marks every row of a transfer as failed.
func (m *Memory) FailTransfer(_ context.Context, transferID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, row := range m.transit {
		if row.TransferID == transferID {
			row.Failed = true
			m.transit[key] = row
		}
	}
	return nil
}

// RecordDecision stores an arbitration decision for an event.
func (m *Memory) RecordDecision(_ context.Context, d Decision) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.decisions[d.EventID] = d
	return nil
}

// Decision looks up the arbitration decision recorded for an event.
func (m *Memory) Decision(_ context.Context, id domain.EventID) (Decision, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.decisions[id]
	return d, ok, nil
}
