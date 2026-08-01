// Package central holds everything the central server persists: the replicated event
// log partitioned by node, the reference data only central owns, in-transit transfer
// balances, per-node sync cursors and arbitration decisions.
//
// Store is one wide interface rather than several narrow ones on purpose: it
// describes a single cohesive thing, and there are exactly two implementations —
// Memory for tests and the integration suite, Postgres for deployment — validated by
// one shared contract test.
package central

import (
	"context"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// CentralNode is the node identity central's own events carry, so a compensation is
// distinguishable from anything a warehouse produced.
const CentralNode domain.NodeID = "central"

// Verdict is arbitration's outcome for one event.
type Verdict string

const (
	// VerdictAccepted means the event passed every central-enforced invariant.
	VerdictAccepted Verdict = "accepted"
	// VerdictRejected means it did not, and a compensating event was emitted. The
	// original event is still persisted: the log is immutable history, mistakes
	// included.
	VerdictRejected Verdict = "rejected"
)

// Decision is the audit record of arbitration. CompensatingID is nil for an
// acceptance.
type Decision struct {
	EventID        domain.EventID
	Verdict        Verdict
	Reason         string
	CompensatingID *domain.EventID
}

// ReceiptFact is what central remembers about one received line, so it can later see
// that two nodes received against the same purchase order or keyed the same supplier
// delivery note. No node can see either.
type ReceiptFact struct {
	EventID      domain.EventID
	Node         domain.NodeID
	PORef        string
	DeliveryNote string
	SKU          string
	QtyBase      float64
}

// InTransitRow is the balance of goods that have left the source warehouse but not
// yet arrived at the destination, for one transfer and one SKU/lot. It belongs to
// neither node. Key.Location is empty because in-transit stock is at no location.
// Dispatched minus Received is the live in-transit quantity, and it must be zero once
// the transfer closes.
type InTransitRow struct {
	TransferID   string
	Key          domain.StockKey
	FromNode     domain.NodeID
	ToNode       domain.NodeID
	Dispatched   float64
	Received     float64
	DispatchedAt time.Time
	Failed       bool
}

// Outbound is one entry of a node's downstream queue. Ord is a per-store monotonic
// position, and it is what a node's cursor stores so a resumed session picks up
// exactly where it left off.
type Outbound struct {
	Ord uint64
	Env domain.Envelope
}

// Store is everything central persists.
type Store interface {
	Close() error

	// Append stores node events idempotently by (node, seq) and returns only those
	// that were new, which is what arbitration then runs over.
	Append(ctx context.Context, envs []domain.Envelope) ([]domain.Envelope, error)
	// Events returns the whole log in total HLC order, for replay and determinism
	// checks.
	Events(ctx context.Context) ([]domain.Envelope, error)
	// EmitCentral seals central's own events — compensations and item-master
	// updates — assigning central's sequence numbers and HLC readings, and appends
	// them. causation is non-nil exactly when the events compensate a rejection.
	EmitCentral(ctx context.Context, events []domain.Event, causation *domain.EventID,
		now time.Time) ([]domain.Envelope, error)

	// PushedSeq is the highest sequence of a node's own events central has stored.
	PushedSeq(ctx context.Context, node domain.NodeID) (uint64, error)
	SetPushedSeq(ctx context.Context, node domain.NodeID, seq uint64) error
	// DeliveredOrd is the highest outbound position a node has acknowledged.
	DeliveredOrd(ctx context.Context, node domain.NodeID) (uint64, error)
	SetDeliveredOrd(ctx context.Context, node domain.NodeID, ord uint64) error

	// Enqueue adds events to a node's downstream queue.
	Enqueue(ctx context.Context, target domain.NodeID, envs []domain.Envelope) error
	// Outbound reads up to limit queued events for a node above afterOrd.
	Outbound(ctx context.Context, target domain.NodeID, afterOrd uint64, limit int) ([]Outbound, error)

	UpsertItem(ctx context.Context, item domain.Item) error
	Item(ctx context.Context, sku string) (domain.Item, bool, error)
	UpsertPurchaseOrder(ctx context.Context, poRef, sku string, ordered float64) error
	PurchaseOrder(ctx context.Context, poRef, sku string) (float64, bool, error)
	// RegisterNode records that a node exists and which SKUs it refuses to stock.
	RegisterNode(ctx context.Context, node domain.NodeID, rejects []string) error
	NodeConfig(ctx context.Context, node domain.NodeID) (rejects map[string]bool, known bool, err error)

	// RecordReceipt is idempotent per event id, so a resent batch cannot
	// double-count a receipt against its purchase order.
	RecordReceipt(ctx context.Context, f ReceiptFact) error
	ReceivedAgainstPO(ctx context.Context, poRef, sku string) (float64, error)
	// Receipt reads back the fact recorded for one event, so a validator re-run by
	// a crash-retry can subtract the event's own already-folded contribution from
	// the aggregate it checks against and reach the same verdict it reached first
	// time round.
	Receipt(ctx context.Context, id domain.EventID) (ReceiptFact, bool, error)
	// DeliveryNoteFirstSeen returns the event that first keyed this note for this
	// SKU, which is how a duplicate is identified.
	DeliveryNoteFirstSeen(ctx context.Context, note, sku string) (domain.EventID, bool, error)

	RecordDispatch(ctx context.Context, row InTransitRow) error
	// AddReceived is idempotent per (event, SKU, lot): a retried arbitration that
	// already folded eventID's line into the balance is a no-op the second time.
	AddReceived(ctx context.Context, eventID domain.EventID, transferID string, k domain.StockKey, qty float64) error
	// ReceivedFromEvent is the quantity AddReceived already folded into this
	// transfer line on behalf of one event, or 0 if none. It is the transfer-side
	// counterpart of Receipt: it lets a retried validator exclude its own
	// contribution from the in-transit balance.
	ReceivedFromEvent(ctx context.Context, eventID domain.EventID, transferID string, k domain.StockKey) (float64, error)
	InTransit(ctx context.Context, transferID string, k domain.StockKey) (InTransitRow, bool, error)
	// OpenTransfers returns transfers dispatched before the given instant that are
	// still carrying stock. They are reported as discrepancies, never
	// auto-compensated: a lost truck is a human problem.
	OpenTransfers(ctx context.Context, dispatchedBefore time.Time) ([]InTransitRow, error)
	FailTransfer(ctx context.Context, transferID string) error

	RecordDecision(ctx context.Context, d Decision) error
	Decision(ctx context.Context, id domain.EventID) (Decision, bool, error)
}
