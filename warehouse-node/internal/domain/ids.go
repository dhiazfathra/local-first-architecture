// Package domain holds the pure warehouse domain: commands in, events or a
// named rule violation out. It performs no I/O, touches no database, and never
// reads the clock — callers pass the instant.
package domain

import "strconv"

// NodeID identifies one warehouse node. It is also the tiebreaker in HLC
// ordering, so it must be unique and stable for the life of the node.
type NodeID string

// EventID is the globally unique identity of an event: the node that emitted it
// plus that node's monotonic sequence number. Central never renumbers events.
type EventID struct {
	NodeID NodeID `json:"node_id"`
	Seq    uint64 `json:"seq"`
}

// String renders an EventID as "<node>/<seq>" for logs and audit records.
func (e EventID) String() string {
	return string(e.NodeID) + "/" + strconv.FormatUint(e.Seq, 10)
}

// LocationCode identifies a physical place within one warehouse.
type LocationCode string

// External is the sentinel location standing for everything outside this
// warehouse: suppliers, customers, other nodes. Every movement names a From and
// a To, so receiving is external->receiving and picking is pick->external. That
// keeps the stock projection a single code path and makes reconciliation a sum
// that must come to zero.
const External LocationCode = "external"

// LocationType classifies what a location is for.
type LocationType string

// The five location types a warehouse node models.
const (
	LocReceiving  LocationType = "receiving"
	LocBulk       LocationType = "bulk"
	LocPick       LocationType = "pick"
	LocStaging    LocationType = "staging"
	LocQuarantine LocationType = "quarantine"
)

// Valid reports whether t is one of the known location types.
func (t LocationType) Valid() bool {
	switch t {
	case LocReceiving, LocBulk, LocPick, LocStaging, LocQuarantine:
		return true
	default:
		return false
	}
}

// StockKey is the grain of every stock balance: one SKU, in one location, of one
// lot. LotID is empty for SKUs that are not lot-tracked.
type StockKey struct {
	SKU      string       `json:"sku"`
	Location LocationCode `json:"location"`
	LotID    string       `json:"lot_id"`
}
