package domain

import (
	"math"
	"time"
)

// UoM is a unit of measure code, such as "EA" or "CASE".
type UoM string

// Item is one row of the item master. The master is owned by central and
// replicated read-only to every node: a node never emits Item events, it applies
// ItemUpserted events arriving on the sync stream. A node's copy can therefore be
// stale, which is why central re-checks SKU existence after the fact.
type Item struct {
	SKU string `json:"sku"`
	// Description is free text shown to the operator.
	Description string `json:"description"`
	// BaseUoM is the unit every stock balance is stored in.
	BaseUoM UoM `json:"base_uom"`
	// AltUoM maps an alternate unit to how many base units it contains.
	AltUoM map[UoM]float64 `json:"alt_uom,omitempty"`
	// LotTracked requires every movement of this SKU to name a lot.
	LotTracked bool `json:"lot_tracked"`
	// ShelfLifeDays is how long after receipt a new lot stays usable.
	ShelfLifeDays int `json:"shelf_life_days"`
	// Deleted marks a SKU withdrawn at central. Nodes with a stale master may
	// still transact against it; central compensates those events.
	Deleted bool `json:"deleted"`
}

// ToBase converts an operator-entered quantity into base units, enforcing the
// node-side "UoM conversion valid for item" invariant: the unit must be the base
// unit or one of this item's declared alternates, with a usable factor.
// NaN and ±Inf are rejected explicitly: both slip past a plain `<= 0` check and
// would otherwise multiply into stock balances that no later count can repair.
func (i Item) ToBase(qty float64, u UoM) (float64, error) {
	if math.IsNaN(qty) || math.IsInf(qty, 0) || qty <= 0 {
		return 0, Violation(RuleQtyPositive, "quantity %v must be finite and greater than zero", qty)
	}
	if u == i.BaseUoM {
		return qty, nil
	}
	factor, ok := i.AltUoM[u]
	if !ok {
		return 0, Violation(RuleUoMValid, "uom %q is not base %q nor an alternate of sku %s", u, i.BaseUoM, i.SKU)
	}
	if math.IsNaN(factor) || math.IsInf(factor, 0) || factor <= 0 {
		return 0, Violation(RuleUoMValid, "uom %q of sku %s has conversion factor %v, which must be finite and positive", u, i.SKU, factor)
	}
	base := qty * factor
	if math.IsInf(base, 0) {
		return 0, Violation(RuleUoMValid, "uom %q of sku %s converts %v to a non-finite base quantity", u, i.SKU, qty)
	}
	return base, nil
}

// Lot is a received batch of one SKU with an expiry date. A zero ExpiresOn means
// the lot never expires (either the item never had a shelf life, or one hasn't
// been able to date it yet — see State.backfillLotExpiry).
type Lot struct {
	ID  string `json:"id"`
	SKU string `json:"sku"`
	// ReceivedAt is the instant this lot was first received, kept so expiry can
	// still be dated correctly if the item master's shelf life arrives later.
	ReceivedAt time.Time `json:"received_at"`
	ExpiresOn  time.Time `json:"expires_on"`
}

// ExpiredAt reports whether the lot is past expiry at instant t. Expiry is a
// date, not a timestamp: the whole expiry day is still usable, so only the day
// after counts as expired. That coarseness is why the node's local clock is good
// enough to enforce this invariant without consulting central.
//
// Only ExpiresOn's calendar year/month/day are read, and the boundary is built in
// UTC. Truncating the instant instead would fold the zone offset of a non-UTC
// ExpiresOn into the comparison and expire the lot up to a day early or late.
func (l Lot) ExpiredAt(t time.Time) bool {
	if l.ExpiresOn.IsZero() {
		return false
	}
	y, m, d := l.ExpiresOn.Date()
	last := time.Date(y, m, d+1, 0, 0, 0, 0, time.UTC)
	return !t.UTC().Before(last)
}
