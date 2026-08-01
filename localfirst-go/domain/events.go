// Package domain is the ONLY inventory-aware package. Every other package moves
// opaque records; this one decides what they mean.
//
// One aggregate: Stock, keyed (SKU, Location). Every Location belongs to exactly
// one node, so no two nodes ever write the same key — that is the whole conflict
// model. See docs/adr/0001-per-node-ownership.md.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// Record type discriminators. They are strings in the log, not Go types, so a
// stored log stays readable and forward-compatible.
const (
	TypeReceived = "inventory.Received"
	TypeIssued   = "inventory.Issued"
	TypeMoved    = "inventory.Moved"
)

// Received adds stock at a location owned by the authoring node.
type Received struct {
	SKU      string `json:"sku"`
	Location string `json:"location"`
	Qty      int64  `json:"qty"`
}

// Issued removes stock from a location owned by the authoring node.
type Issued struct {
	SKU      string `json:"sku"`
	Location string `json:"location"`
	Qty      int64  `json:"qty"`
}

// Moved transfers stock. From must be owned by the authoring node; To may live
// on another node. There is no in-transit accounting: the goods are absent from
// the global sum between the decrement and the increment, and that gap is an
// accepted, documented limitation (docs/limitations.md).
type Moved struct {
	SKU  string `json:"sku"`
	From string `json:"from"`
	To   string `json:"to"`
	Qty  int64  `json:"qty"`
}

// ErrBadPayload means a record's bytes did not decode as its declared type.
var ErrBadPayload = errors.New("domain: malformed payload")

func decode[E any](r eventlog.Record) (E, error) {
	var ev E
	if err := json.Unmarshal(r.Payload, &ev); err != nil {
		return ev, fmt.Errorf("%w: %s: %v", ErrBadPayload, r.Type, err)
	}
	return ev, nil
}

// encode marshals one of the three event structs. It returns no error because
// those structs contain only strings and ints, which json.Marshal cannot fail
// on — inventing an unreachable error branch would just be untestable code.
func encode(ev any) []byte {
	b, _ := json.Marshal(ev) //nolint:errchkjson // only string/int fields
	return b
}
