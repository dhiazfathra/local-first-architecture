package projection

import (
	"database/sql"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// StockRow is one line of the stock-on-hand read model. Available is Qty minus every
// active reservation on the same key.
type StockRow struct {
	SKU       string
	Location  domain.LocationCode
	LotID     string
	Qty       float64
	Available float64
}

// applyStock folds a payload's movements into stock_on_hand. Because every movement
// is balanced this is one code path: subtract at From, add at To. Rows reaching zero
// are deleted so the table is a canonical representation of the same balances no
// matter which order events arrived in.
//
// home is this node's own identity. Central relays TransferDispatched to the
// destination, and TransferReceived back to the source, purely so each side's
// transfer metadata (applyTransfer, in transfer.go) stays current — neither relay
// represents stock physically moving at the node receiving the relay, and folding
// it into stock_on_hand there would apply the other node's From/To locations
// against this node's own balances. home == "" (used by tests that only ever
// project a single node's own events without a home set) always applies the move.
func applyStock(tx *sql.Tx, home domain.NodeID, payload any) error {
	switch p := payload.(type) {
	case domain.TransferDispatched:
		if home != "" && home != p.FromNode {
			return nil
		}
	case domain.TransferReceived:
		toNode, err := transferToNode(tx, p.TransferID)
		if err != nil {
			return err
		}
		if home != "" && toNode != "" && home != toNode {
			return nil
		}
	}
	for _, m := range MovementsOf(payload) {
		if err := addStock(tx, m.FromKey(), -m.Qty); err != nil {
			return err
		}
		if err := addStock(tx, m.ToKey(), m.Qty); err != nil {
			return err
		}
	}
	return nil
}

// transferToNode looks up the destination node of a transfer from the transfers
// projection (populated by applyTransfer from the TransferDispatched half), so
// applyStock can tell whether a TransferReceived it is folding in happened at this
// node or is a metadata-only relay from central. An unknown transfer (not yet seen)
// returns "" and applyStock treats it as "apply", matching prior behavior. Any other
// query failure, including a missing transfers table, is returned as an error.
func transferToNode(tx *sql.Tx, id string) (domain.NodeID, error) {
	var to string
	err := tx.QueryRow(`SELECT to_node FROM transfers WHERE id = ?`, id).Scan(&to)
	switch {
	case err == sql.ErrNoRows:
		return "", nil
	case err != nil:
		return "", fmt.Errorf("look up transfer %s destination: %w", id, err)
	}
	return domain.NodeID(to), nil
}

func addStock(tx *sql.Tx, k domain.StockKey, delta float64) error {
	if _, err := tx.Exec(`INSERT INTO stock_on_hand (sku, location, lot_id, qty) VALUES (?, ?, ?, ?)
		ON CONFLICT (sku, location, lot_id) DO UPDATE SET qty = qty + excluded.qty`,
		k.SKU, string(k.Location), k.LotID, delta); err != nil {
		return fmt.Errorf("adjust stock at %+v: %w", k, err)
	}
	if _, err := tx.Exec(`DELETE FROM stock_on_hand WHERE sku = ? AND location = ? AND lot_id = ? AND abs(qty) < 1e-9`,
		k.SKU, string(k.Location), k.LotID); err != nil {
		return fmt.Errorf("prune zero stock at %+v: %w", k, err)
	}
	return nil
}

// StockOnHand returns operator-visible balances, optionally filtered by SKU and
// location. Empty arguments mean "no filter". The external sentinel is excluded: it
// is bookkeeping that makes balances sum to zero, not a place in the warehouse.
// Negative balances are included, because a location driven negative by a
// compensation is precisely what the operator must see.
func (s *Set) StockOnHand(sku string, location domain.LocationCode) ([]StockRow, error) {
	rows, err := s.db.Query(`
		SELECT h.sku, h.location, h.lot_id, h.qty,
		       h.qty - coalesce((SELECT sum(r.qty) FROM reservations r
		                          WHERE r.sku = h.sku AND r.location = h.location
		                            AND r.lot_id = h.lot_id AND r.status = ?), 0)
		FROM stock_on_hand h
		WHERE h.location <> ?
		  AND (? = '' OR h.sku = ?)
		  AND (? = '' OR h.location = ?)
		ORDER BY h.sku, h.location, h.lot_id`,
		string(domain.ResActive), string(domain.External), sku, sku, string(location), string(location))
	if err != nil {
		return nil, fmt.Errorf("query stock on hand: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []StockRow{}
	for rows.Next() {
		var r StockRow
		var loc string
		if err := rows.Scan(&r.SKU, &loc, &r.LotID, &r.Qty, &r.Available); err != nil {
			return nil, fmt.Errorf("scan stock row: %w", err)
		}
		r.Location = domain.LocationCode(loc)
		out = append(out, r)
	}
	// Not covered: same as Reservations' rows.Err() — only a driver-level failure
	// mid-iteration reaches this, not table corruption. Parked, as eventlog's own
	// DB-fault branches were.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stock rows: %w", err)
	}
	return out, nil
}

// Balance is the raw stored quantity at one key, including the external sentinel.
// Summing every Balance in the projection must come to zero.
func (s *Set) Balance(k domain.StockKey) (float64, error) {
	var qty sql.NullFloat64
	err := s.db.QueryRow(`SELECT qty FROM stock_on_hand WHERE sku = ? AND location = ? AND lot_id = ?`,
		k.SKU, string(k.Location), k.LotID).Scan(&qty)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read balance at %+v: %w", k, err)
	}
	return qty.Float64, nil
}
