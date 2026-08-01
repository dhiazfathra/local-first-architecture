package projection

import (
	"database/sql"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// ReservationRow is one line of the reservations read model.
type ReservationRow struct {
	ID       string
	SKU      string
	Location domain.LocationCode
	LotID    string
	Qty      float64
	Status   domain.ReservationStatus
}

// applyReservation folds reservation events into the reservations table. Reservations
// are node-local — no other node consults them — but they still replicate to central
// for visibility, and central's copy is built by this same code.
func applyReservation(tx *sql.Tx, payload any) error {
	switch p := payload.(type) {
	case domain.StockReserved:
		if _, err := tx.Exec(`INSERT INTO reservations (id, sku, location, lot_id, qty, status)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (id) DO UPDATE SET sku = excluded.sku, location = excluded.location,
				lot_id = excluded.lot_id, qty = excluded.qty, status = excluded.status`,
			p.ReservationID, p.Key.SKU, string(p.Key.Location), p.Key.LotID, p.Qty, string(domain.ResActive)); err != nil {
			return fmt.Errorf("insert reservation %s: %w", p.ReservationID, err)
		}
	case domain.ReservationReleased:
		return setReservationStatus(tx, p.ReservationID, domain.ResReleased)
	case domain.ReservationConsumed:
		return setReservationStatus(tx, p.ReservationID, domain.ResConsumed)
	}
	return nil
}

func setReservationStatus(tx *sql.Tx, id string, status domain.ReservationStatus) error {
	if _, err := tx.Exec(`UPDATE reservations SET status = ? WHERE id = ?`, string(status), id); err != nil {
		return fmt.Errorf("set reservation %s status: %w", id, err)
	}
	return nil
}

// Reservations returns every reservation the node knows about, in id order.
func (s *Set) Reservations() ([]ReservationRow, error) {
	rows, err := s.db.Query(`SELECT id, sku, location, lot_id, qty, status FROM reservations ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query reservations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ReservationRow{}
	for rows.Next() {
		var r ReservationRow
		var loc, status string
		if err := rows.Scan(&r.ID, &r.SKU, &loc, &r.LotID, &r.Qty, &status); err != nil {
			return nil, fmt.Errorf("scan reservation row: %w", err)
		}
		r.Location, r.Status = domain.LocationCode(loc), domain.ReservationStatus(status)
		out = append(out, r)
	}
	// Not covered: rows.Err() only surfaces a driver-level failure mid-iteration
	// (a dropped connection, a cancelled context), not anything reachable by
	// corrupting table contents or dropping tables. Parked, as eventlog's own
	// DB-fault branches were.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reservation rows: %w", err)
	}
	return out, nil
}
