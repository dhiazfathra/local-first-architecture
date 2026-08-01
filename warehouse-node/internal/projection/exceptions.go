package projection

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// ExceptionKind distinguishes the two things an operator must be told about.
type ExceptionKind string

const (
	// ExceptionCompensation is an event central emitted to undo the effect of an
	// event this node had already appended. The node cannot refuse it; it can only
	// show it.
	ExceptionCompensation ExceptionKind = "compensation"
	// ExceptionNegativeBalance is a location driven below zero, which happens when a
	// compensation reverses stock that had already been picked and shipped.
	// Compensation is deliberately not cascaded through downstream events, so this
	// is an accepted outcome that a human resolves with a stock count.
	ExceptionNegativeBalance ExceptionKind = "negative_balance"
)

// ReasonReceiptReversal is the reason recorded for a compensating
// ReceiptLineRecorded. Unlike StockAdjusted that payload carries no reason field,
// because the reason lives on the StockAdjusted emitted alongside it.
const ReasonReceiptReversal = "receipt_line_reversal"

// ExceptionRow is one operator-visible exception. ID is the compensating event's ID
// for a compensation, and a key-derived identity for a negative balance, because a
// negative balance is a standing condition rather than a single event.
type ExceptionRow struct {
	ID         string
	Kind       ExceptionKind
	Reason     string
	CausedBy   string
	Key        domain.StockKey
	Qty        float64
	RecordedAt time.Time
	Resolved   bool
}

// negativeID is the stable identity of the negative-balance exception for one stock
// key, so repeated dips below zero update one row instead of accumulating rows.
func negativeID(k domain.StockKey) string {
	return "negative:" + k.SKU + "|" + string(k.Location) + "|" + k.LotID
}

// applyException records the exceptions one event creates: the compensation itself
// if the event is one, and any balance the event left negative. It must run after
// applyStock, because it reads the balances applyStock has just written.
func applyException(tx *sql.Tx, env domain.Envelope, payload any) error {
	if env.CausationID != nil {
		if err := recordCompensation(tx, env, payload); err != nil {
			return err
		}
	}
	return refreshNegatives(tx, env, payload)
}

func recordCompensation(tx *sql.Tx, env domain.Envelope, payload any) error {
	reason := ReasonReceiptReversal
	if a, ok := payload.(domain.StockAdjusted); ok {
		reason = a.Reason
	}
	var key domain.StockKey
	var qty float64
	if moves := MovementsOf(payload); len(moves) > 0 {
		key, qty = moves[0].FromKey(), moves[0].Qty
		if key.Location == domain.External {
			key = moves[0].ToKey()
		}
	}
	_, err := tx.Exec(`INSERT INTO exceptions
		(id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
		ON CONFLICT (id) DO NOTHING`,
		env.ID.String(), string(ExceptionCompensation), reason, env.CausationID.String(),
		key.SKU, string(key.Location), key.LotID, qty, env.RecordedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("record compensation %s: %w", env.ID, err)
	}
	return nil
}

// refreshNegatives re-checks every key the event touched. A key below zero is
// flagged unresolved; a key back at or above zero resolves an existing flag. The
// external sentinel is skipped: it is bookkeeping and is negative by design.
func refreshNegatives(tx *sql.Tx, env domain.Envelope, payload any) error {
	for _, m := range MovementsOf(payload) {
		for _, k := range []domain.StockKey{m.FromKey(), m.ToKey()} {
			if k.Location == domain.External {
				continue
			}
			if err := refreshNegative(tx, env, k); err != nil {
				return err
			}
		}
	}
	return nil
}

func refreshNegative(tx *sql.Tx, env domain.Envelope, k domain.StockKey) error {
	var qty sql.NullFloat64
	err := tx.QueryRow(`SELECT qty FROM stock_on_hand WHERE sku = ? AND location = ? AND lot_id = ?`,
		k.SKU, string(k.Location), k.LotID).Scan(&qty)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("read balance for negative check at %+v: %w", k, err)
	}
	if qty.Float64 < 0 {
		if _, err := tx.Exec(`INSERT INTO exceptions
			(id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved)
			VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, 0)
			ON CONFLICT (id) DO UPDATE SET qty = excluded.qty, recorded_at = excluded.recorded_at, resolved = 0`,
			negativeID(k), string(ExceptionNegativeBalance), "negative_after_compensation",
			k.SKU, string(k.Location), k.LotID, qty.Float64,
			env.RecordedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("flag negative balance at %+v: %w", k, err)
		}
		return nil
	}
	if _, err := tx.Exec(`UPDATE exceptions SET resolved = 1, qty = ? WHERE id = ?`,
		qty.Float64, negativeID(k)); err != nil {
		return fmt.Errorf("resolve negative balance at %+v: %w", k, err)
	}
	return nil
}

// Exceptions returns every exception, unresolved first so the operator sees what
// still needs a human, then by ID for a stable order.
func (s *Set) Exceptions() ([]ExceptionRow, error) {
	rows, err := s.db.Query(`SELECT id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved
		FROM exceptions ORDER BY resolved, id`)
	if err != nil {
		return nil, fmt.Errorf("query exceptions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ExceptionRow{}
	for rows.Next() {
		var r ExceptionRow
		var kind, loc, at string
		var resolved int
		if err := rows.Scan(&r.ID, &kind, &r.Reason, &r.CausedBy,
			&r.Key.SKU, &loc, &r.Key.LotID, &r.Qty, &at, &resolved); err != nil {
			return nil, fmt.Errorf("scan exception row: %w", err)
		}
		r.Kind, r.Key.Location, r.Resolved = ExceptionKind(kind), domain.LocationCode(loc), resolved == 1
		if r.RecordedAt, err = time.Parse(time.RFC3339Nano, at); err != nil {
			return nil, fmt.Errorf("parse exception timestamp %q: %w", at, err)
		}
		out = append(out, r)
	}
	// Not covered: same as StockOnHand's and Reservations' rows.Err() — only a
	// driver-level failure mid-iteration reaches this, not table corruption. Parked,
	// as eventlog's own DB-fault branches were.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate exception rows: %w", err)
	}
	return out, nil
}
