package projection

import (
	"database/sql"
	"errors"
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
	a, ok := payload.(domain.StockAdjusted)
	if !ok {
		// A rejection can emit more than one compensating event sharing the same
		// CausationID — po_overreceipt, for one, reverses the stock via a
		// StockAdjusted and also books a negative ReceiptLineRecorded line to
		// correct the paperwork. Only the StockAdjusted is operator-visible: it
		// carries the reason and the quantity actually reversed. The paperwork
		// correction has no stock effect of its own and would otherwise show up
		// as a second, misleading exception row for the same rejection.
		return nil
	}
	key, qty := a.Move.FromKey(), a.Move.Qty
	if key.Location == domain.External {
		key = a.Move.ToKey()
	}
	_, err := tx.Exec(`INSERT INTO exceptions
		(id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
		ON CONFLICT (id) DO NOTHING`,
		env.ID.String(), string(ExceptionCompensation), a.Reason, env.CausationID.String(),
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

// touchKey records the highest RecordedAt seen for a key across every movement
// that has ever touched it, regardless of the order those movements are applied
// in: the update only ever advances the stored value, never lowers it, so
// folding the same set of movements in a different order produces the same
// final value. That is what refreshNegative needs, and a plain "stamp the
// current envelope's RecordedAt" does not have: a node applies its own commands
// the instant they are issued but only learns of a central compensation for an
// earlier one later, on the next sync, so which movement is "last" for a key
// depends on arrival order, not just on the log's content. Using the running
// max instead means the negative-balance exception's recorded_at always comes
// out the same, live or replayed — which is what the determinism check in
// internal/integration/determinism_test.go relies on.
func touchKey(tx *sql.Tx, k domain.StockKey, env domain.Envelope) (string, error) {
	var recordedAt string
	err := tx.QueryRow(`INSERT INTO key_touched (sku, location, lot_id, recorded_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (sku, location, lot_id) DO UPDATE SET recorded_at =
			CASE WHEN excluded.recorded_at > key_touched.recorded_at
				THEN excluded.recorded_at ELSE key_touched.recorded_at END
		RETURNING recorded_at`,
		k.SKU, string(k.Location), k.LotID, env.RecordedAt.UTC().Format(time.RFC3339Nano)).Scan(&recordedAt)
	if err != nil {
		return "", fmt.Errorf("touch key %+v: %w", k, err)
	}
	return recordedAt, nil
}

func refreshNegative(tx *sql.Tx, env domain.Envelope, k domain.StockKey) error {
	var qty sql.NullFloat64
	err := tx.QueryRow(`SELECT qty FROM stock_on_hand WHERE sku = ? AND location = ? AND lot_id = ?`,
		k.SKU, string(k.Location), k.LotID).Scan(&qty)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read balance for negative check at %+v: %w", k, err)
	}
	recordedAt, err := touchKey(tx, k, env)
	if err != nil {
		return err
	}
	if qty.Float64 < 0 {
		if _, err := tx.Exec(`INSERT INTO exceptions
			(id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved)
			VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, 0)
			ON CONFLICT (id) DO UPDATE SET qty = excluded.qty, recorded_at = excluded.recorded_at, resolved = 0`,
			negativeID(k), string(ExceptionNegativeBalance), "negative_after_compensation",
			k.SKU, string(k.Location), k.LotID, qty.Float64, recordedAt); err != nil {
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
