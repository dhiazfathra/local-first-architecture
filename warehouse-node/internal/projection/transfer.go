package projection

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// TransferRow is the node's view of one inter-node transfer: how much left, how much
// has arrived, and what central made of it. Dispatched and Received are totals in
// base units across every line.
type TransferRow struct {
	ID           string
	FromNode     domain.NodeID
	ToNode       domain.NodeID
	Dispatched   float64
	Received     float64
	Status       domain.TransferStatus
	DispatchedAt time.Time
}

// applyTransfer folds the two halves of a transfer, plus central's rejection, into
// the transfers table. A destination node sees the dispatch first because central
// forwards it, but the upsert tolerates either order.
func applyTransfer(tx *sql.Tx, env domain.Envelope, payload any) error {
	switch p := payload.(type) {
	case domain.TransferDispatched:
		if _, err := tx.Exec(`INSERT INTO transfers
			(id, from_node, to_node, dispatched, received, status, dispatched_at)
			VALUES (?, ?, ?, ?, 0, ?, ?)
			ON CONFLICT (id) DO UPDATE SET from_node = excluded.from_node,
				to_node = excluded.to_node, dispatched = excluded.dispatched,
				dispatched_at = excluded.dispatched_at,
				status = CASE WHEN transfers.received >= excluded.dispatched
				              THEN ? ELSE transfers.status END`,
			p.TransferID, string(p.FromNode), string(p.ToNode), totalQty(p.Lines),
			string(domain.TransferInFlight), env.RecordedAt.UTC().Format(time.RFC3339Nano),
			string(domain.TransferComplete)); err != nil {
			return fmt.Errorf("project transfer dispatch %s: %w", p.TransferID, err)
		}
	case domain.TransferReceived:
		if _, err := tx.Exec(`INSERT INTO transfers
			(id, from_node, to_node, dispatched, received, status, dispatched_at)
			VALUES (?, '', '', 0, ?, ?, '')
			ON CONFLICT (id) DO UPDATE SET received = transfers.received + excluded.received,
				status = CASE WHEN transfers.received + excluded.received >= transfers.dispatched
				                   AND transfers.dispatched > 0
				              THEN ? ELSE transfers.status END`,
			p.TransferID, totalQty(p.Lines), string(domain.TransferInFlight),
			string(domain.TransferComplete)); err != nil {
			return fmt.Errorf("project transfer receipt %s: %w", p.TransferID, err)
		}
	case domain.StockAdjusted:
		if p.Reason != domain.ReasonTransferRejected {
			return nil
		}
		// Central emits the transfer_rejected compensation against the transfer as
		// its aggregate, which is how the node learns the transfer failed.
		if _, err := tx.Exec(`UPDATE transfers SET status = ? WHERE id = ?`,
			string(domain.TransferFailed), env.AggregateID); err != nil {
			return fmt.Errorf("mark transfer %s failed: %w", env.AggregateID, err)
		}
	}
	return nil
}

func totalQty(lines []domain.Movement) float64 {
	var total float64
	for _, m := range lines {
		total += m.Qty
	}
	return total
}

// Transfers returns every transfer this node participates in, in ID order.
func (s *Set) Transfers() ([]TransferRow, error) {
	rows, err := s.db.Query(`SELECT id, from_node, to_node, dispatched, received, status, dispatched_at
		FROM transfers ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query transfers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []TransferRow{}
	for rows.Next() {
		var r TransferRow
		var from, to, status, at string
		if err := rows.Scan(&r.ID, &from, &to, &r.Dispatched, &r.Received, &status, &at); err != nil {
			return nil, fmt.Errorf("scan transfer row: %w", err)
		}
		r.FromNode, r.ToNode = domain.NodeID(from), domain.NodeID(to)
		r.Status = domain.TransferStatus(status)
		if at != "" {
			if r.DispatchedAt, err = time.Parse(time.RFC3339Nano, at); err != nil {
				return nil, fmt.Errorf("parse dispatch timestamp %q: %w", at, err)
			}
		}
		out = append(out, r)
	}
	// Not covered: same as StockOnHand's and Reservations' rows.Err() — only a
	// driver-level failure mid-iteration reaches this, not table corruption. Parked,
	// as eventlog's own DB-fault branches were.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transfer rows: %w", err)
	}
	return out, nil
}
