// Package projection builds the node's read models from the event log. Projections
// are derived, never commanded: any of them can be thrown away and rebuilt by
// replaying the log.
package projection

import (
	"database/sql"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/eventlog"
)

// Version is the projection schema/logic version. Bump it whenever the way any
// projection is computed changes: Open then empties every projection table and
// replays the whole log, which is only safe because replay is deterministic.
const Version = 1

// projectionSchema creates the read-model tables. applied records, per originating
// node, the highest sequence already folded in, which makes Apply idempotent.
const projectionSchema = `
CREATE TABLE IF NOT EXISTS stock_on_hand (
    sku      TEXT NOT NULL,
    location TEXT NOT NULL,
    lot_id   TEXT NOT NULL,
    qty      REAL NOT NULL,
    PRIMARY KEY (sku, location, lot_id)
);

CREATE TABLE IF NOT EXISTS reservations (
    id       TEXT PRIMARY KEY,
    sku      TEXT NOT NULL,
    location TEXT NOT NULL,
    lot_id   TEXT NOT NULL,
    qty      REAL NOT NULL,
    status   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_applied (
    node_id TEXT    NOT NULL,
    seq     INTEGER NOT NULL,
    PRIMARY KEY (node_id, seq)
);
`

// projectionTables is every table Rebuild empties. Adding a projection means adding
// its table here.
var projectionTables = []string{"stock_on_hand", "reservations", "projection_applied"}

// Set is the collection of read models for one node, stored alongside its log.
type Set struct {
	log *eventlog.Log
	db  *sql.DB
}

// Open creates the projection tables, rebuilds them if the stored version differs
// from Version, and then folds in any log events not yet applied.
func Open(l *eventlog.Log) (*Set, error) {
	s := &Set{log: l, db: l.DB()}
	if _, err := s.db.Exec(projectionSchema); err != nil {
		return nil, fmt.Errorf("apply projection schema: %w", err)
	}
	stored, err := l.ProjectionVersion()
	if err != nil {
		return nil, err
	}
	if stored != Version {
		return s, s.Rebuild()
	}
	return s, s.CatchUp()
}

// Rebuild empties every projection table and replays the entire log in total HLC
// order, then records the current Version.
func (s *Set) Rebuild() error {
	for _, table := range projectionTables {
		if _, err := s.db.Exec(`DELETE FROM ` + table); err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}
	if err := s.CatchUp(); err != nil {
		return err
	}
	return s.log.SetProjectionVersion(Version)
}

// CatchUp folds in every log event this set has not already applied.
func (s *Set) CatchUp() error {
	envs, err := s.log.ReadAll()
	if err != nil {
		return err
	}
	for _, env := range envs {
		if err := s.Apply(env); err != nil {
			return err
		}
	}
	return nil
}

// Apply folds one event into every projection. It is idempotent: the exact
// (node, seq) pair already applied is recorded and re-checked, so re-delivery on a
// resumed sync stream cannot double-count stock. This is deliberately not "skip if
// seq <= the highest seen" — that would treat the highest sequence as a contiguous
// cursor and permanently lose any event that arrives out of order (seq 4 delivered
// after seq 5 would be dropped forever). Recording every applied seq individually
// means an out-of-order arrival is still folded in exactly once.
//
// An event that cannot be decoded is an error, never a skip: silently ignoring an
// event forks this node's state from the rest of the system.
func (s *Set) Apply(env domain.Envelope) error {
	already, err := s.isApplied(env.ID.NodeID, env.ID.Seq)
	if err != nil {
		return err
	}
	if already {
		return nil
	}
	payload, err := domain.DecodePayload(env)
	if err != nil {
		return fmt.Errorf("project %s: %w", env.ID, err)
	}

	// Not covered: db.Begin() only errors here on a closed/exhausted pool, and this
	// call site is preceded by isApplied's own query on the same *sql.DB, which
	// fails first and identically whenever the DB is unusable — there is no fault
	// that reaches Begin without already having failed isApplied. Reproducing it
	// would need a fault-injection seam disproportionate to this task (parked, as
	// eventlog's own DB-fault branches were).
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin projection tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := applyStock(tx, s.log.NodeID(), payload); err != nil {
		return err
	}
	if err := applyReservation(tx, payload); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO projection_applied (node_id, seq) VALUES (?, ?)
		ON CONFLICT (node_id, seq) DO NOTHING`, string(env.ID.NodeID), env.ID.Seq); err != nil {
		return fmt.Errorf("record applied %s: %w", env.ID, err)
	}
	// Not covered: SQLite has no deferred constraints to violate at commit time,
	// so provoking a commit-specific failure (as opposed to one of the statement
	// errors already covered above) needs a fault-injection seam disproportionate
	// to this task (parked, as eventlog's own DB-fault branches were).
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit projection tx: %w", err)
	}
	return nil
}

// IsApplied reports whether this exact event has already been folded into the
// projections. Callers that must recover from a failed application (Service.apply)
// use this as the durable source of truth for "does this event still need work",
// rather than a version-vector or highest-seq comparison that would mistake a
// not-yet-applied gap for done.
func (s *Set) IsApplied(id domain.EventID) (bool, error) { return s.isApplied(id.NodeID, id.Seq) }

// isApplied reports whether this exact (node, seq) event has already been folded
// into the projections, not merely whether a higher sequence for the node has been
// seen — a gappy delivery order must not cause a lower, not-yet-applied sequence to
// be mistaken for already-applied.
func (s *Set) isApplied(node domain.NodeID, seq uint64) (bool, error) {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM projection_applied WHERE node_id = ? AND seq = ?`,
		string(node), seq).Scan(&one)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("read applied for %s/%d: %w", node, seq, err)
	}
	return true, nil
}

// MovementsOf returns the stock movements a payload carries, and is the single place
// that knows which event types move stock. Everything else — receipt paperwork,
// reservations, count lines, item master — moves nothing.
func MovementsOf(payload any) []domain.Movement {
	switch p := payload.(type) {
	case domain.GoodsReceived:
		return []domain.Movement{p.Move}
	case domain.PutAway:
		return []domain.Movement{p.Move}
	case domain.Picked:
		return []domain.Movement{p.Move}
	case domain.StockAdjusted:
		return []domain.Movement{p.Move}
	case domain.TransferDispatched:
		return p.Lines
	case domain.TransferReceived:
		return p.Lines
	default:
		return nil
	}
}
