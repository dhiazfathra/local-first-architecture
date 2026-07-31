package eventlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	_ "modernc.org/sqlite" // pure-Go driver, no cgo

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// SQLiteLog is the per-node local log.
type SQLiteLog struct {
	dsn string
	db  *sql.DB
}

// OpenSQLite opens (creating if needed) a log at dsn and applies the schema.
// dsn may be ":memory:" for a throwaway log.
func OpenSQLite(dsn string) (*SQLiteLog, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", dsn, err)
	}
	// One writer at a time: SQLite serialises writes anyway, and a single
	// connection keeps ":memory:" from becoming several distinct databases.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema to %q: %w", dsn, err)
	}
	return &SQLiteLog{dsn: dsn, db: db}, nil
}

// DSN reports where this log is stored, so a crash fault can reopen it.
func (l *SQLiteLog) DSN() string { return l.dsn }

// Close releases the database handle.
func (l *SQLiteLog) Close() error {
	if err := l.db.Close(); err != nil {
		return fmt.Errorf("close sqlite %q: %w", l.dsn, err)
	}
	return nil
}

const insertEvent = `INSERT INTO events
  (node_id, seq, hlc_wall, hlc_logical, sku, kind, payload)
  VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT (node_id, seq) DO NOTHING`

// Append is idempotent on (NodeID, Seq).
func (l *SQLiteLog) Append(ctx context.Context, e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	// Event's payload fields (int64/*MetaSet/*bool) always marshal cleanly;
	// no input shape here can make json.Marshal fail.
	payload, _ := e.MarshalPayload()
	if _, err := l.db.ExecContext(ctx, insertEvent,
		string(e.ID.NodeID), int64(e.ID.Seq), e.HLC.Wall, int64(e.HLC.Logical),
		e.SKU, int(e.Kind), payload); err != nil {
		return fmt.Errorf("append %v: %w", e.ID, err)
	}
	return nil
}

// AppendLocal allocates the next Seq for the node mint stamps and inserts in
// the same transaction, so no crash can leave a sequence gap.
func (l *SQLiteLog) AppendLocal(ctx context.Context, mint func(Seq) Event) (Event, error) {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, fmt.Errorf("begin append-local tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	probe := mint(0)
	var next int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE node_id = ?`,
		string(probe.ID.NodeID)).Scan(&next); err != nil {
		return Event{}, fmt.Errorf("allocate seq for %q: %w", probe.ID.NodeID, err)
	}

	e := mint(Seq(next))
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	// See Append: this payload shape can never fail to marshal.
	payload, _ := e.MarshalPayload()
	if _, err := tx.ExecContext(ctx, insertEvent,
		string(e.ID.NodeID), int64(e.ID.Seq), e.HLC.Wall, int64(e.HLC.Logical),
		e.SKU, int(e.Kind), payload); err != nil {
		return Event{}, fmt.Errorf("insert %v: %w", e.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return Event{}, fmt.Errorf("commit %v: %w", e.ID, err)
	}
	return e, nil
}

// Since streams events not covered by vv, ordered by HLC. Ordering is a
// readability convenience: crdt.Apply is order-independent by construction.
func (l *SQLiteLog) Since(ctx context.Context, vv VersionVector) iter.Seq2[Event, error] {
	return l.stream(ctx, vv, selectEvents+` ORDER BY hlc_wall, hlc_logical, node_id`)
}

// EventsForSKU streams the events for one SKU not covered by after. This is
// the projection read path.
func (l *SQLiteLog) EventsForSKU(ctx context.Context, sku string, after VersionVector) iter.Seq2[Event, error] {
	return l.stream(ctx, after, selectEvents+` WHERE sku = ? ORDER BY hlc_wall, hlc_logical, node_id`, sku)
}

// stream runs query and yields every decoded event not covered by skip.
// Filtering in Go rather than SQL keeps one code path for both queries; the
// harness never grows a log large enough for that to matter.
func (l *SQLiteLog) stream(ctx context.Context, skip VersionVector, query string, args ...any) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		rows, err := l.db.QueryContext(ctx, query, args...)
		if err != nil {
			yield(Event{}, fmt.Errorf("query events: %w", err))
			return
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			e, err := scanEvent(rows)
			if err != nil {
				yield(Event{}, err)
				return
			}
			if skip.Contains(e.ID) {
				continue
			}
			if !yield(e, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(Event{}, fmt.Errorf("iterate events: %w", err))
		}
	}
}

// scanner is the subset of *sql.Row / *sql.Rows scanEvent needs, so the same
// decode path serves both backends.
type scanner interface{ Scan(dest ...any) error }

func scanEvent(s scanner) (Event, error) {
	var (
		e       Event
		node    string
		seq     int64
		logical int64
		kind    int
		payload []byte
	)
	if err := s.Scan(&node, &seq, &e.HLC.Wall, &logical, &e.SKU, &kind, &payload); err != nil {
		return Event{}, fmt.Errorf("scan event row: %w", err)
	}
	e.ID = EventID{NodeID: clock.NodeID(node), Seq: Seq(seq)}
	e.HLC.Logical = uint32(logical)
	e.HLC.NodeID = e.ID.NodeID
	e.Kind = Kind(kind)
	if err := e.UnmarshalPayload(payload); err != nil {
		return Event{}, err
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}

// VersionVector reports the highest Seq held per node.
func (l *SQLiteLog) VersionVector(ctx context.Context) (VersionVector, error) {
	rows, err := l.db.QueryContext(ctx, versionVectorQuery)
	if err != nil {
		return nil, fmt.Errorf("query version vector: %w", err)
	}
	defer func() { _ = rows.Close() }()
	vv := VersionVector{}
	for rows.Next() {
		var (
			node     string
			frontier int64
		)
		if err := rows.Scan(&node, &frontier); err != nil {
			return nil, fmt.Errorf("scan version vector row: %w", err)
		}
		vv[clock.NodeID(node)] = Seq(frontier)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate version vector: %w", err)
	}
	return vv, nil
}

// Cursor returns the highest Seq of peer's OWN events we last saw it ack.
// This is a diagnostic value for regression detection only (see
// sync.warnOnRegression) -- it is never consulted to decide what a session
// exchanges. That decision always uses a freshly computed, full
// VersionVector on both sides, so a single scalar here is sufficient even
// though a log can hold events relayed from many origins.
func (l *SQLiteLog) Cursor(ctx context.Context, peer clock.NodeID) (Seq, error) {
	var last int64
	err := l.db.QueryRowContext(ctx,
		`SELECT last_seq FROM sync_cursors WHERE peer_node_id = ?`, string(peer)).Scan(&last)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read cursor for %q: %w", peer, err)
	}
	return Seq(last), nil
}

// SetCursor records progress against peer. It never moves a cursor backwards:
// a peer that reports a regressed version vector must not shrink our record.
func (l *SQLiteLog) SetCursor(ctx context.Context, peer clock.NodeID, last Seq) error {
	if _, err := l.db.ExecContext(ctx,
		`INSERT INTO sync_cursors (peer_node_id, last_seq) VALUES (?, ?)
		 ON CONFLICT (peer_node_id) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)`,
		string(peer), int64(last)); err != nil {
		return fmt.Errorf("set cursor for %q: %w", peer, err)
	}
	return nil
}

// LoadSnapshot returns the stored state for sku, or ErrNoSnapshot.
func (l *SQLiteLog) LoadSnapshot(ctx context.Context, sku string) ([]byte, VersionVector, error) {
	var state, covers []byte
	err := l.db.QueryRowContext(ctx,
		`SELECT state, covers FROM snapshots WHERE sku = ?`, sku).Scan(&state, &covers)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil, fmt.Errorf("%w for %q", ErrNoSnapshot, sku)
	case err != nil:
		return nil, nil, fmt.Errorf("load snapshot %q: %w", sku, err)
	}
	vv := VersionVector{}
	if err := json.Unmarshal(covers, &vv); err != nil {
		return nil, nil, fmt.Errorf("decode snapshot covers for %q: %w", sku, err)
	}
	return state, vv, nil
}

// SaveSnapshot replaces the snapshot for sku.
func (l *SQLiteLog) SaveSnapshot(ctx context.Context, sku string, state []byte, covers VersionVector) error {
	// covers is a VersionVector (map[clock.NodeID]Seq); no value of that type
	// can fail to marshal.
	blob, _ := json.Marshal(covers)
	if _, err := l.db.ExecContext(ctx,
		`INSERT INTO snapshots (sku, state, covers) VALUES (?, ?, ?)
		 ON CONFLICT (sku) DO UPDATE SET state = excluded.state, covers = excluded.covers`,
		sku, state, blob); err != nil {
		return fmt.Errorf("save snapshot %q: %w", sku, err)
	}
	return nil
}
