package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// pgSchema mirrors the SQLite schema column-for-column, translated to Postgres
// types, plus a projection table that exists only for reporting. Central is a
// merge participant, not an authority: nothing here can reject an event.
var pgSchema = `
CREATE TABLE IF NOT EXISTS events (
  node_id     TEXT     NOT NULL,
  seq         BIGINT   NOT NULL,
  hlc_wall    BIGINT   NOT NULL,
  hlc_logical BIGINT   NOT NULL,
  sku         TEXT     NOT NULL,
  kind        INTEGER  NOT NULL,
  payload     BYTEA    NOT NULL,
  PRIMARY KEY (node_id, seq)
);
CREATE INDEX IF NOT EXISTS events_by_hlc ON events (hlc_wall, hlc_logical, node_id);
CREATE INDEX IF NOT EXISTS events_by_sku ON events (sku);

CREATE TABLE IF NOT EXISTS snapshots (
  sku        TEXT   NOT NULL PRIMARY KEY,
  state      BYTEA  NOT NULL,
  covers     BYTEA  NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_cursors (
  peer_node_id TEXT   NOT NULL PRIMARY KEY,
  last_seq     BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS seq_watermark (
  node_id TEXT   NOT NULL PRIMARY KEY,
  seq     BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS projections (
  sku            TEXT   NOT NULL PRIMARY KEY,
  quantity       BIGINT NOT NULL,
  name           TEXT   NOT NULL,
  reorder_point  BIGINT NOT NULL,
  deleted        BOOLEAN NOT NULL
);
`

// PostgresLog is the central store. It implements exactly the same interface as
// the per-node SQLite log and runs the same crdt.Apply, so there is no second
// merge implementation anywhere in this project.
type PostgresLog struct {
	pool *pgxpool.Pool
}

// OpenPostgres connects to dsn and applies the schema.
func OpenPostgres(ctx context.Context, dsn string) (*PostgresLog, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, pgSchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply postgres schema: %w", err)
	}
	return &PostgresLog{pool: pool}, nil
}

// Close releases the pool.
func (l *PostgresLog) Close() error {
	l.pool.Close()
	return nil
}

const pgInsertEvent = `INSERT INTO events
  (node_id, seq, hlc_wall, hlc_logical, sku, kind, payload)
  VALUES ($1, $2, $3, $4, $5, $6, $7)
  ON CONFLICT (node_id, seq) DO NOTHING`

// Append is idempotent on (node_id, seq), exactly as in SQLite.
func (l *PostgresLog) Append(ctx context.Context, e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	payload, err := e.MarshalPayload()
	if err != nil {
		return err
	}
	if _, err := l.pool.Exec(ctx, pgInsertEvent,
		string(e.ID.NodeID), int64(e.ID.Seq), e.HLC.Wall, int64(e.HLC.Logical),
		e.SKU, int(e.Kind), payload); err != nil {
		return fmt.Errorf("append %v: %w", e.ID, err)
	}
	return nil
}

// AppendLocal allocates and inserts in one transaction, so no crash can leave a
// sequence gap.
//
// SQLite's single writer connection makes MAX(seq)+1 safe by accident; the
// Postgres connection pool does not serialize concurrent transactions the same
// way. Without a lock, two concurrent AppendLocal calls for the same node_id
// can both read the same MAX(seq), both compute the same next value, and both
// proceed to insert -- one insert wins, the other's ON CONFLICT DO NOTHING
// silently no-ops, and that caller believes its (distinct!) event was appended
// when nothing was written for it. pg_advisory_xact_lock keyed by node_id
// serializes exactly the callers that would collide, without taking a
// table-wide lock, and releases automatically on commit or rollback.
//
// The high-water mark is GREATEST(surviving rows, seq_watermark) -- see the
// SQLite AppendLocal comment: compaction deletes rows, so MAX(seq) alone
// re-issues seqs this node already emitted and silently loses the event.
func (l *PostgresLog) AppendLocal(ctx context.Context, mint func(Seq) Event) (Event, error) {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return Event{}, fmt.Errorf("begin append-local tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	probe := mint(0)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`,
		string(probe.ID.NodeID)); err != nil {
		return Event{}, fmt.Errorf("lock seq allocator for %q: %w", probe.ID.NodeID, err)
	}
	var next int64
	if err := tx.QueryRow(ctx,
		`SELECT GREATEST(
		   COALESCE((SELECT MAX(seq) FROM events WHERE node_id = $1), 0),
		   COALESCE((SELECT seq FROM seq_watermark WHERE node_id = $1), 0)
		 ) + 1`,
		string(probe.ID.NodeID)).Scan(&next); err != nil {
		return Event{}, fmt.Errorf("allocate seq for %q: %w", probe.ID.NodeID, err)
	}
	e := mint(Seq(next))
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	payload, err := e.MarshalPayload()
	if err != nil {
		return Event{}, err
	}
	if _, err := tx.Exec(ctx, pgInsertEvent,
		string(e.ID.NodeID), int64(e.ID.Seq), e.HLC.Wall, int64(e.HLC.Logical),
		e.SKU, int(e.Kind), payload); err != nil {
		return Event{}, fmt.Errorf("insert %v: %w", e.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Event{}, fmt.Errorf("commit %v: %w", e.ID, err)
	}
	return e, nil
}

// Since streams events not covered by vv, in HLC order.
func (l *PostgresLog) Since(ctx context.Context, vv VersionVector) iter.Seq2[Event, error] {
	return l.stream(ctx, vv, selectEvents+` ORDER BY hlc_wall, hlc_logical, node_id`)
}

// EventsForSKU streams one SKU's events not covered by after.
func (l *PostgresLog) EventsForSKU(ctx context.Context, sku string, after VersionVector) iter.Seq2[Event, error] {
	return l.stream(ctx, after,
		selectEvents+` WHERE sku = $1 ORDER BY hlc_wall, hlc_logical, node_id`, sku)
}

func (l *PostgresLog) stream(ctx context.Context, skip VersionVector, query string, args ...any) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		rows, err := l.pool.Query(ctx, query, args...)
		if err != nil {
			yield(Event{}, fmt.Errorf("query events: %w", err))
			return
		}
		defer rows.Close()
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

// CountForSKU reports how many events remain for sku.
func (l *PostgresLog) CountForSKU(ctx context.Context, sku string) (int, error) {
	var n int
	if err := l.pool.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE sku = $1`, sku).Scan(&n); err != nil {
		return 0, fmt.Errorf("count events for %q: %w", sku, err)
	}
	return n, nil
}

// VersionVector reports the highest seq held per node.
func (l *PostgresLog) VersionVector(ctx context.Context) (VersionVector, error) {
	rows, err := l.pool.Query(ctx, versionVectorQuery)
	if err != nil {
		return nil, fmt.Errorf("query version vector: %w", err)
	}
	defer rows.Close()
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

// Cursor returns the highest seq of peer's events we have applied.
func (l *PostgresLog) Cursor(ctx context.Context, peer clock.NodeID) (Seq, error) {
	var last int64
	err := l.pool.QueryRow(ctx,
		`SELECT last_seq FROM sync_cursors WHERE peer_node_id = $1`, string(peer)).Scan(&last)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read cursor for %q: %w", peer, err)
	}
	return Seq(last), nil
}

// SetCursor records progress against peer, never moving backwards.
func (l *PostgresLog) SetCursor(ctx context.Context, peer clock.NodeID, last Seq) error {
	if _, err := l.pool.Exec(ctx,
		`INSERT INTO sync_cursors (peer_node_id, last_seq) VALUES ($1, $2)
		 ON CONFLICT (peer_node_id) DO UPDATE
		 SET last_seq = GREATEST(sync_cursors.last_seq, excluded.last_seq)`,
		string(peer), int64(last)); err != nil {
		return fmt.Errorf("set cursor for %q: %w", peer, err)
	}
	return nil
}

// LoadSnapshot returns the stored state for sku, or ErrNoSnapshot.
func (l *PostgresLog) LoadSnapshot(ctx context.Context, sku string) ([]byte, VersionVector, error) {
	var state, covers []byte
	err := l.pool.QueryRow(ctx,
		`SELECT state, covers FROM snapshots WHERE sku = $1`, sku).Scan(&state, &covers)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
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
func (l *PostgresLog) SaveSnapshot(ctx context.Context, sku string, state []byte, covers VersionVector) error {
	blob, err := json.Marshal(covers)
	if err != nil {
		return fmt.Errorf("encode snapshot covers for %q: %w", sku, err)
	}
	if _, err := l.pool.Exec(ctx,
		`INSERT INTO snapshots (sku, state, covers) VALUES ($1, $2, $3)
		 ON CONFLICT (sku) DO UPDATE SET state = excluded.state, covers = excluded.covers`,
		sku, state, blob); err != nil {
		return fmt.Errorf("save snapshot %q: %w", sku, err)
	}
	return nil
}

// Compact applies the same two mandatory gates as SQLite: dominated by upTo AND
// folded into a local snapshot.
func (l *PostgresLog) Compact(ctx context.Context, upTo VersionVector) error {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin compact tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `SELECT sku, covers FROM snapshots`)
	if err != nil {
		return fmt.Errorf("query snapshots: %w", err)
	}
	covered := map[string]VersionVector{}
	for rows.Next() {
		var (
			sku  string
			blob []byte
		)
		if err := rows.Scan(&sku, &blob); err != nil {
			rows.Close()
			return fmt.Errorf("scan snapshot row: %w", err)
		}
		vv := VersionVector{}
		if err := json.Unmarshal(blob, &vv); err != nil {
			rows.Close()
			return fmt.Errorf("decode covers for %q: %w", sku, err)
		}
		covered[sku] = vv
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate snapshots: %w", err)
	}

	for sku, snapVV := range covered {
		for node, seq := range snapVV {
			peerSeq, ok := upTo[node]
			if !ok {
				continue
			}
			safeSeq := min(seq, peerSeq)
			// VersionVector reports each node's highest gap-free seq starting
			// at 1. Deleting this node's covered prefix while a later,
			// uncovered event from the same node survives (in any SKU -- seq
			// is a per-node counter shared across SKUs) would strand that
			// event: it would still be held, but the gap left behind makes
			// VersionVector stop reporting the node at all, which would make
			// us re-request data we already have, or make a peer believe we
			// lack data we hold. So skip this node's deletion entirely rather
			// than risk that; it will compact cleanly once nothing of its
			// remains beyond the safe point.
			var stranded bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM events WHERE node_id = $1 AND seq > $2)`,
				string(node), int64(safeSeq)).Scan(&stranded); err != nil {
				return fmt.Errorf("check stranding for %q/%q: %w", sku, node, err)
			}
			if stranded {
				continue
			}
			// Raise the durable high-water mark BEFORE dropping the rows, so
			// a crash mid-compaction can only over-report, never re-issue a
			// seq. seq is a per-node counter shared across SKUs, so this mark
			// is per-node too, not per (sku, node).
			if _, err := tx.Exec(ctx,
				`INSERT INTO seq_watermark (node_id, seq) VALUES ($1, $2)
				 ON CONFLICT (node_id) DO UPDATE
				 SET seq = GREATEST(seq_watermark.seq, excluded.seq)`,
				string(node), int64(safeSeq)); err != nil {
				return fmt.Errorf("raise watermark for %q: %w", node, err)
			}
			if _, err := tx.Exec(ctx,
				`DELETE FROM events WHERE sku = $1 AND node_id = $2 AND seq <= $3`,
				sku, string(node), int64(safeSeq)); err != nil {
				return fmt.Errorf("compact %q/%q: %w", sku, node, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit compact: %w", err)
	}
	return nil
}

// Anomaly is a reportable condition in the central projection. Negative stock
// is the canonical one: it is correct CRDT behavior, not a bug, and central
// reports it rather than rejecting the events that caused it.
type Anomaly struct {
	SKU      string
	Quantity int64
}

// UpsertProjection writes the merged state for sku into the reporting table.
// It takes scalars rather than a *crdt.ItemState so this package never needs
// to import crdt -- crdt already imports eventlog (for SQLLog and Event), and
// the reverse edge would be an import cycle. node.Node.Merge type-asserts its
// log against a projectionSink interface with exactly this signature and
// calls it after every accepted merge, so this satisfies that interface via
// structural typing with no direct dependency between the packages.
func (l *PostgresLog) UpsertProjection(ctx context.Context, sku string,
	quantity int64, name string, reorderPoint int64, deleted bool) error {
	if _, err := l.pool.Exec(ctx,
		`INSERT INTO projections (sku, quantity, name, reorder_point, deleted)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (sku) DO UPDATE SET quantity = excluded.quantity, name = excluded.name,
		   reorder_point = excluded.reorder_point, deleted = excluded.deleted`,
		sku, quantity, name, reorderPoint, deleted); err != nil {
		return fmt.Errorf("upsert projection %q: %w", sku, err)
	}
	return nil
}

// Anomalies lists SKUs whose merged quantity is negative.
func (l *PostgresLog) Anomalies(ctx context.Context) ([]Anomaly, error) {
	rows, err := l.pool.Query(ctx,
		`SELECT sku, quantity FROM projections WHERE quantity < 0 ORDER BY sku`)
	if err != nil {
		return nil, fmt.Errorf("query anomalies: %w", err)
	}
	defer rows.Close()
	var out []Anomaly
	for rows.Next() {
		var a Anomaly
		if err := rows.Scan(&a.SKU, &a.Quantity); err != nil {
			return nil, fmt.Errorf("scan anomaly row: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate anomalies: %w", err)
	}
	return out, nil
}
