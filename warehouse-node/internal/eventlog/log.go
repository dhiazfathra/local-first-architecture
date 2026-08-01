package eventlog

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"; no cgo

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// Log is one node's append-only event log.
type Log struct {
	db     *sql.DB
	nodeID domain.NodeID
	now    func() time.Time

	// mu guards seq and clock: Emit must assign a unique, gapless sequence and a
	// monotonic HLC even under concurrent gRPC handlers.
	mu    sync.Mutex
	seq   uint64
	clock domain.HLC
}

// Open opens or creates the log at path. now supplies wall time; it is injected so
// tests are deterministic and so no other package needs to reach for time.Now.
func Open(path string, nodeID domain.NodeID, now func() time.Time) (*Log, error) {
	dsn := path + "?" + strings.Join([]string{
		"_pragma=journal_mode(WAL)",
		"_pragma=busy_timeout(5000)",
		"_pragma=synchronous(FULL)",
	}, "&")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite at %s: %w", path, err)
	}
	// One writer at a time: Emit/Ingest already serialize under l.mu, and a pooled
	// sql.DB would otherwise let SQLite operations run concurrently on this file.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		return nil, errors.Join(fmt.Errorf("apply schema: %w", err), db.Close())
	}
	l := &Log{db: db, nodeID: nodeID, now: now}
	if err := l.recover(); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return l, nil
}

// recover restores the sequence counter and clock from storage so a restart never
// reuses a sequence number nor emits an HLC below one already written.
func (l *Log) recover() error {
	seq, err := l.HighestSeq(l.nodeID)
	if err != nil {
		return err
	}
	l.seq = seq
	row := l.db.QueryRow(`SELECT hlc_wall, hlc_counter FROM events ORDER BY hlc_wall DESC, hlc_counter DESC LIMIT 1`)
	var wall int64
	var counter uint32
	switch err := row.Scan(&wall, &counter); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("recover clock: %w", err)
	}
	l.clock = domain.HLC{Wall: wall, Counter: counter, Node: l.nodeID}
	return nil
}

// Close releases the database handle.
func (l *Log) Close() error { return l.db.Close() }

// NodeID is the identity of the node owning this log.
func (l *Log) NodeID() domain.NodeID { return l.nodeID }

// Emit seals locally produced events into envelopes and appends them atomically.
// Sequence numbers and HLC readings are assigned here, which is why the domain
// needs no clock. causation is non-nil only when this log belongs to central and
// the events compensate a rejected event.
//
// The whole batch is one transaction, so a failure or crash part-way through leaves
// the log exactly as it was and no sequence numbers are burnt.
func (l *Log) Emit(events []domain.Event, causation *domain.EventID) ([]domain.Envelope, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	nowMillis := l.now().UnixMilli()
	seq, clock := l.seq, l.clock
	envs := make([]domain.Envelope, 0, len(events))
	for _, e := range events {
		seq++
		clock = domain.Tick(clock, nowMillis, l.nodeID)
		env, err := domain.NewEnvelope(domain.EventID{NodeID: l.nodeID, Seq: seq}, clock,
			time.UnixMilli(nowMillis).UTC(), causation, e)
		if err != nil {
			return nil, err
		}
		envs = append(envs, env)
	}
	if _, err := l.insert(envs, false); err != nil {
		return nil, err
	}
	l.seq, l.clock = seq, clock
	return envs, nil
}

// Ingest appends envelopes produced elsewhere — central's compensations, item-master
// updates, and transfer events destined for this node. It returns how many were new.
// Re-ingesting an event already present is a no-op, so a resumed stream may safely
// resend an overlapping batch.
func (l *Log) Ingest(envs []domain.Envelope) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	nowMillis := l.now().UnixMilli()
	clock := l.clock
	for _, e := range envs {
		// Merging every remote reading keeps causality: anything this node emits
		// after seeing these events sorts after them.
		clock = domain.Merge(clock, e.HLC, nowMillis, l.nodeID)
	}
	added, err := l.insert(envs, true)
	if err != nil {
		return 0, err
	}
	l.clock = clock
	// If any of the ingested events came from this node (a resend of our own
	// events echoed back), keep the sequence counter ahead of them.
	seq, err := l.highestSeq(l.nodeID)
	if err != nil {
		return 0, err
	}
	if seq > l.seq {
		l.seq = seq
	}
	return added, nil
}

// insert writes envelopes in a single transaction, reporting how many rows were
// newly added. When ignoreConflicts is true, an envelope whose (node_id, seq) is
// already stored is skipped rather than erroring — Ingest tolerates and expects
// replayed events; Emit assigns a fresh local sequence and should never conflict,
// so a conflict there surfaces as an error instead of being silently discarded.
func (l *Log) insert(envs []domain.Envelope, ignoreConflicts bool) (int, error) {
	tx, err := l.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once committed

	query := `
		INSERT INTO events (node_id, seq, aggregate_id, type, hlc_wall, hlc_counter, hlc_node,
		                    recorded_at, causation_node, causation_seq, payload)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if ignoreConflicts {
		query += `
		ON CONFLICT (node_id, seq) DO NOTHING`
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, fmt.Errorf("prepare insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	var added int64
	for _, e := range envs {
		var causeNode any
		var causeSeq any
		if e.CausationID != nil {
			causeNode, causeSeq = string(e.CausationID.NodeID), e.CausationID.Seq
		}
		res, err := stmt.Exec(string(e.ID.NodeID), e.ID.Seq, e.AggregateID, e.Type,
			e.HLC.Wall, e.HLC.Counter, string(e.HLC.Node),
			e.RecordedAt.UTC().Format(time.RFC3339Nano), causeNode, causeSeq, []byte(e.Payload))
		if err != nil {
			return 0, fmt.Errorf("append %s: %w", e.ID, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("rows affected for %s: %w", e.ID, err)
		}
		added += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return int(added), nil
}

const selectColumns = `node_id, seq, aggregate_id, type, hlc_wall, hlc_counter, hlc_node,
	recorded_at, causation_node, causation_seq, payload`

// ReadAll returns every event in total HLC order, which is the order a projection
// must replay to be deterministic.
func (l *Log) ReadAll() ([]domain.Envelope, error) {
	return l.query(`SELECT ` + selectColumns + ` FROM events
		ORDER BY hlc_wall, hlc_counter, hlc_node, node_id, seq`)
}

// ReadOwnAfter returns up to limit of this node's own events with a sequence above
// after, in sequence order. This is what the sync client pushes upstream, and the
// limit is what keeps a week-long backlog to bounded chunks.
func (l *Log) ReadOwnAfter(seq uint64, limit int) ([]domain.Envelope, error) {
	return l.query(`SELECT `+selectColumns+` FROM events
		WHERE node_id = ? AND seq > ? ORDER BY seq LIMIT ?`, string(l.nodeID), seq, limit)
}

func (l *Log) query(q string, args ...any) ([]domain.Envelope, error) {
	rows, err := l.db.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []domain.Envelope
	for rows.Next() {
		var (
			e          domain.Envelope
			nodeID     string
			hlcNode    string
			recordedAt string
			causeNode  sql.NullString
			causeSeq   sql.NullInt64
			payload    []byte
		)
		if err := rows.Scan(&nodeID, &e.ID.Seq, &e.AggregateID, &e.Type, &e.HLC.Wall, &e.HLC.Counter,
			&hlcNode, &recordedAt, &causeNode, &causeSeq, &payload); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		e.ID.NodeID = domain.NodeID(nodeID)
		e.HLC.Node = domain.NodeID(hlcNode)
		if e.RecordedAt, err = time.Parse(time.RFC3339Nano, recordedAt); err != nil {
			return nil, fmt.Errorf("parse recorded_at of %s: %w", e.ID, err)
		}
		if causeNode.Valid {
			e.CausationID = &domain.EventID{NodeID: domain.NodeID(causeNode.String), Seq: uint64(causeSeq.Int64)}
		}
		e.Payload = payload
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return out, nil
}

// HighestSeq is the highest sequence number stored for node, or zero if none.
func (l *Log) HighestSeq(node domain.NodeID) (uint64, error) {
	return l.highestSeq(node)
}

func (l *Log) highestSeq(node domain.NodeID) (uint64, error) {
	var seq sql.NullInt64
	if err := l.db.QueryRow(`SELECT max(seq) FROM events WHERE node_id = ?`, string(node)).Scan(&seq); err != nil {
		return 0, fmt.Errorf("highest seq for %s: %w", node, err)
	}
	if !seq.Valid {
		return 0, nil
	}
	return uint64(seq.Int64), nil
}

// VersionVector reports the highest sequence held per originating node. It is what
// the Hello frame carries so central knows what this node already has.
func (l *Log) VersionVector() (map[domain.NodeID]uint64, error) {
	rows, err := l.db.Query(`SELECT node_id, max(seq) FROM events GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("version vector: %w", err)
	}
	defer func() { _ = rows.Close() }()

	vv := map[domain.NodeID]uint64{}
	for rows.Next() {
		var node string
		var seq uint64
		if err := rows.Scan(&node, &seq); err != nil {
			return nil, fmt.Errorf("scan version vector: %w", err)
		}
		vv[domain.NodeID(node)] = seq
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate version vector: %w", err)
	}
	return vv, nil
}

// Cursor reads a persisted sync cursor; an unset cursor reads as zero.
func (l *Log) Cursor(name string) (uint64, error) {
	var v sql.NullInt64
	err := l.db.QueryRow(`SELECT value FROM cursors WHERE name = ?`, name).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read cursor %s: %w", name, err)
	}
	return uint64(v.Int64), nil
}

// SetCursor persists a sync cursor.
func (l *Log) SetCursor(name string, value uint64) error {
	if _, err := l.db.Exec(`INSERT INTO cursors (name, value) VALUES (?, ?)
		ON CONFLICT (name) DO UPDATE SET value = excluded.value`, name, value); err != nil {
		return fmt.Errorf("write cursor %s: %w", name, err)
	}
	return nil
}

// ProjectionVersion is the projection schema version the stored projections were
// built with; zero means they have never been built.
func (l *Log) ProjectionVersion() (int, error) {
	var v sql.NullInt64
	err := l.db.QueryRow(`SELECT version FROM projection_version WHERE id = 1`).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read projection version: %w", err)
	}
	return int(v.Int64), nil
}

// SetProjectionVersion records the version the projections were last built with.
func (l *Log) SetProjectionVersion(v int) error {
	if _, err := l.db.Exec(`INSERT INTO projection_version (id, version) VALUES (1, ?)
		ON CONFLICT (id) DO UPDATE SET version = excluded.version`, v); err != nil {
		return fmt.Errorf("write projection version: %w", err)
	}
	return nil
}

// DB exposes the handle so the projection package can keep its tables in the same
// file and rebuild them in the same transaction as a cursor update.
func (l *Log) DB() *sql.DB { return l.db }
