package eventlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"

	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
)

// schema is the entire storage design.
//
// PRIMARY KEY (node_id, seq) is the duplicate-delivery story: every write path
// uses INSERT OR IGNORE, so re-delivering a record the log already holds is a
// no-op. That is what makes the sync protocol safe to retry and resume.
const schema = `
CREATE TABLE IF NOT EXISTS records (
  node_id     TEXT    NOT NULL,
  seq         INTEGER NOT NULL,
  hlc_wall    INTEGER NOT NULL,
  hlc_logical INTEGER NOT NULL,
  type        TEXT    NOT NULL,
  payload     BLOB    NOT NULL,
  PRIMARY KEY (node_id, seq)
);
CREATE TABLE IF NOT EXISTS cursors (peer TEXT PRIMARY KEY, last_seq INTEGER NOT NULL);
`

// Store is one node's log: a single SQLite file.
type Store struct {
	db     *sql.DB
	nodeID string

	mu      sync.Mutex // serialises append: one author, dense sequence.
	nextSeq uint64
}

// Open opens (creating if needed) the log at path for nodeID.
func Open(path, nodeID string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("eventlog: open %s: %w", path, err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("eventlog: schema: %w", err)
	}
	s := &Store{db: db, nodeID: nodeID}
	if err := db.QueryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM records WHERE node_id = ?`, nodeID,
	).Scan(&s.nextSeq); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("eventlog: read own sequence: %w", err)
	}
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// NodeID returns the id this store authors records under.
func (s *Store) NodeID() string { return s.nodeID }

// Append validates, stores and projects one locally authored record in a single
// transaction: validate -> insert -> project. Any failure rolls all three back,
// so a rejected command leaves no trace and the projection can never disagree
// with the log. v and p may be nil.
func (s *Store) Append(
	ctx context.Context, typ string, payload []byte, ts clock.HLC, v Validator, p Projector,
) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec := Record{NodeID: s.nodeID, Seq: s.nextSeq + 1, Clock: ts, Type: typ, Payload: payload}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, fmt.Errorf("eventlog: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once committed

	if v != nil {
		if err := v.Check(rec); err != nil {
			return Record{}, fmt.Errorf("eventlog: append rejected: %w", err)
		}
	}
	if err := insert(ctx, tx, rec); err != nil {
		return Record{}, err
	}
	var commit func()
	if p != nil {
		if commit, err = p.Project(rec); err != nil {
			return Record{}, fmt.Errorf("eventlog: project: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Record{}, fmt.Errorf("eventlog: commit: %w", err)
	}
	if commit != nil {
		commit()
	}
	s.nextSeq = rec.Seq
	return rec, nil
}

// Merge stores records authored elsewhere and returns how many were new.
// Duplicates are ignored and are not re-projected, which is what makes an
// at-least-once sync protocol correct. p may be nil — central passes nil and
// maintains no live projector at all; its global sum is a SQL query
// (PGStore.GlobalSum) computed by replaying stored records on demand.
func (s *Store) Merge(ctx context.Context, recs []Record, p Projector) (int, error) {
	if len(recs) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("eventlog: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	inserted, highestOwn := 0, s.nextSeq
	var commits []func()
	for _, r := range recs {
		res, err := exec(ctx, tx, r)
		if err != nil {
			return 0, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("eventlog: rows affected: %w", err)
		}
		if n == 0 { // already held: nothing to project
			continue
		}
		inserted++
		if r.NodeID == s.nodeID && r.Seq > highestOwn {
			highestOwn = r.Seq
		}
		if p != nil {
			commit, err := p.Project(r)
			if err != nil {
				return 0, fmt.Errorf("eventlog: project %s/%d: %w", r.NodeID, r.Seq, err)
			}
			commits = append(commits, commit)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("eventlog: commit: %w", err)
	}
	for _, c := range commits {
		c()
	}
	s.nextSeq = highestOwn
	return inserted, nil
}

func insert(ctx context.Context, tx *sql.Tx, r Record) error {
	_, err := exec(ctx, tx, r)
	return err
}

func exec(ctx context.Context, tx *sql.Tx, r Record) (sql.Result, error) {
	res, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO records (node_id, seq, hlc_wall, hlc_logical, type, payload)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		r.NodeID, r.Seq, r.Clock.Wall, r.Clock.Logical, r.Type, nonNil(r.Payload))
	if err != nil {
		return nil, fmt.Errorf("eventlog: insert %s/%d: %w", r.NodeID, r.Seq, err)
	}
	return res, nil
}

// nonNil keeps the NOT NULL payload column happy for events with no fields.
func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// Since returns every held record the caller lacks according to vv, in replay
// order (HLC, then author sequence). A nil vv means "everything".
//
// ponytail: scans the log and filters in Go — clear, and correct for a
// reference architecture. Push the filter into SQL per node if logs get big.
func (s *Store) Since(ctx context.Context, vv VersionVector) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT node_id, seq, hlc_wall, hlc_logical, type, payload FROM records`)
	if err != nil {
		return nil, fmt.Errorf("eventlog: since: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.NodeID, &r.Seq, &r.Clock.Wall, &r.Clock.Logical, &r.Type, &r.Payload); err != nil {
			return nil, fmt.Errorf("eventlog: scan: %w", err)
		}
		r.Clock.NodeID = r.NodeID
		if r.Seq > vv.Get(r.NodeID) {
			out = append(out, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return out, nil
}

// Version returns the highest Seq held per author — this node's answer to
// "what do you already have?".
func (s *Store) Version(ctx context.Context) (VersionVector, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT node_id, MAX(seq) FROM records GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("eventlog: version: %w", err)
	}
	defer func() { _ = rows.Close() }()

	vv := VersionVector{}
	for rows.Next() {
		var node string
		var seq uint64
		if err := rows.Scan(&node, &seq); err != nil {
			return nil, fmt.Errorf("eventlog: scan version: %w", err)
		}
		vv.Observe(node, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: rows: %w", err)
	}
	return vv, nil
}

// Cursor returns how far this node has pushed to peer, 0 if never.
func (s *Store) Cursor(ctx context.Context, peer string) (uint64, error) {
	var seq uint64
	err := s.db.QueryRowContext(ctx, `SELECT last_seq FROM cursors WHERE peer = ?`, peer).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("eventlog: cursor %s: %w", peer, err)
	}
	return seq, nil
}

// SetCursor records how far this node has pushed to peer.
func (s *Store) SetCursor(ctx context.Context, peer string, seq uint64) error {
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO cursors (peer, last_seq) VALUES (?, ?)
		 ON CONFLICT(peer) DO UPDATE SET last_seq = excluded.last_seq`, peer, seq); err != nil {
		return fmt.Errorf("eventlog: set cursor %s: %w", peer, err)
	}
	return nil
}
