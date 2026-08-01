// Package central stores every node's records in Postgres and answers global
// questions. It is NOT a different kind of participant: it speaks the same sync
// protocol as a node and satisfies the same sync.Log contract.
package central

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// pgSchema mirrors the SQLite schema, including the composite primary key that
// makes duplicate delivery free.
const pgSchema = `
CREATE TABLE IF NOT EXISTS records (
  node_id     TEXT   NOT NULL,
  seq         BIGINT NOT NULL,
  hlc_wall    BIGINT NOT NULL,
  hlc_logical BIGINT NOT NULL,
  type        TEXT   NOT NULL,
  payload     BYTEA  NOT NULL,
  PRIMARY KEY (node_id, seq)
);
CREATE TABLE IF NOT EXISTS cursors (peer TEXT PRIMARY KEY, last_seq BIGINT NOT NULL);
`

// PGStore is central's log.
type PGStore struct {
	pool   *pgxpool.Pool
	nodeID string
}

// OpenPG connects to dsn and ensures the schema exists.
func OpenPG(ctx context.Context, dsn, nodeID string) (*PGStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("central: pool: %w", err)
	}
	if _, err := pool.Exec(ctx, pgSchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("central: schema: %w", err)
	}
	return &PGStore{pool: pool, nodeID: nodeID}, nil
}

// Close releases the pool.
func (s *PGStore) Close() { s.pool.Close() }

// NodeID returns central's own id. Central authors no records of its own, but
// the protocol is symmetric, so it still has an identity.
func (s *PGStore) NodeID() string { return s.nodeID }

// TruncateForTest empties the tables. Exported so the test package (which is
// external, central_test) can isolate cases; it is never called by binaries.
func (s *PGStore) TruncateForTest(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE records, cursors`)
	return err
}

// Merge stores records from a peer and returns how many were new.
func (s *PGStore) Merge(ctx context.Context, recs []eventlog.Record, p eventlog.Projector) (int, error) {
	if len(recs) == 0 {
		return 0, nil
	}
	inserted := 0
	// Each record gets its own transaction: insert, project and commit that
	// one record before moving to the next. A later record's failure then
	// leaves every earlier record both durably stored and reflected in the
	// projector, never diverging — a single batch-wide tx could commit() a
	// record's in-memory effect and then have a later record's error roll
	// every record's DB write back together.
	for _, r := range recs {
		n, err := s.mergeOne(ctx, r, p)
		if err != nil {
			return inserted, err
		}
		inserted += n
	}
	return inserted, nil
}

// mergeOne stores and, if new, projects a single record inside its own
// transaction, returning 1 if it was newly inserted or 0 if it was a
// duplicate.
func (s *PGStore) mergeOne(ctx context.Context, r eventlog.Record, p eventlog.Projector) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("central: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		`INSERT INTO records (node_id, seq, hlc_wall, hlc_logical, type, payload)
		 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (node_id, seq) DO NOTHING`,
		r.NodeID, r.Seq, r.Clock.Wall, r.Clock.Logical, r.Type, r.Payload)
	if err != nil {
		return 0, fmt.Errorf("central: insert %s/%d: %w", r.NodeID, r.Seq, err)
	}
	if tag.RowsAffected() == 0 {
		return 0, nil
	}
	var commit func()
	if p != nil {
		if commit, err = p.Project(r); err != nil {
			return 0, fmt.Errorf("central: project %s/%d: %w", r.NodeID, r.Seq, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("central: commit: %w", err)
	}
	if commit != nil {
		commit()
	}
	return 1, nil
}

// Since returns every record the caller lacks according to vv, in replay order.
func (s *PGStore) Since(ctx context.Context, vv eventlog.VersionVector) ([]eventlog.Record, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT node_id, seq, hlc_wall, hlc_logical, type, payload FROM records`)
	if err != nil {
		return nil, fmt.Errorf("central: since: %w", err)
	}
	defer rows.Close()

	var out []eventlog.Record
	for rows.Next() {
		var r eventlog.Record
		var logical int64
		if err := rows.Scan(&r.NodeID, &r.Seq, &r.Clock.Wall, &logical, &r.Type, &r.Payload); err != nil {
			return nil, fmt.Errorf("central: scan: %w", err)
		}
		r.Clock.Logical, r.Clock.NodeID = uint32(logical), r.NodeID
		if r.Seq > vv.Get(r.NodeID) {
			out = append(out, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("central: rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return out, nil
}

// Version returns the highest sequence held per author.
func (s *PGStore) Version(ctx context.Context) (eventlog.VersionVector, error) {
	rows, err := s.pool.Query(ctx, `SELECT node_id, MAX(seq) FROM records GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("central: version: %w", err)
	}
	defer rows.Close()

	vv := eventlog.VersionVector{}
	for rows.Next() {
		var node string
		var seq uint64
		if err := rows.Scan(&node, &seq); err != nil {
			return nil, fmt.Errorf("central: scan version: %w", err)
		}
		vv.Observe(node, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("central: rows: %w", err)
	}
	return vv, nil
}

// Cursor reports how far central has pushed to peer.
func (s *PGStore) Cursor(ctx context.Context, peer string) (uint64, error) {
	var seq uint64
	err := s.pool.QueryRow(ctx, `SELECT last_seq FROM cursors WHERE peer = $1`, peer).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("central: cursor %s: %w", peer, err)
	}
	return seq, nil
}

// SetCursor records how far central has pushed to peer.
func (s *PGStore) SetCursor(ctx context.Context, peer string, seq uint64) error {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO cursors (peer, last_seq) VALUES ($1,$2)
		 ON CONFLICT (peer) DO UPDATE SET last_seq = EXCLUDED.last_seq`, peer, seq); err != nil {
		return fmt.Errorf("central: set cursor %s: %w", peer, err)
	}
	return nil
}

// GlobalSum totals stock per SKU across every node.
//
// It is a SUM, never a merge: because each location belongs to exactly one node,
// no two nodes ever contribute a value for the same key, so adding per-node
// balances is exact rather than a reconciliation heuristic.
func (s *PGStore) GlobalSum(ctx context.Context) (map[string]int64, error) {
	recs, err := s.Since(ctx, nil)
	if err != nil {
		return nil, err
	}
	state, err := projection.FoldRecords[domain.State](domain.BalanceReducer{}, recs)
	if err != nil {
		return nil, err
	}
	sum := map[string]int64{}
	for k, qty := range state {
		sum[k.SKU] += qty
	}
	return sum, nil
}

// compile-time proof that central is just a peer.
var _ interface {
	NodeID() string
	Since(context.Context, eventlog.VersionVector) ([]eventlog.Record, error)
	Merge(context.Context, []eventlog.Record, eventlog.Projector) (int, error)
	Version(context.Context) (eventlog.VersionVector, error)
	Cursor(context.Context, string) (uint64, error)
	SetCursor(context.Context, string, uint64) error
} = (*PGStore)(nil)
