package eventlog

import (
	"context"
	"errors"
	"iter"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// ErrNoSnapshot means no snapshot exists for a SKU yet; the caller folds from
// the start of the log.
var ErrNoSnapshot = errors.New("no snapshot")

// Log is a node's append-only source of truth. Two implementations exist and
// share this interface so central (Postgres) is a merge participant running the
// same code path as a node (SQLite), never an authority.
type Log interface {
	// Append stores a remote or replayed event. It is idempotent on
	// (NodeID, Seq): re-appending an event already held is a no-op. This is
	// the whole duplicate-delivery defense.
	Append(ctx context.Context, e Event) error

	// AppendLocal allocates the next local Seq and inserts the event mint
	// produces in a single transaction, so a crash can never leave a gap.
	// Implementations may call mint more than once (e.g. once to read the
	// node id before the real Seq is known); mint must be side-effect free
	// and return an equivalent event for the same Seq on every call.
	AppendLocal(ctx context.Context, mint func(Seq) Event) (Event, error)

	// Since streams every held event not covered by vv, in HLC order.
	Since(ctx context.Context, vv VersionVector) iter.Seq2[Event, error]

	// VersionVector reports, per node, the highest Seq N such that every
	// event 1..N from that node is held -- a gap-free contiguous prefix, NOT
	// a raw MAX(seq). This matters under reordered delivery: if a node's
	// event 2 is stored before its event 1 (the harness's Reorder fault
	// permutes whole Events batches, which can interleave more than one
	// origin, so this is reachable even though a single batch's own events
	// are never reordered relative to each other), a raw MAX(seq) would
	// report that node's contribution as "covered through 2" while event 1
	// was never actually received -- Contains(id1) would then lie, a peer
	// computing what to send us would believe we already have it, and it
	// would never be sent again. Computing the contiguous prefix instead
	// means a gap simply doesn't advance the vector past it; the gap-filling
	// event, whenever it eventually arrives (Reorder delays, it does not
	// drop), closes it and the vector advances then.
	VersionVector(ctx context.Context) (VersionVector, error)

	// LoadSnapshot returns the serialized state for sku and the version
	// vector it folds in, or ErrNoSnapshot.
	LoadSnapshot(ctx context.Context, sku string) ([]byte, VersionVector, error)

	// SaveSnapshot replaces the snapshot for sku.
	SaveSnapshot(ctx context.Context, sku string, state []byte, covers VersionVector) error

	// Cursor returns the highest Seq of peer's events we have applied, 0 if
	// we have never synced with peer. Persisting it makes sync resumable.
	Cursor(ctx context.Context, peer clock.NodeID) (Seq, error)

	// SetCursor records progress against peer.
	SetCursor(ctx context.Context, peer clock.NodeID, last Seq) error

	// Compact deletes events dominated by upTo and covered by a local
	// snapshot. Never call it with a vector a peer has not acked.
	Compact(ctx context.Context, upTo VersionVector) error

	Close() error
}

// schema is applied verbatim by both backends. Kept in one place so the two
// implementations cannot drift.
const schema = `
CREATE TABLE IF NOT EXISTS events (
  node_id     TEXT    NOT NULL,
  seq         INTEGER NOT NULL,
  hlc_wall    INTEGER NOT NULL,
  hlc_logical INTEGER NOT NULL,
  sku         TEXT    NOT NULL,
  kind        INTEGER NOT NULL,
  payload     BLOB    NOT NULL,
  PRIMARY KEY (node_id, seq)
);
CREATE INDEX IF NOT EXISTS events_by_hlc ON events (hlc_wall, hlc_logical, node_id);
CREATE INDEX IF NOT EXISTS events_by_sku ON events (sku);

CREATE TABLE IF NOT EXISTS snapshots (
  sku        TEXT    NOT NULL PRIMARY KEY,
  state      BLOB    NOT NULL,
  covers     BLOB    NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_cursors (
  peer_node_id TEXT NOT NULL PRIMARY KEY,
  last_seq     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS seq_watermark (
  node_id TEXT    NOT NULL PRIMARY KEY,
  seq     INTEGER NOT NULL
);
`

const selectEvents = `SELECT node_id, seq, hlc_wall, hlc_logical, sku, kind, payload FROM events`

// versionVectorQuery computes, per node_id, the highest seq in the
// gap-free run starting at 1 -- NOT a raw MAX(seq). "seq - ROW_NUMBER()"
// is constant within a contiguous run of seqs and only equals 0 for the run
// that starts at seq 1; GROUP BY that expression and keep the grp-0 group's
// max. A node with a gap right after seq 1 (or missing seq 1 entirely) is
// correctly reported with a lower frontier, or omitted if it has no seq 1
// yet -- both ROW_NUMBER() OVER and this style of "gaps and islands" query
// work identically in SQLite (3.25+, already required for iter.Seq2) and
// Postgres, so this text is shared verbatim by both backends.
//
// The run does not start at seq 1 once compaction has run: seq_watermark
// records the highest seq ever deleted for a node, and those events really
// happened -- compaction only means "a snapshot folds this in", never "this
// event stopped existing". So the watermark is both a floor on the reported
// frontier and the offset the contiguous run resumes from, and surviving rows
// at or below it are ignored (they are already implied as held).
const versionVectorQuery = `
SELECT node_id, MAX(frontier) AS frontier
FROM (
  SELECT node_id, seq AS frontier FROM seq_watermark
  UNION ALL
  SELECT node_id, MAX(seq) AS frontier
  FROM (
    SELECT e.node_id AS node_id, e.seq AS seq,
           e.seq - ROW_NUMBER() OVER (PARTITION BY e.node_id ORDER BY e.seq)
                 - COALESCE(w.seq, 0) AS grp
    FROM events e
    LEFT JOIN seq_watermark w ON w.node_id = e.node_id
    WHERE e.seq > COALESCE(w.seq, 0)
  ) contiguous
  WHERE grp = 0
  GROUP BY node_id
) merged
GROUP BY node_id`
