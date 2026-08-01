// Package eventlog is the node's source of truth: an append-only SQLite event log
// with idempotent ingestion, persisted sync cursors and deterministic read order.
package eventlog

// schema is applied on every Open. Every statement is idempotent so opening an
// existing database is a no-op.
//
// events is keyed (node_id, seq): that pair is the event's global identity, so
// re-ingesting an event the log already holds conflicts on the primary key and is
// silently ignored. That is what makes sync retries and overlapping batches safe.
//
// events_order indexes the HLC triple, which is the total order reads use. Physical
// arrival order is irrelevant, so a replay is deterministic.
const schema = `
CREATE TABLE IF NOT EXISTS events (
    node_id        TEXT    NOT NULL,
    seq            INTEGER NOT NULL,
    aggregate_id   TEXT    NOT NULL,
    type           TEXT    NOT NULL,
    hlc_wall       INTEGER NOT NULL,
    hlc_counter    INTEGER NOT NULL,
    hlc_node       TEXT    NOT NULL,
    recorded_at    TEXT    NOT NULL,
    causation_node TEXT,
    causation_seq  INTEGER,
    payload        BLOB    NOT NULL,
    PRIMARY KEY (node_id, seq),
    CHECK (node_id <> ''),
    CHECK (seq > 0)
);

CREATE INDEX IF NOT EXISTS events_order ON events (hlc_wall, hlc_counter, hlc_node, node_id, seq);

CREATE TABLE IF NOT EXISTS cursors (
    name  TEXT    PRIMARY KEY,
    value INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);
`

// Cursor names. Pushed is the highest sequence of this node's own events that
// central has acknowledged; Pulled is the highest count of events accepted from
// central. Persisting both is what makes the sync stream resumable after a week
// offline rather than restarting from zero.
const (
	CursorPushed = "pushed_to_central"
	CursorPulled = "pulled_from_central"
)
