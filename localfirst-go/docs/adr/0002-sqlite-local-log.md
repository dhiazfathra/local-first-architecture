# 0002 — SQLite as the local log, keyed (node_id, seq)

- **Status:** accepted
- **Date:** 2026-07-30

## Context

Each node needs a durable, ordered, append-only record of the facts it has
authored plus every fact it has learned from peers. It must survive process
restart and power loss, be readable by a curious engineer with no tooling, and
require nothing to be installed or running alongside the node — because a node
that needs a server to accept a write is not local-first.

## Decision

One SQLite file per node, through `modernc.org/sqlite` — a pure-Go driver, so
the whole repo builds with `CGO_ENABLED=0` and a reader can `go build` it on any
machine with no toolchain archaeology.

Two tables, and that is the entire storage layer:

```sql
CREATE TABLE records (
  node_id     TEXT    NOT NULL,
  seq         INTEGER NOT NULL,
  hlc_wall    INTEGER NOT NULL,
  hlc_logical INTEGER NOT NULL,
  type        TEXT    NOT NULL,
  payload     BLOB    NOT NULL,
  PRIMARY KEY (node_id, seq)
);
CREATE TABLE cursors (peer TEXT PRIMARY KEY, last_seq INTEGER NOT NULL);
```

**The composite primary key `(node_id, seq)` is the highest-leverage idea in
this repo.** A record's identity is "the Nth thing node X ever said", which is
decided by its author and is stable forever. Combined with `INSERT OR IGNORE`
in `eventlog.Store.Merge`, receiving a record you already have costs one index
probe and changes nothing.

That single line is what buys the whole sync protocol: replication can be
at-least-once, batches can be re-sent after a mid-stream disconnect, a peer can
rewind its version vector and ask again, and none of it needs deduplication
logic, transaction ids, or an exactly-once delivery guarantee that no network
can provide anyway.

Appending is `validate → append → project` inside **one** SQLite transaction:
`eventlog.Store.Append` calls `Validator.Check`, inserts, then calls
`Projector.Project`, which returns a `commit func()` applied only after the
transaction commits. A rejected command leaves no trace, and in-memory derived
state can never disagree with the log on disk.

Reads are `Since(VersionVector)` and nothing else. There is no query language,
no index beyond the primary key, and no way to ask the log a domain question —
that is `projection`'s job.

## Consequences

Good: zero operational surface, a single file to copy or inspect, crash-safe
transactions from a library everyone already trusts, and duplicate delivery made
free rather than made careful.

Bad, and accepted:

- One writer process per file. A node is that process; do not point two at one
  file.
- The log grows without bound. There is no compaction and no snapshotting, so
  replay cost grows linearly with history. See
  [limitations](../limitations.md).
- `Since` scans; with no secondary indexes, "give me everything after these
  marks" is a table scan filtered by `(node_id, seq)`. Fine for a reference
  architecture, not for a million records.

## Alternatives rejected

- **An embedded key-value store (Pebble, BoltDB).** Faster, and it would cost us
  the transaction that spans validate/append/project, plus the ability to open
  the log with `sqlite3` and just look.
- **A cgo SQLite driver.** Faster still, and it would break `CGO_ENABLED=0`
  builds and cross-compilation, for a repo whose deliverable is readability.
- **Append-only file with a hand-rolled framing format.** Educational for
  exactly one afternoon, then a source of corruption bugs.
