# Task 9 report: internal/eventlog

## What was built

- `warehouse-node/internal/eventlog/schema.go` — idempotent DDL for `events`
  (keyed `(node_id, seq)`, indexed on `(hlc_wall, hlc_counter, hlc_node, node_id, seq)`
  for total-order reads), `cursors`, and `projection_version`. Cursor name
  constants `CursorPushed` / `CursorPulled`.
- `warehouse-node/internal/eventlog/log.go` — `Log` type wrapping
  `*sql.DB` (modernc.org/sqlite, pure Go, no cgo) with:
  - `Open(path, nodeID, now) (*Log, error)` — applies schema, recovers the
    in-memory sequence counter and HLC clock from stored rows so a restart
    never reuses a sequence number or emits a clock reading below one
    already on disk.
  - `Emit(events, causation) ([]domain.Envelope, error)` — assigns seq +
    HLC per event via `domain.Tick`/`domain.NewEnvelope`, writes the whole
    batch in one transaction so a mid-batch failure (e.g. an unencodable
    payload) leaves the log and the in-memory counters untouched.
  - `Ingest(envs) (int, error)` — idempotent append via
    `ON CONFLICT (node_id, seq) DO NOTHING`; merges every remote HLC via
    `domain.Merge` so local events causally sort after anything ingested;
    advances the local sequence counter if the ingested batch includes a
    resend of this node's own events.
  - `ReadAll()`, `ReadOwnAfter(seq, limit)`, `HighestSeq(node)`,
    `VersionVector()`, `Cursor`/`SetCursor`, `ProjectionVersion`/
    `SetProjectionVersion`, and `DB()` (exposes the handle for the future
    projection package to share the same file/transaction).
- `warehouse-node/internal/eventlog/log_test.go` — the brief's full test
  suite plus four additional tests to close coverage gaps the brief's own
  two examples didn't reach:
  - `TestReadAllFailsOnCorruptHLCColumn` — a non-numeric `hlc_wall` fails
    the scan loudly instead of silently zeroing.
  - `TestVersionVectorFailsOnCorruptSeqColumn` — same for `VersionVector`'s
    scan.
  - `TestIngestOwnEventsAdvancesLocalSequence` — exercises the branch where
    an ingested envelope belongs to this node and must push the sequence
    counter forward.
  - `TestOpenFailsOnACorruptDatabaseFile` — a pre-existing non-SQLite file
    at the target path fails schema application rather than silently
    proceeding.
- `warehouse-node/go.mod` / `go.sum` — added `modernc.org/sqlite` and its
  transitive pure-Go dependencies (`modernc.org/libc`, `modernc.org/memory`,
  `modernc.org/mathutil`, `golang.org/x/sys`, etc.). No cgo anywhere in the
  new dependency tree.

`internal/domain` was not modified; `eventlog` only imports its exported
`Event`, `Envelope`, `EventID`, `HLC`, `NodeID`, `Tick`, `Merge`,
`NewEnvelope`.

## Test results

```
go test ./... -cover
ok   .../internal/domain     100.0% of statements
ok   .../internal/eventlog    93.8% of statements
golangci-lint run   -> 0 issues
```

All tests pass, including the reopen/recovery test, the atomic-emit
partial-failure test, the idempotent/overlapping-ingest tests, and the
replay-determinism test (folding two logs that received the same remote
events in reverse arrival order into identical `domain.State.Stock` maps).

## Coverage gap (93.8%, not 100%)

The uncovered lines are all "a query/exec that already succeeded once in
this call now fails" branches with no fault-injection seam in this
package:

- `Open`: `sql.Open` returning an error itself, and `recover()` failing
  *after* schema application already succeeded (its `HighestSeq` call, and
  the non-`ErrNoRows` branch of the clock-recovery `Scan`).
- `Ingest`: the second `countEvents()` call failing when the first one
  (moments earlier, same transacontrolled connection) succeeded.
- `insert`: `tx.Prepare` and `tx.Commit` failing independently of `tx.Begin`
  (which *is* covered, via `TestOperationsFailAfterClose`).
- `query` / `VersionVector`: `rows.Err()` returning non-nil after a
  successful `rows.Next()` loop.

Reaching these would require either mocking `database/sql` behind an
interface (a real abstraction, not currently justified by any other
caller) or driver-level fault injection — both disproportionate to what
this task needs. The four extra tests added here do close every gap that
was reachable through legitimate data corruption or resource-lifecycle
scenarios (corrupt columns, a corrupt file, a closed DB, an own-node
resend). Flagging this honestly rather than adding a mock DB layer just to
hit a number.

## Commit

`d956a67` — `feat(eventlog): add append-only SQLite event log`

## Fix round (2026-08-01)

Reviewer found two of the three "unreachable without a mock" gaps were
actually reachable with the same corrupt-column technique already used
elsewhere in this file, and asked for both to be closed:

- `recover()`'s non-`ErrNoRows` `Scan` branch (log.go ~line 61): added
  `TestRecoverFailsOnCorruptClockColumn`. It opens a log, emits one event,
  corrupts `hlc_wall` to `'not-a-number'` via `db.Exec`, closes the log, then
  reopens at the same path — `recover`'s clock-scan fails exactly as
  claimed impossible.
- `insert`'s `tx.Prepare` branch (log.go ~line 154), independent of
  `tx.Begin`: added `TestInsertFailsWhenPrepareFailsIndependentlyOfBegin`.
  It drops the `events` table via `l.DB().Exec("DROP TABLE events")` before
  calling `Emit` — `Begin` succeeds, `Prepare` fails with "no such table:
  events".

Both tests added to `warehouse-node/internal/eventlog/log_test.go`.

### Coverage before/after

```
go test ./... -cover
before: internal/eventlog  93.8% of statements
after:  internal/eventlog  95.9% of statements
```

### Test + lint output

```
$ go test ./... -cover
ok   .../internal/domain     (cached)  coverage: 100.0% of statements
ok   .../internal/eventlog   0.586s    coverage: 95.9% of statements

$ golangci-lint run ./...
0 issues.
```

### Remaining gaps (95.9%, not 100%) — still out of scope

The three gaps the reviewer explicitly told us not to chase remain, and are
still out of reach without an API/seam change:

- `Ingest`'s second `countEvents()` call failing independently after the
  first one succeeded moments earlier on the same connection — there is no
  way to make one `SELECT count(*)` fail and a near-identical one 2 lines
  later succeed without mocking `database/sql`.
- `tx.Commit` failing independently of `tx.Begin`/`tx.Prepare` succeeding —
  same problem: no fault-injection seam exists to fail only `Commit` on an
  otherwise-healthy transaction.
- `rows.Err()` returning non-nil after a successful `Next()` loop in `query`
  — this needs `QueryContext` plus a context cancelled mid-iteration (or a
  driver that injects a late error), which is a seam this package does not
  have today; adding one solely to hit this line would be a speculative
  abstraction with no other caller.

Closing these for real would mean introducing a `database/sql`-compatible
mock/interface seam — a real design change disproportionate to this task's
scope, per the reviewer's own instruction to leave them.
