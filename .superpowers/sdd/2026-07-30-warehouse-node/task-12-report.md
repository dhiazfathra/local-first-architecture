# Task 12 Report: node service — the seam over log, state and projections

## What was built

`warehouse-node/internal/node/service.go` (package `node`), exactly per the brief:

- `Command = func(*domain.State) ([]domain.Event, error)` — lets any `domain.Do*`
  function run through `Execute` without a per-command wrapper.
- `Open(path, id, now) (*Service, error)` — opens the eventlog, opens the
  projection set on top of it, and rebuilds `domain.State` by replaying the whole
  log (state has no durable applied-marker of its own, unlike the projections).
- `Execute(cmd Command) ([]domain.Envelope, error)` — validates against `state`
  first; on a `RuleError` nothing is appended. On success, events are sealed via
  `log.Emit`, then folded into projections and state via `apply`.
- `Ingest(envs []domain.Envelope) (int, error)` — folds in envelopes from
  elsewhere unconditionally (no validation path); returns how many were new.
- `RegisterLocation`, `StockOnHand`, `Reservations`, `Exceptions`, `Transfers`,
  `Close`, `NodeID`, `Log` — thin pass-throughs, `RegisterLocation` being the one
  local (non-replicated) command since locations are node-owned.
- `apply` folds into projections first (idempotent via their durable applied
  marker), then always rebuilds state wholesale from the log — this ordering is
  what rules out double-applying a non-idempotent stock movement after a partial
  failure and retry (rationale documented in the doc comment on `apply`).
- Everything mutating is serialized under one `sync.Mutex`, matching the brief's
  "one operator per node, consistency over throughput" requirement.

Implementation matches the brief's Step 3 verbatim; no deviation was needed.

## Tests

`warehouse-node/internal/node/service_test.go`, brief's Step 1 test file plus
additional fault-injection tests added to close remaining coverage gaps:

- The six required tests from the brief (execute-appends-and-projects,
  reject-nothing-appended, ingest-idempotent-compensations,
  concurrent-execute-serializes, state-survives-reopen, queries-and-open-errors).
- `TestOpenFailsWhenProjectionVersionUnreadable` — corrupts `projection_version`
  so `projection.Open` fails inside `node.Open`.
- `TestOpenFailsWhenLogCannotBeReplayedIntoState` — corrupts an event's `payload`
  BLOB so `rebuildState`'s `fresh.Apply` fails on reopen.
- `TestRegisterLocationRejectsUnknownType` — exercises the invalid-`LocationType`
  branch directly.
- `TestApplyPropagatesAnIsAppliedFailure` — drops `projection_applied` so
  `apply`'s `IsApplied` call fails.
- `TestApplyPropagatesASetApplyFailure` — drops `stock_on_hand` so `apply`'s
  `set.Apply` call fails.

All fault injection follows the exact pattern already established in
`internal/projection/*_test.go` and `internal/eventlog/log_test.go` (corrupt a
column or drop a table via the exposed `*sql.DB` handle).

## Coverage: 98.3% — one parked branch (documented in-source)

One branch remains unreachable without a fault-injection seam disproportionate to
its value: in `rebuildState`, the `s.log.ReadAll()` error path. By the time
`rebuildState` runs inside `apply`, the same call has already run `Emit`/`Ingest`
(which itself requires the `events` table to be writable) and successfully folded
the same envelopes into the projections — there is no way to make `ReadAll` fail
on that data without an earlier step in the same call failing first. This matches
the same category of parked branch documented in the Task 10 and Task 11 reports
(`rows.Err()`, `Begin()` faults with no seam). Marked in-source with a `// Not
covered:` comment on the `rebuildState` error return.

All other branches — including every error path in `Open`, `Execute`, `Ingest`,
`RegisterLocation`, and `apply` — are covered by fault injection or direct
argument testing.

## Verification

```
go test ./... -cover
  internal/domain      100.0%
  internal/eventlog     95.9% (pre-existing, untouched by this task)
  internal/node          98.3%
  internal/projection    97.3% (pre-existing, untouched by this task)

golangci-lint run
  0 issues.
```

## Commit

`feat(node): add the service seam over log, state and projections`

## Fix round (2026-08-01) — reviewer finding

Reviewer showed the "unreachable" claim for `rebuildState`'s `ReadAll` error
branch was wrong: `rebuildState` replays the *entire* log, not just the rows
just written, so a pre-existing committed row that gets corrupted later (e.g.
`UPDATE events SET recorded_at = 'not-a-time' WHERE seq = 1` after a prior
successful write) makes a subsequent, unrelated `Execute`/`Ingest` call fail
at `apply → rebuildState → ReadAll` — same corrupt-column pattern already
used by `TestOpenFailsWhenLogCannotBeReplayedIntoState`, just reached via
`Execute` instead of `Open`.

- Added `TestExecuteFailsWhenRebuildStateCannotReadAnAlreadyCommittedRow`:
  registers a location successfully, corrupts that committed row's
  `recorded_at` column via the exposed `*sql.DB`, then calls `Execute` again
  and asserts it returns the `ReadAll` error.
- Removed the `// Not covered:` comment on `rebuildState`'s `ReadAll` error
  return — the branch is now covered, not parked.

### Verification

```
go test ./... -cover
  internal/node          100.0%   (was 98.3%)

golangci-lint run
  0 issues.
```
