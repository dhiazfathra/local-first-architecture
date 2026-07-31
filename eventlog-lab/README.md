# eventlog-lab

A CRDT event-log laboratory. A local-first inventory node whose source of truth
is a local append-only event log, where concurrent edits across nodes merge
mathematically with no central arbitration. **The test harness is the product.**

This is a learning vehicle, not a reusable library.

## What it proves

A property-based harness partitions nodes, skews clocks, duplicates and reorders
deliveries, and crashes nodes mid-append, then asserts:

1. **Convergence** — nodes that exchanged the same event set compute identical state.
2. **No lost event** — every event durably acknowledged to a caller is reflected
   in every replica's projected state after sync, before compaction is ever
   allowed to discard its row. Compaction may later delete the row once every
   peer has acked past it and a snapshot already folds it in; that does not
   lose the event's effect.
3. **Order independence** — replaying a merged log in any causally-valid order
   yields identical state.

## Layout

| Package | Role |
|---|---|
| `clock/` | Hybrid logical clocks: monotonic `Now`, causal `Observe`, total order. |
| `eventlog/` | Append-only log on SQLite (per node) and Postgres (central). Snapshots, compaction. |
| `crdt/` | Merge semantics: PN-counter quantity, LWW metadata. One implementation, shared by every replica. |
| `sync/` | Bidirectional gRPC replication over an injectable transport. |
| `node/` | Wiring and the in-process op API. |
| `internal/jepsenlite/` | The deterministic fault simulator. |
| `cmd/lab/` | CLI. |

## Running

```bash
go test ./... -cover          # unit, property, and harness tests
```

### CLI

```bash
# Serve replication and sync with central every 5s.
lab node --id A --db a.db --central localhost:9000 --listen :9001

# Local ops. These never touch the network -- a node is fully usable offline.
lab op --id A --db a.db receive SKU-1 10
lab op --id A --db a.db pick    SKU-1 3

# Merged state of one SKU.
lab state --id A --db a.db SKU-1

# The harness, from the command line. Prints a convergence report; exits
# non-zero with a reproducing seed if any property is violated.
lab sim --seed 42 --nodes 3 --ops 500 --faults partition,skew,dup
```

Faults: `partition`, `asym`, `skew`, `dup`, `reorder`, `crash`, `slow`, `all`.

Central-store tests need Postgres:

```bash
docker run --rm -d -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:16
export EVENTLOG_LAB_PG_DSN='postgres://postgres:pg@127.0.0.1:5432/postgres?sslmode=disable'
```

They carry a `//go:build integration` tag, so they are not part of the default
`go test ./...` gate above (nothing skips; the tests are absent from that
build entirely). Run them explicitly:

```bash
go test -tags=integration ./eventlog/ ./crdt/ -run 'Postgres|Anomal' -v
```

## Known and accepted limitation: negative stock

Two nodes, partitioned, each pick 8 units of an item holding 10. Both writes are
valid locally. After sync, every replica converges on a quantity of **−6**.

This is correct CRDT behavior and this project accepts it. A commutative counter
cannot enforce a global invariant like "quantity >= 0" without exactly the
coordination that local-first architecture exists to avoid. Negative stock is a
**reportable anomaly** at central (`projections.quantity < 0`), never a rejected
write — central is a merge participant, not an authority.

Rejection semantics belong in a system that accepts the coordination cost. That
is not this project.

## Non-goals

Authentication, multi-tenancy, UI, HTTP API, tombstone garbage collection beyond
`Compact`, real ERP domain rules, performance tuning, reusable-library packaging.
