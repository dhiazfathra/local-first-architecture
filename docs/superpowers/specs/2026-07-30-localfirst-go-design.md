# localfirst-go — Reference Architecture for Local-First Event Sourcing in Go

**Date:** 2026-07-30
**Location:** `localfirst-go/` (own `go.mod`, module `github.com/dhiazfathra/local-first-architecture/localfirst-go`)
**Purpose:** Readable reference architecture. A domain-agnostic local-first event log and sync engine, demonstrated with the thinnest possible inventory domain. Readability is the deliverable.

## Goal

Someone who has never seen local-first architecture should be able to read this repo in an hour and understand: how a local event log becomes a source of truth, how it reconciles with a central store, and where their own domain plugs in.

Every design choice optimizes for clarity over capability. Where a more powerful option exists, the spec names it and points at the sibling project that implements it.

## Non-Goals

CRDT merge semantics (see `eventlog-lab`), central arbitration and compensating events (see `warehouse-node`), auth, UI, performance work, production hardening. This repo does not try to be all three.

## Conflict model: per-node ownership

Each node exclusively owns its own locations. No two nodes ever write to the same `(SKU, Location)` key. Central computes global stock as the **sum of per-node balances** — a sum, never a merge.

This is not a simplification that hides a problem; it is a real architectural pattern. Partitioning writes by ownership removes conflict as a category rather than solving it. Most local-first systems that appear to need CRDTs actually need a clearer ownership boundary.

Stated limitation, documented in the README: if two nodes must write the same key, this design does not apply, and the reader should look at `eventlog-lab` (mathematical merge) or `warehouse-node` (arbitration).

Cross-node movement still exists — a transfer — and works without shared keys: source decrements its own balance, destination increments its own. Central sees both as ordinary per-node deltas. No in-transit accounting, no two-phase handshake; the goods are simply absent from the global sum between the two events, and that gap is documented as accepted.

## Domain (deliberately minimal)

One aggregate, `Stock`, keyed `(SKU, Location)`, where every `Location` belongs to exactly one node.

Three events:

```go
type Received struct { SKU, Location string; Qty int64 }
type Issued   struct { SKU, Location string; Qty int64 }
type Moved    struct { SKU, From, To string; Qty int64 }  // To may live on another node
```

One invariant, checked locally before append: balance never goes negative. A node owns its locations, so it can always decide this alone. That single fact is why this design is so much smaller than the other two.

## Architecture — the part that is the point

Six packages. The domain-agnostic ones know nothing about inventory.

```
localfirst-go/
  eventlog/     domain-agnostic append-only log        (no inventory imports)
  clock/        hybrid logical clock                    (no inventory imports)
  sync/         domain-agnostic gRPC replication        (no inventory imports)
  projection/   domain-agnostic replay driver           (no inventory imports)
  domain/       THE ONLY inventory-aware package
  transport/grpc/ node API for clients
  cmd/node/     node binary
  cmd/central/  central binary
  docs/         ADRs, diagrams, swap-the-domain guide
```

The boundary is enforced by an architecture test: `eventlog`, `clock`, `sync`, and `projection` must not import `domain`. If that test passes, the claim "the engine is domain-agnostic" is verified, not asserted.

### The plug-in seam

The engine moves opaque event bytes. The domain supplies interpretation:

```go
// package eventlog
type Record struct {
    NodeID  string
    Seq     uint64
    Clock   clock.HLC
    Type    string          // domain-defined
    Payload []byte          // domain-defined
}

// package projection — the entire extension point
type Reducer[S any] interface {
    Zero() S
    Apply(state S, r eventlog.Record) (S, error)
}

func Fold[S any](ctx context.Context, log eventlog.Reader, r Reducer[S]) (S, error)
```

A new domain implements one `Reducer` and one command-to-`Record` encoder. Nothing else changes. `docs/swapping-the-domain.md` walks through replacing inventory with a second toy domain end to end, as proof.

Command validation also plugs in, since the engine must not know what "negative stock" means:

```go
// package eventlog
type Validator interface {
    // Called inside the append transaction, against the current projected state.
    Check(r Record) error
}
```

Appending is `validate → append → project`, all in one SQLite transaction, so a rejected command leaves no trace and a projection can never disagree with the log.

### `eventlog/`

SQLite (`modernc.org/sqlite`, no cgo), one file per node.

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

The primary key on `(node_id, seq)` is the whole duplicate-delivery story — `INSERT OR IGNORE` and re-delivery is a no-op. Documented prominently, because it is the single highest-leverage idea in the repo.

Reads are `Since(VersionVector)`; there is no query language.

### `clock/`

Hybrid logical clock: wall millis, logical counter, node ID as final tiebreak. Monotonic across backwards wall-clock jumps. Used here only for **ordering and audit** — not for conflict resolution, since ownership means there is nothing to resolve. Documented as such, so a reader does not assume HLC implies LWW.

### `sync/`

gRPC bidirectional stream, resumable, symmetric:

```proto
service Sync { rpc Replicate(stream Frame) returns (stream Frame); }
message Frame {
  oneof body {
    Hello   hello   = 1;  // node id + version vector
    Batch   batch   = 2;  // records the peer lacks
    Ack     ack     = 3;  // version vector after apply
  }
}
```

One `Frame` type in both directions, because with per-node ownership central is a peer with a bigger disk, not a different protocol. That symmetry is a teaching point: it makes node-to-node sync a configuration change rather than a new code path, and `cmd/central` differs from `cmd/node` only in storage backend and having no local domain commands.

Central stores the same records in Postgres and projects per-node balances plus a global sum view.

### `transport/grpc/`

Node API: `Receive`, `Issue`, `Move`, `Balance`, `Balances`. Thin — decode request, call domain command, return. Kept separate from `domain` so the reader sees that the domain has no transport dependency.

## Documentation (a first-class deliverable, not an afterthought)

- `README.md` — what local-first means, the three-sentence version of this architecture, when to use it, when not to (pointing at the sibling projects)
- `docs/adr/0001-per-node-ownership.md` — why ownership beats merge when it applies
- `docs/adr/0002-sqlite-local-log.md` — why SQLite, why the composite primary key
- `docs/adr/0003-hlc-for-ordering-only.md` — why HLC without LWW
- `docs/adr/0004-symmetric-sync-protocol.md` — why central is just a peer
- `docs/adr/0005-domain-agnostic-engine.md` — the `Reducer`/`Validator` seam
- `docs/swapping-the-domain.md` — worked example replacing the domain
- `docs/architecture.md` — package diagram, event lifecycle diagram, sync sequence diagram (mermaid)
- `docs/limitations.md` — honest list: no concurrent same-key writes, no compaction, transfer gap in the global sum, no auth

Diagrams as mermaid in markdown — reviewable in a diff, no binary assets.

## `make demo`

Docker Compose: Postgres, one central, three nodes. A `demo` target that:

1. starts everything
2. receives stock at each node
3. cuts node 3 off the network (compose network disconnect)
4. runs 20 ops against the isolated node 3, showing them succeed
5. shows central's global sum missing node 3's changes
6. reconnects
7. shows convergence, and node 3's events now in the sum

Printed step by step with the actual numbers. This is the artifact that makes local-first click for a reader in 60 seconds.

## Testing

- `clock/`: monotonicity, backwards jumps, total ordering
- `eventlog/`: idempotent append, validator rejection leaves no record, projection-in-transaction consistency, replay determinism
- `projection/`: `Fold` over a fixed record set, generic over two different reducers to prove the seam
- `domain/`: negative-balance rejection, move across nodes, table-driven
- `sync/`: in-process transport, catch-up after offline window, resume after mid-stream disconnect, duplicate batch is a no-op
- **Architecture test:** parse imports of `eventlog`, `clock`, `sync`, `projection`; fail if any reaches `domain`
- **Demo test:** the `make demo` scenario as a Go integration test, so the documentation cannot rot

100% coverage per repo standard.

## Error handling

- Validator rejection: transaction rolled back, error returned naming the invariant. Nothing appended.
- Central unreachable: node fully operational, backoff retry, cursor unchanged.
- Unknown record type during projection: hard error, not skipped — a reference architecture must not teach silent divergence.
- Version vector regression from a peer: re-send from the lower point; idempotent append makes it safe.

## Milestones

1. `clock/` + tests
2. `eventlog/` + `Validator` seam + tests
3. `projection/` generic `Fold` + `Reducer` seam + two-reducer test
4. `domain/` inventory reducer, commands, invariant
5. `transport/grpc/` + `cmd/node`
6. `sync/` symmetric protocol + `cmd/central` on Postgres
7. Architecture test, ADRs, diagrams
8. `make demo` + demo integration test
9. `docs/swapping-the-domain.md` with its worked example
