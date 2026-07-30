# eventlog-lab — CRDT Event Log Laboratory

**Date:** 2026-07-30
**Location:** `eventlog-lab/` (own `go.mod`, module `github.com/dhiazfathra/local-first-architecture/eventlog-lab`)
**Purpose:** Learning vehicle. Prove convergence of a CRDT-based local-first event log under adversarial conditions. The test harness is the product.

## Goal

Build a local-first inventory node whose source of truth is a local append-only event log, where concurrent edits across nodes merge **mathematically** without central arbitration. Central store is a merge participant, not an authority.

Success criterion: a property-based harness that partitions nodes, skews clocks, duplicates and reorders deliveries, and crashes nodes mid-append, and still asserts:

1. **Convergence** — all nodes that have exchanged the same event set compute identical state.
2. **No lost event** — every event durably acknowledged to a caller appears in every replica's log after sync.
3. **Order independence** — replaying a node's merged log in any causally-valid order yields identical state.

## Non-Goals

Authentication, multi-tenancy, UI, HTTP API, garbage collection of tombstones beyond compaction, real ERP domain rules, performance tuning. Explicitly not a library for reuse — that is `localfirst-go`.

## Domain (deliberately toy)

One aggregate: `StockItem`, keyed by `SKU` (string).

State per item:

| Field | Type | CRDT |
|---|---|---|
| `Quantity` | int64 | PN-counter over node IDs |
| `Name` | string | LWW-register |
| `ReorderPoint` | int64 | LWW-register |
| `Deleted` | bool | LWW-register (tombstone; add-wins never applies — last writer decides) |

Three event types:

```go
type Event struct {
    ID       EventID   // {NodeID, Seq} — globally unique, no UUID needed
    HLC      HLC       // hybrid logical clock at emit time
    SKU      string
    Kind     Kind      // KindQuantityDelta | KindMetaSet | KindDeleteSet
    Delta    int64     // KindQuantityDelta only; sign carries increment/decrement
    Meta     *MetaSet  // KindMetaSet only
    DeletedTo *bool    // KindDeleteSet only
}
```

`KindQuantityDelta` carries a signed delta, not an absolute value — this is what makes quantity commutative.

## Architecture

Five packages, each independently testable.

### `clock/` — hybrid logical clocks

```go
type HLC struct {
    Wall    int64  // unix millis
    Logical uint32 // tiebreak counter
    NodeID  NodeID // final tiebreak, total order
}

func (c *Clock) Now() HLC                   // monotonic, never regresses
func (c *Clock) Observe(remote HLC) HLC      // merge remote clock, advance local
func (a HLC) Before(b HLC) bool              // total order: Wall, Logical, NodeID
```

Injected wall-clock source so tests can skew it. `Observe` is what keeps LWW sane across nodes with drifting clocks: receiving a remote event with a future timestamp bumps the local clock, so a subsequent local write always wins over what it causally follows.

`Now()` must be monotonic even if the injected wall clock jumps backwards — clamp to last-issued and bump `Logical`.

### `eventlog/` — append-only local log

SQLite (`modernc.org/sqlite`, no cgo).

```sql
CREATE TABLE events (
  node_id     TEXT    NOT NULL,
  seq         INTEGER NOT NULL,
  hlc_wall    INTEGER NOT NULL,
  hlc_logical INTEGER NOT NULL,
  sku         TEXT    NOT NULL,
  kind        INTEGER NOT NULL,
  payload     BLOB    NOT NULL,   -- JSON, kind-specific fields
  PRIMARY KEY (node_id, seq)
);
CREATE INDEX events_by_hlc ON events (hlc_wall, hlc_logical, node_id);
CREATE INDEX events_by_sku ON events (sku);

CREATE TABLE snapshots (
  sku        TEXT    NOT NULL PRIMARY KEY,
  state      BLOB    NOT NULL,   -- serialized CRDT state incl. per-node counters
  covers     BLOB    NOT NULL    -- version vector this snapshot folds in
);

CREATE TABLE sync_cursors (
  peer_node_id TEXT NOT NULL PRIMARY KEY,
  last_seq     INTEGER NOT NULL  -- highest seq of theirs we have applied
);
```

Interface:

```go
type Log interface {
    Append(ctx context.Context, e Event) error            // idempotent on (NodeID, Seq)
    Since(ctx context.Context, vv VersionVector) (iter.Seq2[Event, error])
    VersionVector(ctx context.Context) (VersionVector, error)
    Compact(ctx context.Context, upTo VersionVector) error
}
```

`Append` is idempotent by primary key — this is the entire duplicate-delivery defense. No dedup table, no bloom filter.

`Seq` for locally-originated events is allocated inside the same transaction as the insert, so a crash between allocation and insert cannot leave a gap.

### `crdt/` — merge semantics

```go
type ItemState struct {
    Pos    map[NodeID]int64 // PN-counter positive half
    Neg    map[NodeID]int64 // PN-counter negative half
    Name         LWW[string]
    ReorderPoint LWW[int64]
    Deleted      LWW[bool]
}

func (s *ItemState) Apply(e Event)   // must be commutative, associative, idempotent
func (s ItemState) Quantity() int64  // sum(Pos) - sum(Neg)
```

`Apply` for `KindQuantityDelta` adds `|Delta|` into `Pos[e.ID.NodeID]` or `Neg[...]` — but a naive `+=` is **not idempotent** under duplicate delivery. Resolution: the PN-counter half is stored per-node as a running total, and `Apply` is only ever driven by the log's projection over a deduplicated event set. Idempotence is provided by `Append`'s primary key, not by `Apply`. This must be stated in code comments and asserted by a test that applies the same event twice via the log path and once via the direct path, and shows why only the log path is safe.

`LWW[T]` holds `{Value T; At HLC}`; `Set` accepts the new value only if `existing.At.Before(new.At)`. Total order on HLC means ties are impossible.

### `sync/` — bidirectional gRPC replication

Proto:

```proto
service Sync {
  rpc Replicate(stream ClientFrame) returns (stream ServerFrame);
}

message ClientFrame {
  oneof body {
    Hello  hello  = 1;  // my node id + my version vector
    Events events = 2;  // batch of my events the peer lacks
    Ack    ack    = 3;  // version vector after applying peer's batch
  }
}

message ServerFrame {
  oneof body {
    Welcome welcome = 1; // central's version vector
    Events  events  = 2;
    Ack     ack     = 3;
  }
}
```

Protocol per session: exchange version vectors, each side streams what the other lacks, each side `Append`s and acks. No arbitration frames — central cannot reject, only merge. Resumable: cursors persist, so a dropped stream resumes from the last ack rather than restarting.

Central store is a second `eventlog.Log` backed by Postgres with the same schema, plus a projection of merged `ItemState` per SKU for reporting. Central runs the identical `crdt.Apply` code — no second implementation.

### `node/` — wiring

Each node: local log + clock + projection cache + sync client. Exposes an in-process API (`Receive`, `Pick`, `SetMeta`, `Delete`, `Get`) that the CLI and the harness both drive. No HTTP.

## Snapshots and compaction

Projection is `fold(Apply, events for SKU)`. Snapshot after N events per SKU, recording the version vector it covers. Read path: load snapshot, apply only events not covered. `Compact` deletes events dominated by every peer's acked version vector **and** covered by a local snapshot — never events a peer has not yet seen.

Compaction correctness is a harness property: compact aggressively on one node, then have a stale peer sync, and assert convergence still holds.

## Test harness (`internal/jepsenlite/`)

A deterministic simulator, not real networking. Nodes run in-process against in-memory SQLite; the `sync` layer talks over an injectable transport the harness controls.

Faults, each independently togglable and composable:

| Fault | Mechanism |
|---|---|
| Partition | transport drops frames between a node pair for a window |
| Asymmetric partition | drops one direction only |
| Clock skew | per-node wall clock offset, including backwards jumps |
| Duplicate delivery | transport re-sends a random prior frame |
| Reorder | transport buffers and permutes frames within a window |
| Crash mid-append | log wrapper aborts the transaction, then reopens the DB |
| Slow peer | delays acks past the next batch |

Harness shape: generate a random op schedule (`Receive`/`Pick`/`SetMeta`/`Delete` across nodes and SKUs) plus a random fault schedule, run to quiescence with all faults healed and all peers synced, then check the three properties. Seeded, so a failure prints a reproducing seed.

Additional targeted tests, not left to random search:

- `Apply` commutativity: for a fixed event set, every permutation yields the same `ItemState` (exhaustive for small sets, sampled above that).
- HLC monotonicity under backwards wall-clock jumps.
- Idempotence: replay a full log twice, state unchanged.
- Concurrent decrement below zero: two nodes each pick 8 of 10 units. Quantity converges to −6 on every replica. **This is correct CRDT behavior and the spec accepts it** — negative stock is a reportable anomaly at central, not a rejected write. Documenting this limitation honestly is the point; `warehouse-node` is where rejection lives.

100% coverage target per the repo standard, with the harness counted as tests, not as covered code.

## CLI (`cmd/lab/`)

```
lab node --id A --db a.db --central localhost:9000
lab op   --id A receive SKU-1 10
lab op   --id A pick    SKU-1 3
lab state --id A SKU-1
lab sim  --seed 42 --nodes 3 --ops 500 --faults partition,skew,dup
```

`lab sim` runs the harness from the command line and prints a convergence report.

## Error handling

- Local `Append` failure is returned to the caller and the op is not acknowledged — the "no lost event" property only covers acknowledged events.
- Sync failures retry with jittered backoff; a node stays fully usable offline indefinitely.
- Malformed remote event (unknown kind, unparseable payload): reject that frame, log loudly, continue the session. Never persist an event the local `crdt` package cannot apply, or convergence breaks silently.
- Version vector regression from a peer (claims to have less than it acked before): log and re-send from the lower point. Harmless given idempotent append.

## Milestones

1. `clock/` + property tests
2. `eventlog/` on SQLite + idempotence and crash tests
3. `crdt/` + commutativity tests
4. `node/` wiring + CLI ops
5. `sync/` over gRPC + central Postgres merge
6. `internal/jepsenlite/` harness + the three properties
7. Snapshots and compaction, harness extended to cover stale-peer compaction
