# eventlog-lab — CRDT Event Log Laboratory

**Date:** 2026-07-30
**Location:** `eventlog-lab/` (own `go.mod`, module `github.com/dhiazfathra/local-first-architecture/eventlog-lab`)
**Purpose:** Learning vehicle. Prove convergence of a CRDT-based local-first event log under adversarial conditions. The test harness is the product.

## Goal

Build a local-first inventory node whose source of truth is a local append-only event log, where concurrent edits across nodes merge **mathematically** without central arbitration. Central store is a merge participant, not an authority.

Success criterion: a property-based harness that partitions nodes, skews clocks, duplicates and reorders deliveries, and crashes nodes mid-append, and still asserts:

1. **Convergence** — all nodes that have exchanged the same event set compute identical state.
2. **No lost event** — every event durably acknowledged to a caller is reflected in every replica's *projected state* after sync — i.e. it was applied at least once on every replica before any snapshot/compaction was allowed to discard it. This is a replay-equivalent-state guarantee, not a promise that the raw event row survives forever: `Compact` may delete an event's row once every peer has acked past it and a local snapshot already folds it in (see Snapshots and compaction). Losing the row after that point does not lose the event's effect.
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

`KindQuantityDelta` carries a signed delta, not an absolute value — this is what makes quantity commutative. `Delta` is `int64`; `Delta == math.MinInt64` is rejected by `Event.Validate` because `-Delta` (needed to store its magnitude in the PN-counter's `Neg` half) is not representable as a positive `int64`. Everything in `[math.MinInt64+1, math.MaxInt64]` is accepted at the event level; see `crdt/` below for the corresponding running-total overflow bound.

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
  covers     BLOB    NOT NULL    -- version vector restricted to nodes that have
                                 -- contributed an event for THIS sku (see below)
);

CREATE TABLE sync_cursors (
  peer_node_id TEXT    NOT NULL PRIMARY KEY,
  last_seq     INTEGER NOT NULL  -- diagnostic only, see below: peer's own
                                 -- highest seq we last saw it ack, NOT the
                                 -- mechanism that decides what to sync
);
```

`last_seq` is intentionally a single scalar, not a full `VersionVector`: it exists only to detect and log a peer claiming to have regressed (`peerVV[peer] < last_seq`), by comparing against that same peer's own contribution to its own last-reported vector — a single number is exactly sufficient for that, one node compared with itself. It is **not** what decides which events to exchange in a session: every session re-exchanges each side's complete, freshly-computed `VersionVector` in `Hello`/`Welcome` (see `sync/` below), so a session's correctness never depends on this stored cursor being complete or current, and a merged log holding events relayed from many origins is already handled correctly by the fresh, full vector exchanged every time.

**`VersionVector` reports a gap-free contiguous prefix per node, not a raw highest-seq.** The harness's `Reorder` fault permutes whole `Events` batches, and a single batch can carry events from more than one origin interleaved by HLC order, so two events from the same origin node can legitimately land in different batches that arrive out of order. If `VersionVector` reported a raw `MAX(seq)` per node, receiving that node's event 2 before its event 1 would report the node as "covered through 2" while event 1 was never actually received — any peer using `Contains`/`Dominates` against that vector to decide what this replica still needs would wrongly conclude event 1 is already here and never send it again, losing it permanently. Instead, `VersionVector` computes the highest seq in the gap-free run starting at 1 (a "gaps and islands" query); a gap simply holds the frontier back until the missing event arrives, which it eventually does, since `Reorder` delays frames rather than dropping them. The map shape (`map[NodeID]Seq`) and the `Contains`/`Dominates`/`Observe`/`Merge` operations are unchanged — only the query populating the map accounts for gaps, so no contiguous-prefix-plus-explicit-gap-list structure is needed.

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

func (s *ItemState) Apply(e Event)   // must be commutative and associative; NOT idempotent on its own — see below
func (s ItemState) Quantity() int64  // sum(Pos) - sum(Neg)
```

`Apply` for `KindQuantityDelta` adds `|Delta|` into `Pos[e.ID.NodeID]` or `Neg[...]` — a naive `+=` is **not idempotent** under duplicate delivery: calling `Apply` twice with the same event double-counts. `Apply` itself never deduplicates and is not required to. The system's idempotence guarantee lives one layer up, at the log: `Append`'s primary key on `(NodeID, Seq)` guarantees an event is stored at most once, so `fold(Apply, events read back from the log)` sees each event exactly once regardless of how many times it was delivered or appended over the wire. Any caller that drives `Apply` directly from a stream that has *not* passed through `Append`-deduplicated storage (e.g. a raw event feed) is responsible for its own dedup — `Apply` will happily double-count otherwise. This must be stated in code comments and asserted by a test that (a) reads the same physical log twice into two fresh `ItemState`s and shows identical results, and (b) calls `Apply` twice directly with the same event on one `ItemState` and shows the count doubles, demonstrating why only the log path is safe.

`Pos`/`Neg` running totals are `int64` and accumulate `|Delta|` per node over the node's entire lifetime. The one arithmetic operation that can actually overflow on a single event — negating `Delta` to store its magnitude — is closed off at the trust boundary: `Event.Validate` rejects `Delta == math.MinInt64`, the only `int64` value whose negation doesn't fit back in `int64`. Cumulative overflow of `sum(Pos)` or `sum(Neg)` after an astronomical number of events is a known, accepted ceiling of using fixed-width `int64` totals for this toy domain — not a realistic operating condition (it needs more picks/receives than an `int64` counter can log in the first place) — and is intentionally not guarded with `big.Int` or saturation logic; upgrade path if it ever matters: widen the counters or check `sum` in `Quantity()`.

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

**Known limitation — no protocol outcome for a permanently invalid batch.** A malformed/unparseable event (see Error handling) is rejected, logged, and the session continues, but the receiver's version vector never advances past it, so the sender's `Since` query includes it again on every future session: a persistently malformed event retries forever with no automatic resolution. This is an accepted gap, not an oversight — resolving it properly means either a wire-level rejection/quarantine frame or a durably persisted per-peer skip-list, and both are more machinery than this learning project's scope justifies for what should be a rare condition between compliant replicas (it indicates a genuine bug or version skew, not routine operation). The manual remediation path is to fix or remove the offending row at its origin node. Upgrade path if this ever needs to be automatic: add a `Reject{event_ids}` frame to the protocol (see `sync/sync.proto`) and a persisted skip-list the sender consults before resending.

Protocol per session: exchange version vectors, each side streams what the other lacks, each side `Append`s and acks. No arbitration frames — central cannot reject, only merge. Resumable: cursors persist, so a dropped stream resumes from the last ack rather than restarting.

Central store is a second `eventlog.Log` backed by Postgres with the same schema, plus a projection of merged `ItemState` per SKU for reporting. Central runs the identical `crdt.Apply` code — no second implementation.

### `node/` — wiring

Each node: local log + clock + projection cache + sync client. Exposes an in-process API (`Receive`, `Pick`, `SetMeta`, `Delete`, `Get`) that the CLI and the harness both drive. No HTTP.

## Snapshots and compaction

Projection is `fold(Apply, events for SKU)`. Snapshot after N events per SKU, recording the version vector it covers. Read path: load snapshot, apply only events not covered.

`covers` is a `VersionVector` populated **only from the origin nodes that have actually written an event for this SKU** — it is not a slice of the node's global version vector, it is a distinct, smaller map built by observing each per-SKU event's `EventID` as it is folded. This is safe to compare against another node's *global* `VersionVector` (e.g. a peer's acked cursor) with the existing `Dominates`, because `Dominates` only inspects keys present in `covers`: since a node's `Seq` numbering is shared across all SKUs, `peerVV.Dominates(snapshot.covers)` is true exactly when the peer has received every event this SKU's snapshot folded in, with no separate per-SKU sequence space required.

`Compact` deletes events dominated by every peer's acked version vector **and** covered by a local snapshot — never events a peer has not yet seen.

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
| Reorder | transport buffers and permutes frames within a window, including frames carrying different sequence numbers from the same origin node (see the `VersionVector` gap-handling note above, which is what makes this safe) |
| Crash mid-append | log wrapper aborts the transaction, then reopens the DB |
| Slow peer | delays acks past the next batch |

Harness shape: generate a random op schedule (`Receive`/`Pick`/`SetMeta`/`Delete` across nodes and SKUs) plus a random fault schedule, run to quiescence with all faults healed and all peers synced, then check the three properties. Seeded, so a failure prints a reproducing seed.

Additional targeted tests, not left to random search:

- `Apply` commutativity: for a fixed event set, every permutation yields the same `ItemState` (exhaustive for small sets, sampled above that).
- HLC monotonicity under backwards wall-clock jumps.
- Idempotence-via-log: reading the same log twice into two fresh `ItemState`s (the only supported replay path) yields identical state; a separate test calls `Apply` twice directly on one `ItemState` with the same event and shows the count doubles, documenting that `Apply` itself is not idempotent — only `Append`'s primary key makes the end-to-end system idempotent.
- Concurrent decrement below zero: two nodes each pick 8 of 10 units. Quantity converges to −6 on every replica. **This is correct CRDT behavior and the spec accepts it** — negative stock is a reportable anomaly at central, not a rejected write. Documenting this limitation honestly is the point; `warehouse-node` is where rejection lives.

100% coverage target per the repo standard, with the harness counted as tests, not as covered code.

## CLI (`cmd/lab/`)

```text
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
- Malformed remote event (unknown kind, unparseable payload): reject that frame, log loudly, continue the session. Never persist an event the local `crdt` package cannot apply, or convergence breaks silently. There is currently no protocol-level outcome distinguishing this from a transient drop, so a persistently malformed event retries every session indefinitely — an accepted, documented limitation (see `sync/` above), not silent data loss, since the event was never valid in the first place.
- Version vector regression from a peer (claims to have less than it acked before): log and re-send from the lower point. Harmless given idempotent append.

## Milestones

1. `clock/` + property tests
2. `eventlog/` on SQLite + idempotence and crash tests
3. `crdt/` + commutativity tests
4. `node/` wiring + CLI ops
5. `sync/` over gRPC + central Postgres merge
6. `internal/jepsenlite/` harness + the three properties
7. Snapshots and compaction, harness extended to cover stale-peer compaction
