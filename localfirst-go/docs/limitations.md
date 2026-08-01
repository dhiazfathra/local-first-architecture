# Limitations

Everything this repo deliberately does not do. None of these is a bug or a
"later"; each is a boundary chosen so the rest could stay readable. Read this
before lifting any of it into production.

## No concurrent writes to the same key

The load-bearing constraint. Each `Location` belongs to exactly one node, and a
node may only author events for locations it owns — `domain.ErrNotOwned`
otherwise. Because no two nodes ever write the same `(SKU, Location)`, there is
no conflict to resolve, and central computes global stock by **summing** per-node
balances rather than merging them.

Consequences if you break that rule: nothing in this codebase detects it. Two
nodes writing one key would both append happily, both pass their own local
negative-balance check against their own partial view, and central would sum
their balances into a number that means nothing. There is no alarm, because
detection would require the coordination that ownership exists to avoid.

If your problem genuinely has two writers per key, this design does not apply:

- `eventlog-lab` — mathematical merge (CRDTs), for when convergence must be a
  property of the data type.
- `warehouse-node` — central arbitration and compensating events, for when a
  single authority must be able to accept, reject and repair after the fact.

Also unmodelled: **reassigning** a location from one node to another. Ownership
is configuration handed to `cmd/node` via `-locations`, and this system has no
protocol for moving it safely while both nodes hold history.

## A node's projection carries keys it does not own, and they can go negative

`BalanceReducer` applies both halves of a `Moved` — debit `From`, credit `To` —
and it does so wherever the record lands. So when a node syncs a `Moved`
authored elsewhere whose destination it owns, it credits its own location *and*
debits the source location, leaving a negative balance for a key belonging to
another node.

That is expected. A node's projection is authoritative only for the locations it
owns; the negative entries are the shadow of movements it was not party to.
Nothing reads them, and the negative-balance guard cannot fire on them because
merged records skip validation by design (`Append` takes a nil `Validator` on the
sync path — a node cannot veto history that already happened elsewhere).

The trap: do not build a report on `Inventory.Balances()` and present it as
global stock. Filter to owned locations, or read the sum at central. This repo
does not filter for you, because the filter would imply the projection is
supposed to be globally meaningful, and it is not.

## No compaction, no snapshots

The log grows forever. `eventlog` has no compaction, no archiving, and no
snapshotting, so `projection.Fold` replays every record ever written on each
process start, and startup cost grows linearly with history. A node that has
been running for a year replays a year.

This is left out on purpose: snapshotting adds a second source of truth and the
question "is the snapshot consistent with the log?", which is precisely the
confusion a first reading should not have to hold. The real fix is a periodic
state snapshot plus a `Fold`-from-snapshot path, and it belongs to whoever needs
it.

Related: `Since(VersionVector)` scans, since the only index is the
`(node_id, seq)` primary key. Fine at demo scale, wrong at a million records.

## The transfer gap in the global sum

A cross-node move is one `Moved` event authored by the source node: the source
decrements a location it owns, the destination increments a location it owns.
That is what lets transfers work with no shared key and no two-phase handshake.

The cost is real: between the moment central learns of the source's decrement
and the moment it learns of the destination's increment, the transferred units
exist in no node's balance and are **absent from
`central.PGStore.GlobalSum`**. The global total dips and then recovers.

Nothing in this repo models goods in transit, and no reader should treat
`GlobalSum` as an instantaneous physical truth. It is eventually consistent, and
during a partition — the `make demo` scenario cuts a node off for twenty
operations — it can be stale by an unbounded amount of time. If in-transit
accounting matters to you, that is an in-transit *location*, owned by whichever
node is accountable for the goods, and it is a domain change rather than an
engine change.

## No authentication, no authorization, no transport security

gRPC is served plaintext (`insecure.NewCredentials()`), on both the node client
API and the sync service. Any process that can reach a node's port can issue
commands, and any process that can reach the sync port can inject records
attributed to **any** `node_id` — the log stores the author claimed in the
record, and verifies nothing.

There is no user identity anywhere in the system, so "who received this stock?"
is not a question the log can answer. Nothing is encrypted at rest either: a
node's SQLite file is readable by anything with filesystem access, which is also
why it is so convenient to inspect.

Do not put this on a network you do not fully control.

## Other omissions, briefly

- **Payload schema evolution.** Payloads are opaque bytes; the engine cannot
  migrate them. Add a field to `domain.Received` and old records still decode,
  but rename one and replay breaks. There is no versioning scheme and no
  upcasting.
- **No deletion, no retention, no GDPR erasure path.** The log is append-only
  and complete, by construction.
- **Unknown record types are fatal.** A node running an older binary than its
  peers halts on the first record it cannot interpret
  (`eventlog.ErrUnknownType`). That is chosen over silent divergence, and it
  means rolling deploys need care: deploy readers before writers.
- **One writer process per SQLite file.** Nothing enforces it.
- **No backpressure or batching limits in sync.** `Since` returns everything a
  peer lacks in memory; a very long offline window means a very large slice.
- **No metrics, no tracing, no structured operational logging** beyond
  `slog.Error` at binary exit.
- **`Logical` in `clock.HLC` is a `uint32`** and is not protected against
  overflow within a single stalled millisecond.
- **Central is a single Postgres instance** with no replication story, and the
  demo's Postgres has no durable volume.

## What this repo is for

Reading. It exists so that someone with no exposure to event sourcing, hybrid
logical clocks or local-first architecture can understand the shape of the
solution in an hour. Every entry above is a place where clarity was chosen over
capability, and each one names where to go next.
