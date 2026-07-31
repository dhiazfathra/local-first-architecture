# warehouse-node — Offline-First Warehouse Service with Central Arbitration

**Date:** 2026-07-30
**Location:** `warehouse-node/` (own `go.mod`, module `github.com/dhiazfathra/local-first-architecture/warehouse-node`)
**Purpose:** Realistic ERP prototype. Warehouse nodes keep operating through network loss; central validates and can reject, emitting compensating events back down.

## Goal

A warehouse node whose source of truth is its own local event log. Operators receive goods, put away, pick, count, and transfer between warehouses with no network. Central reconciles asynchronously, enforces cross-node invariants no single node can check, and pushes compensating events for what it rejects.

Domain rules carry equal weight to sync mechanics. The interesting design work is choosing which invariants a node can enforce alone and which genuinely require central.

## Non-Goals

UI, authentication/authorization (single trusted operator per node), purchase-order lifecycle beyond a stub PO reference, financials/costing, multi-tenancy, barcode hardware, reporting beyond the projections listed.

## Invariant ownership — the central design decision

| Invariant | Enforced where | Why |
|---|---|---|
| Stock at a location never negative | **Node**, synchronously at command time | Node exclusively owns its own locations. It can decide alone. |
| Reservation cannot exceed available | **Node** | Same ownership argument. |
| Lot not expired at pick time | **Node** | Local clock is good enough; expiry is a date, not a timestamp race. |
| UoM conversion valid for item | **Node** | Item master is replicated read-only from central. |
| SKU exists in item master | **Node**, against replicated master; **central** re-checks | Node master may be stale — a node created against a since-deleted SKU is rejected at central. |
| Receipt does not exceed open PO quantity | **Central** | Two nodes can receive against the same PO. No node sees the other's receipt. |
| Transfer destination node exists and accepts the item | **Central** | Node has no authority over another node's configuration. |
| No duplicate receipt of the same supplier delivery note | **Central** | Cross-node duplicate detection. |
| Transfer received quantity ≤ dispatched quantity | **Central** | Requires both halves, which live on different nodes. |

Node-enforceable invariants are checked **before** the event is appended — the command is rejected outright and the operator sees the error immediately. Validation, append, and state/projection update happen while holding the node service's single command mutex, so two concurrent gRPC commands can never both observe the same available stock, both pass validation, and both append conflicting events — the second command sees the first one's effect before it validates. Central-enforceable invariants are checked **after** the fact, and rejection arrives as a compensating event.

## Domain model

Aggregates, each with its own event stream keyed by aggregate ID:

**`Item`** — replicated read-only from central. SKU, description, base UoM, alternate UoMs with conversion factors, lot-tracked flag, shelf-life days. Node never emits `Item` events; it applies them from central.

**`StockLocation`** — node-owned. Location code, type (`receiving` / `bulk` / `pick` / `staging` / `quarantine`).

**`StockOnHand`** — the projection that matters, keyed `(SKU, LocationCode, LotID)`. Never commanded directly; derived from events.

**`Receipt`** — node-owned lifecycle: `ReceiptOpened` → `ReceiptLineRecorded`* → `ReceiptClosed`. Carries supplier delivery-note number and PO reference.

**`Transfer`** — spans two nodes, two-phase:
- source emits `TransferDispatched{TransferID, FromNode, ToNode, Lines}` and decrements its own stock immediately
- central applies `TransferDispatched` into an **in-transit** balance owned by neither node
- destination emits `TransferReceived{TransferID, Lines}` when the truck arrives and increments its own stock
- central closes the transfer, in-transit returns to zero

In-transit is where the goods live during the gap. It is a first-class balance at central, not an accounting fudge. A `TransferReceived` with no matching `TransferDispatched` is rejected; a dispatch unmatched past a configurable window is reported as a discrepancy, not auto-compensated (a lost truck is a human problem).

**`StockCount`** — node-owned: `CountStarted` → `CountLineCounted`* → `CountClosed`, emitting `StockAdjusted` per variance line with a reason code.

**`Reservation`** — node-owned. `StockReserved{ReservationID, SKU, Location, Lot, Qty}` / `ReservationReleased` / `ReservationConsumed`. Reservations are node-local and never sync-relevant to other nodes, but they do sync to central for visibility.

### Events

```go
type Envelope struct {
    ID          EventID   // {NodeID, Seq}
    AggregateID string
    Type        string
    HLC         HLC
    RecordedAt  time.Time // wall clock at emit, for audit only
    CausationID *EventID  // set on compensating events: what this compensates
    Payload     json.RawMessage
}
```

Payload types: `GoodsReceived`, `PutAway`, `Picked`, `StockAdjusted`, `StockReserved`, `ReservationReleased`, `ReservationConsumed`, `TransferDispatched`, `TransferReceived`, `ReceiptOpened`, `ReceiptLineRecorded`, `ReceiptClosed`, `CountStarted`, `CountLineCounted`, `CountClosed`.

Movement events (`PutAway`, `Picked`, `TransferDispatched`, `TransferReceived`) always carry **from** and **to** as explicit location references, with `external` as a sentinel for the outside world. Every movement is a balanced pair. This makes the stock projection a single code path and makes reconciliation a sum that must come to zero.

## Central arbitration

Central applies each incoming node event through a chain of validators. Outcomes:

- **accept** — persist, project
- **reject** — persist the original event (the log is immutable history, including mistakes), then emit a compensating event attributed to central with `CausationID` pointing at the rejected event

Compensating event types:

| Rejection | Compensation |
|---|---|
| Receipt exceeds open PO | `StockAdjusted` reversing only the excess quantity of that receipt line, reason `po_overreceipt`, plus `ReceiptLineRecorded` reversal |
| Duplicate delivery note | `StockAdjusted` reversing exactly the duplicated line's quantity, reason `duplicate_receipt` |
| Unknown or deleted SKU | `StockAdjusted` reversing exactly the rejected event's SKU/location/lot quantity, reason `unknown_sku`, and the SKU is flagged for manual cleanup — never zeroing the aggregate, which would also erase unrelated stock recorded against that SKU before or after it was deleted |
| Transfer to unknown/rejecting node | `StockAdjusted` restoring source stock, reason `transfer_rejected`, transfer marked failed |
| `TransferReceived` exceeds dispatched | `StockAdjusted` removing the excess at destination, reason `transfer_overreceipt` |
| `TransferReceived` with no matching `TransferDispatched` on record | Treated as the zero-dispatched-quantity case of the over-receipt rule above: `StockAdjusted` removing the full received quantity at destination, reason `transfer_overreceipt`, and the transfer's in-transit balance is left at zero since no dispatch ever moved it there. This is a distinct scenario from a dispatch that times out with no receipt (line above): here a *receipt* exists with nothing to match against it, whereas a timed-out dispatch has no receipt to compensate — it is reported as a discrepancy instead, per the rule above. |

Compensating events flow down on the same sync stream and are applied by the node exactly like any other event. A node cannot refuse a compensation — that is what makes central authoritative. The node surfaces them in an **exceptions** projection so the operator sees what was reversed and why.

**Critical property:** compensation must be safe even though the node has continued working. Compensating a receipt whose stock has already been picked and shipped can drive a location negative. The spec accepts negative balances arising from compensation, flags them in the exceptions projection, and requires human resolution via a stock count. Attempting to cascade compensation through downstream events is explicitly out of scope — that way lies distributed rollback, and real ERPs do not do it either.

**Arbitration is atomic and idempotent.** Each incoming event's verdict, projection update, and compensation intent are persisted in a single Postgres transaction against the decisions table (`event_id`, `verdict`, `reason`, `compensating_event_id`), with a unique constraint on `event_id`. Before evaluating a rule, the arbiter first checks for an existing decision for that `event_id`; if one exists, its recorded verdict and `compensating_event_id` are returned as-is instead of re-running validators. This makes retries after a crash or a redelivered event safe: an event can be evaluated at most once, and a compensating event is emitted at most once per rejected event.

## Architecture

```text
warehouse-node/
  cmd/node/          node server: gRPC API + sync client
  cmd/central/       central server: gRPC sync server + arbitration
  internal/domain/   aggregates, commands, node-side invariants — no I/O
  internal/eventlog/ SQLite append-only log, version vectors, cursors
  internal/projection/ stock-on-hand, reservations, exceptions, transfers
  internal/sync/     gRPC bidirectional replication client + server
  internal/arbiter/  central validators + compensating event emission
  proto/             sync.proto, node_api.proto
```

`internal/domain` is pure: commands in, events or a validation error out, no database. This is where the node-side invariant tests live, and they need no fixtures beyond a state struct.

### Storage

Node: SQLite (`modernc.org/sqlite`), one file. Events table keyed `(node_id, seq)` — idempotent append. Projection tables rebuilt by replaying the log; a `projection_version` row forces a rebuild when projection code changes.

Central: Postgres. Same event schema plus `node_id` partitioning, in-transit balances, per-node cursors, arbitration decisions table (`event_id`, `verdict`, `reason`, `compensating_event_id`).

### Sync

gRPC bidirectional stream, resumable via persisted cursors. Node opens the stream; frames:

```proto
service Sync {
  rpc Replicate(stream NodeFrame) returns (stream CentralFrame);
}
message NodeFrame  { oneof body { Hello hello = 1; EventBatch events = 2; Ack ack = 3; } }
message CentralFrame { oneof body { Welcome welcome = 1; EventBatch events = 2; Ack ack = 3; } }
```

Central pushes: item-master updates, compensating events, and transfer events destined for this node. Node pushes its own events. Batching with a size cap; a node offline for a week catches up in bounded chunks.

### Node API

gRPC, for the operator client (`proto/node_api.proto`): `Receive`, `PutAway`, `Pick`, `Reserve`, `ReleaseReservation`, `StartCount`, `CountLine`, `CloseCount`, `DispatchTransfer`, `ReceiveTransfer`, plus queries `StockOnHand`, `Exceptions`, `Transfers`. Every command returns immediately from local state. Nothing blocks on central.

## Testing

- `internal/domain`: table-driven tests per invariant, pure, no I/O. Negative stock, over-reservation, expired lot, bad UoM, unknown SKU against a stale master.
- `internal/node`: concurrent-pick regression test — two goroutines racing `Service.Execute` against the same available stock must not both succeed; exactly one observes the other's effect and is rejected for over-reservation/negative stock.
- `internal/eventlog`: idempotent append, crash mid-transaction, replay determinism, projection rebuild equivalence.
- `internal/arbiter`: one test per rejection rule, asserting the exact compensating event emitted, including `CausationID`.
- **Integration:** two nodes plus central, in-process, transport injectable. Scenarios:
  - both nodes receive against the same PO past its quantity → exactly one over-receipt compensation
  - same delivery note entered at two nodes → duplicate compensation
  - full transfer happy path, asserting in-transit is non-zero only between dispatch and receipt
  - transfer dispatched, destination node reconfigured to reject the item → source stock restored
  - node works offline for 200 ops, syncs, receives a compensation for op 3, exceptions projection shows it
  - compensation drives a location negative because stock was already picked → negative balance flagged, not hidden
- Determinism: replay every integration scenario's final log on a fresh projection and assert identical state.

100% coverage per repo standard.

## Error handling

- Node-invariant violation: command rejected, nothing appended, error returned with the violated rule named.
- Central unreachable: node fully operational, events queue in the log, backoff retry.
- Central rejects an event: never dropped; compensation emitted, exceptions projection updated. Operator-visible.
- Unparseable event from central: session fails loudly, cursor not advanced, retried. Never skip an event from the authority — skipping silently forks state, and later events may causally depend on this one, so the cursor must not advance past it under any automatic path. If the same event fails repeatedly (a poison event), the raw frame and failure are persisted to a quarantine table and the sync session stops actively retrying it (so it does not spin forever), but stays blocked at that cursor position — it does not resume past it. The quarantined event is surfaced to the operator, who must repair the frame or explicitly acknowledge a deliberate skip; only that action, never a timeout or automatic retry, advances the cursor past a quarantined event.
- Projection code change: version bump triggers full rebuild on startup.

## Milestones

1. `internal/domain` — aggregates, commands, node-side invariants, pure tests
2. `internal/eventlog` — SQLite log, replay, idempotence
3. `internal/projection` — stock-on-hand, reservations
4. `cmd/node` — gRPC node API, fully offline-capable
5. `internal/sync` + `cmd/central` — replication, Postgres central, projections
6. `internal/arbiter` — validators and compensating events
7. Transfers end to end, in-transit balances
8. Counts, adjustments, exceptions projection
9. Integration scenario suite
