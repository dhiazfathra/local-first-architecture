# 0001 — Per-node ownership instead of merge

- **Status:** accepted
- **Date:** 2026-07-30

## Context

Every node in this system accepts writes while offline. Two nodes can therefore
produce facts about the world at the same instant with no way to consult each
other. The classic question follows: what happens when both of them change the
same thing?

The usual answers are expensive. A CRDT gives you a merge function that is
associative, commutative and idempotent, so any order of arrival converges on
the same value — powerful, and a real cost in both concepts and code. Central
arbitration keeps a single authority that accepts or rejects after the fact and
emits compensating events — also real, also a lot of machinery, and it puts the
network back on the critical path for correctness.

Both answers solve "two writers, one key". We asked a cheaper question first:
does this system actually need two writers on one key?

## Decision

Partition write authority by ownership. Each `Location` belongs to exactly one
node, and a node may only author events for the locations it owns — enforced
locally by `domain.ErrNotOwned` before anything is appended. No two nodes ever
write the same `(SKU, Location)` key.

Central therefore never merges. It stores every node's records and computes
global stock as a **sum of per-node balances** (`central.PGStore.GlobalSum`).
Addition is associative and commutative and needs no conflict rules, because
each addend has exactly one author.

Cross-node movement still works. A transfer is one `Moved` event authored by
the source node: the source decrements a location it owns, and the destination
increments a location it owns. There is no shared key, no in-transit ledger and
no two-phase handshake.

## Consequences

Good: conflict stops being a category of problem rather than becoming a solved
problem. The negative-balance invariant can be checked by a single node with no
network, which is the reason this repo is a fraction of the size of its
siblings. A reader can hold the whole conflict story in their head: there
isn't one.

Bad, and accepted:

- If two nodes genuinely must write the same key, this design does not apply.
  Do not bend it. Read [eventlog-lab](../../../eventlog-lab) for mathematical
  merge, or [warehouse-node](../../../warehouse-node) for arbitration and
  compensating events.
- Ownership must be assigned somewhere outside this system, and reassigning a
  location between nodes is not modelled at all.
- `cmd/node` treats an empty `-locations` list as "own everything" rather than
  "own nothing", which disables the ownership check entirely. That is a known
  constraint for single-node and demo/central usage only; a real multi-node
  deployment must configure at least one owned location per node, or this
  invariant does not hold.
- Between the source's decrement and the destination's increment reaching
  central, transferred goods are absent from the global sum. That gap is real
  and documented in [limitations](../limitations.md).

## Alternatives rejected

- **CRDT-valued balances (PN-Counter per key).** Correct and unnecessary here:
  a counter that only one node increments is just an integer.
- **Central arbitration with compensating events.** Buys the ability to
  overspend and repair. We have no overspend to repair, because the owner of a
  location always knows its true balance.
