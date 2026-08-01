# 0003 — Hybrid logical clocks, for ordering and audit only

- **Status:** accepted
- **Date:** 2026-07-30

## Context

Records need a timestamp. Two properties matter: a human reading the log must be
able to relate an event to the time it happened, and any two records must have a
stable, total order that never changes and never runs backwards.

Wall-clock time alone gives the first and not the second. Machine clocks drift,
NTP steps them backwards, and two events in the same millisecond tie. A Lamport
counter gives the second and not the first: it orders perfectly and tells you
nothing about when anything happened.

## Decision

Use a hybrid logical clock: `clock.HLC{Wall int64; Logical uint32; NodeID string}`.

- `Wall` is milliseconds since the Unix epoch, so the value is meaningful to a
  human.
- `Logical` increments when wall time fails to advance — including when the OS
  clock jumps *backwards*, in which case `Wall` is held at its previous value
  and `Logical` moves instead. `clock.Clock.Now` therefore never returns a
  timestamp less than or equal to one it already returned.
- `NodeID` is the final tiebreak in `HLC.Compare`, so any two distinct HLCs from
  any two nodes have a defined order. Total, not partial.
- `clock.Clock.Observe(remote)` pulls the local clock up to any peer timestamp
  seen during sync, so a node's own future timestamps sort after everything it
  has already learned.

**And that is all it is used for: ordering and audit.** It is deliberately
*not* used for conflict resolution.

## Consequences

The important consequence is the one a reader is likely to get wrong: seeing an
HLC in an event-sourced system usually implies last-write-wins somewhere. Here
it does not, and there is no code path that compares two timestamps to decide
which fact survives. Per-node ownership ([ADR 0001](0001-per-node-ownership.md))
means two facts never compete, so there is nothing to resolve. If you later
introduce shared-key writes, an HLC will *not* quietly save you — LWW discards
data, and you would be choosing that, not inheriting it.

Good: a stable total order for display, debugging and deterministic replay;
monotonicity that survives a hostile clock; no coordination of any kind.

Bad, and accepted:

- `Wall` is not trustworthy as absolute time. A node with a badly skewed clock
  produces skewed but correctly ordered timestamps, and `Observe` will drag
  peers forward to meet it. HLC bounds divergence from real time only as well as
  the worst clock in the cluster.
- `Logical` is a `uint32`. A node producing more than four billion events in a
  single stalled millisecond has a different problem.
- HLC does not capture causality between events the way a full vector clock
  would. We do not need it: causality within one node is its `seq`, and across
  nodes there is none to capture.

## Alternatives rejected

- **Wall clock only.** Ties and backwards jumps, so replay order changes between
  runs. A reference architecture must not be nondeterministic.
- **Lamport counters only.** Correct ordering, useless log output.
- **True vector clocks per record.** Records causality we have no use for and
  grows with the number of nodes.
