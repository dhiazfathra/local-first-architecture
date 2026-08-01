# 0004 — One symmetric protocol; central is just a peer

- **Status:** accepted
- **Date:** 2026-07-30

## Context

Nodes must exchange records with central, and — eventually, for some
deployments — with each other directly. The obvious shape is an asymmetric API:
nodes `Push` their records upward and `Pull` central's records downward. That
shape hardcodes a hierarchy into the wire format, and node-to-node sync then
needs a third RPC that behaves like both.

## Decision

One gRPC bidirectional stream, `rpc Replicate(stream Frame) returns (stream
Frame)`, with **one** `syncpb.Frame` message type flowing in both directions:

```proto
message Frame {
  oneof body {
    Hello hello = 1; // node id + version vector
    Batch batch = 2; // records the peer lacks
    Ack   ack   = 3; // version vector after apply
  }
}
```

One function, `sync.Exchange(ctx, log, projector, stream, initiator bool)`,
implements the entire protocol and is used by both sides. The only asymmetry in
the system is that boolean — who speaks first — and it is a parameter, not a
code path:

1. initiator → `Hello{node_id, version}` ("here is what I already have")
2. responder → `Hello`, then `Batch*` (records the initiator lacks), then `Ack`
3. initiator merges, sends its own `Batch*` and `Ack`, closes its send side
4. responder merges, sees `Ack` then EOF, returns

`Hello` carries a version vector (`map[nodeID]highestSeqSeen`), so "what do you
need" is answered by `eventlog.Store.Since` with no negotiation round trips.

Both `cmd/node` and `cmd/central` register the same `sync.NewServer` and can run
the same `sync.NewClient`. The two binaries differ in exactly two ways: central's
log lives in Postgres (`central.PGStore`) instead of SQLite, and central has no
local domain commands.

## Consequences

Good: node-to-node replication is a configuration change — point a
`sync.NewClient` at another node — not a new protocol, a new service, or a new
test suite. Resumability is free: if a stream dies at any point, both sides
simply re-`Hello` with their current vectors next time, and the idempotent
append from [ADR 0002](0002-sqlite-local-log.md) makes every re-delivered record
a no-op. A version vector that has *regressed* is equally safe — you just
re-send from the lower mark. There is no partial-batch bookkeeping anywhere in
the codebase.

Bad, and accepted:

- A `oneof` means every receive is a type switch, and a frame arriving out of
  the expected order is a protocol error (`sync.ErrProtocol`) rather than
  something the type system prevented.
- Symmetry costs central some efficiency: it answers full `Since` queries per
  peer rather than serving a purpose-built feed.
- Records cross the wire with no authentication and no transport security. See
  [limitations](../limitations.md).
- Every peer eventually holds every record it is told about. There is no
  filtering or subscription by location, so this does not scale to many nodes
  with disjoint interests.

## Alternatives rejected

- **Asymmetric `Push`/`Pull` RPCs.** Simpler to read one call at a time, and it
  bakes the hierarchy into the wire format; node-to-node then needs a third
  path.
- **Central-arbitrated command submission.** Correctness would depend on
  reaching central, which is the opposite of local-first.
- **Long-polling HTTP or webhooks.** More moving parts than a stream, and
  resumption becomes the application's problem.
