# localfirst-go

A readable reference architecture for **local-first event sourcing** in Go.

Not a library, not a framework, and not production software. It is a small, whole
system you can read in an hour, built so that the interesting ideas are visible
rather than buried.

## What "local-first" means

Software is usually server-first: your client is a window onto a database
somewhere else, and without the network it can show you things but not change
them. Local-first inverts that. Every node keeps a **complete copy of its own
data on local disk** and accepts writes with no network at all. Synchronising
with anyone else is background reconciliation, never a precondition for getting
work done.

The consequence people notice is that it works on a train. The consequence that
matters architecturally is harder: if two participants can both accept writes
while unable to talk, you need an answer for what happens when they disagree.
Most of the difficulty in local-first systems is that one question.

## This architecture, in three sentences

Each node owns an append-only log of immutable facts in a local SQLite file, and
derives all of its current state by replaying that log — so a write is one local
transaction that validates, appends and projects together, and never touches the
network. Nodes reconcile with a central store over one symmetric gRPC stream
where records are identified by `(node_id, seq)`, which makes re-delivery a
no-op and therefore makes the protocol resumable without any deduplication
logic. Conflict is removed rather than solved: every location belongs to exactly
one node, so central computes global stock as a **sum** of per-node balances and
never has to merge anything.

Read [docs/architecture.md](docs/architecture.md) for the diagrams, or
[docs/adr/](docs/adr) for why each of those choices was made.

## Seeing it work

```bash
cd localfirst-go
make demo
```

That brings up Postgres, one central and three nodes in Docker Compose, receives
stock at each node, **cuts node 3 off the network**, runs 20 operations against
the isolated node showing every one succeed, prints central's global sum with
node 3's changes visibly missing, reconnects, and prints the converged sum.
Actual numbers, step by step, in about a minute. It is the fastest way to
understand what local-first buys you and what it costs you.

The same scenario runs as a Go integration test (`go test ./demo/...`), so the
demo cannot drift from the documentation describing it.

## Layout

```text
localfirst-go/
  clock/           hybrid logical clock                  (domain-agnostic)
  eventlog/        append-only SQLite log + seams         (domain-agnostic)
  projection/      generic replay driver                  (domain-agnostic)
  sync/            symmetric gRPC replication             (domain-agnostic)
  domain/          THE ONLY inventory-aware package
  transport/grpc/  node client API
  central/         Postgres-backed peer log + global sum
  cmd/node/        node binary
  cmd/central/     central binary
  arch/            architecture test: the engine must not reach the domain
  demo/            the seven-step scenario, shared by make demo and its test
  examples/        a second domain, proving the engine seam
  docs/            ADRs, diagrams, limitations, swap-the-domain guide
```

The four domain-agnostic packages are the reusable part. That they are genuinely
domain-agnostic is not a comment — `arch/arch_test.go` parses the real import
graph and fails the build if any of them can reach `domain` by any path.

## Where your domain plugs in

Two interfaces and a command encoder:

```go
// reading — projection
type Reducer[S any] interface {
	Zero() S
	Apply(state S, r eventlog.Record) (S, error)
}

// writing — eventlog, called inside the append transaction
type Validator interface {
	Check(r Record) error
}
```

[docs/swapping-the-domain.md](docs/swapping-the-domain.md) replaces the
inventory domain with an unrelated one end to end, in compiling and tested code
at [examples/tasklist](examples/tasklist).

## When to use this shape

- Writers partition cleanly by ownership — one warehouse, one till, one vehicle,
  one field engineer — and no two of them ever need to change the same record.
- Availability while disconnected is a requirement rather than a nice-to-have.
- Every invariant you must enforce can be checked by a single writer from its own
  data. In this repo: "balance never goes negative", decidable locally because
  only one node writes that balance.
- A complete, replayable audit trail is worth its storage cost to you.

## When not to use it

- **Two writers genuinely share a key.** Then read `eventlog-lab` for
  mathematical merge (CRDTs), or `warehouse-node` for central arbitration with
  compensating events. Do not bend ownership to fit; nothing here detects the
  violation.
- **An invariant spans nodes** — a global reservation limit, uniqueness across
  the fleet, double-booking prevention. Local-first cannot enforce that without
  coordination, and coordination is what it gave up.
- **Readers need the global truth to be current.** Central's sum is eventually
  consistent, and during a partition it is stale by however long the partition
  lasts.
- **History is a liability**, or must be deletable on request. The log is
  append-only and never forgets.

Every boundary is spelled out in
[docs/limitations.md](docs/limitations.md) — including the ones that would bite
you in production, such as plaintext gRPC with no authentication and a log that
never compacts.

## Working on it

```bash
make test   # go test ./... -cover  (starts Postgres; 100% coverage is the standard)
make lint   # golangci-lint run
make proto  # regenerate protobuf/gRPC code
make demo   # the seven-step offline-and-converge demonstration
```

Tests require Docker for Postgres, which `make test` starts for you. Nothing
requires cgo: the whole module builds with `CGO_ENABLED=0`.

## Sibling reference architectures

Three repos, one problem, three different answers. This one is the smallest on
purpose.

- **localfirst-go** (here) — conflict removed by per-node ownership.
- **eventlog-lab** — conflict resolved mathematically, with CRDT merge
  semantics.
- **warehouse-node** — conflict arbitrated centrally, with compensating events.

If you are new to any of this, read this one first: it is the only one of the
three where you can hold the entire conflict story in your head, because there
isn't one.
