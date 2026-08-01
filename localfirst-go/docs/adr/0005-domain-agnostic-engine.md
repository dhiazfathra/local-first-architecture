# 0005 — A domain-agnostic engine with exactly two seams

- **Status:** accepted
- **Date:** 2026-07-30

## Context

This repo is a reference architecture, so its real output is a set of packages a
reader can lift into their own problem. That only works if the log, the clock,
the projection driver and the sync protocol contain nothing about inventory. The
temptation is constant and small: one `if r.Type == "inventory.Moved"` inside
`sync`, one balance check inside `eventlog`, and the engine quietly becomes an
inventory system with extra steps.

## Decision

The engine moves opaque bytes. `eventlog.Record` carries a domain-defined `Type`
string and a domain-defined `Payload []byte`, and no engine package ever looks
inside either.

Interpretation plugs in at exactly two seams — one for writing, one for reading.

**Reading** — `projection.Reducer[S]`, generic over the state type, so the
engine never names a domain type:

```go
type Reducer[S any] interface {
	Zero() S
	Apply(state S, r eventlog.Record) (S, error)
}

func Fold[S any](ctx context.Context, log eventlog.Reader, r Reducer[S]) (S, error)
```

**Writing** — `eventlog.Validator`, called inside the append transaction against
current projected state, because the engine cannot know what "negative stock"
means:

```go
type Validator interface {
	Check(r Record) error
}
```

`eventlog.Projector` is the third small interface and belongs to the same seam
as `Validator`: it applies a record to derived state and returns a `commit
func()` the store runs only after the transaction commits.

A new domain therefore supplies one `Reducer`, one `Validator`/`Projector` pair
(usually the same object — a validator that asks "would my reducer accept
this?"), and one command-to-`Record` encoder. Nothing else changes.
[swapping-the-domain](../swapping-the-domain.md) replaces inventory with a
second toy domain end to end as proof, and its worked example compiles and is
tested in CI.

The boundary is enforced, not asserted. `arch/arch_test.go` loads the real
import graph with `golang.org/x/tools/go/packages` and fails if `eventlog`,
`clock`, `sync` or `projection` can reach `domain` by any path, direct or
transitive. A second test in the same file points the check at
`transport/grpc`, which legitimately does import `domain`, and fails if no path
is found — so the guard cannot pass vacuously.

An unknown `Type` during projection is a hard error (`eventlog.ErrUnknownType`),
never a skipped record. A reference architecture must not teach silent
divergence: two nodes with different binaries must fail loudly, not quietly
compute different states.

## Consequences

Good: the engine is reusable, the claim is machine-checked, and the extension
surface is small enough to state in one paragraph. `projection`'s own test folds
the same records through two unrelated reducers with different state types,
which is the seam demonstrated rather than described.

Bad, and accepted:

- Payloads are opaque, so the engine cannot validate, index, or migrate them.
  Payload schema evolution is entirely the domain's problem, and this repo does
  not solve it.
- Every domain event costs a JSON encode/decode. Readability over throughput.
- `Fold` replays from the beginning every time; with no snapshots, startup cost
  grows with history.
- Generics push some errors to instantiation sites, where the message is longer
  than a reader new to Go generics might like.

## Alternatives rejected

- **Typed events in the log via an interface with a registry.** Nicer at the
  call site, and it drags a domain type registry into `eventlog`, which is
  exactly the leak the architecture test exists to prevent.
- **Skip unknown record types for forward compatibility.** Tempting, and it
  makes divergence silent. Rejected on teaching grounds.
- **Code generation per domain.** Removes the encode cost and replaces a
  readable seam with a build step.
