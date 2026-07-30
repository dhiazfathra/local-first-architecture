# localfirst-go Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `localfirst-go/`, a readable reference architecture for local-first event sourcing: a domain-agnostic SQLite event log, hybrid logical clock, generic projection driver and symmetric gRPC sync engine, demonstrated with a minimal inventory domain and a `make demo` that shows a node working offline and converging.

**Architecture:** Six packages. `eventlog`, `clock`, `sync`, `projection` are domain-agnostic and never import `domain` (enforced by an architecture test that parses imports). The domain plugs in through exactly two interfaces: `projection.Reducer[S]` (replay) and `eventlog.Validator` (pre-append invariant check inside the append transaction). Conflicts are removed by construction: each node exclusively owns its locations, so central sums per-node balances instead of merging them.

**Tech Stack:** Go (latest stable, generics required), `modernc.org/sqlite` (pure Go, no cgo), `google.golang.org/grpc` + `google.golang.org/protobuf`, `github.com/jackc/pgx/v5` (central store), Docker Compose (demo), `golangci-lint`.

## Concepts for a reader with zero context

Read this once; every task assumes it.

- **Event sourcing:** the durable truth is an append-only list of immutable facts ("received 5 of SKU-1 at A"). Current state ("A holds 5") is *derived* by replaying facts. State can always be rebuilt; facts are never updated in place.
- **Local-first:** each node keeps its own complete log on local disk and can accept writes with no network. Sync is a background reconciliation, never a precondition for work.
- **Hybrid logical clock (HLC):** a timestamp `(wall_millis, logical_counter, node_id)`. It tracks wall time closely enough for humans, but never goes backwards even if the OS clock jumps back, because when wall time does not advance the logical counter increments instead. Node ID is the final tiebreak so any two timestamps are totally ordered. Here it is used only for **ordering and audit**, not conflict resolution — ownership means there is nothing to resolve.
- **Version vector:** `map[nodeID]highestSeqSeen`. "What I already have, per author." A peer sends its vector; you reply with everything you hold above those marks.
- **Idempotent append:** records are keyed `(node_id, seq)`. Re-delivering a record is an `INSERT OR IGNORE` no-op. This one line is what makes an at-least-once, resumable sync protocol safe.

## Global Constraints

- Module path: `github.com/dhiazfathra/local-first-architecture/localfirst-go`, located in top-level dir `localfirst-go/`, its own `go.mod`. No Go code exists in the repo yet.
- Go latest stable. Generics required (`Reducer[S any]`, `Fold[S any]`).
- SQLite driver: `modernc.org/sqlite` only. No cgo anywhere (`CGO_ENABLED=0` must build).
- Central store: Postgres via `github.com/jackc/pgx/v5`.
- Sync: gRPC bidirectional stream, **one** `Frame` message type in both directions.
- `eventlog`, `clock`, `sync`, `projection` MUST NOT import `domain`. Enforced by test, not convention.
- Coding standards (hard requirements): minimal boilerplate, SOLID, DRY, **100% test coverage** — every function, branch and edge case tested.
- Every task ends with `go test ./... -cover` and `golangci-lint run` passing, both clean, before its commit step. No task concludes with failing or skipped tests.
- Unknown record type during projection is a **hard error**, never skipped.
- Documentation is a deliverable: five ADRs, `docs/architecture.md` (mermaid), `docs/swapping-the-domain.md`, `docs/limitations.md`, `README.md`.

## File Structure

All paths relative to `localfirst-go/`.

| File | Responsibility |
| --- | --- |
| `go.mod`, `Makefile`, `.golangci.yml` | module, build/test/lint/demo entrypoints |
| `clock/hlc.go`, `clock/hlc_test.go` | HLC value type + monotonic clock |
| `eventlog/record.go` | `Record`, `VersionVector`, `Reader`, `Validator`, `Projector` |
| `eventlog/store.go` | SQLite store: schema, `Append`, `Merge`, `Since`, `Version`, cursors |
| `eventlog/store_test.go` | idempotency, validator rollback, replay determinism |
| `projection/fold.go`, `projection/fold_test.go` | generic `Reducer[S]` + `Fold[S]` |
| `domain/events.go` | the three events, type constants, JSON codec |
| `domain/reducer.go` | `State`, `BalanceReducer` |
| `domain/inventory.go` | commands, ownership, negative-balance invariant |
| `domain/*_test.go` | table-driven domain tests |
| `sync/sync.proto`, `sync/syncpb/*` | `Frame`/`Hello`/`Batch`/`Ack` |
| `sync/session.go` | symmetric session logic (used by both sides) |
| `sync/server.go`, `sync/client.go` | gRPC server impl + resumable client loop |
| `sync/sync_test.go` | in-process transport, catch-up, resume, duplicate batch |
| `transport/grpc/node.proto`, `node.go` | node client API |
| `cmd/node/main.go`, `cmd/central/main.go` | binaries |
| `central/pgstore.go` | Postgres-backed log + global sum view |
| `arch/arch_test.go` | import-boundary architecture test |
| `demo/scenario.go`, `demo/scenario_test.go` | 7-step demo scenario + its in-process integration test |
| `demo/env.go`, `demo/env_test.go`, `demo/env_internal_test.go` | container-driving environment (gRPC + real network partition) |
| `demo/cmd/demo/main.go` | the `make demo` entrypoint |
| `Dockerfile`, `docker-compose.yml` | both binaries; Postgres, central, 3 nodes |
| `examples/tasklist/` | second domain, proving the engine seam (Task 13) |
| `docs/adr/000{1..5}-*.md` | the five decision records |
| `docs/architecture.md`, `docs/limitations.md`, `docs/swapping-the-domain.md`, `README.md` | the prose deliverables |
| `docs/links_test.go` | guard: documented files exist, relative links resolve |

---

### Task 1: Module bootstrap, Makefile, lint config

**Files:**
- Create: `localfirst-go/go.mod`, `localfirst-go/Makefile`, `localfirst-go/.golangci.yml`, `localfirst-go/doc.go`, `localfirst-go/doc_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: module path `github.com/dhiazfathra/local-first-architecture/localfirst-go`; `make test`, `make lint`, `make demo` targets; constant `localfirst.Version = "0.1.0"`.

- [ ] **Step 1: Create the module**

```bash
mkdir -p localfirst-go && cd localfirst-go
go mod init github.com/dhiazfathra/local-first-architecture/localfirst-go
```

- [ ] **Step 2: Write the failing test**

`localfirst-go/doc_test.go`:

```go
package localfirst_test

import (
	"testing"

	localfirst "github.com/dhiazfathra/local-first-architecture/localfirst-go"
)

func TestVersion(t *testing.T) {
	if localfirst.Version == "" {
		t.Fatal("Version must not be empty")
	}
}
```

- [ ] **Step 3: Run it and watch it fail**

Run: `cd localfirst-go && go test ./... -run TestVersion`
Expected: FAIL — `undefined: localfirst.Version`.

- [ ] **Step 4: Write the package doc file**

`localfirst-go/doc.go`:

```go
// Package localfirst is a readable reference architecture for local-first
// event sourcing.
//
// The engine packages (clock, eventlog, projection, sync) are domain-agnostic:
// they move opaque event bytes and know nothing about inventory. The domain
// package supplies interpretation through two seams, projection.Reducer and
// eventlog.Validator. See docs/architecture.md.
package localfirst

// Version is the reference-architecture revision, reported by both binaries.
const Version = "0.1.0"
```

- [ ] **Step 5: Add the Makefile**

`localfirst-go/Makefile`:

```make
.PHONY: test lint proto demo demo-down

test:
	CGO_ENABLED=0 go test ./... -cover

lint:
	golangci-lint run

proto:
	protoc --go_out=. --go_opt=module=github.com/dhiazfathra/local-first-architecture/localfirst-go \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/dhiazfathra/local-first-architecture/localfirst-go \
	       sync/sync.proto transport/grpc/node.proto

demo:
	docker compose up -d --build
	go run ./demo/cmd/demo
	docker compose down -v

demo-down:
	docker compose down -v
```

- [ ] **Step 6: Add the lint config**

`localfirst-go/.golangci.yml`:

```yaml
version: "2"
linters:
  enable:
    - errcheck
    - govet
    - ineffassign
    - staticcheck
    - unused
    - revive
    - misspell
formatters:
  enable:
    - gofmt
issues:
  exclude-rules:
    - path: syncpb/
      linters: [revive, staticcheck]
```

- [ ] **Step 7: Verify green and clean**

Run: `cd localfirst-go && go test ./... -cover && golangci-lint run`
Expected: `ok  …/localfirst-go  coverage: [no statements]` or 100%, and no lint output.

- [ ] **Step 8: Commit**

```bash
git add localfirst-go
git commit -m "chore(localfirst-go): bootstrap module, Makefile, lint config"
```

---

### Task 2: `clock/` — hybrid logical clock

**Why:** every record carries a timestamp that must be totally ordered and never move backwards, even when the host clock does. Nodes also *observe* peer timestamps during sync so their own clock never lags what they have already seen.

**Files:**
- Create: `localfirst-go/clock/hlc.go`
- Test: `localfirst-go/clock/hlc_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type HLC struct { Wall int64; Logical uint32; NodeID string }`
  - `func (h HLC) Compare(o HLC) int`, `func (h HLC) Before(o HLC) bool`, `func (h HLC) String() string`
  - `type Clock struct{…}`, `func New(nodeID string, now func() time.Time) *Clock`
  - `func (c *Clock) Now() HLC`, `func (c *Clock) Observe(remote HLC) HLC`

- [ ] **Step 1: Write the failing tests**

`localfirst-go/clock/hlc_test.go`:

```go
package clock_test

import (
	"sync"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
)

// fakeNow returns times from a slice, repeating the last one forever.
func fakeNow(ms ...int64) func() time.Time {
	i := 0
	return func() time.Time {
		v := ms[i]
		if i < len(ms)-1 {
			i++
		}
		return time.UnixMilli(v)
	}
}

func TestNowAdvancesWithWallClock(t *testing.T) {
	c := clock.New("n1", fakeNow(1000, 1001))
	first, second := c.Now(), c.Now()
	if first.Wall != 1000 || first.Logical != 0 {
		t.Fatalf("first = %v, want wall 1000 logical 0", first)
	}
	if second.Wall != 1001 || second.Logical != 0 {
		t.Fatalf("second = %v, want wall 1001 logical 0", second)
	}
}

func TestNowUsesLogicalCounterWhenWallStalls(t *testing.T) {
	c := clock.New("n1", fakeNow(1000))
	for i, want := range []uint32{0, 1, 2} {
		got := c.Now()
		if got.Wall != 1000 || got.Logical != want {
			t.Fatalf("call %d = %v, want wall 1000 logical %d", i, got, want)
		}
	}
}

func TestNowIsMonotonicAcrossBackwardsJump(t *testing.T) {
	c := clock.New("n1", fakeNow(2000, 1000, 1000))
	first := c.Now()
	for i := 0; i < 2; i++ {
		got := c.Now()
		if !first.Before(got) {
			t.Fatalf("after backwards jump got %v, not after %v", got, first)
		}
		if got.Wall != 2000 {
			t.Fatalf("wall regressed to %d", got.Wall)
		}
		first = got
	}
}

func TestObserve(t *testing.T) {
	tests := []struct {
		name        string
		localWall   int64
		remote      clock.HLC
		wantWall    int64
		wantLogical uint32
	}{
		{"remote ahead adopts remote wall", 1000, clock.HLC{Wall: 5000, Logical: 3, NodeID: "n2"}, 5000, 4},
		{"remote equal bumps logical", 1000, clock.HLC{Wall: 1000, Logical: 7, NodeID: "n2"}, 1000, 8},
		{"remote behind keeps local", 9000, clock.HLC{Wall: 10, Logical: 1, NodeID: "n2"}, 9000, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := clock.New("n1", fakeNow(tc.localWall))
			got := c.Observe(tc.remote)
			if got.Wall != tc.wantWall || got.Logical != tc.wantLogical {
				t.Fatalf("got %v, want wall %d logical %d", got, tc.wantWall, tc.wantLogical)
			}
			if got.NodeID != "n1" {
				t.Fatalf("NodeID = %q, want n1", got.NodeID)
			}
			if next := c.Now(); !got.Before(next) {
				t.Fatalf("Now() after Observe returned %v, not after %v", next, got)
			}
		})
	}
}

func TestCompareTotalOrder(t *testing.T) {
	a := clock.HLC{Wall: 1, Logical: 0, NodeID: "a"}
	b := clock.HLC{Wall: 1, Logical: 0, NodeID: "b"}
	c := clock.HLC{Wall: 1, Logical: 1, NodeID: "a"}
	d := clock.HLC{Wall: 2, Logical: 0, NodeID: "a"}
	tests := []struct {
		name string
		x, y clock.HLC
		want int
	}{
		{"equal", a, a, 0},
		{"node id tiebreak", a, b, -1},
		{"node id tiebreak reversed", b, a, 1},
		{"logical beats equal wall", a, c, -1},
		{"wall dominates", c, d, -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.x.Compare(tc.y); got != tc.want {
				t.Fatalf("Compare = %d, want %d", got, tc.want)
			}
			if got := tc.x.Before(tc.y); got != (tc.want < 0) {
				t.Fatalf("Before = %v, want %v", got, tc.want < 0)
			}
		})
	}
}

func TestString(t *testing.T) {
	got := clock.HLC{Wall: 1700000000000, Logical: 2, NodeID: "n1"}.String()
	if want := "1700000000000.2@n1"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestClockIsSafeForConcurrentUse(t *testing.T) {
	c := clock.New("n1", time.Now)
	var wg sync.WaitGroup
	seen := make(chan clock.HLC, 200)
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); seen <- c.Now() }()
		go func() { defer wg.Done(); seen <- c.Observe(clock.HLC{Wall: 1, NodeID: "n2"}) }()
	}
	wg.Wait()
	close(seen)
	uniq := map[clock.HLC]bool{}
	for h := range seen {
		if uniq[h] {
			t.Fatalf("duplicate timestamp %v issued", h)
		}
		uniq[h] = true
	}
}

func TestNewDefaultsToWallClock(t *testing.T) {
	c := clock.New("n1", nil)
	if got := c.Now(); got.Wall == 0 {
		t.Fatal("New(nil) must default to the system clock")
	}
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd localfirst-go && go test ./clock/... -race`
Expected: FAIL — `no required module provides package …/clock` / undefined identifiers.

- [ ] **Step 3: Implement**

`localfirst-go/clock/hlc.go`:

```go
// Package clock implements a hybrid logical clock (HLC).
//
// An HLC timestamp is (wall millis, logical counter, node id). It stays close
// to wall time so humans can read it, but never regresses: when wall time does
// not advance — including when the host clock jumps backwards — the logical
// counter increments instead. The node id is the final tiebreak, which makes
// any two timestamps totally ordered.
//
// In this architecture HLCs are used for ordering and audit ONLY. Because each
// node exclusively owns its keys there is no conflict to resolve, so nothing
// here implements last-writer-wins. See docs/adr/0003-hlc-for-ordering-only.md.
package clock

import (
	"strconv"
	"sync"
	"time"
)

// HLC is a hybrid logical clock timestamp.
type HLC struct {
	Wall    int64  // Unix milliseconds.
	Logical uint32 // Ticks within the same wall millisecond.
	NodeID  string // Final tiebreak; also records who issued the timestamp.
}

// Compare orders h against o: -1 if h sorts first, 0 if identical, 1 otherwise.
func (h HLC) Compare(o HLC) int {
	switch {
	case h.Wall != o.Wall:
		return sign(h.Wall - o.Wall)
	case h.Logical != o.Logical:
		return sign(int64(h.Logical) - int64(o.Logical))
	case h.NodeID != o.NodeID:
		if h.NodeID < o.NodeID {
			return -1
		}
		return 1
	}
	return 0
}

// Before reports whether h sorts strictly before o.
func (h HLC) Before(o HLC) bool { return h.Compare(o) < 0 }

// String renders the timestamp as wall.logical@node.
func (h HLC) String() string {
	return strconv.FormatInt(h.Wall, 10) + "." + strconv.FormatUint(uint64(h.Logical), 10) + "@" + h.NodeID
}

func sign(d int64) int {
	if d < 0 {
		return -1
	}
	return 1
}

// Clock issues strictly increasing HLC timestamps for one node.
// All methods are safe for concurrent use.
type Clock struct {
	nodeID string
	now    func() time.Time

	mu   sync.Mutex
	last HLC
}

// New returns a Clock for nodeID. now may be nil, in which case time.Now is
// used; tests inject a fake to exercise wall-clock jumps.
func New(nodeID string, now func() time.Time) *Clock {
	if now == nil {
		now = time.Now
	}
	return &Clock{nodeID: nodeID, now: now}
}

// Now returns the next timestamp, strictly greater than every timestamp this
// Clock has previously returned or observed.
func (c *Clock) Now() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.advance(c.now().UnixMilli())
}

// Observe folds a peer timestamp in, then returns the next local timestamp.
// It guarantees the result sorts after remote, so a node's clock can never lag
// a record it has already accepted.
func (c *Clock) Observe(remote HLC) HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.advance(c.now().UnixMilli(), remote)
}

// advance implements the HLC update rule against the highest of: physical
// time, the last issued timestamp, and any observed peer timestamps.
func (c *Clock) advance(physical int64, peers ...HLC) HLC {
	next := HLC{Wall: physical, Logical: 0, NodeID: c.nodeID}
	for _, p := range append(peers, c.last) {
		switch {
		case p.Wall > next.Wall:
			next.Wall, next.Logical = p.Wall, p.Logical+1
		case p.Wall == next.Wall && p.Logical >= next.Logical:
			next.Logical = p.Logical + 1
		}
	}
	c.last = next
	return next
}
```

- [ ] **Step 4: Run the tests**

Run: `cd localfirst-go && go test ./clock/... -race -cover`
Expected: PASS, `coverage: 100.0% of statements`. If coverage is below 100%, add the missing table row — do not lower the bar.

- [ ] **Step 5: Lint**

Run: `cd localfirst-go && golangci-lint run`
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add localfirst-go/clock
git commit -m "feat(clock): hybrid logical clock with monotonic Now and Observe"
```

---

### Task 3: `eventlog/` value types and the plug-in seams

**Why:** this file is the whole contract between the engine and any domain. The engine moves `Record`s with an opaque `Type` string and `Payload` bytes. It asks a `Validator` whether an append is allowed, and a `Projector` to fold it into derived state — both *inside* the append transaction, so a rejected command leaves no trace and a projection can never disagree with the log.

**Files:**
- Create: `localfirst-go/eventlog/record.go`
- Test: `localfirst-go/eventlog/record_test.go`

**Interfaces:**
- Consumes: `clock.HLC` (Task 2).
- Produces:
  - `type Record struct { NodeID string; Seq uint64; Clock clock.HLC; Type string; Payload []byte }`
  - `type VersionVector map[string]uint64` with `Get(node string) uint64`, `Observe(node string, seq uint64)`, `Clone() VersionVector`, `Equal(o VersionVector) bool`
  - `type Reader interface { Since(ctx context.Context, vv VersionVector) ([]Record, error) }`
  - `type Validator interface { Check(r Record) error }`
  - `type Projector interface { Project(r Record) (commit func(), err error) }`
  - `func (r Record) Less(o Record) bool`
  - `var ErrUnknownType = errors.New("eventlog: unknown record type")`

- [ ] **Step 1: Write the failing tests**

`localfirst-go/eventlog/record_test.go`:

```go
package eventlog_test

import (
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

func TestVersionVector(t *testing.T) {
	vv := eventlog.VersionVector{}
	if got := vv.Get("n1"); got != 0 {
		t.Fatalf("Get on empty = %d, want 0", got)
	}
	vv.Observe("n1", 5)
	vv.Observe("n1", 3) // regression must be ignored: vectors only grow
	if got := vv.Get("n1"); got != 5 {
		t.Fatalf("Get = %d, want 5", got)
	}
	vv.Observe("n2", 1)

	clone := vv.Clone()
	if !clone.Equal(vv) {
		t.Fatal("clone must equal source")
	}
	clone.Observe("n2", 9)
	if vv.Get("n2") != 1 {
		t.Fatal("mutating the clone must not touch the source")
	}
	if clone.Equal(vv) {
		t.Fatal("diverged vectors must not be equal")
	}
	if (eventlog.VersionVector{"n1": 5}).Equal(vv) {
		t.Fatal("vectors of different length must not be equal")
	}
	var nilVV eventlog.VersionVector
	if nilVV.Get("n1") != 0 {
		t.Fatal("Get on a nil vector must be 0")
	}
	if !nilVV.Equal(eventlog.VersionVector{}) {
		t.Fatal("nil and empty vectors are equal")
	}
}

func TestRecordLess(t *testing.T) {
	mk := func(node string, seq uint64, wall int64) eventlog.Record {
		return eventlog.Record{NodeID: node, Seq: seq, Clock: clock.HLC{Wall: wall, NodeID: node}}
	}
	tests := []struct {
		name string
		a, b eventlog.Record
		want bool
	}{
		{"earlier clock first", mk("n1", 1, 10), mk("n2", 1, 20), true},
		{"later clock not first", mk("n2", 1, 20), mk("n1", 1, 10), false},
		{"same clock falls back to seq", mk("n1", 1, 10), mk("n1", 2, 10), true},
		{"identical is not less", mk("n1", 1, 10), mk("n1", 1, 10), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Less(tc.b); got != tc.want {
				t.Fatalf("Less = %v, want %v", got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd localfirst-go && go test ./eventlog/...`
Expected: FAIL — undefined `eventlog.VersionVector`, `eventlog.Record`.

- [ ] **Step 3: Implement**

`localfirst-go/eventlog/record.go`:

```go
// Package eventlog is the domain-agnostic append-only log.
//
// It stores Records — opaque (Type, Payload) pairs stamped with the authoring
// node, a per-node sequence number and an HLC timestamp — and knows nothing
// about what they mean. A domain plugs in through two interfaces defined here,
// Validator and Projector, plus projection.Reducer for replay.
package eventlog

import (
	"context"
	"errors"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
)

// ErrUnknownType is returned when a domain is asked to interpret a record type
// it does not know. Projection MUST fail hard on it rather than skip the
// record: silently ignoring unknown events is how replicas silently diverge.
var ErrUnknownType = errors.New("eventlog: unknown record type")

// Record is one immutable fact. (NodeID, Seq) is the primary key: the authoring
// node assigns Seq densely from 1, so re-delivering a record is a no-op insert.
type Record struct {
	NodeID  string    // Who authored it.
	Seq     uint64    // Author's own dense sequence, starting at 1.
	Clock   clock.HLC // Ordering and audit only.
	Type    string    // Domain-defined discriminator.
	Payload []byte    // Domain-defined encoding.
}

// Less orders records for replay: HLC first, then the author's sequence so the
// order is total and identical on every replica.
func (r Record) Less(o Record) bool {
	if c := r.Clock.Compare(o.Clock); c != 0 {
		return c < 0
	}
	return r.Seq < o.Seq
}

// VersionVector maps node id to the highest contiguous Seq held for that node.
// It is the entire "what do you already have?" question in the sync protocol.
type VersionVector map[string]uint64

// Get returns the highest Seq held for node, or 0 if none. Safe on a nil map.
func (v VersionVector) Get(node string) uint64 { return v[node] }

// Observe raises the mark for node to seq. Marks never decrease, which is what
// makes a peer's stale or regressed vector harmless.
func (v VersionVector) Observe(node string, seq uint64) {
	if seq > v[node] {
		v[node] = seq
	}
}

// Clone returns an independent copy.
func (v VersionVector) Clone() VersionVector {
	out := make(VersionVector, len(v))
	for k, s := range v {
		out[k] = s
	}
	return out
}

// Equal reports whether the two vectors hold the same marks.
func (v VersionVector) Equal(o VersionVector) bool {
	if len(v) != len(o) {
		return false
	}
	for k, s := range v {
		if o[k] != s {
			return false
		}
	}
	return true
}

// Reader is the read half of a log. There is deliberately no query language:
// everything downstream is built by folding records in order.
type Reader interface {
	// Since returns every held record the caller lacks according to vv,
	// in replay order. A nil vv means "everything".
	Since(ctx context.Context, vv VersionVector) ([]Record, error)
}

// Validator decides whether a locally authored record may be appended.
//
// Check is called INSIDE the append transaction, against the current projected
// state, before the row is visible. Returning an error rolls the whole
// transaction back, so a rejected command leaves no trace in the log.
type Validator interface {
	Check(r Record) error
}

// Projector folds a record into derived in-memory state.
//
// Project is called inside the append transaction and must NOT mutate anything
// observable; it returns a commit closure that the store calls only after the
// transaction commits. That two-phase shape is why a projection can never
// disagree with the log: either both happen or neither does.
type Projector interface {
	Project(r Record) (commit func(), err error)
}
```

- [ ] **Step 4: Run the tests**

Run: `cd localfirst-go && go test ./eventlog/... -cover`
Expected: PASS, 100% of the statements in `record.go`.

- [ ] **Step 5: Lint, then commit**

```bash
cd localfirst-go && golangci-lint run
git add localfirst-go/eventlog
git commit -m "feat(eventlog): Record, VersionVector and the Validator/Projector seams"
```

---

### Task 4: `eventlog/` SQLite store — idempotent append, validator rollback, cursors

**Why:** one SQLite file per node is the local source of truth. The composite primary key `(node_id, seq)` is the highest-leverage idea in the repo: with `INSERT OR IGNORE`, duplicate delivery costs nothing, which is what lets the sync protocol be at-least-once and resumable without any deduplication logic.

**Files:**
- Create: `localfirst-go/eventlog/store.go`
- Test: `localfirst-go/eventlog/store_test.go`

**Interfaces:**
- Consumes: `Record`, `VersionVector`, `Validator`, `Projector` (Task 3); `clock.HLC` (Task 2).
- Produces:
  - `func Open(path, nodeID string) (*Store, error)`, `func (s *Store) Close() error`, `func (s *Store) NodeID() string`
  - `func (s *Store) Append(ctx context.Context, typ string, payload []byte, ts clock.HLC, v Validator, p Projector) (Record, error)`
  - `func (s *Store) Merge(ctx context.Context, recs []Record, p Projector) (int, error)`
  - `func (s *Store) Since(ctx context.Context, vv VersionVector) ([]Record, error)` — satisfies `Reader`
  - `func (s *Store) Version(ctx context.Context) (VersionVector, error)`
  - `func (s *Store) Cursor(ctx context.Context, peer string) (uint64, error)`, `func (s *Store) SetCursor(ctx context.Context, peer string, seq uint64) error`

- [ ] **Step 1: Add the driver dependency**

```bash
cd localfirst-go && go get modernc.org/sqlite
```

- [ ] **Step 2: Write the failing tests**

`localfirst-go/eventlog/store_test.go`:

```go
package eventlog_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// countProjector records what it was asked to project and whether the store
// committed it, so tests can assert the two-phase contract.
type countProjector struct {
	staged, committed []string
	err               error
}

func (c *countProjector) Project(r eventlog.Record) (func(), error) {
	if c.err != nil {
		return nil, c.err
	}
	c.staged = append(c.staged, r.Type)
	return func() { c.committed = append(c.committed, r.Type) }, nil
}

type rejectAll struct{ err error }

func (r rejectAll) Check(eventlog.Record) error { return r.err }

func open(t *testing.T, nodeID string) *eventlog.Store {
	t.Helper()
	s, err := eventlog.Open(filepath.Join(t.TempDir(), nodeID+".db"), nodeID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenRejectsBadPath(t *testing.T) {
	if _, err := eventlog.Open(filepath.Join(t.TempDir(), "nope", "x.db"), "n1"); err == nil {
		t.Fatal("Open into a missing directory must fail")
	}
}

func TestAppendAssignsDenseSequences(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	c := clock.New("n1", nil)
	p := &countProjector{}
	for i := uint64(1); i <= 3; i++ {
		got, err := s.Append(ctx, "T", []byte(`{}`), c.Now(), nil, p)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if got.Seq != i || got.NodeID != "n1" {
			t.Fatalf("Append = %+v, want seq %d node n1", got, i)
		}
	}
	if len(p.committed) != 3 {
		t.Fatalf("committed %d projections, want 3", len(p.committed))
	}
	if s.NodeID() != "n1" {
		t.Fatalf("NodeID = %q", s.NodeID())
	}
}

func TestAppendResumesSequenceAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "n1.db")
	first, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := first.Append(ctx, "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	second, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close() }()
	got, err := second.Append(ctx, "T", nil, clock.HLC{Wall: 2, NodeID: "n1"}, nil, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got.Seq != 2 {
		t.Fatalf("Seq after reopen = %d, want 2", got.Seq)
	}
}

func TestValidatorRejectionLeavesNoRecord(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	boom := errors.New("balance would go negative")
	p := &countProjector{}

	_, err := s.Append(ctx, "T", []byte(`{}`), clock.HLC{Wall: 1, NodeID: "n1"}, rejectAll{boom}, p)
	if !errors.Is(err, boom) {
		t.Fatalf("Append error = %v, want %v", err, boom)
	}
	recs, err := s.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("rejected append left %d records", len(recs))
	}
	if len(p.staged) != 0 || len(p.committed) != 0 {
		t.Fatal("rejected append must not reach the projector")
	}
	// The sequence number must not be burned either.
	next, err := s.Append(ctx, "T", nil, clock.HLC{Wall: 2, NodeID: "n1"}, nil, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if next.Seq != 1 {
		t.Fatalf("Seq = %d after rejection, want 1", next.Seq)
	}
}

func TestProjectorFailureRollsBackTheAppend(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	boom := errors.New("unknown type")
	_, err := s.Append(ctx, "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, &countProjector{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("Append error = %v, want %v", err, boom)
	}
	recs, _ := s.Since(ctx, nil)
	if len(recs) != 0 {
		t.Fatalf("projector failure left %d records", len(recs))
	}
}

func TestMergeIsIdempotent(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	remote := []eventlog.Record{
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 10, NodeID: "n2"}, Type: "T", Payload: []byte("a")},
		{NodeID: "n2", Seq: 2, Clock: clock.HLC{Wall: 11, NodeID: "n2"}, Type: "T", Payload: []byte("b")},
	}
	p := &countProjector{}
	n, err := s.Merge(ctx, remote, p)
	if err != nil || n != 2 {
		t.Fatalf("Merge = (%d, %v), want (2, nil)", n, err)
	}
	// Re-delivering the same batch, plus one new record, inserts only the new one.
	n, err = s.Merge(ctx, append(remote, eventlog.Record{
		NodeID: "n2", Seq: 3, Clock: clock.HLC{Wall: 12, NodeID: "n2"}, Type: "T",
	}), p)
	if err != nil || n != 1 {
		t.Fatalf("re-merge = (%d, %v), want (1, nil)", n, err)
	}
	recs, _ := s.Since(ctx, nil)
	if len(recs) != 3 {
		t.Fatalf("held %d records, want 3", len(recs))
	}
	if len(p.committed) != 3 {
		t.Fatalf("projected %d records, want 3 (duplicates must not re-project)", len(p.committed))
	}
}

func TestMergeAdvancesOwnSequence(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	// Central-style merge of records authored by this node id (e.g. restored
	// from a peer): the local counter must jump past them.
	if _, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n1", Seq: 7, Clock: clock.HLC{Wall: 1, NodeID: "n1"}, Type: "T"},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	got, err := s.Append(ctx, "T", nil, clock.HLC{Wall: 2, NodeID: "n1"}, nil, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got.Seq != 8 {
		t.Fatalf("Seq = %d, want 8", got.Seq)
	}
}

func TestMergeEmptyBatchIsANoOp(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	if n, err := s.Merge(ctx, nil, nil); n != 0 || err != nil {
		t.Fatalf("Merge(nil) = (%d, %v), want (0, nil)", n, err)
	}
}

func TestMergeProjectorFailureRollsBack(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	boom := errors.New("unknown type")
	_, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n2"}, Type: "?"},
	}, &countProjector{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("Merge error = %v, want %v", err, boom)
	}
	if recs, _ := s.Since(ctx, nil); len(recs) != 0 {
		t.Fatalf("rolled-back merge left %d records", len(recs))
	}
}

func TestSinceFiltersByVersionVectorAndReplaysDeterministically(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	if _, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n2", Seq: 2, Clock: clock.HLC{Wall: 30, NodeID: "n2"}, Type: "B"},
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 10, NodeID: "n2"}, Type: "A"},
		{NodeID: "n3", Seq: 1, Clock: clock.HLC{Wall: 20, NodeID: "n3"}, Type: "C"},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	all, err := s.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	var order []string
	for _, r := range all {
		order = append(order, r.Type)
	}
	if want := []string{"A", "C", "B"}; !equal(order, want) {
		t.Fatalf("replay order = %v, want %v (HLC order, insertion order irrelevant)", order, want)
	}
	// Replaying twice gives byte-identical results.
	again, _ := s.Since(ctx, nil)
	for i := range all {
		if all[i].Type != again[i].Type || all[i].Seq != again[i].Seq {
			t.Fatalf("replay is not deterministic at %d", i)
		}
	}

	partial, err := s.Since(ctx, eventlog.VersionVector{"n2": 1})
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(partial) != 2 || partial[0].Type != "C" || partial[1].Type != "B" {
		t.Fatalf("Since(vv) = %+v, want C then B", partial)
	}
	if got, err := s.Since(ctx, eventlog.VersionVector{"n2": 99, "n3": 99}); err != nil || len(got) != 0 {
		t.Fatalf("Since with a vector ahead of us = (%d, %v), want (0, nil)", len(got), err)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestVersion(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	vv, err := s.Version(ctx)
	if err != nil || len(vv) != 0 {
		t.Fatalf("Version on empty log = (%v, %v), want empty", vv, err)
	}
	if _, err := s.Append(ctx, "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n2", Seq: 4, Clock: clock.HLC{Wall: 2, NodeID: "n2"}, Type: "T"},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	vv, err = s.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if !vv.Equal(eventlog.VersionVector{"n1": 1, "n2": 4}) {
		t.Fatalf("Version = %v, want {n1:1, n2:4}", vv)
	}
}

func TestCursors(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	if got, err := s.Cursor(ctx, "central"); got != 0 || err != nil {
		t.Fatalf("unknown cursor = (%d, %v), want (0, nil)", got, err)
	}
	if err := s.SetCursor(ctx, "central", 5); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	if err := s.SetCursor(ctx, "central", 9); err != nil {
		t.Fatalf("SetCursor upsert: %v", err)
	}
	if got, err := s.Cursor(ctx, "central"); got != 9 || err != nil {
		t.Fatalf("Cursor = (%d, %v), want (9, nil)", got, err)
	}
}

func TestOperationsFailAfterClose(t *testing.T) {
	ctx := context.Background()
	s, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := s.Append(ctx, "T", nil, clock.HLC{}, nil, nil); err == nil {
		t.Error("Append after Close must fail")
	}
	if _, err := s.Merge(ctx, []eventlog.Record{{NodeID: "n2", Seq: 1}}, nil); err == nil {
		t.Error("Merge after Close must fail")
	}
	if _, err := s.Since(ctx, nil); err == nil {
		t.Error("Since after Close must fail")
	}
	if _, err := s.Version(ctx); err == nil {
		t.Error("Version after Close must fail")
	}
	if _, err := s.Cursor(ctx, "central"); err == nil {
		t.Error("Cursor after Close must fail")
	}
	if err := s.SetCursor(ctx, "central", 1); err == nil {
		t.Error("SetCursor after Close must fail")
	}
}
```

- [ ] **Step 3: Run and watch it fail**

Run: `cd localfirst-go && go test ./eventlog/...`
Expected: FAIL — `undefined: eventlog.Open`.

- [ ] **Step 4: Implement the store**

`localfirst-go/eventlog/store.go`:

```go
package eventlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"

	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
)

// schema is the entire storage design.
//
// PRIMARY KEY (node_id, seq) is the duplicate-delivery story: every write path
// uses INSERT OR IGNORE, so re-delivering a record the log already holds is a
// no-op. That is what makes the sync protocol safe to retry and resume.
const schema = `
CREATE TABLE IF NOT EXISTS records (
  node_id     TEXT    NOT NULL,
  seq         INTEGER NOT NULL,
  hlc_wall    INTEGER NOT NULL,
  hlc_logical INTEGER NOT NULL,
  type        TEXT    NOT NULL,
  payload     BLOB    NOT NULL,
  PRIMARY KEY (node_id, seq)
);
CREATE TABLE IF NOT EXISTS cursors (peer TEXT PRIMARY KEY, last_seq INTEGER NOT NULL);
`

// Store is one node's log: a single SQLite file.
type Store struct {
	db     *sql.DB
	nodeID string

	mu      sync.Mutex // serialises append: one author, dense sequence.
	nextSeq uint64
}

// Open opens (creating if needed) the log at path for nodeID.
func Open(path, nodeID string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("eventlog: open %s: %w", path, err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("eventlog: schema: %w", err)
	}
	s := &Store{db: db, nodeID: nodeID}
	if err := db.QueryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM records WHERE node_id = ?`, nodeID,
	).Scan(&s.nextSeq); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("eventlog: read own sequence: %w", err)
	}
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// NodeID returns the id this store authors records under.
func (s *Store) NodeID() string { return s.nodeID }

// Append validates, stores and projects one locally authored record in a single
// transaction: validate -> insert -> project. Any failure rolls all three back,
// so a rejected command leaves no trace and the projection can never disagree
// with the log. v and p may be nil.
func (s *Store) Append(
	ctx context.Context, typ string, payload []byte, ts clock.HLC, v Validator, p Projector,
) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec := Record{NodeID: s.nodeID, Seq: s.nextSeq + 1, Clock: ts, Type: typ, Payload: payload}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, fmt.Errorf("eventlog: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once committed

	if v != nil {
		if err := v.Check(rec); err != nil {
			return Record{}, fmt.Errorf("eventlog: append rejected: %w", err)
		}
	}
	if err := insert(ctx, tx, rec); err != nil {
		return Record{}, err
	}
	var commit func()
	if p != nil {
		if commit, err = p.Project(rec); err != nil {
			return Record{}, fmt.Errorf("eventlog: project: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Record{}, fmt.Errorf("eventlog: commit: %w", err)
	}
	if commit != nil {
		commit()
	}
	s.nextSeq = rec.Seq
	return rec, nil
}

// Merge stores records authored elsewhere and returns how many were new.
// Duplicates are ignored and are not re-projected, which is what makes an
// at-least-once sync protocol correct. p may be nil (a node that only tracks
// its own balances); central passes its global projector.
func (s *Store) Merge(ctx context.Context, recs []Record, p Projector) (int, error) {
	if len(recs) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("eventlog: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	inserted, highestOwn := 0, s.nextSeq
	var commits []func()
	for _, r := range recs {
		res, err := exec(ctx, tx, r)
		if err != nil {
			return 0, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("eventlog: rows affected: %w", err)
		}
		if n == 0 { // already held: nothing to project
			continue
		}
		inserted++
		if r.NodeID == s.nodeID && r.Seq > highestOwn {
			highestOwn = r.Seq
		}
		if p != nil {
			commit, err := p.Project(r)
			if err != nil {
				return 0, fmt.Errorf("eventlog: project %s/%d: %w", r.NodeID, r.Seq, err)
			}
			commits = append(commits, commit)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("eventlog: commit: %w", err)
	}
	for _, c := range commits {
		c()
	}
	s.nextSeq = highestOwn
	return inserted, nil
}

func insert(ctx context.Context, tx *sql.Tx, r Record) error {
	_, err := exec(ctx, tx, r)
	return err
}

func exec(ctx context.Context, tx *sql.Tx, r Record) (sql.Result, error) {
	res, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO records (node_id, seq, hlc_wall, hlc_logical, type, payload)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		r.NodeID, r.Seq, r.Clock.Wall, r.Clock.Logical, r.Type, nonNil(r.Payload))
	if err != nil {
		return nil, fmt.Errorf("eventlog: insert %s/%d: %w", r.NodeID, r.Seq, err)
	}
	return res, nil
}

// nonNil keeps the NOT NULL payload column happy for events with no fields.
func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// Since returns every held record the caller lacks according to vv, in replay
// order (HLC, then author sequence). A nil vv means "everything".
//
// ponytail: scans the log and filters in Go — clear, and correct for a
// reference architecture. Push the filter into SQL per node if logs get big.
func (s *Store) Since(ctx context.Context, vv VersionVector) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT node_id, seq, hlc_wall, hlc_logical, type, payload FROM records`)
	if err != nil {
		return nil, fmt.Errorf("eventlog: since: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.NodeID, &r.Seq, &r.Clock.Wall, &r.Clock.Logical, &r.Type, &r.Payload); err != nil {
			return nil, fmt.Errorf("eventlog: scan: %w", err)
		}
		r.Clock.NodeID = r.NodeID
		if r.Seq > vv.Get(r.NodeID) {
			out = append(out, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return out, nil
}

// Version returns the highest Seq held per author — this node's answer to
// "what do you already have?".
func (s *Store) Version(ctx context.Context) (VersionVector, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT node_id, MAX(seq) FROM records GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("eventlog: version: %w", err)
	}
	defer func() { _ = rows.Close() }()

	vv := VersionVector{}
	for rows.Next() {
		var node string
		var seq uint64
		if err := rows.Scan(&node, &seq); err != nil {
			return nil, fmt.Errorf("eventlog: scan version: %w", err)
		}
		vv.Observe(node, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: rows: %w", err)
	}
	return vv, nil
}

// Cursor returns how far this node has pushed to peer, 0 if never.
func (s *Store) Cursor(ctx context.Context, peer string) (uint64, error) {
	var seq uint64
	err := s.db.QueryRowContext(ctx, `SELECT last_seq FROM cursors WHERE peer = ?`, peer).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("eventlog: cursor %s: %w", peer, err)
	}
	return seq, nil
}

// SetCursor records how far this node has pushed to peer.
func (s *Store) SetCursor(ctx context.Context, peer string, seq uint64) error {
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO cursors (peer, last_seq) VALUES (?, ?)
		 ON CONFLICT(peer) DO UPDATE SET last_seq = excluded.last_seq`, peer, seq); err != nil {
		return fmt.Errorf("eventlog: set cursor %s: %w", peer, err)
	}
	return nil
}
```

- [ ] **Step 5: Run the tests**

Run: `cd localfirst-go && go test ./eventlog/... -race -cover`
Expected: PASS, `coverage: 100.0% of statements`. If `Since`'s `rows.Err()` or `Scan` branches are uncovered, that is acceptable *only* if `go tool cover -func` shows them as the sole gap — instead, cover them by adding a test that inserts a row with a NULL type directly (`s.DB()` is not exported, so use a second `sql.Open` against the same file in the test to `INSERT INTO records(node_id,seq,hlc_wall,hlc_logical,type,payload) VALUES('n9',1,1,0,NULL,x'')`, then assert `Since` returns a scan error).

- [ ] **Step 6: Verify the closed-store branches**

Run: `cd localfirst-go && go test ./eventlog/... -run TestOperationsFailAfterClose -v`
Expected: PASS with no `Error` output.

- [ ] **Step 7: Lint, then commit**

```bash
cd localfirst-go && golangci-lint run
git add localfirst-go/eventlog localfirst-go/go.mod localfirst-go/go.sum
git commit -m "feat(eventlog): SQLite store with idempotent append and validator seam"
```

---

### Task 5: `projection/` — generic `Fold` over a `Reducer[S]`

**Why:** this is the entire extension point for reading. `Fold` is generic over the state type, so the engine never names a domain type. The proof that the seam is real is a test that folds the *same* records through two unrelated reducers with different state types.

**Files:**
- Create: `localfirst-go/projection/fold.go`
- Test: `localfirst-go/projection/fold_test.go`

**Interfaces:**
- Consumes: `eventlog.Record`, `eventlog.Reader`, `eventlog.VersionVector` (Tasks 3–4).
- Produces:
  - `type Reducer[S any] interface { Zero() S; Apply(state S, r eventlog.Record) (S, error) }`
  - `func Fold[S any](ctx context.Context, log eventlog.Reader, r Reducer[S]) (S, error)`
  - `func FoldRecords[S any](r Reducer[S], recs []eventlog.Record) (S, error)`

- [ ] **Step 1: Write the failing tests**

`localfirst-go/projection/fold_test.go`:

```go
package projection_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// staticLog is an in-memory eventlog.Reader: Fold must not need a database.
type staticLog struct {
	recs []eventlog.Record
	err  error
}

func (s staticLog) Since(context.Context, eventlog.VersionVector) ([]eventlog.Record, error) {
	return s.recs, s.err
}

// --- reducer 1: state is an int64 total ---

type sumReducer struct{}

func (sumReducer) Zero() int64 { return 0 }
func (sumReducer) Apply(total int64, r eventlog.Record) (int64, error) {
	switch r.Type {
	case "add":
		return total + int64(len(r.Payload)), nil
	case "reset":
		return 0, nil
	default:
		return total, eventlog.ErrUnknownType
	}
}

// --- reducer 2: state is a map, a completely different shape ---

type countReducer struct{}

func (countReducer) Zero() map[string]int { return map[string]int{} }
func (countReducer) Apply(m map[string]int, r eventlog.Record) (map[string]int, error) {
	if r.Type != "add" && r.Type != "reset" {
		return m, eventlog.ErrUnknownType
	}
	m[r.Type]++
	return m, nil
}

func fixture() []eventlog.Record {
	mk := func(seq uint64, typ, payload string) eventlog.Record {
		return eventlog.Record{
			NodeID: "n1", Seq: seq, Clock: clock.HLC{Wall: int64(seq), NodeID: "n1"},
			Type: typ, Payload: []byte(payload),
		}
	}
	return []eventlog.Record{mk(1, "add", "abc"), mk(2, "add", "de"), mk(3, "reset", ""), mk(4, "add", "f")}
}

// TestFoldIsGenericOverUnrelatedReducers is the proof of the seam: one Fold,
// two reducers, two different state types, same records.
func TestFoldIsGenericOverUnrelatedReducers(t *testing.T) {
	ctx, log := context.Background(), staticLog{recs: fixture()}

	total, err := projection.Fold[int64](ctx, log, sumReducer{})
	if err != nil {
		t.Fatalf("Fold(sumReducer): %v", err)
	}
	if total != 1 {
		t.Fatalf("total = %d, want 1 (reset clears the 5 bytes before it)", total)
	}

	counts, err := projection.Fold[map[string]int](ctx, log, countReducer{})
	if err != nil {
		t.Fatalf("Fold(countReducer): %v", err)
	}
	if counts["add"] != 3 || counts["reset"] != 1 {
		t.Fatalf("counts = %v, want add:3 reset:1", counts)
	}
}

func TestFoldOnEmptyLogReturnsZero(t *testing.T) {
	got, err := projection.Fold[int64](context.Background(), staticLog{}, sumReducer{})
	if err != nil || got != 0 {
		t.Fatalf("Fold(empty) = (%d, %v), want (0, nil)", got, err)
	}
}

func TestFoldPropagatesReadError(t *testing.T) {
	boom := errors.New("disk gone")
	_, err := projection.Fold[int64](context.Background(), staticLog{err: boom}, sumReducer{})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

// TestFoldFailsHardOnUnknownType pins the error-handling rule: an unrecognised
// record type stops replay. Skipping it would teach silent divergence.
func TestFoldFailsHardOnUnknownType(t *testing.T) {
	log := staticLog{recs: []eventlog.Record{
		{NodeID: "n1", Seq: 1, Type: "add", Payload: []byte("x")},
		{NodeID: "n1", Seq: 2, Type: "FutureEvent"},
	}}
	_, err := projection.Fold[int64](context.Background(), log, sumReducer{})
	if !errors.Is(err, eventlog.ErrUnknownType) {
		t.Fatalf("err = %v, want ErrUnknownType", err)
	}
	if got := err.Error(); !contains(got, "n1/2") || !contains(got, "FutureEvent") {
		t.Fatalf("error %q must name the offending record and type", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestFoldRecords(t *testing.T) {
	got, err := projection.FoldRecords[int64](sumReducer{}, fixture())
	if err != nil || got != 1 {
		t.Fatalf("FoldRecords = (%d, %v), want (1, nil)", got, err)
	}
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd localfirst-go && go test ./projection/...`
Expected: FAIL — `undefined: projection.Fold`.

- [ ] **Step 3: Implement**

`localfirst-go/projection/fold.go`:

```go
// Package projection replays a log into derived state.
//
// It is domain-agnostic: Fold is generic over the state type S, so nothing here
// names an inventory concept. A new domain implements one Reducer and is done.
package projection

import (
	"context"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// Reducer interprets records. It is the entire read-side extension point.
//
// Apply must be pure with respect to the log: replaying the same records in the
// same order must always produce the same state, on any machine, forever.
type Reducer[S any] interface {
	// Zero returns the state before any record has been applied.
	Zero() S
	// Apply folds one record into state. An unrecognised r.Type must return an
	// error wrapping eventlog.ErrUnknownType — never silently ignore it.
	Apply(state S, r eventlog.Record) (S, error)
}

// Fold reads the whole log and replays it through r.
func Fold[S any](ctx context.Context, log eventlog.Reader, r Reducer[S]) (S, error) {
	recs, err := log.Since(ctx, nil)
	if err != nil {
		var zero S
		return zero, fmt.Errorf("projection: read log: %w", err)
	}
	return FoldRecords(r, recs)
}

// FoldRecords replays an already-read slice. Useful for catch-up after a sync
// batch, and for tests that do not want a log at all.
func FoldRecords[S any](r Reducer[S], recs []eventlog.Record) (S, error) {
	state := r.Zero()
	for _, rec := range recs {
		next, err := r.Apply(state, rec)
		if err != nil {
			var zero S
			return zero, fmt.Errorf("projection: record %s/%d type %q: %w",
				rec.NodeID, rec.Seq, rec.Type, err)
		}
		state = next
	}
	return state, nil
}
```

- [ ] **Step 4: Run the tests**

Run: `cd localfirst-go && go test ./projection/... -cover`
Expected: PASS, `coverage: 100.0% of statements`.

- [ ] **Step 5: Lint, then commit**

```bash
cd localfirst-go && golangci-lint run
git add localfirst-go/projection
git commit -m "feat(projection): generic Fold over Reducer[S] with hard failure on unknown types"
```

---

### Task 6: `domain/` — the only inventory-aware package

**Why:** everything so far knows nothing about stock. This task supplies the interpretation: three events, one aggregate keyed `(SKU, Location)`, one invariant (balance never negative), and ownership — a node may only author events for locations it owns, which is what removes conflict as a category.

**Files:**
- Create: `localfirst-go/domain/events.go`, `localfirst-go/domain/reducer.go`, `localfirst-go/domain/inventory.go`
- Test: `localfirst-go/domain/reducer_test.go`, `localfirst-go/domain/inventory_test.go`

**Interfaces:**
- Consumes: `eventlog.Record/Validator/Projector/ErrUnknownType`, `eventlog.Store.Append`, `projection.Fold`, `clock.Clock`.
- Produces:
  - `type Received struct { SKU, Location string; Qty int64 }`, `type Issued` (same fields), `type Moved struct { SKU, From, To string; Qty int64 }`
  - `const TypeReceived = "inventory.Received"`, `TypeIssued`, `TypeMoved`
  - `type Key struct { SKU, Location string }`, `type State map[Key]int64`, `func (s State) Clone() State`
  - `type BalanceReducer struct{}` implementing `projection.Reducer[State]`
  - `var ErrNegativeBalance`, `ErrNotOwned`, `ErrBadQty`
  - `type Inventory struct{…}`, `func NewInventory(ctx context.Context, store *eventlog.Store, clk *clock.Clock, owned []string) (*Inventory, error)`
  - `func (i *Inventory) Receive(ctx context.Context, sku, location string, qty int64) error`
  - `func (i *Inventory) Issue(ctx context.Context, sku, location string, qty int64) error`
  - `func (i *Inventory) Move(ctx context.Context, sku, from, to string, qty int64) error`
  - `func (i *Inventory) Balance(sku, location string) int64`, `func (i *Inventory) Balances() State`
  - `func (i *Inventory) Check(r eventlog.Record) error`, `func (i *Inventory) Project(r eventlog.Record) (func(), error)`

- [ ] **Step 1: Write the failing reducer test**

`localfirst-go/domain/reducer_test.go`:

```go
package domain_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

func rec(t *testing.T, seq uint64, typ string, ev any) eventlog.Record {
	t.Helper()
	payload, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return eventlog.Record{
		NodeID: "n1", Seq: seq, Clock: clock.HLC{Wall: int64(seq), NodeID: "n1"},
		Type: typ, Payload: payload,
	}
}

func TestBalanceReducer(t *testing.T) {
	tests := []struct {
		name    string
		recs    []eventlog.Record
		want    domain.State
		wantErr error
	}{
		{
			name: "received accumulates",
			recs: []eventlog.Record{
				rec(t, 1, domain.TypeReceived, domain.Received{SKU: "S", Location: "A", Qty: 5}),
				rec(t, 2, domain.TypeReceived, domain.Received{SKU: "S", Location: "A", Qty: 2}),
			},
			want: domain.State{{SKU: "S", Location: "A"}: 7},
		},
		{
			name: "issued subtracts",
			recs: []eventlog.Record{
				rec(t, 1, domain.TypeReceived, domain.Received{SKU: "S", Location: "A", Qty: 5}),
				rec(t, 2, domain.TypeIssued, domain.Issued{SKU: "S", Location: "A", Qty: 3}),
			},
			want: domain.State{{SKU: "S", Location: "A"}: 2},
		},
		{
			name: "moved shifts between locations, even across nodes",
			recs: []eventlog.Record{
				rec(t, 1, domain.TypeReceived, domain.Received{SKU: "S", Location: "A", Qty: 5}),
				rec(t, 2, domain.TypeMoved, domain.Moved{SKU: "S", From: "A", To: "Z", Qty: 4}),
			},
			want: domain.State{{SKU: "S", Location: "A"}: 1, {SKU: "S", Location: "Z"}: 4},
		},
		{
			name:    "unknown type is a hard error",
			recs:    []eventlog.Record{{NodeID: "n1", Seq: 1, Type: "inventory.Teleported"}},
			wantErr: eventlog.ErrUnknownType,
		},
		{
			name:    "corrupt payload is a hard error",
			recs:    []eventlog.Record{{NodeID: "n1", Seq: 1, Type: domain.TypeReceived, Payload: []byte("{")}},
			wantErr: domain.ErrBadPayload,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := projection.FoldRecords[domain.State](domain.BalanceReducer{}, tc.recs)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FoldRecords: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("state = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("state[%v] = %d, want %d (full: %v)", k, got[k], v, got)
				}
			}
		})
	}
}

func TestStateCloneIsIndependent(t *testing.T) {
	s := domain.State{{SKU: "S", Location: "A"}: 1}
	c := s.Clone()
	c[domain.Key{SKU: "S", Location: "A"}] = 99
	if s[domain.Key{SKU: "S", Location: "A"}] != 1 {
		t.Fatal("Clone must not alias the source")
	}
}

func TestZeroIsEmpty(t *testing.T) {
	if got := (domain.BalanceReducer{}).Zero(); len(got) != 0 {
		t.Fatalf("Zero = %v, want empty", got)
	}
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd localfirst-go && go test ./domain/...`
Expected: FAIL — `undefined: domain.Received`.

- [ ] **Step 3: Write the events**

`localfirst-go/domain/events.go`:

```go
// Package domain is the ONLY inventory-aware package. Every other package moves
// opaque records; this one decides what they mean.
//
// One aggregate: Stock, keyed (SKU, Location). Every Location belongs to exactly
// one node, so no two nodes ever write the same key — that is the whole conflict
// model. See docs/adr/0001-per-node-ownership.md.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// Record type discriminators. They are strings in the log, not Go types, so a
// stored log stays readable and forward-compatible.
const (
	TypeReceived = "inventory.Received"
	TypeIssued   = "inventory.Issued"
	TypeMoved    = "inventory.Moved"
)

// Received adds stock at a location owned by the authoring node.
type Received struct {
	SKU      string `json:"sku"`
	Location string `json:"location"`
	Qty      int64  `json:"qty"`
}

// Issued removes stock from a location owned by the authoring node.
type Issued struct {
	SKU      string `json:"sku"`
	Location string `json:"location"`
	Qty      int64  `json:"qty"`
}

// Moved transfers stock. From must be owned by the authoring node; To may live
// on another node. There is no in-transit accounting: the goods are absent from
// the global sum between the decrement and the increment, and that gap is an
// accepted, documented limitation (docs/limitations.md).
type Moved struct {
	SKU  string `json:"sku"`
	From string `json:"from"`
	To   string `json:"to"`
	Qty  int64  `json:"qty"`
}

// ErrBadPayload means a record's bytes did not decode as its declared type.
var ErrBadPayload = errors.New("domain: malformed payload")

func decode[E any](r eventlog.Record) (E, error) {
	var ev E
	if err := json.Unmarshal(r.Payload, &ev); err != nil {
		return ev, fmt.Errorf("%w: %s: %v", ErrBadPayload, r.Type, err)
	}
	return ev, nil
}

// encode marshals one of the three event structs. It returns no error because
// those structs contain only strings and ints, which json.Marshal cannot fail
// on — inventing an unreachable error branch would just be untestable code.
func encode(ev any) []byte {
	b, _ := json.Marshal(ev) //nolint:errchkjson // only string/int fields
	return b
}
```

- [ ] **Step 4: Write the reducer**

`localfirst-go/domain/reducer.go`:

```go
package domain

import (
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// Key identifies one stock balance.
type Key struct {
	SKU      string
	Location string
}

// State is the projected balance per key. Derived data only: it can always be
// rebuilt by replaying the log.
type State map[Key]int64

// Clone returns an independent copy, so a validator can try an event against a
// hypothetical future state without touching the live one.
func (s State) Clone() State {
	out := make(State, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

// BalanceReducer folds inventory records into balances.
// It is the read-side half of the plug-in seam: projection.Reducer[State].
type BalanceReducer struct{}

// Zero returns an empty balance set.
func (BalanceReducer) Zero() State { return State{} }

// Apply folds one record. It mutates and returns the same map — safe because
// FoldRecords threads a single state value through the replay.
func (BalanceReducer) Apply(s State, r eventlog.Record) (State, error) {
	switch r.Type {
	case TypeReceived:
		ev, err := decode[Received](r)
		if err != nil {
			return s, err
		}
		s[Key{ev.SKU, ev.Location}] += ev.Qty
	case TypeIssued:
		ev, err := decode[Issued](r)
		if err != nil {
			return s, err
		}
		s[Key{ev.SKU, ev.Location}] -= ev.Qty
	case TypeMoved:
		ev, err := decode[Moved](r)
		if err != nil {
			return s, err
		}
		s[Key{ev.SKU, ev.From}] -= ev.Qty
		s[Key{ev.SKU, ev.To}] += ev.Qty
	default:
		return s, fmt.Errorf("%w: %q", eventlog.ErrUnknownType, r.Type)
	}
	return s, nil
}
```

- [ ] **Step 5: Run the reducer tests**

Run: `cd localfirst-go && go test ./domain/... -run 'TestBalanceReducer|TestState|TestZero' -cover`
Expected: PASS.

- [ ] **Step 6: Write the failing command test**

`localfirst-go/domain/inventory_test.go`:

```go
package domain_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

func newInv(t *testing.T, owned ...string) (*domain.Inventory, *eventlog.Store) {
	t.Helper()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	inv, err := domain.NewInventory(context.Background(), store, clock.New("n1", nil), owned)
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	return inv, store
}

func TestCommands(t *testing.T) {
	tests := []struct {
		name    string
		run     func(inv *domain.Inventory) error
		wantErr error
		want    map[domain.Key]int64
	}{
		{
			name: "receive then issue",
			run: func(inv *domain.Inventory) error {
				if err := inv.Receive(context.Background(), "S", "A", 10); err != nil {
					return err
				}
				return inv.Issue(context.Background(), "S", "A", 4)
			},
			want: map[domain.Key]int64{{SKU: "S", Location: "A"}: 6},
		},
		{
			name: "issue below zero is rejected and leaves no trace",
			run: func(inv *domain.Inventory) error {
				if err := inv.Receive(context.Background(), "S", "A", 2); err != nil {
					return err
				}
				return inv.Issue(context.Background(), "S", "A", 3)
			},
			wantErr: domain.ErrNegativeBalance,
			want:    map[domain.Key]int64{{SKU: "S", Location: "A"}: 2},
		},
		{
			name: "move to another node's location decrements only locally",
			run: func(inv *domain.Inventory) error {
				if err := inv.Receive(context.Background(), "S", "A", 5); err != nil {
					return err
				}
				return inv.Move(context.Background(), "S", "A", "REMOTE-Z", 5)
			},
			want: map[domain.Key]int64{
				{SKU: "S", Location: "A"}:        0,
				{SKU: "S", Location: "REMOTE-Z"}: 5,
			},
		},
		{
			name: "move more than held is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Move(context.Background(), "S", "A", "REMOTE-Z", 1)
			},
			wantErr: domain.ErrNegativeBalance,
		},
		{
			name: "receiving into a location this node does not own is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Receive(context.Background(), "S", "NOT-MINE", 1)
			},
			wantErr: domain.ErrNotOwned,
		},
		{
			name: "issuing from a location this node does not own is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Issue(context.Background(), "S", "NOT-MINE", 1)
			},
			wantErr: domain.ErrNotOwned,
		},
		{
			name: "moving out of a location this node does not own is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Move(context.Background(), "S", "NOT-MINE", "A", 1)
			},
			wantErr: domain.ErrNotOwned,
		},
		{
			name: "zero quantity is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Receive(context.Background(), "S", "A", 0)
			},
			wantErr: domain.ErrBadQty,
		},
		{
			name: "negative quantity is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Issue(context.Background(), "S", "A", -1)
			},
			wantErr: domain.ErrBadQty,
		},
		{
			name: "negative move quantity is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Move(context.Background(), "S", "A", "B", -1)
			},
			wantErr: domain.ErrBadQty,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv, _ := newInv(t, "A", "B")
			err := tc.run(inv)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for k, want := range tc.want {
				if got := inv.Balance(k.SKU, k.Location); got != want {
					t.Fatalf("Balance(%s,%s) = %d, want %d", k.SKU, k.Location, got, want)
				}
			}
		})
	}
}

func TestRejectedCommandAppendsNothing(t *testing.T) {
	ctx := context.Background()
	inv, store := newInv(t, "A")
	if err := inv.Receive(ctx, "S", "A", 1); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := inv.Issue(ctx, "S", "A", 99); !errors.Is(err, domain.ErrNegativeBalance) {
		t.Fatalf("Issue: %v, want ErrNegativeBalance", err)
	}
	recs, err := store.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("log holds %d records, want 1", len(recs))
	}
}

func TestNewInventoryReplaysExistingLog(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "n1.db")
	store, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inv, err := domain.NewInventory(ctx, store, clock.New("n1", nil), []string{"A"})
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	if err := inv.Receive(ctx, "S", "A", 7); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	restored, err := domain.NewInventory(ctx, reopened, clock.New("n1", nil), []string{"A"})
	if err != nil {
		t.Fatalf("NewInventory after restart: %v", err)
	}
	if got := restored.Balance("S", "A"); got != 7 {
		t.Fatalf("replayed balance = %d, want 7 (state must be rebuilt from the log)", got)
	}
}

func TestNewInventoryFailsOnCorruptLog(t *testing.T) {
	ctx := context.Background()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	if _, err := store.Merge(ctx, []eventlog.Record{
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n2"}, Type: "inventory.Teleported"},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if _, err := domain.NewInventory(ctx, store, clock.New("n1", nil), []string{"A"}); !errors.Is(err, eventlog.ErrUnknownType) {
		t.Fatalf("err = %v, want ErrUnknownType", err)
	}
}

func TestBalancesSnapshotIsACopy(t *testing.T) {
	inv, _ := newInv(t, "A")
	if err := inv.Receive(context.Background(), "S", "A", 3); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	snap := inv.Balances()
	snap[domain.Key{SKU: "S", Location: "A"}] = 999
	if inv.Balance("S", "A") != 3 {
		t.Fatal("Balances() must return a copy")
	}
}

// TestProjectAppliesRemoteRecords covers the Projector half of the seam, which
// central and any global view rely on.
func TestProjectAppliesRemoteRecords(t *testing.T) {
	inv, store := newInv(t, "A")
	remote := eventlog.Record{
		NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n2"},
		Type: domain.TypeReceived, Payload: mustJSON(t, domain.Received{SKU: "S", Location: "Q", Qty: 4}),
	}
	if _, err := store.Merge(context.Background(), []eventlog.Record{remote}, inv); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if got := inv.Balance("S", "Q"); got != 4 {
		t.Fatalf("Balance = %d, want 4", got)
	}
	if _, err := inv.Project(eventlog.Record{NodeID: "n2", Seq: 2, Type: "inventory.Teleported"}); !errors.Is(err, eventlog.ErrUnknownType) {
		t.Fatalf("Project(unknown) = %v, want ErrUnknownType", err)
	}
}

func TestCheckRejectsUnknownType(t *testing.T) {
	inv, _ := newInv(t, "A")
	if err := inv.Check(eventlog.Record{NodeID: "n1", Seq: 1, Type: "inventory.Teleported"}); !errors.Is(err, eventlog.ErrUnknownType) {
		t.Fatalf("Check = %v, want ErrUnknownType", err)
	}
}

func TestOwnsEverythingWhenNoLocationsConfigured(t *testing.T) {
	// Central and tests pass no owned list: ownership checks are then off.
	inv, _ := newInv(t)
	if err := inv.Receive(context.Background(), "S", "anywhere", 1); err != nil {
		t.Fatalf("Receive: %v", err)
	}
}
```

Add this helper to `localfirst-go/domain/reducer_test.go`:

```go
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
```

- [ ] **Step 7: Run and watch it fail**

Run: `cd localfirst-go && go test ./domain/...`
Expected: FAIL — `undefined: domain.NewInventory`.

- [ ] **Step 8: Implement the aggregate**

`localfirst-go/domain/inventory.go`:

```go
package domain

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// Command errors. Each names the invariant it protects.
var (
	// ErrNegativeBalance is the one business invariant: stock never goes below
	// zero. A node owns its locations, so it can always decide this alone,
	// locally, offline — which is why this design needs no coordination.
	ErrNegativeBalance = errors.New("domain: balance would go negative")
	// ErrNotOwned means the command targets a location owned by another node.
	ErrNotOwned = errors.New("domain: location not owned by this node")
	// ErrBadQty means a non-positive quantity.
	ErrBadQty = errors.New("domain: quantity must be positive")
)

// Inventory is the node's aggregate: commands in, records out, balances kept.
// It implements both halves of the write-side seam, eventlog.Validator and
// eventlog.Projector, so the log can validate and project inside one
// transaction without knowing what stock is.
type Inventory struct {
	store *eventlog.Store
	clk   *clock.Clock
	owned map[string]bool // empty means "own everything" (central, tests)

	mu    sync.RWMutex
	state State
}

// NewInventory rebuilds balances by replaying the whole log, then serves
// commands. owned lists the locations this node exclusively writes to; an empty
// list disables ownership checks.
func NewInventory(
	ctx context.Context, store *eventlog.Store, clk *clock.Clock, owned []string,
) (*Inventory, error) {
	state, err := projection.Fold[State](ctx, store, BalanceReducer{})
	if err != nil {
		return nil, fmt.Errorf("domain: replay: %w", err)
	}
	set := make(map[string]bool, len(owned))
	for _, loc := range owned {
		set[loc] = true
	}
	return &Inventory{store: store, clk: clk, owned: set, state: state}, nil
}

// Receive adds qty of sku at location.
func (i *Inventory) Receive(ctx context.Context, sku, location string, qty int64) error {
	if err := i.guard(qty, location); err != nil {
		return err
	}
	return i.append(ctx, TypeReceived, Received{SKU: sku, Location: location, Qty: qty})
}

// Issue removes qty of sku from location.
func (i *Inventory) Issue(ctx context.Context, sku, location string, qty int64) error {
	if err := i.guard(qty, location); err != nil {
		return err
	}
	return i.append(ctx, TypeIssued, Issued{SKU: sku, Location: location, Qty: qty})
}

// Move transfers qty of sku from a location this node owns to any location,
// including one owned by another node. There is no handshake with the
// destination: it will apply the same record when it syncs.
func (i *Inventory) Move(ctx context.Context, sku, from, to string, qty int64) error {
	if err := i.guard(qty, from); err != nil {
		return err
	}
	return i.append(ctx, TypeMoved, Moved{SKU: sku, From: from, To: to, Qty: qty})
}

func (i *Inventory) guard(qty int64, location string) error {
	if qty <= 0 {
		return fmt.Errorf("%w: got %d", ErrBadQty, qty)
	}
	if len(i.owned) > 0 && !i.owned[location] {
		return fmt.Errorf("%w: %s", ErrNotOwned, location)
	}
	return nil
}

func (i *Inventory) append(ctx context.Context, typ string, ev any) error {
	// The store calls i.Check then i.Project inside one transaction.
	_, err := i.store.Append(ctx, typ, encode(ev), i.clk.Now(), i, i)
	return err
}

// Check implements eventlog.Validator: it replays r against a copy of the
// current state and refuses if any balance it touches would go negative.
// Called inside the append transaction, before the row is visible.
func (i *Inventory) Check(r eventlog.Record) error {
	i.mu.RLock()
	trial := i.state.Clone()
	i.mu.RUnlock()

	next, err := BalanceReducer{}.Apply(trial, r)
	if err != nil {
		return err
	}
	for k, v := range next {
		if v < 0 {
			return fmt.Errorf("%w: %s at %s would be %d", ErrNegativeBalance, k.SKU, k.Location, v)
		}
	}
	return nil
}

// Project implements eventlog.Projector: it computes the next state but does
// not publish it until the store's transaction has committed.
func (i *Inventory) Project(r eventlog.Record) (func(), error) {
	i.mu.RLock()
	staged := i.state.Clone()
	i.mu.RUnlock()

	next, err := BalanceReducer{}.Apply(staged, r)
	if err != nil {
		return nil, err
	}
	return func() {
		i.mu.Lock()
		i.state = next
		i.mu.Unlock()
	}, nil
}

// Balance returns the projected balance for one key.
func (i *Inventory) Balance(sku, location string) int64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.state[Key{SKU: sku, Location: location}]
}

// Balances returns a snapshot copy of all balances.
func (i *Inventory) Balances() State {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.state.Clone()
}
```

- [ ] **Step 9: Run the whole suite**

Run: `cd localfirst-go && go test ./... -race -cover`
Expected: PASS everywhere, `domain` at `coverage: 100.0% of statements`.

- [ ] **Step 10: Lint, then commit**

```bash
cd localfirst-go && golangci-lint run
git add localfirst-go/domain
git commit -m "feat(domain): inventory events, balance reducer, ownership and negative-balance invariant"
```

---

### Task 7: `sync/` — the symmetric replication protocol

**Why:** with per-node ownership, central is a peer with a bigger disk, not a different kind of participant. So there is **one** `Frame` message travelling in both directions and **one** exchange function used by both sides. The only difference between the two roles is who speaks first, which is a parameter, not a code path. That is what makes node-to-node sync a configuration change.

**The exchange, in order (this is the whole protocol):**

1. initiator → `Hello{node_id, version}` ("here is what I already have")
2. responder → `Hello{node_id, version}`, then `Batch*` (records the initiator lacks), then `Ack{version}` ("that is everything from me")
3. initiator merges the batches, then sends its own `Batch*` and `Ack{version}`, then closes its send side
4. responder merges, sees the `Ack` then EOF, returns

Resumability falls out for free: if the stream dies at any point, both sides simply re-`Hello` with their current vectors next time, and idempotent append makes any re-delivered record a no-op.

**Files:**
- Create: `localfirst-go/sync/sync.proto`, `localfirst-go/sync/convert.go`, `localfirst-go/sync/session.go`, `localfirst-go/sync/server.go`, `localfirst-go/sync/client.go`
- Generated: `localfirst-go/sync/syncpb/sync.pb.go`, `localfirst-go/sync/syncpb/sync_grpc.pb.go`
- Test: `localfirst-go/sync/convert_test.go`, `localfirst-go/sync/sync_test.go`

**Interfaces:**
- Consumes: `eventlog.Record`, `eventlog.VersionVector`, `eventlog.Projector`, `*eventlog.Store` (Tasks 3–4).
- Produces:
  - `type Log interface { NodeID() string; Since(ctx, VersionVector) ([]Record, error); Merge(ctx, []Record, Projector) (int, error); Version(ctx) (VersionVector, error); Cursor(ctx, peer string) (uint64, error); SetCursor(ctx, peer string, seq uint64) error }`
  - `type Stream interface { Send(*syncpb.Frame) error; Recv() (*syncpb.Frame, error) }`
  - `func Exchange(ctx context.Context, log Log, p eventlog.Projector, s Stream, initiator bool) (eventlog.VersionVector, error)`
  - `func NewServer(log Log, p eventlog.Projector) *Server` (implements `syncpb.SyncServer`)
  - `func NewClient(cc grpc.ClientConnInterface, log Log, p eventlog.Projector, peer string) *Client`
  - `func (c *Client) SyncOnce(ctx context.Context) (eventlog.VersionVector, error)`
  - `func (c *Client) Run(ctx context.Context, every time.Duration)`
  - `var ErrProtocol = errors.New("sync: protocol violation")`

- [ ] **Step 1: Write the proto and generate**

`localfirst-go/sync/sync.proto`:

```proto
syntax = "proto3";

package localfirst.sync.v1;

option go_package = "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb";

// Sync is symmetric: the same Frame type flows in both directions, because
// with per-node ownership central is just a peer with a bigger disk.
service Sync {
  rpc Replicate(stream Frame) returns (stream Frame);
}

message Frame {
  oneof body {
    Hello hello = 1; // node id + version vector
    Batch batch = 2; // records the peer lacks
    Ack   ack   = 3; // version vector after apply
  }
}

message Hello {
  string node_id = 1;
  map<string, uint64> version = 2;
}

message Batch {
  repeated Record records = 1;
}

message Ack {
  map<string, uint64> version = 1;
}

message Record {
  string node_id     = 1;
  uint64 seq         = 2;
  int64  hlc_wall    = 3;
  uint32 hlc_logical = 4;
  string type        = 5;
  bytes  payload     = 6;
}
```

```bash
cd localfirst-go
go get google.golang.org/grpc google.golang.org/protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
make proto
```

- [ ] **Step 2: Write the failing conversion test**

`localfirst-go/sync/convert_test.go`:

```go
package sync_test

import (
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

func TestRecordRoundTrip(t *testing.T) {
	in := eventlog.Record{
		NodeID: "n1", Seq: 9, Clock: clock.HLC{Wall: 42, Logical: 3, NodeID: "n1"},
		Type: "T", Payload: []byte("hello"),
	}
	got := lfsync.RecordFromPB(lfsync.RecordToPB(in))
	if string(got.Payload) != string(in.Payload) {
		t.Fatalf("payload = %q, want %q", got.Payload, in.Payload)
	}
	if got.NodeID != in.NodeID || got.Seq != in.Seq || got.Clock != in.Clock || got.Type != in.Type {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
}

func TestVersionVectorRoundTrip(t *testing.T) {
	in := eventlog.VersionVector{"n1": 3, "n2": 7}
	if got := lfsync.VersionFromPB(lfsync.VersionToPB(in)); !got.Equal(in) {
		t.Fatalf("round trip = %v, want %v", got, in)
	}
	if got := lfsync.VersionFromPB(nil); len(got) != 0 {
		t.Fatalf("nil map = %v, want empty", got)
	}
}

func TestRecordFromPBRestoresClockNodeID(t *testing.T) {
	got := lfsync.RecordFromPB(&syncpb.Record{NodeId: "n5", Seq: 1})
	if got.Clock.NodeID != "n5" {
		t.Fatalf("Clock.NodeID = %q, want n5", got.Clock.NodeID)
	}
}
```

- [ ] **Step 3: Implement the converters**

`localfirst-go/sync/convert.go`:

```go
// Package sync replicates records between peers over one symmetric gRPC stream.
// It is domain-agnostic: it moves opaque records and never interprets them.
package sync

import (
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// RecordToPB converts a log record to the wire form.
func RecordToPB(r eventlog.Record) *syncpb.Record {
	return &syncpb.Record{
		NodeId: r.NodeID, Seq: r.Seq, HlcWall: r.Clock.Wall,
		HlcLogical: r.Clock.Logical, Type: r.Type, Payload: r.Payload,
	}
}

// RecordFromPB converts a wire record back. The HLC's node id is derived from
// the record's author, so it is never sent twice.
func RecordFromPB(p *syncpb.Record) eventlog.Record {
	return eventlog.Record{
		NodeID: p.GetNodeId(), Seq: p.GetSeq(), Type: p.GetType(), Payload: p.GetPayload(),
		Clock: clock.HLC{Wall: p.GetHlcWall(), Logical: p.GetHlcLogical(), NodeID: p.GetNodeId()},
	}
}

// VersionToPB converts a version vector to the wire form.
func VersionToPB(v eventlog.VersionVector) map[string]uint64 { return map[string]uint64(v.Clone()) }

// VersionFromPB converts a wire version vector back.
func VersionFromPB(m map[string]uint64) eventlog.VersionVector {
	return eventlog.VersionVector(m).Clone()
}
```

- [ ] **Step 4: Run the conversion tests**

Run: `cd localfirst-go && go test ./sync/... -run RoundTrip -v`
Expected: PASS.

- [ ] **Step 5: Write the failing protocol tests**

`localfirst-go/sync/sync_test.go`:

```go
package sync_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

func store(t *testing.T, nodeID string) *eventlog.Store {
	t.Helper()
	s, err := eventlog.Open(filepath.Join(t.TempDir(), nodeID+".db"), nodeID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// serve starts an in-process gRPC server over bufconn — no ports, no Docker.
func serve(t *testing.T, log lfsync.Log, p eventlog.Projector) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, lfsync.NewServer(log, p))
	go func() { _ = srv.Serve(lis) }()
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close(); srv.Stop(); _ = lis.Close() })
	return cc
}

func appendN(t *testing.T, s *eventlog.Store, n int) {
	t.Helper()
	c := clock.New(s.NodeID(), nil)
	for i := 0; i < n; i++ {
		if _, err := s.Append(context.Background(), domain.TypeReceived,
			mustJSONBytes(t, domain.Received{SKU: "S", Location: s.NodeID(), Qty: 1}), c.Now(), nil, nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func mustJSONBytes(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestSyncIsBidirectionalInOnePass: each side ends up holding both logs.
func TestSyncIsBidirectionalInOnePass(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 3)
	appendN(t, central, 2)

	cc := serve(t, central, nil)
	client := lfsync.NewClient(cc, node, nil, "central")
	if _, err := client.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	want := eventlog.VersionVector{"n1": 3, "central": 2}
	for name, s := range map[string]*eventlog.Store{"node": node, "central": central} {
		vv, err := s.Version(ctx)
		if err != nil {
			t.Fatalf("%s Version: %v", name, err)
		}
		if !vv.Equal(want) {
			t.Fatalf("%s holds %v, want %v", name, vv, want)
		}
	}
	if cur, err := node.Cursor(ctx, "central"); err != nil || cur != 3 {
		t.Fatalf("cursor = (%d, %v), want (3, nil)", cur, err)
	}
}

// TestCatchUpAfterOfflineWindow is the local-first claim in test form: the node
// keeps appending while unreachable, then one sync closes the gap.
func TestCatchUpAfterOfflineWindow(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n3"), store(t, "central")

	cc := serve(t, central, nil)
	client := lfsync.NewClient(cc, node, nil, "central")
	appendN(t, node, 1)
	if _, err := client.SyncOnce(ctx); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	// Offline window: 20 local appends with no sync at all.
	appendN(t, node, 20)
	if got, err := central.Version(ctx); err != nil || got.Get("n3") != 1 {
		t.Fatalf("central saw %v during the offline window, want n3:1", got)
	}
	if _, err := client.SyncOnce(ctx); err != nil {
		t.Fatalf("catch-up sync: %v", err)
	}
	if got, err := central.Version(ctx); err != nil || got.Get("n3") != 21 {
		t.Fatalf("central holds n3:%d after catch-up, want 21", got.Get("n3"))
	}
}

// flakyStream fails the nth Send, simulating a mid-stream disconnect.
type flakyStream struct {
	lfsync.Stream
	failAfter int
	sent      int
}

func (f *flakyStream) Send(fr *syncpb.Frame) error {
	f.sent++
	if f.sent > f.failAfter {
		return errors.New("connection reset")
	}
	return f.Stream.Send(fr)
}

// TestResumeAfterMidStreamDisconnect: a broken exchange loses no data and the
// next exchange completes it, because Hello always restates the real vectors.
func TestResumeAfterMidStreamDisconnect(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 5)

	cc := serve(t, central, nil)
	raw, err := syncpb.NewSyncClient(cc).Replicate(ctx)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	// Fail on the very first frame we try to send: the Hello never lands.
	if _, err := lfsync.Exchange(ctx, node, nil, &flakyStream{Stream: raw, failAfter: 0}, true); err == nil {
		t.Fatal("Exchange must report the disconnect")
	}
	if got, err := central.Version(ctx); err != nil || len(got) != 0 {
		t.Fatalf("central holds %v after a failed exchange, want nothing", got)
	}

	client := lfsync.NewClient(cc, node, nil, "central")
	if _, err := client.SyncOnce(ctx); err != nil {
		t.Fatalf("resumed sync: %v", err)
	}
	if got, err := central.Version(ctx); err != nil || got.Get("n1") != 5 {
		t.Fatalf("central holds n1:%d, want 5", got.Get("n1"))
	}
}

// TestDuplicateBatchIsANoOp: syncing twice with nothing new changes nothing.
func TestDuplicateBatchIsANoOp(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 4)

	cc := serve(t, central, nil)
	client := lfsync.NewClient(cc, node, nil, "central")
	for i := 0; i < 3; i++ {
		if _, err := client.SyncOnce(ctx); err != nil {
			t.Fatalf("sync %d: %v", i, err)
		}
	}
	recs, err := central.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(recs) != 4 {
		t.Fatalf("central holds %d records after 3 syncs, want 4", len(recs))
	}
}

// TestCentralProjectsAGlobalSum shows why Merge takes a Projector.
func TestCentralProjectsAGlobalSum(t *testing.T) {
	ctx := context.Background()
	n1, n2, central := store(t, "n1"), store(t, "n2"), store(t, "central")
	appendN(t, n1, 3) // 3 units at location "n1"
	appendN(t, n2, 2) // 2 units at location "n2"

	global, err := domain.NewInventory(ctx, central, clock.New("central", nil), nil)
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	cc := serve(t, central, global)
	for _, s := range []*eventlog.Store{n1, n2} {
		if _, err := lfsync.NewClient(cc, s, nil, "central").SyncOnce(ctx); err != nil {
			t.Fatalf("sync %s: %v", s.NodeID(), err)
		}
	}
	if got := global.Balance("S", "n1") + global.Balance("S", "n2"); got != 5 {
		t.Fatalf("global sum = %d, want 5", got)
	}
}

func TestExchangeRejectsAMissingHello(t *testing.T) {
	ctx := context.Background()
	central := store(t, "central")
	cc := serve(t, central, nil)
	stream, err := syncpb.NewSyncClient(cc).Replicate(ctx)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	// Speak out of turn: a Batch where a Hello belongs.
	if err := stream.Send(&syncpb.Frame{Body: &syncpb.Frame_Batch{Batch: &syncpb.Batch{}}}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("server must reject a first frame that is not a Hello")
	}
}

func TestExchangeRejectsAnUnexpectedFrame(t *testing.T) {
	ctx := context.Background()
	node := store(t, "n1")
	// A stream that says Hello, then sends a second Hello where a Batch or Ack
	// belongs.
	s := &scriptedStream{frames: []*syncpb.Frame{
		{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
		{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
	}}
	if _, err := lfsync.Exchange(ctx, node, nil, s, true); !errors.Is(err, lfsync.ErrProtocol) {
		t.Fatalf("err = %v, want ErrProtocol", err)
	}
}

// scriptedStream replays canned frames and discards everything sent to it.
type scriptedStream struct {
	frames []*syncpb.Frame
	i      int
}

func (s *scriptedStream) Send(*syncpb.Frame) error { return nil }
func (s *scriptedStream) Recv() (*syncpb.Frame, error) {
	if s.i >= len(s.frames) {
		return nil, io.EOF
	}
	f := s.frames[s.i]
	s.i++
	return f, nil
}

func TestClientRunKeepsGoingWhenThePeerIsUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	node := store(t, "n1")
	appendN(t, node, 1)

	cc, err := grpc.NewClient("passthrough:///dead",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return nil, errors.New("network unreachable")
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = cc.Close() }()

	done := make(chan struct{})
	go func() { lfsync.NewClient(cc, node, nil, "central").Run(ctx, 10*time.Millisecond); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must return when its context is cancelled")
	}
	// The node stayed operational and its cursor was not advanced.
	if cur, err := node.Cursor(context.Background(), "central"); err != nil || cur != 0 {
		t.Fatalf("cursor = (%d, %v), want (0, nil) — a failed sync must not advance it", cur, err)
	}
}
```

Add `"encoding/json"` and `"io"` to this file's imports.

- [ ] **Step 6: Run and watch it fail**

Run: `cd localfirst-go && go test ./sync/...`
Expected: FAIL — `undefined: lfsync.NewServer`, `lfsync.Exchange`, `lfsync.NewClient`.

- [ ] **Step 7: Implement the exchange**

`localfirst-go/sync/session.go`:

```go
package sync

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// ErrProtocol reports a peer that spoke out of turn.
var ErrProtocol = errors.New("sync: protocol violation")

// batchSize caps records per frame, so one exchange streams instead of building
// a single enormous message.
const batchSize = 100

// Log is the storage contract sync needs. Both *eventlog.Store (SQLite, nodes)
// and central's Postgres store satisfy it — which is what makes central a peer
// rather than a special case.
type Log interface {
	NodeID() string
	Since(ctx context.Context, vv eventlog.VersionVector) ([]eventlog.Record, error)
	Merge(ctx context.Context, recs []eventlog.Record, p eventlog.Projector) (int, error)
	Version(ctx context.Context) (eventlog.VersionVector, error)
	Cursor(ctx context.Context, peer string) (uint64, error)
	SetCursor(ctx context.Context, peer string, seq uint64) error
}

// Stream is the half of a gRPC bidirectional stream both roles share. The
// generated client and server stream types both satisfy it, which is how one
// Exchange function serves both ends.
type Stream interface {
	Send(*syncpb.Frame) error
	Recv() (*syncpb.Frame, error)
}

// Exchange runs one full replication pass and returns the local version vector
// afterwards. initiator selects who speaks first; nothing else differs between
// the two roles.
func Exchange(
	ctx context.Context, log Log, p eventlog.Projector, s Stream, initiator bool,
) (eventlog.VersionVector, error) {
	if initiator {
		if err := sendHello(ctx, log, s); err != nil {
			return nil, err
		}
	}
	frame, err := s.Recv()
	if err != nil {
		return nil, fmt.Errorf("sync: awaiting hello: %w", err)
	}
	hello := frame.GetHello()
	if hello == nil {
		return nil, fmt.Errorf("%w: first frame was %T, want Hello", ErrProtocol, frame.GetBody())
	}
	peerVV := VersionFromPB(hello.GetVersion())

	if !initiator {
		if err := sendHello(ctx, log, s); err != nil {
			return nil, err
		}
		if err := pushAndAck(ctx, log, s, peerVV); err != nil {
			return nil, err
		}
	}
	if err := drain(ctx, log, p, s); err != nil {
		return nil, err
	}
	if initiator {
		if err := pushAndAck(ctx, log, s, peerVV); err != nil {
			return nil, err
		}
	}
	return log.Version(ctx)
}

func sendHello(ctx context.Context, log Log, s Stream) error {
	vv, err := log.Version(ctx)
	if err != nil {
		return err
	}
	if err := s.Send(&syncpb.Frame{Body: &syncpb.Frame_Hello{
		Hello: &syncpb.Hello{NodeId: log.NodeID(), Version: VersionToPB(vv)},
	}}); err != nil {
		return fmt.Errorf("sync: send hello: %w", err)
	}
	return nil
}

// pushAndAck sends everything the peer lacks, then an Ack meaning "that is all
// from me".
func pushAndAck(ctx context.Context, log Log, s Stream, peerVV eventlog.VersionVector) error {
	recs, err := log.Since(ctx, peerVV)
	if err != nil {
		return err
	}
	for start := 0; start < len(recs); start += batchSize {
		end := min(start+batchSize, len(recs))
		batch := &syncpb.Batch{Records: make([]*syncpb.Record, 0, end-start)}
		for _, r := range recs[start:end] {
			batch.Records = append(batch.Records, RecordToPB(r))
		}
		if err := s.Send(&syncpb.Frame{Body: &syncpb.Frame_Batch{Batch: batch}}); err != nil {
			return fmt.Errorf("sync: send batch: %w", err)
		}
	}
	vv, err := log.Version(ctx)
	if err != nil {
		return err
	}
	if err := s.Send(&syncpb.Frame{Body: &syncpb.Frame_Ack{Ack: &syncpb.Ack{Version: VersionToPB(vv)}}}); err != nil {
		return fmt.Errorf("sync: send ack: %w", err)
	}
	return nil
}

// drain applies the peer's batches until its Ack (or a clean EOF).
func drain(ctx context.Context, log Log, p eventlog.Projector, s Stream) error {
	for {
		frame, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return nil // peer hung up after sending everything it had
		}
		if err != nil {
			return fmt.Errorf("sync: receive: %w", err)
		}
		switch body := frame.GetBody().(type) {
		case *syncpb.Frame_Batch:
			recs := make([]eventlog.Record, 0, len(body.Batch.GetRecords()))
			for _, pb := range body.Batch.GetRecords() {
				recs = append(recs, RecordFromPB(pb))
			}
			if _, err := log.Merge(ctx, recs, p); err != nil {
				return err
			}
		case *syncpb.Frame_Ack:
			return nil
		default:
			return fmt.Errorf("%w: unexpected %T mid-stream", ErrProtocol, body)
		}
	}
}
```

- [ ] **Step 8: Implement the server and client**

`localfirst-go/sync/server.go`:

```go
package sync

import (
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// Server is the responder half. It is only three lines because the protocol is
// symmetric: all the logic lives in Exchange.
type Server struct {
	syncpb.UnimplementedSyncServer
	log  Log
	// A node passes its *domain.Inventory so merged peer records update its
	// in-memory balances. Central passes nil: its global sum is a SQL query
	// over the records table, not an in-memory projection.
	proj eventlog.Projector
}

// NewServer returns a Sync service backed by log.
func NewServer(log Log, p eventlog.Projector) *Server { return &Server{log: log, proj: p} }

// Replicate handles one peer's replication stream.
func (s *Server) Replicate(stream syncpb.Sync_ReplicateServer) error {
	_, err := Exchange(stream.Context(), s.log, s.proj, stream, false)
	return err
}
```

`localfirst-go/sync/client.go`:

```go
package sync

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// Client is the initiator half.
type Client struct {
	rpc  syncpb.SyncClient
	log  Log
	proj eventlog.Projector
	peer string // cursor key: which peer this client talks to
}

// NewClient returns a client that replicates log with peer over cc.
func NewClient(cc grpc.ClientConnInterface, log Log, p eventlog.Projector, peer string) *Client {
	return &Client{rpc: syncpb.NewSyncClient(cc), log: log, proj: p, peer: peer}
}

// SyncOnce runs one exchange and, on success, advances the peer cursor to the
// highest own-record sequence the peer now holds. On failure the cursor is left
// untouched: the node stays fully operational and simply retries.
func (c *Client) SyncOnce(ctx context.Context) (eventlog.VersionVector, error) {
	stream, err := c.rpc.Replicate(ctx)
	if err != nil {
		return nil, err
	}
	vv, err := Exchange(ctx, c.log, c.proj, stream, true)
	if err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	if err := c.log.SetCursor(ctx, c.peer, vv.Get(c.log.NodeID())); err != nil {
		return nil, err
	}
	return vv, nil
}

// Run syncs every interval until ctx is done. A failure is logged and retried
// with a capped backoff; it never stops the node from accepting local commands,
// because nothing here is on the command path.
func (c *Client) Run(ctx context.Context, every time.Duration) {
	backoff := every
	const maxBackoff = 30 * time.Second
	for {
		if _, err := c.SyncOnce(ctx); err != nil {
			slog.WarnContext(ctx, "sync failed, node still operational",
				"peer", c.peer, "retry_in", backoff, "err", err)
			backoff = min(backoff*2, maxBackoff)
		} else {
			backoff = every
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}
```

- [ ] **Step 9: Run the tests**

Run: `cd localfirst-go && go test ./sync/... -race -cover`
Expected: PASS, `coverage: 100.0% of statements` for the hand-written files. Check with `go tool cover -func` that generated `syncpb` files are excluded from your reading (they are a separate package). If `pushAndAck`'s multi-frame loop is uncovered, add a test that appends 150 records and asserts the peer receives all 150.

- [ ] **Step 10: Add the batching test**

Append to `localfirst-go/sync/sync_test.go`:

```go
func TestLargeLogIsSentInMultipleFrames(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 150) // > batchSize (100)

	cc := serve(t, central, nil)
	if _, err := lfsync.NewClient(cc, node, nil, "central").SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	recs, err := central.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(recs) != 150 {
		t.Fatalf("central holds %d records, want 150", len(recs))
	}
}
```

Run: `cd localfirst-go && go test ./sync/... -cover`
Expected: PASS, 100%.

- [ ] **Step 11: Lint, then commit**

```bash
cd localfirst-go && golangci-lint run
git add localfirst-go/sync localfirst-go/go.mod localfirst-go/go.sum
git commit -m "feat(sync): symmetric single-Frame gRPC replication with resumable exchange"
```

---

### Task 8: `transport/grpc/` and `cmd/node` — the node binary

**Why:** the domain must not know about transport, so the mapping from RPC to command lives in its own package. It is deliberately thin: decode, call, map errors to status codes. Keeping it separate is how a reader sees that `domain` has no transport dependency.

**Files:**
- Create: `localfirst-go/transport/grpc/node.proto`, `localfirst-go/transport/grpc/node.go`, `localfirst-go/cmd/node/node.go`, `localfirst-go/cmd/node/main.go`
- Generated: `localfirst-go/transport/grpc/nodepb/node.pb.go`, `node_grpc.pb.go`
- Test: `localfirst-go/transport/grpc/node_test.go`, `localfirst-go/cmd/node/node_test.go`

**Interfaces:**
- Consumes: `*domain.Inventory` and its command methods (Task 6), `sync.NewServer`, `sync.NewClient`, `sync.Client.Run` (Task 7), `eventlog.Open`, `clock.New`.
- Produces:
  - `func NewNodeService(inv *domain.Inventory) *NodeService` (implements `nodepb.NodeServer`)
  - `type Config struct { NodeID, DBPath, Listen, CentralAddr string; Locations []string; SyncEvery time.Duration }`
  - `func Run(ctx context.Context, cfg Config) error` in package `main` of `cmd/node`

- [ ] **Step 1: Write the proto and generate**

`localfirst-go/transport/grpc/node.proto`:

```proto
syntax = "proto3";

package localfirst.node.v1;

option go_package = "github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb";

// Node is the client-facing API of one node. Every call is served entirely from
// local state: no network hop to central, ever.
service Node {
  rpc Receive(ReceiveRequest) returns (Empty);
  rpc Issue(IssueRequest) returns (Empty);
  rpc Move(MoveRequest) returns (Empty);
  rpc Balance(BalanceRequest) returns (BalanceResponse);
  rpc Balances(Empty) returns (BalancesResponse);
}

message Empty {}

message ReceiveRequest { string sku = 1; string location = 2; int64 qty = 3; }
message IssueRequest   { string sku = 1; string location = 2; int64 qty = 3; }
message MoveRequest    { string sku = 1; string from = 2; string to = 3; int64 qty = 4; }

message BalanceRequest  { string sku = 1; string location = 2; }
message BalanceResponse { int64 qty = 1; }

message BalanceEntry { string sku = 1; string location = 2; int64 qty = 3; }
message BalancesResponse { repeated BalanceEntry entries = 1; }
```

Add `transport/grpc/node.proto` to the `proto` target (already listed in Task 1's Makefile), then run `make proto`.

- [ ] **Step 2: Write the failing transport test**

`localfirst-go/transport/grpc/node_test.go`:

```go
package grpc_test

import (
	"context"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	transport "github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

func service(t *testing.T) *transport.NodeService {
	t.Helper()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	inv, err := domain.NewInventory(context.Background(), store, clock.New("n1", nil), []string{"A"})
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	return transport.NewNodeService(inv)
}

func TestNodeServiceHappyPath(t *testing.T) {
	ctx, svc := context.Background(), service(t)
	if _, err := svc.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 10}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if _, err := svc.Issue(ctx, &nodepb.IssueRequest{Sku: "S", Location: "A", Qty: 3}); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := svc.Move(ctx, &nodepb.MoveRequest{Sku: "S", From: "A", To: "REMOTE", Qty: 2}); err != nil {
		t.Fatalf("Move: %v", err)
	}
	got, err := svc.Balance(ctx, &nodepb.BalanceRequest{Sku: "S", Location: "A"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got.GetQty() != 5 {
		t.Fatalf("Balance = %d, want 5", got.GetQty())
	}
	all, err := svc.Balances(ctx, &nodepb.Empty{})
	if err != nil {
		t.Fatalf("Balances: %v", err)
	}
	if len(all.GetEntries()) != 2 {
		t.Fatalf("Balances returned %d entries, want 2", len(all.GetEntries()))
	}
}

func TestNodeServiceErrorMapping(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(svc *transport.NodeService) error
		want codes.Code
	}{
		{
			name: "negative balance is a failed precondition",
			call: func(s *transport.NodeService) error {
				_, err := s.Issue(ctx, &nodepb.IssueRequest{Sku: "S", Location: "A", Qty: 1})
				return err
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "unowned location is permission denied",
			call: func(s *transport.NodeService) error {
				_, err := s.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "X", Qty: 1})
				return err
			},
			want: codes.PermissionDenied,
		},
		{
			name: "bad quantity is invalid argument",
			call: func(s *transport.NodeService) error {
				_, err := s.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 0})
				return err
			},
			want: codes.InvalidArgument,
		},
		{
			name: "unowned move source is permission denied",
			call: func(s *transport.NodeService) error {
				_, err := s.Move(ctx, &nodepb.MoveRequest{Sku: "S", From: "X", To: "A", Qty: 1})
				return err
			},
			want: codes.PermissionDenied,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(service(t))
			if got := status.Code(err); got != tc.want {
				t.Fatalf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

func TestNodeServiceMapsUnexpectedErrorsToInternal(t *testing.T) {
	// A closed store makes Append fail for a reason the domain does not name.
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inv, err := domain.NewInventory(context.Background(), store, clock.New("n1", nil), []string{"A"})
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = transport.NewNodeService(inv).Receive(context.Background(),
		&nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 1})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code = %v (err %v), want Internal", got, err)
	}
}
```

- [ ] **Step 3: Run and watch it fail**

Run: `cd localfirst-go && go test ./transport/...`
Expected: FAIL — `undefined: transport.NewNodeService`.

- [ ] **Step 4: Implement the transport**

`localfirst-go/transport/grpc/node.go`:

```go
// Package grpc adapts the node's client API to gRPC. It is deliberately thin:
// decode the request, call one domain command, map the error. The domain has no
// dependency on this package — the arrow points one way only.
package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// NodeService serves the client API from one node's local state.
type NodeService struct {
	nodepb.UnimplementedNodeServer
	inv *domain.Inventory
}

// NewNodeService wraps an inventory aggregate.
func NewNodeService(inv *domain.Inventory) *NodeService { return &NodeService{inv: inv} }

// Receive adds stock.
func (s *NodeService) Receive(ctx context.Context, req *nodepb.ReceiveRequest) (*nodepb.Empty, error) {
	return empty(s.inv.Receive(ctx, req.GetSku(), req.GetLocation(), req.GetQty()))
}

// Issue removes stock.
func (s *NodeService) Issue(ctx context.Context, req *nodepb.IssueRequest) (*nodepb.Empty, error) {
	return empty(s.inv.Issue(ctx, req.GetSku(), req.GetLocation(), req.GetQty()))
}

// Move transfers stock, possibly to a location owned by another node.
func (s *NodeService) Move(ctx context.Context, req *nodepb.MoveRequest) (*nodepb.Empty, error) {
	return empty(s.inv.Move(ctx, req.GetSku(), req.GetFrom(), req.GetTo(), req.GetQty()))
}

// Balance reports one projected balance.
func (s *NodeService) Balance(_ context.Context, req *nodepb.BalanceRequest) (*nodepb.BalanceResponse, error) {
	return &nodepb.BalanceResponse{Qty: s.inv.Balance(req.GetSku(), req.GetLocation())}, nil
}

// Balances reports every projected balance this node holds.
func (s *NodeService) Balances(context.Context, *nodepb.Empty) (*nodepb.BalancesResponse, error) {
	state := s.inv.Balances()
	out := &nodepb.BalancesResponse{Entries: make([]*nodepb.BalanceEntry, 0, len(state))}
	for k, qty := range state {
		out.Entries = append(out.Entries, &nodepb.BalanceEntry{Sku: k.SKU, Location: k.Location, Qty: qty})
	}
	return out, nil
}

func empty(err error) (*nodepb.Empty, error) {
	if err != nil {
		return nil, toStatus(err)
	}
	return &nodepb.Empty{}, nil
}

// toStatus maps domain errors to gRPC codes. Anything unrecognised is Internal:
// a transport must never turn an unknown failure into a success.
func toStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrNegativeBalance):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrNotOwned):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, domain.ErrBadQty):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
```

- [ ] **Step 5: Run the transport tests**

Run: `cd localfirst-go && go test ./transport/... -cover`
Expected: PASS, 100% for `node.go`.

- [ ] **Step 6: Write the failing node-binary test**

`localfirst-go/cmd/node/node_test.go`:

```go
package main

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// freeAddr asks the OS for an unused port so parallel tests never collide.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

func TestRunServesCommandsAndShutsDownCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	addr := freeAddr(t)
	cfg := Config{
		NodeID:    "n1",
		DBPath:    filepath.Join(t.TempDir(), "n1.db"),
		Listen:    addr,
		Locations: []string{"A"},
		SyncEvery: time.Hour, // no central in this test
	}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = cc.Close() }()
	client := nodepb.NewNodeClient(cc)

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = client.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 4})
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	bal, err := client.Balance(ctx, &nodepb.BalanceRequest{Sku: "S", Location: "A"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if bal.GetQty() != 4 {
		t.Fatalf("Balance = %d, want 4", bal.GetQty())
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not shut down")
	}
}

func TestRunFailsOnAnUnusableDBPath(t *testing.T) {
	err := Run(context.Background(), Config{
		NodeID: "n1", DBPath: filepath.Join(t.TempDir(), "missing", "n1.db"),
		Listen: freeAddr(t), SyncEvery: time.Hour,
	})
	if err == nil {
		t.Fatal("Run must fail when the log cannot be opened")
	}
}

func TestRunFailsOnAnUnusableListenAddress(t *testing.T) {
	err := Run(context.Background(), Config{
		NodeID: "n1", DBPath: filepath.Join(t.TempDir(), "n1.db"),
		Listen: "256.256.256.256:1", SyncEvery: time.Hour,
	})
	if err == nil {
		t.Fatal("Run must fail when it cannot listen")
	}
}

func TestRunDialsCentralWhenConfigured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	// Central is not running: the node must still come up and serve.
	err := Run(ctx, Config{
		NodeID: "n1", DBPath: filepath.Join(t.TempDir(), "n1.db"),
		Listen: freeAddr(t), Locations: []string{"A"},
		CentralAddr: freeAddr(t), SyncEvery: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Run = %v, want nil: an unreachable central must not stop the node", err)
	}
}

// A Moved event is authored by the SOURCE node but credits a location the
// DESTINATION node owns. The destination learns of it only through sync, so
// this test is the one that proves Run passes its Inventory as the sync
// projector: with a nil projector the record lands in n2's log but never
// reaches its in-memory balances, and Balance stays 0 until a restart replays
// the log.
//
// n2 dials n1 as its "central". That works because the sync protocol is
// symmetric (see docs/adr/0004-symmetric-sync-protocol.md) — central is a peer
// with a bigger disk, not a different protocol.
func TestSyncedMovedEventUpdatesTheDestinationNodesBalances(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr1, addr2 := freeAddr(t), freeAddr(t)
	go func() {
		_ = Run(ctx, Config{
			NodeID: "n1", DBPath: filepath.Join(t.TempDir(), "n1.db"),
			Listen: addr1, Locations: []string{"A"}, SyncEvery: time.Hour,
		})
	}()
	go func() {
		_ = Run(ctx, Config{
			NodeID: "n2", DBPath: filepath.Join(t.TempDir(), "n2.db"),
			Listen: addr2, Locations: []string{"B"},
			CentralAddr: addr1, SyncEvery: 20 * time.Millisecond,
		})
	}()

	dial := func(addr string) nodepb.NodeClient {
		cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("NewClient %s: %v", addr, err)
		}
		t.Cleanup(func() { _ = cc.Close() })
		return nodepb.NewNodeClient(cc)
	}
	n1, n2 := dial(addr1), dial(addr2)

	// n1 stocks its own location, then ships 4 units to B, which n2 owns.
	deadline := time.Now().Add(5 * time.Second)
	var err error
	for {
		_, err = n1.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 10})
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("n1.Receive: %v", err)
	}
	if _, err := n1.Move(ctx, &nodepb.MoveRequest{Sku: "S", From: "A", To: "B", Qty: 4}); err != nil {
		t.Fatalf("n1.Move: %v", err)
	}

	// n2 must converge to 4 at B purely through sync.
	deadline = time.Now().Add(5 * time.Second)
	var got int64
	for time.Now().Before(deadline) {
		bal, err := n2.Balance(ctx, &nodepb.BalanceRequest{Sku: "S", Location: "B"})
		if err == nil {
			if got = bal.GetQty(); got == 4 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("n2 Balance(S, B) = %d, want 4 — is Run still passing nil as the sync projector?", got)
}
```

Note on what n2's state looks like after this test: `BalanceReducer` applies both halves of a `Moved`, so n2's projection also carries `A: -4` for a location it does not own. That is expected and harmless — a node's projection is authoritative only for its own locations, and the global sum lives at central. `docs/limitations.md` (Task 14) records it.

- [ ] **Step 7: Implement `cmd/node`**

`localfirst-go/cmd/node/node.go`:

```go
package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
	transport "github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// Config is everything a node needs. Locations are the ones this node
// exclusively owns; nothing else may write to them.
type Config struct {
	NodeID      string
	DBPath      string
	Listen      string
	CentralAddr string // empty means "run standalone"
	Locations   []string
	SyncEvery   time.Duration
}

// Run brings up one node: local log, projected balances, the client API, the
// Sync service (so any peer can replicate from it) and a background sync client.
// It returns when ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	store, err := eventlog.Open(cfg.DBPath, cfg.NodeID)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	inv, err := domain.NewInventory(ctx, store, clock.New(cfg.NodeID, nil), cfg.Locations)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("node: listen %s: %w", cfg.Listen, err)
	}
	srv := grpc.NewServer()
	nodepb.RegisterNodeServer(srv, transport.NewNodeService(inv))
	// inv, NOT nil: records merged from peers must reach this node's in-memory
	// projection. A Moved authored by another node whose destination is one of
	// OUR locations increments our balance, and without a projector here that
	// increment stays invisible until the process restarts and replays the log.
	syncpb.RegisterSyncServer(srv, lfsync.NewServer(store, inv))

	if cfg.CentralAddr != "" {
		cc, err := grpc.NewClient(cfg.CentralAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("node: dial central: %w", err)
		}
		defer func() { _ = cc.Close() }()
		go lfsync.NewClient(cc, store, inv, "central").Run(ctx, cfg.SyncEvery)
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	select {
	case <-ctx.Done():
		srv.GracefulStop()
		return nil
	case err := <-errc:
		return err
	}
}
```

`localfirst-go/cmd/node/main.go`:

```go
// Command node runs one local-first inventory node: it owns some locations,
// accepts commands with no network, and reconciles with central in the
// background.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	var cfg Config
	var locations string
	flag.StringVar(&cfg.NodeID, "id", "node-1", "this node's id; also authors every record")
	flag.StringVar(&cfg.DBPath, "db", "node.db", "path to this node's SQLite log")
	flag.StringVar(&cfg.Listen, "listen", ":8080", "client + sync listen address")
	flag.StringVar(&cfg.CentralAddr, "central", "", "central address; empty to run standalone")
	flag.StringVar(&locations, "locations", "", "comma-separated locations this node owns")
	flag.DurationVar(&cfg.SyncEvery, "sync-every", 2*time.Second, "sync interval")
	flag.Parse()
	if locations != "" {
		cfg.Locations = strings.Split(locations, ",")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, cfg); err != nil {
		slog.Error("node stopped", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 8: Run the node tests**

Run: `cd localfirst-go && go test ./cmd/node/... -race -cover`
Expected: PASS. `main()` itself is not covered; that is the one accepted exception, and it exists precisely because every line of logic lives in `Run`. Verify with `go tool cover -func` that `main` is the only uncovered function in the package.

- [ ] **Step 9: Full suite, lint, commit**

```bash
cd localfirst-go && go test ./... -race -cover && golangci-lint run
git add localfirst-go/transport localfirst-go/cmd/node
git commit -m "feat(transport,node): thin gRPC node API and the node binary"
```

---

### Task 9: `central/` on Postgres and `cmd/central`

**Why:** central holds the same records in a bigger database and answers "what is the global stock?" as a **sum of per-node balances** — never a merge. It speaks the identical protocol, so `cmd/central` differs from `cmd/node` in exactly two ways: the storage backend, and having no local domain commands.

**Files:**
- Create: `localfirst-go/central/pgstore.go`, `localfirst-go/central/pgstore_test.go`, `localfirst-go/cmd/central/central.go`, `localfirst-go/cmd/central/main.go`, `localfirst-go/cmd/central/central_test.go`
- Modify: `localfirst-go/Makefile` (add `pg-up`, make `test` depend on it)

**Interfaces:**
- Consumes: `sync.Log` (Task 7), `eventlog.Record`, `eventlog.VersionVector`, `eventlog.Projector`, `domain.BalanceReducer`, `projection.FoldRecords`.
- Produces:
  - `func OpenPG(ctx context.Context, dsn, nodeID string) (*PGStore, error)`, `func (s *PGStore) Close()`
  - `PGStore` satisfies `sync.Log`: `NodeID`, `Since`, `Merge`, `Version`, `Cursor`, `SetCursor`
  - `func (s *PGStore) GlobalSum(ctx context.Context) (map[string]int64, error)` — SKU → total across all nodes
  - `func Run(ctx context.Context, cfg Config) error` in package `main` of `cmd/central`, `type Config struct { DSN, Listen, NodeID string }`

- [ ] **Step 1: Make Postgres available to the test suite**

Add to `localfirst-go/Makefile` and change `test` to depend on it. Postgres is required, not optional: a skipped test is a lie about coverage.

```make
PG_DSN ?= postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable

.PHONY: pg-up pg-down
pg-up:
	docker compose up -d postgres
	@until docker compose exec -T postgres pg_isready -U localfirst >/dev/null 2>&1; do sleep 0.3; done

pg-down:
	docker compose rm -sf postgres

test: pg-up
	LOCALFIRST_PG_DSN="$(PG_DSN)" CGO_ENABLED=0 go test ./... -cover
```

- [ ] **Step 2: Add the dependency**

```bash
cd localfirst-go && go get github.com/jackc/pgx/v5
```

- [ ] **Step 3: Write the failing Postgres test**

`localfirst-go/central/pgstore_test.go`:

```go
package central_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/central"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// pgStore returns a store on a schema-isolated database. It FAILS rather than
// skips when Postgres is absent: run `make test`, which starts it.
func pgStore(t *testing.T) *central.PGStore {
	t.Helper()
	dsn := os.Getenv("LOCALFIRST_PG_DSN")
	if dsn == "" {
		t.Fatal("LOCALFIRST_PG_DSN is unset — run `make test`, which starts Postgres")
	}
	s, err := central.OpenPG(context.Background(), dsn, "central")
	if err != nil {
		t.Fatalf("OpenPG: %v", err)
	}
	if err := s.TruncateForTest(context.Background()); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func received(t *testing.T, node string, seq uint64, loc string, qty int64) eventlog.Record {
	t.Helper()
	payload, err := json.Marshal(domain.Received{SKU: "S", Location: loc, Qty: qty})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return eventlog.Record{
		NodeID: node, Seq: seq, Clock: clock.HLC{Wall: int64(seq), NodeID: node},
		Type: domain.TypeReceived, Payload: payload,
	}
}

func TestPGStoreSatisfiesTheSameContractAsSQLite(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)

	if s.NodeID() != "central" {
		t.Fatalf("NodeID = %q", s.NodeID())
	}
	batch := []eventlog.Record{received(t, "n1", 1, "A", 5), received(t, "n2", 1, "B", 3)}
	n, err := s.Merge(ctx, batch, nil)
	if err != nil || n != 2 {
		t.Fatalf("Merge = (%d, %v), want (2, nil)", n, err)
	}
	if n, err := s.Merge(ctx, batch, nil); err != nil || n != 0 {
		t.Fatalf("re-Merge = (%d, %v), want (0, nil): duplicates must be no-ops", n, err)
	}
	if n, err := s.Merge(ctx, nil, nil); err != nil || n != 0 {
		t.Fatalf("Merge(nil) = (%d, %v), want (0, nil)", n, err)
	}

	vv, err := s.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if !vv.Equal(eventlog.VersionVector{"n1": 1, "n2": 1}) {
		t.Fatalf("Version = %v", vv)
	}
	all, err := s.Since(ctx, nil)
	if err != nil || len(all) != 2 {
		t.Fatalf("Since(nil) = (%d, %v), want 2", len(all), err)
	}
	partial, err := s.Since(ctx, eventlog.VersionVector{"n1": 1})
	if err != nil || len(partial) != 1 || partial[0].NodeID != "n2" {
		t.Fatalf("Since(vv) = (%+v, %v), want only n2's record", partial, err)
	}

	if got, err := s.Cursor(ctx, "n1"); got != 0 || err != nil {
		t.Fatalf("Cursor = (%d, %v), want (0, nil)", got, err)
	}
	if err := s.SetCursor(ctx, "n1", 4); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	if err := s.SetCursor(ctx, "n1", 6); err != nil {
		t.Fatalf("SetCursor upsert: %v", err)
	}
	if got, err := s.Cursor(ctx, "n1"); got != 6 || err != nil {
		t.Fatalf("Cursor = (%d, %v), want (6, nil)", got, err)
	}
}

// TestGlobalSumIsASumNotAMerge is the conflict model in test form.
func TestGlobalSumIsASumNotAMerge(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	if _, err := s.Merge(ctx, []eventlog.Record{
		received(t, "n1", 1, "A", 5), // node 1 owns A
		received(t, "n2", 1, "B", 3), // node 2 owns B
		received(t, "n3", 1, "C", 7), // node 3 owns C
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	sum, err := s.GlobalSum(ctx)
	if err != nil {
		t.Fatalf("GlobalSum: %v", err)
	}
	if sum["S"] != 15 {
		t.Fatalf("global sum = %d, want 15 (5+3+7, summed because no key is shared)", sum["S"])
	}
}

func TestGlobalSumFailsHardOnAnUnknownRecordType(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	if _, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n1", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n1"}, Type: "inventory.Teleported", Payload: []byte("{}")},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if _, err := s.GlobalSum(ctx); err == nil {
		t.Fatal("GlobalSum must fail on an unknown type, not skip it")
	}
}

func TestOpenPGRejectsABadDSN(t *testing.T) {
	if _, err := central.OpenPG(context.Background(), "postgres://nobody@127.0.0.1:1/none", "central"); err == nil {
		t.Fatal("OpenPG must fail against an unreachable database")
	}
}

func TestPGStoreFailsAfterClose(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	s.Close()
	if _, err := s.Version(ctx); err == nil {
		t.Error("Version after Close must fail")
	}
	if _, err := s.Since(ctx, nil); err == nil {
		t.Error("Since after Close must fail")
	}
	if _, err := s.Merge(ctx, []eventlog.Record{received(t, "n1", 1, "A", 1)}, nil); err == nil {
		t.Error("Merge after Close must fail")
	}
	if _, err := s.Cursor(ctx, "n1"); err == nil {
		t.Error("Cursor after Close must fail")
	}
	if err := s.SetCursor(ctx, "n1", 1); err == nil {
		t.Error("SetCursor after Close must fail")
	}
	if _, err := s.GlobalSum(ctx); err == nil {
		t.Error("GlobalSum after Close must fail")
	}
}

// TestMergeProjectsWhenGivenAProjector covers the projector path central uses
// to keep a live in-memory view.
func TestMergeProjectsWhenGivenAProjector(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	p := &recorder{}
	if _, err := s.Merge(ctx, []eventlog.Record{received(t, "n1", 1, "A", 5)}, p); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if len(p.committed) != 1 {
		t.Fatalf("projected %d records, want 1", len(p.committed))
	}
}

type recorder struct{ committed []eventlog.Record }

func (r *recorder) Project(rec eventlog.Record) (func(), error) {
	return func() { r.committed = append(r.committed, rec) }, nil
}
```

- [ ] **Step 4: Run and watch it fail**

Run: `cd localfirst-go && make pg-up && LOCALFIRST_PG_DSN="postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable" go test ./central/...`
Expected: FAIL — `undefined: central.OpenPG`. (`make pg-up` needs `docker-compose.yml`; if it is not written yet, create just the `postgres` service now — Task 16 replaces this file wholesale, keeping the same service name, credentials and host port:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: localfirst
      POSTGRES_PASSWORD: localfirst
      POSTGRES_DB: localfirst
    ports: ["5433:5432"]
```
)

- [ ] **Step 5: Implement the Postgres store**

`localfirst-go/central/pgstore.go`:

```go
// Package central stores every node's records in Postgres and answers global
// questions. It is NOT a different kind of participant: it speaks the same sync
// protocol as a node and satisfies the same sync.Log contract.
package central

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// pgSchema mirrors the SQLite schema, including the composite primary key that
// makes duplicate delivery free.
const pgSchema = `
CREATE TABLE IF NOT EXISTS records (
  node_id     TEXT   NOT NULL,
  seq         BIGINT NOT NULL,
  hlc_wall    BIGINT NOT NULL,
  hlc_logical BIGINT NOT NULL,
  type        TEXT   NOT NULL,
  payload     BYTEA  NOT NULL,
  PRIMARY KEY (node_id, seq)
);
CREATE TABLE IF NOT EXISTS cursors (peer TEXT PRIMARY KEY, last_seq BIGINT NOT NULL);
`

// PGStore is central's log.
type PGStore struct {
	pool   *pgxpool.Pool
	nodeID string
}

// OpenPG connects to dsn and ensures the schema exists.
func OpenPG(ctx context.Context, dsn, nodeID string) (*PGStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("central: pool: %w", err)
	}
	if _, err := pool.Exec(ctx, pgSchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("central: schema: %w", err)
	}
	return &PGStore{pool: pool, nodeID: nodeID}, nil
}

// Close releases the pool.
func (s *PGStore) Close() { s.pool.Close() }

// NodeID returns central's own id. Central authors no records of its own, but
// the protocol is symmetric, so it still has an identity.
func (s *PGStore) NodeID() string { return s.nodeID }

// TruncateForTest empties the tables. Exported so the test package (which is
// external, central_test) can isolate cases; it is never called by binaries.
func (s *PGStore) TruncateForTest(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE records, cursors`)
	return err
}

// Merge stores records from a peer and returns how many were new.
func (s *PGStore) Merge(ctx context.Context, recs []eventlog.Record, p eventlog.Projector) (int, error) {
	if len(recs) == 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("central: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted := 0
	var commits []func()
	for _, r := range recs {
		tag, err := tx.Exec(ctx,
			`INSERT INTO records (node_id, seq, hlc_wall, hlc_logical, type, payload)
			 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (node_id, seq) DO NOTHING`,
			r.NodeID, r.Seq, r.Clock.Wall, r.Clock.Logical, r.Type, r.Payload)
		if err != nil {
			return 0, fmt.Errorf("central: insert %s/%d: %w", r.NodeID, r.Seq, err)
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		inserted++
		if p != nil {
			commit, err := p.Project(r)
			if err != nil {
				return 0, fmt.Errorf("central: project %s/%d: %w", r.NodeID, r.Seq, err)
			}
			commits = append(commits, commit)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("central: commit: %w", err)
	}
	for _, c := range commits {
		c()
	}
	return inserted, nil
}

// Since returns every record the caller lacks according to vv, in replay order.
func (s *PGStore) Since(ctx context.Context, vv eventlog.VersionVector) ([]eventlog.Record, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT node_id, seq, hlc_wall, hlc_logical, type, payload FROM records`)
	if err != nil {
		return nil, fmt.Errorf("central: since: %w", err)
	}
	defer rows.Close()

	var out []eventlog.Record
	for rows.Next() {
		var r eventlog.Record
		var logical int64
		if err := rows.Scan(&r.NodeID, &r.Seq, &r.Clock.Wall, &logical, &r.Type, &r.Payload); err != nil {
			return nil, fmt.Errorf("central: scan: %w", err)
		}
		r.Clock.Logical, r.Clock.NodeID = uint32(logical), r.NodeID
		if r.Seq > vv.Get(r.NodeID) {
			out = append(out, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("central: rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return out, nil
}

// Version returns the highest sequence held per author.
func (s *PGStore) Version(ctx context.Context) (eventlog.VersionVector, error) {
	rows, err := s.pool.Query(ctx, `SELECT node_id, MAX(seq) FROM records GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("central: version: %w", err)
	}
	defer rows.Close()

	vv := eventlog.VersionVector{}
	for rows.Next() {
		var node string
		var seq uint64
		if err := rows.Scan(&node, &seq); err != nil {
			return nil, fmt.Errorf("central: scan version: %w", err)
		}
		vv.Observe(node, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("central: rows: %w", err)
	}
	return vv, nil
}

// Cursor reports how far central has pushed to peer.
func (s *PGStore) Cursor(ctx context.Context, peer string) (uint64, error) {
	var seq uint64
	err := s.pool.QueryRow(ctx, `SELECT last_seq FROM cursors WHERE peer = $1`, peer).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("central: cursor %s: %w", peer, err)
	}
	return seq, nil
}

// SetCursor records how far central has pushed to peer.
func (s *PGStore) SetCursor(ctx context.Context, peer string, seq uint64) error {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO cursors (peer, last_seq) VALUES ($1,$2)
		 ON CONFLICT (peer) DO UPDATE SET last_seq = EXCLUDED.last_seq`, peer, seq); err != nil {
		return fmt.Errorf("central: set cursor %s: %w", peer, err)
	}
	return nil
}

// GlobalSum totals stock per SKU across every node.
//
// It is a SUM, never a merge: because each location belongs to exactly one node,
// no two nodes ever contribute a value for the same key, so adding per-node
// balances is exact rather than a reconciliation heuristic.
func (s *PGStore) GlobalSum(ctx context.Context) (map[string]int64, error) {
	recs, err := s.Since(ctx, nil)
	if err != nil {
		return nil, err
	}
	state, err := projection.FoldRecords[domain.State](domain.BalanceReducer{}, recs)
	if err != nil {
		return nil, err
	}
	sum := map[string]int64{}
	for k, qty := range state {
		sum[k.SKU] += qty
	}
	return sum, nil
}

// compile-time proof that central is just a peer.
var _ interface {
	NodeID() string
	Since(context.Context, eventlog.VersionVector) ([]eventlog.Record, error)
	Merge(context.Context, []eventlog.Record, eventlog.Projector) (int, error)
	Version(context.Context) (eventlog.VersionVector, error)
	Cursor(context.Context, string) (uint64, error)
	SetCursor(context.Context, string, uint64) error
} = (*PGStore)(nil)

var _ = clock.HLC{} // keep the clock import honest in doc examples
```

**Note:** delete that last `var _ = clock.HLC{}` line and the `clock` import — it is dead weight and `unused` will flag it. It appears here only to warn you: do not add decorative imports.

- [ ] **Step 6: Implement `cmd/central`**

`localfirst-go/cmd/central/central.go`:

```go
package main

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/central"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// Config is everything central needs. Note what is missing: no owned locations
// and no domain commands. Central only ever receives records.
type Config struct {
	DSN    string
	Listen string
	NodeID string
}

// Run brings up central: Postgres-backed log plus the same Sync service a node
// serves. That is the entire difference between the two binaries.
func Run(ctx context.Context, cfg Config) error {
	store, err := central.OpenPG(ctx, cfg.DSN, cfg.NodeID)
	if err != nil {
		return err
	}
	defer store.Close()

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("central: listen %s: %w", cfg.Listen, err)
	}
	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, lfsync.NewServer(store, nil))

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	select {
	case <-ctx.Done():
		srv.GracefulStop()
		return nil
	case err := <-errc:
		return err
	}
}
```

`localfirst-go/cmd/central/main.go`:

```go
// Command central holds every node's records in Postgres and reports the global
// sum. It is a peer with a bigger disk, not a coordinator.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var cfg Config
	flag.StringVar(&cfg.DSN, "dsn", os.Getenv("LOCALFIRST_PG_DSN"), "Postgres DSN")
	flag.StringVar(&cfg.Listen, "listen", ":9090", "sync listen address")
	flag.StringVar(&cfg.NodeID, "id", "central", "central's peer id")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, cfg); err != nil {
		slog.Error("central stopped", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 7: Write the central binary test**

`localfirst-go/cmd/central/central_test.go`:

```go
package main

import (
	"context"
	"net"
	"os"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("LOCALFIRST_PG_DSN")
	if v == "" {
		t.Fatal("LOCALFIRST_PG_DSN is unset — run `make test`")
	}
	return v
}

func TestRunStartsAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, Config{DSN: dsn(t), Listen: freeAddr(t), NodeID: "central"}) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not shut down")
	}
}

func TestRunFailsOnABadDSN(t *testing.T) {
	if err := Run(context.Background(), Config{
		DSN: "postgres://nobody@127.0.0.1:1/none", Listen: freeAddr(t), NodeID: "central",
	}); err == nil {
		t.Fatal("Run must fail when Postgres is unreachable")
	}
}

func TestRunFailsOnABadListenAddress(t *testing.T) {
	if err := Run(context.Background(), Config{
		DSN: dsn(t), Listen: "256.256.256.256:1", NodeID: "central",
	}); err == nil {
		t.Fatal("Run must fail when it cannot listen")
	}
}
```

- [ ] **Step 8: Run everything**

Run: `cd localfirst-go && make test`
Expected: PASS for all packages; `central` at 100%; `cmd/central` at 100% except `main`.

- [ ] **Step 9: Lint, then commit**

```bash
cd localfirst-go && golangci-lint run
git add localfirst-go/central localfirst-go/cmd/central localfirst-go/Makefile localfirst-go/docker-compose.yml localfirst-go/go.mod localfirst-go/go.sum
git commit -m "feat(central): Postgres-backed peer log with global sum and the central binary"
```

---

### Task 10: The architecture test — prove the engine is domain-agnostic

**Why:** the central claim of this repo is "`eventlog`, `clock`, `sync` and `projection` know nothing about inventory". A comment saying so rots. A test that parses the real import graph does not. This one test is what turns the claim from an assertion into a verified property.

It uses `golang.org/x/tools/go/packages`, which loads the same package metadata the compiler uses, so it sees transitive imports too — an engine package importing something that itself imports `domain` fails just as loudly.

**Files:**
- Create: `localfirst-go/arch/arch_test.go`

**Interfaces:**
- Consumes: nothing at compile time; it inspects the module by path.
- Produces: nothing importable. It is a guard.

- [ ] **Step 1: Add the dependency**

```bash
cd localfirst-go && go get golang.org/x/tools/go/packages
```

- [ ] **Step 2: Write the test**

`localfirst-go/arch/arch_test.go`:

```go
// Package arch holds the architecture guard. It has no production code: its only
// job is to fail the build if the domain leaks into the engine.
package arch_test

import (
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const (
	module = "github.com/dhiazfathra/local-first-architecture/localfirst-go"
	domain = module + "/domain"
)

// engine lists the packages that must remain domain-agnostic. If this list ever
// shrinks, the repo's central claim has been weakened — argue it in an ADR
// first.
var engine = []string{
	module + "/eventlog",
	module + "/clock",
	module + "/sync",
	module + "/projection",
}

// TestEngineDoesNotDependOnTheDomain parses the real import graph — direct and
// transitive — and fails naming the exact path if any engine package can reach
// the domain.
func TestEngineDoesNotDependOnTheDomain(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps}
	pkgs, err := packages.Load(cfg, engine...)
	if err != nil {
		t.Fatalf("load packages: %v", err)
	}
	if n := packages.PrintErrors(pkgs); n > 0 {
		t.Fatalf("%d packages failed to load; fix the build first", n)
	}
	if len(pkgs) != len(engine) {
		t.Fatalf("loaded %d packages, want %d — did a package get renamed?", len(pkgs), len(engine))
	}

	for _, p := range pkgs {
		if path := reaches(p, domain, map[string]bool{}); path != nil {
			t.Errorf("%s must not depend on the domain:\n  %s", p.PkgPath, strings.Join(path, "\n    -> "))
		}
	}
}

// reaches returns the import chain from p to target, or nil if none exists.
func reaches(p *packages.Package, target string, seen map[string]bool) []string {
	if seen[p.PkgPath] {
		return nil
	}
	seen[p.PkgPath] = true

	// Deterministic order, so a failure message is stable across runs.
	names := make([]string, 0, len(p.Imports))
	for path := range p.Imports {
		names = append(names, path)
	}
	sort.Strings(names)

	for _, path := range names {
		if path == target {
			return []string{p.PkgPath, target}
		}
		if !strings.HasPrefix(path, module) {
			continue // third-party and stdlib cannot reach our domain
		}
		if rest := reaches(p.Imports[path], target, seen); rest != nil {
			return append([]string{p.PkgPath}, rest...)
		}
	}
	return nil
}

// TestGuardActuallyDetectsALeak proves the guard is not vacuously green: run it
// against a package that DOES legitimately import the domain and expect a hit.
func TestGuardActuallyDetectsALeak(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps}
	pkgs, err := packages.Load(cfg, module+"/transport/grpc")
	if err != nil {
		t.Fatalf("load packages: %v", err)
	}
	if len(pkgs) != 1 {
		t.Fatalf("loaded %d packages, want 1", len(pkgs))
	}
	path := reaches(pkgs[0], domain, map[string]bool{})
	if path == nil {
		t.Fatal("transport/grpc imports domain, so the guard must find a path; it found none — the guard is broken")
	}
	if path[len(path)-1] != domain {
		t.Fatalf("path %v must end at the domain", path)
	}
}
```

- [ ] **Step 3: Run it**

Run: `cd localfirst-go && go test ./arch/... -v`
Expected: both tests PASS.

- [ ] **Step 4: Prove it fails when it should**

Temporarily add `_ "github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"` to the imports of `localfirst-go/projection/fold.go`, then:

Run: `cd localfirst-go && go test ./arch/...`
Expected: FAIL with `…/projection must not depend on the domain:` and the import chain. **Remove the import again** and re-run to confirm PASS.

- [ ] **Step 5: Lint, then commit**

```bash
cd localfirst-go && golangci-lint run
git add localfirst-go/arch localfirst-go/go.mod localfirst-go/go.sum
git commit -m "test(arch): fail the build if the engine reaches the domain"
```

---

### Task 11: The five ADRs

**Why:** this repo's product is comprehension. An ADR (Architecture Decision Record) is a short, dated, immutable note answering "what did we decide, what did we reject, and what does that cost us". Five decisions carry this whole design; each gets one file, written out in full below. Nothing here is a summary to be expanded later — copy the prose verbatim.

The ADRs come first, before `README.md` and `docs/architecture.md`, because those two link *into* the ADRs. Building the leaves before the index keeps every task's link checker green.

**Files:**
- Create: `localfirst-go/docs/adr/0001-per-node-ownership.md`, `0002-sqlite-local-log.md`, `0003-hlc-for-ordering-only.md`, `0004-symmetric-sync-protocol.md`, `0005-domain-agnostic-engine.md`
- Test: `localfirst-go/docs/links_test.go`

**Interfaces:**
- Consumes: the names established by Tasks 2–9 — `clock.HLC`, `eventlog.Record`, `eventlog.Validator`, `eventlog.Projector`, `projection.Reducer[S]`, `projection.Fold`, `sync.Exchange`, `syncpb.Frame`, `central.PGStore.GlobalSum`. The prose must use these exact spellings.
- Produces: `localfirst-go/docs/adr/000{1..5}-*.md`, and a test-only guard package at `localfirst-go/docs` (no production code) whose `docs.Required` list later tasks extend.

- [ ] **Step 1: Write the failing docs guard**

This is the anti-rot test. Documentation is a deliverable in this repo, so it gets a test like anything else. In this task it has one job: every documented file must exist. Tasks 12–14 extend its `required` table, and Task 14 adds the second half — a relative-link checker — once every linked document exists, so that no task ever ends with a skipped test.

`localfirst-go/docs/links_test.go`:

```go
// Package docs holds no production code. This file is a guard: it fails the
// build if a documented file goes missing or a relative link inside the
// documentation stops resolving.
package docs_test

import (
	"os"
	"path/filepath"
	"testing"
)

// required lists every documentation file this repo promises to ship, relative
// to the module root. Each documentation task appends to it.
var required = []string{
	"docs/adr/0001-per-node-ownership.md",
	"docs/adr/0002-sqlite-local-log.md",
	"docs/adr/0003-hlc-for-ordering-only.md",
	"docs/adr/0004-symmetric-sync-protocol.md",
	"docs/adr/0005-domain-agnostic-engine.md",
}

// root is the module root; this test file lives in docs/.
const root = ".."

func TestRequiredDocsExist(t *testing.T) {
	for _, rel := range required {
		t.Run(rel, func(t *testing.T) {
			if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
				t.Fatalf("documented file missing: %v", err)
			}
		})
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cd localfirst-go && go test ./docs/...`
Expected: FAIL — five `documented file missing` subtests, one per ADR.

- [ ] **Step 3: Write ADR 0001**

`localfirst-go/docs/adr/0001-per-node-ownership.md`:

```markdown
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
- Between the source's decrement and the destination's increment reaching
  central, transferred goods are absent from the global sum. That gap is real
  and documented in [limitations](../limitations.md).

## Alternatives rejected

- **CRDT-valued balances (PN-Counter per key).** Correct and unnecessary here:
  a counter that only one node increments is just an integer.
- **Central arbitration with compensating events.** Buys the ability to
  overspend and repair. We have no overspend to repair, because the owner of a
  location always knows its true balance.
```

- [ ] **Step 4: Write ADR 0002**

`localfirst-go/docs/adr/0002-sqlite-local-log.md`:

```markdown
# 0002 — SQLite as the local log, keyed (node_id, seq)

- **Status:** accepted
- **Date:** 2026-07-30

## Context

Each node needs a durable, ordered, append-only record of the facts it has
authored plus every fact it has learned from peers. It must survive process
restart and power loss, be readable by a curious engineer with no tooling, and
require nothing to be installed or running alongside the node — because a node
that needs a server to accept a write is not local-first.

## Decision

One SQLite file per node, through `modernc.org/sqlite` — a pure-Go driver, so
the whole repo builds with `CGO_ENABLED=0` and a reader can `go build` it on any
machine with no toolchain archaeology.

Two tables, and that is the entire storage layer:

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

**The composite primary key `(node_id, seq)` is the highest-leverage idea in
this repo.** A record's identity is "the Nth thing node X ever said", which is
decided by its author and is stable forever. Combined with `INSERT OR IGNORE`
in `eventlog.Store.Merge`, receiving a record you already have costs one index
probe and changes nothing.

That single line is what buys the whole sync protocol: replication can be
at-least-once, batches can be re-sent after a mid-stream disconnect, a peer can
rewind its version vector and ask again, and none of it needs deduplication
logic, transaction ids, or an exactly-once delivery guarantee that no network
can provide anyway.

Appending is `validate → append → project` inside **one** SQLite transaction:
`eventlog.Store.Append` calls `Validator.Check`, inserts, then calls
`Projector.Project`, which returns a `commit func()` applied only after the
transaction commits. A rejected command leaves no trace, and in-memory derived
state can never disagree with the log on disk.

Reads are `Since(VersionVector)` and nothing else. There is no query language,
no index beyond the primary key, and no way to ask the log a domain question —
that is `projection`'s job.

## Consequences

Good: zero operational surface, a single file to copy or inspect, crash-safe
transactions from a library everyone already trusts, and duplicate delivery made
free rather than made careful.

Bad, and accepted:

- One writer process per file. A node is that process; do not point two at one
  file.
- The log grows without bound. There is no compaction and no snapshotting, so
  replay cost grows linearly with history. See
  [limitations](../limitations.md).
- `Since` scans; with no secondary indexes, "give me everything after these
  marks" is a table scan filtered by `(node_id, seq)`. Fine for a reference
  architecture, not for a million records.

## Alternatives rejected

- **An embedded key-value store (Pebble, BoltDB).** Faster, and it would cost us
  the transaction that spans validate/append/project, plus the ability to open
  the log with `sqlite3` and just look.
- **A cgo SQLite driver.** Faster still, and it would break `CGO_ENABLED=0`
  builds and cross-compilation, for a repo whose deliverable is readability.
- **Append-only file with a hand-rolled framing format.** Educational for
  exactly one afternoon, then a source of corruption bugs.
```

- [ ] **Step 5: Write ADR 0003**

`localfirst-go/docs/adr/0003-hlc-for-ordering-only.md`:

```markdown
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
```

- [ ] **Step 6: Write ADR 0004**

`localfirst-go/docs/adr/0004-symmetric-sync-protocol.md`:

```markdown
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
```

- [ ] **Step 7: Write ADR 0005**

`localfirst-go/docs/adr/0005-domain-agnostic-engine.md`:

```markdown
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
```

- [ ] **Step 8: Run the guard and watch it pass**

Run: `cd localfirst-go && go test ./docs/... -v`
Expected: PASS — five `TestRequiredDocsExist` subtests, one per ADR.

Note the forward references these ADRs contain: `../limitations.md`,
`../swapping-the-domain.md`, `swapping-the-domain.md` and `../architecture.md`
do not exist yet. That is deliberate and is exactly why the link checker lands
in Task 14 rather than here — a task must never end with a skipped or failing
test.

- [ ] **Step 9: Full suite, lint, commit**

Run: `cd localfirst-go && make test && golangci-lint run`
Expected: PASS everywhere, no lint output.

```bash
git add localfirst-go/docs
git commit -m "docs(adr): record the five decisions that carry this design"
```

---

### Task 12: `docs/architecture.md` — package, lifecycle and sync diagrams

**Why:** the ADRs explain each decision; this file is the map that shows how they fit. Three mermaid diagrams — mermaid because it reviews as text in a diff and needs no binary assets — plus the prose that makes each diagram mean something.

**Files:**
- Create: `localfirst-go/docs/architecture.md`
- Modify: `localfirst-go/docs/links_test.go` (add to `required`)

**Interfaces:**
- Consumes: every exported name from Tasks 2–9, spelled exactly: `clock.Clock.Now`, `clock.HLC`, `eventlog.Store.Append`, `eventlog.Store.Merge`, `eventlog.Store.Since`, `eventlog.Validator.Check`, `eventlog.Projector.Project`, `projection.Fold`, `projection.Reducer`, `domain.Inventory`, `domain.BalanceReducer`, `sync.Exchange`, `sync.NewServer`, `sync.NewClient`, `syncpb.Frame`, `central.PGStore.GlobalSum`, `nodepb.NodeServer`.
- Produces: `localfirst-go/docs/architecture.md`.

- [ ] **Step 1: Add it to the guard and watch it fail**

Add `"docs/architecture.md"` to `required` in `localfirst-go/docs/links_test.go`, so the list reads:

```go
var required = []string{
	"docs/adr/0001-per-node-ownership.md",
	"docs/adr/0002-sqlite-local-log.md",
	"docs/adr/0003-hlc-for-ordering-only.md",
	"docs/adr/0004-symmetric-sync-protocol.md",
	"docs/adr/0005-domain-agnostic-engine.md",
	"docs/architecture.md",
}
```

Run: `cd localfirst-go && go test ./docs/... -run TestRequiredDocsExist`
Expected: FAIL — `documented file missing: … docs/architecture.md`.

- [ ] **Step 2: Write the document**

`localfirst-go/docs/architecture.md`:

```markdown
# Architecture

Three diagrams and the prose to read them by. If you only have five minutes,
read the package diagram and the event lifecycle; the sync sequence is the
detail underneath.

## Packages

The dividing line runs down the middle of this diagram: four packages know
nothing about inventory, one package is the entire domain, and two thin layers
join them.

```mermaid
flowchart TB
    subgraph engine["engine — domain-agnostic (must never import domain)"]
        clock["clock<br/>hybrid logical clock"]
        eventlog["eventlog<br/>Record, Validator, Projector<br/>SQLite append-only store"]
        projection["projection<br/>Reducer[S], Fold"]
        sync["sync<br/>symmetric gRPC replication"]
    end

    subgraph app["domain-aware"]
        domain["domain<br/>THE ONLY inventory-aware package<br/>Received, Issued, Moved<br/>BalanceReducer, Inventory"]
        transport["transport/grpc<br/>node client API"]
        central["central<br/>Postgres log + GlobalSum"]
    end

    subgraph bins["binaries"]
        cmdnode["cmd/node"]
        cmdcentral["cmd/central"]
    end

    eventlog --> clock
    projection --> eventlog
    sync --> eventlog
    domain --> eventlog
    domain --> projection
    domain --> clock
    transport --> domain
    central --> eventlog
    central --> projection
    central --> domain
    central --> sync
    cmdnode --> transport
    cmdnode --> sync
    cmdnode --> domain
    cmdcentral --> central
    cmdcentral --> sync

    arch["arch/arch_test.go<br/>parses the real import graph<br/>fails if engine reaches domain"]
    arch -.guards.-> engine
```

Read the arrows: everything in `engine` points only at other `engine` packages
or the standard library. Nothing points right-to-left. `arch/arch_test.go`
enforces that with `golang.org/x/tools/go/packages`, so the claim is verified on
every `go test` rather than maintained by good intentions. See
[ADR 0005](adr/0005-domain-agnostic-engine.md).

`central` is allowed to import `domain` because it must interpret payloads to
compute a global sum — it is an application of the engine, not part of it.

## Event lifecycle

One command, start to finish. The important part is the box: validate, append
and project happen inside a single SQLite transaction, so a rejected command
leaves nothing behind and derived state can never disagree with the log.

```mermaid
flowchart TD
    A["client calls Node.Receive via gRPC"] --> B["transport/grpc NodeService<br/>decode request"]
    B --> C["domain.Inventory.Receive<br/>ownership check: is this location mine?"]
    C -->|not owned| X1["ErrNotOwned — nothing appended"]
    C -->|qty <= 0| X2["ErrBadQty — nothing appended"]
    C -->|ok| D["encode domain.Received to JSON payload<br/>type = domain.TypeReceived"]
    D --> E["clock.Clock.Now → clock.HLC"]
    E --> F["eventlog.Store.Append"]

    subgraph tx["ONE SQLite transaction"]
        F --> G["Validator.Check(record)<br/>domain.Inventory: would the reducer accept this?<br/>balance must not go negative"]
        G -->|error| H["ROLLBACK<br/>ErrNegativeBalance — no record on disk"]
        G -->|ok| I["INSERT OR IGNORE INTO records<br/>PRIMARY KEY (node_id, seq)"]
        I --> J["Projector.Project(record)<br/>returns commit func()"]
        J --> K["COMMIT"]
    end

    K --> L["run commit func(): swap in new domain.State"]
    L --> M["record is durable and locally visible<br/>no network was involved"]
    M --> N["background sync.Client picks it up on its next pass"]
```

Two things a reader should take from this:

- The command path never touches the network. A node with no connectivity, or no
  central configured at all, serves every call in this diagram.
- `Validator.Check` is the seam that lets the engine enforce an invariant it
  cannot understand. `eventlog` knows only that a `Check` returned an error.

Rebuilding state on startup is the same machinery run backwards:
`projection.Fold(ctx, store, domain.BalanceReducer{})` replays every record
through the reducer. An unrecognised `Type` is `eventlog.ErrUnknownType` and
stops the process — never a skipped record.

## Sync sequence

`sync.Exchange` is one function used by both sides. The only difference between
the two participants is the `initiator bool` argument: who speaks first.

```mermaid
sequenceDiagram
    autonumber
    participant N as node-3 (initiator)
    participant C as central (responder)

    Note over N,C: same Frame type in both directions — central is a peer with a bigger disk

    N->>C: Frame{Hello{node_id:"node-3", version:{node-3:41, node-1:12}}}
    C->>N: Frame{Hello{node_id:"central", version:{node-1:12, node-2:7, node-3:20}}}

    Note over C: eventlog Since(node-3's version)<br/>→ records node-3 lacks
    C->>N: Frame{Batch{records:[node-2/1 … node-2/7]}}
    C->>N: Frame{Ack{version:{node-1:12, node-2:7, node-3:20}}}

    Note over N: Merge → INSERT OR IGNORE<br/>duplicates cost nothing<br/>clock.Observe pulls local HLC forward

    N->>C: Frame{Batch{records:[node-3/21 … node-3/41]}}
    N->>C: Frame{Ack{version:{node-1:12, node-2:7, node-3:41}}}
    N--)C: close send side (EOF)

    Note over C: Merge, then recompute GlobalSum<br/>sum of per-node balances, never a merge
```

If that stream dies anywhere — step 3, step 7, mid-batch — nothing needs
repairing. Both sides keep whatever they had already committed, and the next
attempt starts again from `Hello` with current version vectors. Re-delivered
records hit the `(node_id, seq)` primary key and vanish. That is the entire
resumption story, and it is a consequence of
[ADR 0002](adr/0002-sqlite-local-log.md) rather than code anyone had to write.

A version vector that has gone *backwards* (a peer restored from an older
backup) is equally harmless: it asks for records it already has, and gets them
again for free.

## Where your domain plugs in

Two interfaces, and a command encoder:

```go
// reading
type Reducer[S any] interface {
	Zero() S
	Apply(state S, r eventlog.Record) (S, error)
}

// writing, called inside the append transaction
type Validator interface {
	Check(r Record) error
}
```

[swapping-the-domain](swapping-the-domain.md) does it end to end with a second
toy domain, in compiling, tested code. [limitations](limitations.md) lists what
this architecture deliberately does not do.
```

- [ ] **Step 3: Run the guard**

Run: `cd localfirst-go && go test ./docs/... -run TestRequiredDocsExist -v`
Expected: PASS, six subtests.

- [ ] **Step 4: Eyeball the diagrams**

Mermaid syntax errors are silent in a diff and loud on GitHub. Paste each of the
three blocks into <https://mermaid.live> and confirm all three render. A diagram
that fails to parse is a broken deliverable in this repo.

- [ ] **Step 5: Full suite, lint, commit**

Run: `cd localfirst-go && make test && golangci-lint run`
Expected: PASS everywhere, no lint output.

```bash
git add localfirst-go/docs
git commit -m "docs(architecture): package, event-lifecycle and sync diagrams"
```

---

### Task 13: `examples/tasklist/` + `docs/swapping-the-domain.md` — the seam, proven

**Why:** every other task *claims* the engine is domain-agnostic. This one proves it by replacing inventory with a completely unrelated domain — a shared task list — and touching zero engine files. The replacement domain is **real compiling, tested code** at `examples/tasklist/`, and the guide quotes it, so the documentation cannot drift from something that works.

The second domain is chosen to look nothing like the first: no quantities, no arithmetic, string-keyed entities, and an invariant about *existence* rather than about a number. If the seam only fits things shaped like inventory, this task fails to compile.

**Files:**
- Create: `localfirst-go/examples/tasklist/tasklist.go`, `localfirst-go/docs/swapping-the-domain.md`
- Test: `localfirst-go/examples/tasklist/tasklist_test.go`
- Modify: `localfirst-go/docs/links_test.go` (add to `required`)

**Interfaces:**
- Consumes: `eventlog.Record`, `eventlog.ErrUnknownType`, `eventlog.Open`, `*eventlog.Store`, `eventlog.Store.Append(ctx, typ string, payload []byte, ts clock.HLC, v Validator, p Projector) (Record, error)`, `projection.Reducer[S]`, `projection.Fold`, `clock.New`, `clock.Clock.Now` (Tasks 2–5). It must **not** import `domain`.
- Produces:
  - `const TypeAdded = "tasklist.Added"`, `TypeCompleted = "tasklist.Completed"`
  - `type Added struct { ID, Title string }`, `type Completed struct { ID string }`
  - `type Task struct { Title string; Done bool }`, `type State map[string]Task`, `func (s State) Clone() State`
  - `type Reducer struct{}` implementing `projection.Reducer[State]`
  - `var ErrNoSuchTask`, `ErrAlreadyDone`, `ErrDuplicateTask`, `ErrEmptyID`, `ErrBadPayload`
  - `func NewList(ctx context.Context, store *eventlog.Store, clk *clock.Clock) (*List, error)`
  - `func (l *List) Add(ctx context.Context, id, title string) error`, `func (l *List) Complete(ctx context.Context, id string) error`
  - `func (l *List) Tasks() State`, `func (l *List) Check(r eventlog.Record) error`, `func (l *List) Project(r eventlog.Record) (func(), error)`
  - `localfirst-go/docs/swapping-the-domain.md`

- [ ] **Step 1: Write the failing reducer test**

Table-driven, because the reducer is a pure function of `(state, record)` and that is exactly the shape a table tests well.

`localfirst-go/examples/tasklist/tasklist_test.go`:

```go
package tasklist_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/examples/tasklist"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// rec builds a record the way the engine would, with an already-encoded payload.
func rec(t *testing.T, seq uint64, typ string, payload any) eventlog.Record {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return eventlog.Record{
		NodeID:  "n1",
		Seq:     seq,
		Clock:   clock.HLC{Wall: int64(seq), Logical: 0, NodeID: "n1"},
		Type:    typ,
		Payload: body,
	}
}

func TestReducerApply(t *testing.T) {
	tests := []struct {
		name    string
		records []eventlog.Record
		want    tasklist.State
		wantErr error
	}{
		{
			name: "empty log folds to an empty list",
			want: tasklist.State{},
		},
		{
			name:    "one add",
			records: []eventlog.Record{rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "write the ADR"})},
			want:    tasklist.State{"t1": {Title: "write the ADR"}},
		},
		{
			name: "add then complete",
			records: []eventlog.Record{
				rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "write the ADR"}),
				rec(t, 2, tasklist.TypeCompleted, tasklist.Completed{ID: "t1"}),
			},
			want: tasklist.State{"t1": {Title: "write the ADR", Done: true}},
		},
		{
			name: "two independent tasks",
			records: []eventlog.Record{
				rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "a"}),
				rec(t, 2, tasklist.TypeAdded, tasklist.Added{ID: "t2", Title: "b"}),
				rec(t, 3, tasklist.TypeCompleted, tasklist.Completed{ID: "t2"}),
			},
			want: tasklist.State{"t1": {Title: "a"}, "t2": {Title: "b", Done: true}},
		},
		{
			name:    "completing an unknown task is an error, not a no-op",
			records: []eventlog.Record{rec(t, 1, tasklist.TypeCompleted, tasklist.Completed{ID: "ghost"})},
			wantErr: tasklist.ErrNoSuchTask,
		},
		{
			name: "completing twice is an error",
			records: []eventlog.Record{
				rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "a"}),
				rec(t, 2, tasklist.TypeCompleted, tasklist.Completed{ID: "t1"}),
				rec(t, 3, tasklist.TypeCompleted, tasklist.Completed{ID: "t1"}),
			},
			wantErr: tasklist.ErrAlreadyDone,
		},
		{
			name: "adding the same id twice is an error",
			records: []eventlog.Record{
				rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "a"}),
				rec(t, 2, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "a again"}),
			},
			wantErr: tasklist.ErrDuplicateTask,
		},
		{
			name:    "an unknown record type is a hard error",
			records: []eventlog.Record{rec(t, 1, "tasklist.Renamed", map[string]string{"id": "t1"})},
			wantErr: eventlog.ErrUnknownType,
		},
		{
			name: "a corrupt payload is a hard error",
			records: []eventlog.Record{{
				NodeID: "n1", Seq: 1, Type: tasklist.TypeAdded, Payload: []byte("{not json"),
			}},
			wantErr: tasklist.ErrBadPayload,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := projection.FoldRecords[tasklist.State](tasklist.Reducer{}, tc.records)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FoldRecords = %v, want nil", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("state = %v, want %v", got, tc.want)
			}
			for id, want := range tc.want {
				if got[id] != want {
					t.Errorf("state[%q] = %+v, want %+v", id, got[id], want)
				}
			}
		})
	}
}

func TestCloneDoesNotAliasTheOriginal(t *testing.T) {
	s := tasklist.State{"t1": {Title: "a"}}
	c := s.Clone()
	c["t1"] = tasklist.Task{Title: "mutated", Done: true}
	if s["t1"].Title != "a" || s["t1"].Done {
		t.Fatalf("Clone aliased the original: %+v", s["t1"])
	}
}

func newList(t *testing.T) (*tasklist.List, *eventlog.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tasks.db")
	return openList(t, path)
}

func openList(t *testing.T, path string) (*tasklist.List, *eventlog.Store, string) {
	t.Helper()
	store, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	wall := time.Unix(0, 0)
	clk := clock.New("n1", func() time.Time { wall = wall.Add(time.Millisecond); return wall })
	list, err := tasklist.NewList(context.Background(), store, clk)
	if err != nil {
		t.Fatalf("NewList: %v", err)
	}
	return list, store, path
}

func TestAddAndCompleteThroughTheEngine(t *testing.T) {
	list, _, _ := newList(t)
	ctx := context.Background()

	if err := list.Add(ctx, "t1", "write the ADR"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := list.Complete(ctx, "t1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got := list.Tasks()["t1"]; got.Title != "write the ADR" || !got.Done {
		t.Fatalf("task = %+v, want {write the ADR true}", got)
	}
}

func TestAddRejectsAnEmptyID(t *testing.T) {
	list, _, _ := newList(t)
	if err := list.Add(context.Background(), "", "x"); !errors.Is(err, tasklist.ErrEmptyID) {
		t.Fatalf("Add(\"\") = %v, want ErrEmptyID", err)
	}
}

// The whole point of the Validator seam: a rejected command must leave the log
// exactly as it was.
func TestRejectedCommandAppendsNothing(t *testing.T) {
	list, store, _ := newList(t)
	ctx := context.Background()

	before, err := store.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if err := list.Complete(ctx, "ghost"); !errors.Is(err, tasklist.ErrNoSuchTask) {
		t.Fatalf("Complete(ghost) = %v, want ErrNoSuchTask", err)
	}
	after, err := store.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if !before.Equal(after) {
		t.Fatalf("version moved from %v to %v: a rejected command must append nothing", before, after)
	}
	if len(list.Tasks()) != 0 {
		t.Fatalf("tasks = %v, want empty", list.Tasks())
	}
}

func TestDuplicateAddIsRejected(t *testing.T) {
	list, _, _ := newList(t)
	ctx := context.Background()
	if err := list.Add(ctx, "t1", "a"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := list.Add(ctx, "t1", "a again"); !errors.Is(err, tasklist.ErrDuplicateTask) {
		t.Fatalf("second Add = %v, want ErrDuplicateTask", err)
	}
}

// Reopening the file and replaying must reconstruct identical state: the log is
// the truth, the map is a cache.
func TestStateSurvivesAReopen(t *testing.T) {
	list, store, path := newList(t)
	ctx := context.Background()
	if err := list.Add(ctx, "t1", "a"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := list.Add(ctx, "t2", "b"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := list.Complete(ctx, "t1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	want := list.Tasks()
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, _, _ := openList(t, path)
	got := reopened.Tasks()
	if len(got) != len(want) {
		t.Fatalf("replayed %v, want %v", got, want)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("replayed[%q] = %+v, want %+v", id, got[id], w)
		}
	}
}

// Fold reads through eventlog.Reader, so the same reducer works against the
// store as well as against a slice.
func TestFoldOverTheStore(t *testing.T) {
	list, store, _ := newList(t)
	ctx := context.Background()
	if err := list.Add(ctx, "t1", "a"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := projection.Fold[tasklist.State](ctx, store, tasklist.Reducer{})
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	if got["t1"].Title != "a" {
		t.Fatalf("folded %v, want t1 titled a", got)
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cd localfirst-go && go test ./examples/... -v`
Expected: FAIL to build — `no required module provides package …/examples/tasklist`.

- [ ] **Step 3: Write the replacement domain**

`localfirst-go/examples/tasklist/tasklist.go`:

```go
// Package tasklist is the worked example behind docs/swapping-the-domain.md: a
// second, unrelated domain running on the same engine as inventory. It imports
// eventlog, projection and clock, and nothing else of ours — in particular it
// does not import domain. Compare it side by side with domain/ and note that no
// engine file differs between the two.
package tasklist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// Record types. The engine treats these as opaque strings; only this package
// knows what they mean.
const (
	TypeAdded     = "tasklist.Added"
	TypeCompleted = "tasklist.Completed"
)

// Added and Completed are the entire event vocabulary of this domain.
type Added struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

// Completed marks an existing task done.
type Completed struct {
	ID string `json:"id"`
}

// Task is the projected form of one task.
type Task struct {
	Title string
	Done  bool
}

// State is the projection: every task this node knows about, by id.
type State map[string]Task

// Clone returns an independent copy. The reducer never mutates the state it is
// given, so a failed Apply cannot leave a half-updated projection behind.
func (s State) Clone() State {
	out := make(State, len(s))
	for id, t := range s {
		out[id] = t
	}
	return out
}

// Domain errors. Each names the rule it enforces, because the engine cannot.
var (
	ErrNoSuchTask    = errors.New("tasklist: no such task")
	ErrAlreadyDone   = errors.New("tasklist: task already completed")
	ErrDuplicateTask = errors.New("tasklist: task id already exists")
	ErrEmptyID       = errors.New("tasklist: task id must not be empty")
	ErrBadPayload    = errors.New("tasklist: undecodable payload")
)

// Reducer folds records into State. This is one of the two seams: it satisfies
// projection.Reducer[State], so projection.Fold works over it unchanged.
type Reducer struct{}

// Zero is the state of a node that has seen no records.
func (Reducer) Zero() State { return State{} }

// Apply interprets one record. It is a pure function: same state plus same
// record always yields the same result, which is what makes replay
// deterministic.
func (Reducer) Apply(state State, r eventlog.Record) (State, error) {
	switch r.Type {
	case TypeAdded:
		var e Added
		if err := decode(r, &e); err != nil {
			return nil, err
		}
		if _, exists := state[e.ID]; exists {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateTask, e.ID)
		}
		out := state.Clone()
		out[e.ID] = Task{Title: e.Title}
		return out, nil

	case TypeCompleted:
		var e Completed
		if err := decode(r, &e); err != nil {
			return nil, err
		}
		t, exists := state[e.ID]
		if !exists {
			return nil, fmt.Errorf("%w: %s", ErrNoSuchTask, e.ID)
		}
		if t.Done {
			return nil, fmt.Errorf("%w: %s", ErrAlreadyDone, e.ID)
		}
		out := state.Clone()
		t.Done = true
		out[e.ID] = t
		return out, nil

	default:
		// Never skip: a node that silently ignores records it does not
		// understand diverges from its peers without saying so.
		return nil, fmt.Errorf("%w: %s", eventlog.ErrUnknownType, r.Type)
	}
}

func decode(r eventlog.Record, into any) error {
	if err := json.Unmarshal(r.Payload, into); err != nil {
		return fmt.Errorf("%w: %s seq %d: %w", ErrBadPayload, r.Type, r.Seq, err)
	}
	return nil
}

// List is the command side: it owns the projected State, encodes commands into
// records, and serves as both eventlog.Validator and eventlog.Projector — the
// second seam.
type List struct {
	store *eventlog.Store
	clk   *clock.Clock

	mu    sync.Mutex
	state State
}

// NewList rebuilds state by replaying the whole log, then accepts commands.
func NewList(ctx context.Context, store *eventlog.Store, clk *clock.Clock) (*List, error) {
	state, err := projection.Fold[State](ctx, store, Reducer{})
	if err != nil {
		return nil, err
	}
	return &List{store: store, clk: clk, state: state}, nil
}

// Check is eventlog.Validator: the store calls it inside the append
// transaction. "Would my reducer accept this?" is the whole invariant, so the
// rules live in exactly one place.
func (l *List) Check(r eventlog.Record) error {
	_, err := Reducer{}.Apply(l.state, r)
	return err
}

// Project is eventlog.Projector: compute the next state, and hand back a
// closure the store runs only after the transaction commits. If the commit
// fails, l.state is untouched.
func (l *List) Project(r eventlog.Record) (func(), error) {
	next, err := Reducer{}.Apply(l.state, r)
	if err != nil {
		return nil, err
	}
	return func() { l.state = next }, nil
}

// Add appends a tasklist.Added. Note the shape of a command in this
// architecture: validate what you can locally, encode, append. Three lines of
// domain logic and no network.
func (l *List) Add(ctx context.Context, id, title string) error {
	if id == "" {
		return ErrEmptyID
	}
	return l.append(ctx, TypeAdded, Added{ID: id, Title: title})
}

// Complete appends a tasklist.Completed. Whether the task exists is decided by
// Check inside the transaction, against the state at that moment.
func (l *List) Complete(ctx context.Context, id string) error {
	if id == "" {
		return ErrEmptyID
	}
	return l.append(ctx, TypeCompleted, Completed{ID: id})
}

func (l *List) append(ctx context.Context, typ string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("tasklist: encode %s: %w", typ, err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err = l.store.Append(ctx, typ, payload, l.clk.Now(), l, l)
	return err
}

// Tasks returns a snapshot of projected state.
func (l *List) Tasks() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state.Clone()
}
```

- [ ] **Step 4: Run the tests until green**

Run: `cd localfirst-go && go test ./examples/... -race -cover`
Expected: PASS, `coverage: 100.0% of statements`. If `Clone` or `decode` shows uncovered lines, the table above is missing a case — add it rather than lowering the bar.

- [ ] **Step 5: Confirm the example does not reach the domain**

The architecture test from Task 10 guards the engine, not the examples. Check this one by hand, once:

Run: `cd localfirst-go && go list -deps ./examples/tasklist | grep localfirst-go`
Expected: exactly `…/localfirst-go/clock`, `…/localfirst-go/eventlog`, `…/localfirst-go/projection` (plus the example itself). **No `/domain`.** If `domain` appears, the swap is not a swap.

- [ ] **Step 6: Add the guide to the docs guard and watch it fail**

Add `"docs/swapping-the-domain.md"` to `required` in `localfirst-go/docs/links_test.go`:

```go
var required = []string{
	"docs/adr/0001-per-node-ownership.md",
	"docs/adr/0002-sqlite-local-log.md",
	"docs/adr/0003-hlc-for-ordering-only.md",
	"docs/adr/0004-symmetric-sync-protocol.md",
	"docs/adr/0005-domain-agnostic-engine.md",
	"docs/architecture.md",
	"docs/swapping-the-domain.md",
}
```

Run: `cd localfirst-go && go test ./docs/... -run TestRequiredDocsExist`
Expected: FAIL — `documented file missing: … docs/swapping-the-domain.md`.

- [ ] **Step 7: Write the guide**

`localfirst-go/docs/swapping-the-domain.md`:

```markdown
# Swapping the domain

This repo claims its engine is domain-agnostic. Here is the claim cashed in:
inventory replaced, end to end, by a domain that shares nothing with it — no
quantities, no arithmetic, string-keyed entities, and an invariant about
existence rather than about a number.

Everything below is real code. It lives at
[`examples/tasklist`](../examples/tasklist) and its tests run in CI, so this
guide cannot rot into a description of something that no longer compiles.

## The shape of the job

Three things to write. Nothing else.

| You write | Engine interface | Where it is called |
| --- | --- | --- |
| A reducer | `projection.Reducer[S]` | `projection.Fold` on startup, and on every replay |
| A validator/projector | `eventlog.Validator`, `eventlog.Projector` | inside `eventlog.Store.Append`'s transaction |
| A command encoder | none — it is yours | your own command methods |

Files you do **not** touch: `clock/`, `eventlog/`, `projection/`, `sync/`, and
the proto definitions. Verify that claim rather than trusting it —
`git diff --stat` after the swap should show no changes in any of them.

## Step 1 — name the events

Inventory had three. The task list has two, and they carry strings instead of
integers:

```go
const (
	TypeAdded     = "tasklist.Added"
	TypeCompleted = "tasklist.Completed"
)

type Added struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type Completed struct {
	ID string `json:"id"`
}
```

The `Type` string is the only thing the engine sees, and it treats it as an
opaque label. `Payload` is whatever bytes you put there; this example uses JSON
because it is legible in `sqlite3` output.

## Step 2 — define the projected state

```go
type Task struct {
	Title string
	Done  bool
}

type State map[string]Task

// Clone returns an independent copy. The reducer never mutates the state it is
// given, so a failed Apply cannot leave a half-updated projection behind.
func (s State) Clone() State {
	out := make(State, len(s))
	for id, t := range s {
		out[id] = t
	}
	return out
}
```

Note that `State` is a completely different type from `domain.State`
(`map[domain.Key]int64`). The engine never names either of them: `Fold[S any]`
is generic precisely so that it cannot.

## Step 3 — write the reducer

This is the whole reading seam. It is a pure function — same state plus same
record always gives the same answer — which is what makes replay deterministic
and makes the table-driven test in
[`tasklist_test.go`](../examples/tasklist/tasklist_test.go) possible.

```go
type Reducer struct{}

func (Reducer) Zero() State { return State{} }

func (Reducer) Apply(state State, r eventlog.Record) (State, error) {
	switch r.Type {
	case TypeAdded:
		var e Added
		if err := decode(r, &e); err != nil {
			return nil, err
		}
		if _, exists := state[e.ID]; exists {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateTask, e.ID)
		}
		out := state.Clone()
		out[e.ID] = Task{Title: e.Title}
		return out, nil

	case TypeCompleted:
		var e Completed
		if err := decode(r, &e); err != nil {
			return nil, err
		}
		t, exists := state[e.ID]
		if !exists {
			return nil, fmt.Errorf("%w: %s", ErrNoSuchTask, e.ID)
		}
		if t.Done {
			return nil, fmt.Errorf("%w: %s", ErrAlreadyDone, e.ID)
		}
		out := state.Clone()
		t.Done = true
		out[e.ID] = t
		return out, nil

	default:
		// Never skip: a node that silently ignores records it does not
		// understand diverges from its peers without saying so.
		return nil, fmt.Errorf("%w: %s", eventlog.ErrUnknownType, r.Type)
	}
}
```

Two conventions worth copying:

- The `default` branch returns `eventlog.ErrUnknownType` and **never skips**.
  A record you cannot interpret means your binary is older than the fact; a
  loud failure is the only honest response. See
  [ADR 0005](adr/0005-domain-agnostic-engine.md).
- Every rule of the domain lives here, including rules you might have been
  tempted to put in the command handler. That pays off in the next step.

## Step 4 — reuse the reducer as the validator

The engine has to ask "is this append allowed?" without knowing what the answer
depends on. `eventlog.Validator` is that question, and it is called inside the
append transaction against current projected state. Because every rule is in
`Apply`, the validator is two lines:

```go
// Check is eventlog.Validator: the store calls it inside the append
// transaction.
func (l *List) Check(r eventlog.Record) error {
	_, err := Reducer{}.Apply(l.state, r)
	return err
}

// Project is eventlog.Projector: compute the next state, and hand back a
// closure the store runs only after the transaction commits.
func (l *List) Project(r eventlog.Record) (func(), error) {
	next, err := Reducer{}.Apply(l.state, r)
	if err != nil {
		return nil, err
	}
	return func() { l.state = next }, nil
}
```

"Would my reducer accept this?" *is* the invariant. `domain.Inventory` does the
same thing for negative balances. The deferred `commit func()` is why a
transaction that fails to commit cannot leave the in-memory projection ahead of
the log.

## Step 5 — write the command encoder

There is no engine interface here; commands are yours. The pattern is: validate
what is purely local, encode, append.

```go
func NewList(ctx context.Context, store *eventlog.Store, clk *clock.Clock) (*List, error) {
	state, err := projection.Fold[State](ctx, store, Reducer{})
	if err != nil {
		return nil, err
	}
	return &List{store: store, clk: clk, state: state}, nil
}

func (l *List) Add(ctx context.Context, id, title string) error {
	if id == "" {
		return ErrEmptyID
	}
	return l.append(ctx, TypeAdded, Added{ID: id, Title: title})
}

func (l *List) Complete(ctx context.Context, id string) error {
	if id == "" {
		return ErrEmptyID
	}
	return l.append(ctx, TypeCompleted, Completed{ID: id})
}

func (l *List) append(ctx context.Context, typ string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("tasklist: encode %s: %w", typ, err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err = l.store.Append(ctx, typ, payload, l.clk.Now(), l, l)
	return err
}
```

`store.Append(ctx, typ, payload, ts, validator, projector)` is the only engine
call a command makes. Passing `l` twice is not a trick — `List` satisfies both
interfaces, which is the natural result of keeping the rules in one place.

## Step 6 — the swap is done

That is everything. To run this domain instead of inventory in a node, replace
`domain.NewInventory` with `tasklist.NewList` in `cmd/node/node.go` and swap the
`transport/grpc` service for one that exposes `Add`/`Complete` instead of
`Receive`/`Issue`/`Move`. `sync`, `eventlog`, `clock`, `projection`, the
`Frame` protocol and `central`'s replication all work untouched, because none of
them ever looked at a payload.

Two consequences worth stating plainly:

- **Sync needs no changes at all.** `sync.Exchange` moves `eventlog.Record`s. A
  task list replicates and converges under exactly the same protocol as
  inventory, with the same idempotent append and the same resumption behaviour.
- **`central.PGStore.GlobalSum` does not carry over**, and that is correct.
  Summing balances is an inventory question. A task list would ask something
  else — "how many tasks are open across all nodes?" — and that query, like the
  reducer, belongs to the domain.

## What still needs your judgement

The engine is generic; ownership is not. This architecture removes conflict by
partitioning write authority ([ADR 0001](adr/0001-per-node-ownership.md)), and
that partition has to mean something in *your* domain. In inventory it is
locations. In this task list, the natural rule is that a node may only complete
tasks it created — that is the version to build if you take this further. Skip
that question and you have a system where two nodes can complete the same task
offline, which per-node ownership does not cover and
[limitations](limitations.md) rules out.
```

- [ ] **Step 8: Verify the guide's code really is the example's code**

The guide quotes `examples/tasklist/tasklist.go`. Confirm by eye that each Go
block above appears verbatim in the source file, then let the compiler have the
final word:

Run: `cd localfirst-go && go vet ./examples/... && go test ./docs/... -run TestRequiredDocsExist -v`
Expected: no vet output; seven existence subtests PASS.

- [ ] **Step 9: Full suite, lint, commit**

Run: `cd localfirst-go && make test && golangci-lint run`
Expected: PASS everywhere, `examples/tasklist` at 100%, no lint output.

```bash
git add localfirst-go/examples localfirst-go/docs
git commit -m "docs(seam): worked second domain proving the engine seam, with tests"
```

---

### Task 14: `README.md`, `docs/limitations.md`, and the link checker switched on

**Why:** the README is the front door of a repo whose product is comprehension — it has 60 seconds to say what local-first means, what this architecture is, and when *not* to use it. `docs/limitations.md` is the other half of that honesty: a reference architecture that hides its edges teaches people to walk off them. With every linked document now written, the relative-link checker from Task 11 can finally cover the whole tree.

**Files:**
- Create: `localfirst-go/README.md`, `localfirst-go/docs/limitations.md`
- Modify: `localfirst-go/docs/links_test.go` (extend `required`, add `TestRelativeLinksResolve`)

**Interfaces:**
- Consumes: every documented name and path from Tasks 1–13, plus `make test`, `make lint`, `make demo` (Task 1) and the demo scenario that Tasks 15–16 build at `demo/`.
- Produces: `localfirst-go/README.md`, `localfirst-go/docs/limitations.md`, and a `TestRelativeLinksResolve` that all later tasks must keep green.

- [ ] **Step 1: Extend the guard and watch it fail**

Add both files to `required` in `localfirst-go/docs/links_test.go`:

```go
var required = []string{
	"README.md",
	"docs/adr/0001-per-node-ownership.md",
	"docs/adr/0002-sqlite-local-log.md",
	"docs/adr/0003-hlc-for-ordering-only.md",
	"docs/adr/0004-symmetric-sync-protocol.md",
	"docs/adr/0005-domain-agnostic-engine.md",
	"docs/architecture.md",
	"docs/limitations.md",
	"docs/swapping-the-domain.md",
}
```

Run: `cd localfirst-go && go test ./docs/... -run TestRequiredDocsExist`
Expected: FAIL — two `documented file missing` subtests, `README.md` and `docs/limitations.md`.

- [ ] **Step 2: Write `docs/limitations.md`**

`localfirst-go/docs/limitations.md`:

```markdown
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
```

- [ ] **Step 3: Write `README.md`**

`localfirst-go/README.md`:

```markdown
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

```
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
```

- [ ] **Step 4: Run the existence guard**

Run: `cd localfirst-go && go test ./docs/... -run TestRequiredDocsExist -v`
Expected: PASS, nine subtests.

- [ ] **Step 5: Add the link checker and watch it fail**

Every linked document now exists, so the second half of the guard can go in.
Append to `localfirst-go/docs/links_test.go`, and add `"regexp"` and `"strings"`
to its imports:

```go
// link matches an inline markdown link: [text](target).
var link = regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)

// TestRelativeLinksResolve walks every markdown file in the module and checks
// that each relative link points at something that exists. External (http) and
// in-page (#anchor) links are out of scope: this test guards our own tree.
func TestRelativeLinksResolve(t *testing.T) {
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range link.FindAllStringSubmatch(string(body), -1) {
			target := strings.SplitN(m[1], "#", 2)[0]
			if target == "" || strings.Contains(target, "://") {
				continue
			}
			// Sibling reference architectures live outside this module and are
			// not checked out in every environment, so their presence is not
			// this repo's invariant.
			if strings.HasPrefix(target, "../../../") {
				continue
			}
			if _, statErr := os.Stat(filepath.Join(filepath.Dir(path), target)); statErr != nil {
				t.Errorf("%s: broken link %q", path, m[1])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}
```

Run: `cd localfirst-go && go test ./docs/... -run TestRelativeLinksResolve -v`
Expected: PASS. If it reports a broken link, fix the link — the checker is
right. Note that `../limitations.md` from an ADR resolves to `docs/limitations.md`
and `docs/adr` resolves to a directory, which `os.Stat` accepts.

- [ ] **Step 6: Prove the checker is not vacuous**

A guard that cannot fail is not a guard.

Temporarily add `[nope](docs/does-not-exist.md)` to the bottom of `README.md`,
then:

Run: `cd localfirst-go && go test ./docs/... -run TestRelativeLinksResolve`
Expected: FAIL — `../README.md: broken link "docs/does-not-exist.md"`. **Remove
the line** and re-run to confirm PASS.

- [ ] **Step 7: Full suite, lint, commit**

Run: `cd localfirst-go && make test && golangci-lint run`
Expected: PASS everywhere, no lint output.

```bash
git add localfirst-go/README.md localfirst-go/docs
git commit -m "docs: README, limitations, and a link checker that keeps them honest"
```

---

### Task 15: `demo/` — the seven-step scenario and its integration test

**Why:** the demo is the artifact that makes local-first click in 60 seconds, and a demo that only exists as shell commands rots the moment an interface changes. So the seven steps live in **Go**, written against three small interfaces, and are driven by two different environments: real containers with a real network partition (`make demo`, Task 16) and an in-process integration test (here). Same steps, same printed numbers, one implementation.

This task builds the scenario and its test twin. The test is the real integration test the spec asks for: three genuine `eventlog` SQLite stores, three genuine `domain.Inventory` command sides, a genuine `central.PGStore` on Postgres, and genuine `sync` traffic over loopback gRPC.

**The seven steps and their exact numbers:**

| Step | Action | Number the test asserts |
| --- | --- | --- |
| 1 | three nodes and central are up | node-1 owns A, node-2 owns B, node-3 owns C |
| 2 | receive at each node, then sync | global sum = 100 + 50 + 30 = **180** |
| 3 | cut node-3 off | — |
| 4 | 20 × `Issue 1` against node-3 | 20 succeed, 0 fail; node-3 local balance at C = **10** |
| 5 | sync what can sync, read central | global sum still **180** — 20 units unaccounted for |
| 6 | reconnect node-3 | — |
| 7 | sync, read central | global sum = **160** |

**Files:**
- Create: `localfirst-go/demo/scenario.go`
- Test: `localfirst-go/demo/scenario_test.go`

**Interfaces:**
- Consumes: `eventlog.Open`, `*eventlog.Store` (Task 4); `clock.New` (Task 2); `domain.NewInventory`, `domain.Inventory.Receive/Issue/Balance` (Task 6); `sync.NewServer`, `sync.NewClient`, `sync.Client.SyncOnce` (Task 7); `central.OpenPG`, `central.PGStore.GlobalSum` (Task 9); `syncpb.RegisterSyncServer` (Task 7).
- Produces:
  - `const SKU = "SKU-1"`, `const OfflineOps = 20`, `var Seed = []int64{100, 50, 30}`
  - `type Node interface { ID() string; Location() string; Receive(ctx context.Context, sku, location string, qty int64) error; Issue(ctx context.Context, sku, location string, qty int64) error; Balance(ctx context.Context, sku, location string) (int64, error); Sync(ctx context.Context) error }`
  - `type Central interface { GlobalSum(ctx context.Context) (map[string]int64, error) }`
  - `type Env interface { Nodes() []Node; Central() Central; Disconnect(ctx context.Context, nodeID string) error; Reconnect(ctx context.Context, nodeID string) error }`
  - `type Result struct { SeedTotal, SumAfterSeed, IsolatedLocal, SumWhileOffline, SumAfterRejoin int64; OfflineOK, OfflineFailed int }`
  - `func Run(ctx context.Context, env Env, out io.Writer) (Result, error)`
  - `var ErrWantThreeNodes, ErrDidNotConverge, ErrOfflineWriteRejected, ErrLeakedWhileOffline`

- [ ] **Step 1: Write the failing scenario test**

`localfirst-go/demo/scenario_test.go`:

```go
package demo_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/central"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/demo"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// --- the in-process environment -------------------------------------------
//
// Everything below is real: real SQLite logs, a real Postgres central, real
// gRPC sync over loopback. The one thing simulated is the network partition:
// gate=false makes Sync refuse to run, which is what a partition means from the
// scenario's point of view. `make demo` performs a genuine
// `docker network disconnect`; this twin exists to pin the numbers, not to
// re-test Docker.

type testNode struct {
	id, loc string
	inv     *domain.Inventory
	client  *lfsync.Client
	gate    bool
}

var errPartitioned = errors.New("demo: node is partitioned")

func (n *testNode) ID() string       { return n.id }
func (n *testNode) Location() string { return n.loc }

func (n *testNode) Receive(ctx context.Context, sku, location string, qty int64) error {
	return n.inv.Receive(ctx, sku, location, qty)
}

func (n *testNode) Issue(ctx context.Context, sku, location string, qty int64) error {
	return n.inv.Issue(ctx, sku, location, qty)
}

func (n *testNode) Balance(_ context.Context, sku, location string) (int64, error) {
	return n.inv.Balance(sku, location), nil
}

func (n *testNode) Sync(ctx context.Context) error {
	if !n.gate {
		return errPartitioned
	}
	_, err := n.client.SyncOnce(ctx)
	return err
}

type testEnv struct {
	nodes []demo.Node
	pg    *central.PGStore
}

func (e *testEnv) Nodes() []demo.Node { return e.nodes }
func (e *testEnv) Central() demo.Central { return e.pg }

func (e *testEnv) Disconnect(_ context.Context, nodeID string) error {
	return e.setGate(nodeID, false)
}

func (e *testEnv) Reconnect(_ context.Context, nodeID string) error {
	return e.setGate(nodeID, true)
}

func (e *testEnv) setGate(nodeID string, open bool) error {
	for _, n := range e.nodes {
		if tn, ok := n.(*testNode); ok && tn.id == nodeID {
			tn.gate = open
			return nil
		}
	}
	return fmt.Errorf("demo: no such node %q", nodeID)
}

func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("LOCALFIRST_PG_DSN")
	if v == "" {
		t.Fatal("LOCALFIRST_PG_DSN is unset — run `make test`")
	}
	return v
}

// wipe gives the test a clean central. GlobalSum sums across every node in the
// table, so leftovers from another test would be indistinguishable from this
// test's own data.
func wipe(ctx context.Context, t *testing.T, url string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "TRUNCATE records, cursors"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func newEnv(ctx context.Context, t *testing.T) *testEnv {
	t.Helper()
	url := dsn(t)
	wipe(ctx, t, url)

	pg, err := central.OpenPG(ctx, url, "central")
	if err != nil {
		t.Fatalf("OpenPG: %v", err)
	}
	t.Cleanup(pg.Close)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, lfsync.NewServer(pg, nil))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	dir := t.TempDir()
	env := &testEnv{pg: pg}
	for i, loc := range []string{"A", "B", "C"} {
		id := fmt.Sprintf("node-%d", i+1)

		store, err := eventlog.Open(filepath.Join(dir, id+".db"), id)
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		t.Cleanup(func() { _ = store.Close() })

		inv, err := domain.NewInventory(ctx, store, clock.New(id, nil), []string{loc})
		if err != nil {
			t.Fatalf("NewInventory %s: %v", id, err)
		}

		cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial %s: %v", id, err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		env.nodes = append(env.nodes, &testNode{
			id: id, loc: loc, inv: inv,
			client: lfsync.NewClient(cc, store, nil, "central"),
			gate:   true,
		})
	}
	return env
}

// --- the test twin of `make demo` -----------------------------------------

// TestScenarioConvergesAfterAPartition is the integration test the demo cannot
// drift from: it runs the exact same seven steps and pins every printed number.
func TestScenarioConvergesAfterAPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var out strings.Builder
	res, err := demo.Run(ctx, newEnv(ctx, t), &out)
	if err != nil {
		t.Fatalf("Run = %v, want nil\n%s", err, out.String())
	}
	t.Log("\n" + out.String())

	checks := []struct {
		name string
		got  int64
		want int64
	}{
		{"seed total", res.SeedTotal, 180},
		{"sum after seeding", res.SumAfterSeed, 180},
		{"isolated node local balance", res.IsolatedLocal, 10},
		{"sum while node-3 was offline", res.SumWhileOffline, 180},
		{"sum after node-3 rejoined", res.SumAfterRejoin, 160},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
	if res.OfflineOK != demo.OfflineOps || res.OfflineFailed != 0 {
		t.Errorf("offline ops: %d ok / %d failed, want %d / 0", res.OfflineOK, res.OfflineFailed, demo.OfflineOps)
	}

	// The output is a deliverable too: a reader watching `make demo` must see
	// the numbers, not just a spinner.
	for _, want := range []string{"step 1/7", "step 7/7", "180", "160", "20 succeeded"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output is missing %q:\n%s", want, out.String())
		}
	}
}

// --- error paths ----------------------------------------------------------

type stubCentral struct {
	sum map[string]int64
	err error
}

func (s stubCentral) GlobalSum(context.Context) (map[string]int64, error) {
	return s.sum, s.err
}

// scriptedCentral returns a different total on each call, so a test can make
// central's sum move at a moment when the scenario insists it must not.
type scriptedCentral struct {
	sums []int64
	call int
}

func (s *scriptedCentral) GlobalSum(context.Context) (map[string]int64, error) {
	v := s.sums[len(s.sums)-1]
	if s.call < len(s.sums) {
		v = s.sums[s.call]
	}
	s.call++
	return map[string]int64{demo.SKU: v}, nil
}

type stubNode struct {
	id, loc      string
	balance      int64
	receiveErr   error
	issueErr     error
	balanceErr   error
	syncErr      error
}

func (n *stubNode) ID() string       { return n.id }
func (n *stubNode) Location() string { return n.loc }
func (n *stubNode) Receive(context.Context, string, string, int64) error { return n.receiveErr }
func (n *stubNode) Issue(context.Context, string, string, int64) error   { return n.issueErr }
func (n *stubNode) Balance(context.Context, string, string) (int64, error) {
	return n.balance, n.balanceErr
}
func (n *stubNode) Sync(context.Context) error { return n.syncErr }

type stubEnv struct {
	nodes         []demo.Node
	central       demo.Central
	disconnectErr error
	reconnectErr  error
}

func (e *stubEnv) Nodes() []demo.Node                          { return e.nodes }
func (e *stubEnv) Central() demo.Central                       { return e.central }
func (e *stubEnv) Disconnect(context.Context, string) error    { return e.disconnectErr }
func (e *stubEnv) Reconnect(context.Context, string) error     { return e.reconnectErr }

func threeStubs() []demo.Node {
	return []demo.Node{
		&stubNode{id: "node-1", loc: "A"},
		&stubNode{id: "node-2", loc: "B"},
		&stubNode{id: "node-3", loc: "C", balance: 10},
	}
}

var errBoom = errors.New("boom")

func TestRunErrorPaths(t *testing.T) {
	tests := []struct {
		name    string
		env     *stubEnv
		wantErr error
	}{
		{
			name:    "wrong number of nodes",
			env:     &stubEnv{nodes: threeStubs()[:2], central: stubCentral{}},
			wantErr: demo.ErrWantThreeNodes,
		},
		{
			name: "a seed receive fails",
			env: &stubEnv{
				nodes:   []demo.Node{&stubNode{id: "node-1", loc: "A", receiveErr: errBoom}, &stubNode{id: "node-2", loc: "B"}, &stubNode{id: "node-3", loc: "C"}},
				central: stubCentral{sum: map[string]int64{demo.SKU: 180}},
			},
			wantErr: errBoom,
		},
		{
			name: "central is unreadable",
			env: &stubEnv{
				nodes:   threeStubs(),
				central: stubCentral{err: errBoom},
			},
			wantErr: errBoom,
		},
		{
			name: "seeding did not converge",
			env: &stubEnv{
				nodes:   threeStubs(),
				central: stubCentral{sum: map[string]int64{demo.SKU: 7}},
			},
			wantErr: demo.ErrDidNotConverge,
		},
		{
			name: "the partition cannot be created",
			env: &stubEnv{
				nodes:         threeStubs(),
				central:       stubCentral{sum: map[string]int64{demo.SKU: 180}},
				disconnectErr: errBoom,
			},
			wantErr: errBoom,
		},
		{
			name: "an offline write is rejected",
			env: &stubEnv{
				nodes:   []demo.Node{&stubNode{id: "node-1", loc: "A"}, &stubNode{id: "node-2", loc: "B"}, &stubNode{id: "node-3", loc: "C", issueErr: errBoom}},
				central: stubCentral{sum: map[string]int64{demo.SKU: 180}},
			},
			wantErr: demo.ErrOfflineWriteRejected,
		},
		{
			name: "the isolated balance cannot be read",
			env: &stubEnv{
				nodes:   []demo.Node{&stubNode{id: "node-1", loc: "A"}, &stubNode{id: "node-2", loc: "B"}, &stubNode{id: "node-3", loc: "C", balanceErr: errBoom}},
				central: stubCentral{sum: map[string]int64{demo.SKU: 180}},
			},
			wantErr: errBoom,
		},
		{
			name: "central's sum moved during the partition",
			env: &stubEnv{
				nodes: threeStubs(),
				// 180 after seeding, then 200 during the partition: something
				// reached central while node-3 was supposed to be cut off.
				central: &scriptedCentral{sums: []int64{180, 200}},
			},
			wantErr: demo.ErrLeakedWhileOffline,
		},
		{
			name: "reconnect fails",
			env: &stubEnv{
				nodes:        threeStubs(),
				central:      stubCentral{sum: map[string]int64{demo.SKU: 180}},
				reconnectErr: errBoom,
			},
			wantErr: errBoom,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := demo.Run(context.Background(), tc.env, io.Discard)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Run = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// A stub central whose sum never changes reaches step 7 with 180 instead of 160,
// which is exactly the "did not converge" failure a broken sync would produce.
func TestRunDetectsAFailureToConverge(t *testing.T) {
	env := &stubEnv{nodes: threeStubs(), central: stubCentral{sum: map[string]int64{demo.SKU: 180}}}
	_, err := demo.Run(context.Background(), env, io.Discard)
	if !errors.Is(err, demo.ErrDidNotConverge) {
		t.Fatalf("Run = %v, want ErrDidNotConverge", err)
	}
}

// A node whose sync fails after reconnection must stop the demo: silently
// printing a converged number would be the one unforgivable bug in a
// demonstration of convergence.
func TestRunFailsWhenPostRejoinSyncFails(t *testing.T) {
	nodes := threeStubs()
	nodes[0].(*stubNode).syncErr = errBoom
	env := &stubEnv{nodes: nodes, central: stubCentral{sum: map[string]int64{demo.SKU: 180}}}
	if _, err := demo.Run(context.Background(), env, io.Discard); !errors.Is(err, errBoom) {
		t.Fatalf("Run = %v, want errBoom", err)
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cd localfirst-go && make pg-up && LOCALFIRST_PG_DSN="postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable" go test ./demo/...`
Expected: FAIL to build — `no required module provides package …/localfirst-go/demo`.

- [ ] **Step 3: Write the scenario**

`localfirst-go/demo/scenario.go`:

```go
// Package demo runs the seven-step local-first demonstration: three nodes, one
// central, one network partition, and a converged global sum at the end.
//
// The steps are written against three small interfaces so that the identical
// code drives two very different environments: `make demo`, which cuts a real
// container off a real Docker network, and scenario_test.go, which runs
// everything in one process. A demo that only exists as shell commands rots;
// this one is compiled and tested.
package demo

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// SKU is the single product the demo moves around. One SKU keeps the printed
// output readable; nothing in the architecture cares how many there are.
const SKU = "SKU-1"

// OfflineOps is how many writes the isolated node accepts while partitioned.
// Twenty is enough that "they all succeeded" is obviously not luck.
const OfflineOps = 20

// Seed is how much each node receives in step 2, in node order. The three
// numbers are distinct so a reader can tell whose stock is missing from a sum.
var Seed = []int64{100, 50, 30}

// Node is one participant, as the scenario needs to see it: it accepts local
// commands, answers from local state, and can be asked to reconcile.
type Node interface {
	ID() string
	// Location is the single location this node exclusively owns.
	Location() string
	Receive(ctx context.Context, sku, location string, qty int64) error
	Issue(ctx context.Context, sku, location string, qty int64) error
	Balance(ctx context.Context, sku, location string) (int64, error)
	// Sync gives replication a chance to run and reports whether it could.
	// In-process that is one exchange; against containers it is waiting out a
	// background sync interval.
	Sync(ctx context.Context) error
}

// Central is the only thing the scenario asks of central: the global sum, which
// is a sum of per-node balances and never a merge.
type Central interface {
	GlobalSum(ctx context.Context) (map[string]int64, error)
}

// Env is the world the scenario runs in. Disconnect and Reconnect are the whole
// reason this abstraction exists: one implementation cuts a Docker network, the
// other refuses to sync.
type Env interface {
	Nodes() []Node
	Central() Central
	Disconnect(ctx context.Context, nodeID string) error
	Reconnect(ctx context.Context, nodeID string) error
}

// Result is every number the demo printed, so a test can assert on them instead
// of parsing prose.
type Result struct {
	SeedTotal       int64 // what the nodes received in total
	SumAfterSeed    int64 // central's global sum once everyone has synced
	IsolatedLocal   int64 // the partitioned node's own balance after its offline writes
	SumWhileOffline int64 // central's global sum during the partition
	SumAfterRejoin  int64 // central's global sum after convergence
	OfflineOK       int   // offline writes that succeeded
	OfflineFailed   int   // offline writes that failed; must be zero
}

// Scenario failures. Each is a claim in the README that stopped being true.
var (
	ErrWantThreeNodes       = errors.New("demo: scenario needs exactly three nodes")
	ErrDidNotConverge       = errors.New("demo: central's global sum is wrong")
	ErrOfflineWriteRejected = errors.New("demo: a write failed while the node was offline")
	ErrLeakedWhileOffline   = errors.New("demo: central's sum moved while the node was partitioned")
)

// Run executes the seven steps, printing each with its actual numbers, and
// returns them. Any deviation from the expected numbers is an error: a demo of
// convergence that prints a converged-looking number without checking it would
// be worse than no demo.
func Run(ctx context.Context, env Env, out io.Writer) (Result, error) {
	var res Result

	nodes := env.Nodes()
	if len(nodes) != 3 {
		return res, fmt.Errorf("%w: got %d", ErrWantThreeNodes, len(nodes))
	}

	// Step 1 -------------------------------------------------------------
	fmt.Fprintln(out, "step 1/7  three nodes and one central are up")
	for _, n := range nodes {
		fmt.Fprintf(out, "          %s exclusively owns location %s\n", n.ID(), n.Location())
	}

	// Step 2 -------------------------------------------------------------
	fmt.Fprintf(out, "\nstep 2/7  receiving stock at each node (local writes; no network needed)\n")
	for i, n := range nodes {
		if err := n.Receive(ctx, SKU, n.Location(), Seed[i]); err != nil {
			return res, fmt.Errorf("demo: seed %s: %w", n.ID(), err)
		}
		res.SeedTotal += Seed[i]
		fmt.Fprintf(out, "          %s received %3d %s at %s\n", n.ID(), Seed[i], SKU, n.Location())
	}
	if err := syncAll(ctx, nodes); err != nil {
		return res, err
	}
	sum, err := globalSum(ctx, env)
	if err != nil {
		return res, err
	}
	res.SumAfterSeed = sum
	fmt.Fprintf(out, "          central global sum for %s = %d (expected %d)\n", SKU, res.SumAfterSeed, res.SeedTotal)
	if res.SumAfterSeed != res.SeedTotal {
		return res, fmt.Errorf("%w: after seeding it is %d, want %d", ErrDidNotConverge, res.SumAfterSeed, res.SeedTotal)
	}

	// Step 3 -------------------------------------------------------------
	iso := nodes[len(nodes)-1]
	fmt.Fprintf(out, "\nstep 3/7  cutting %s off the network\n", iso.ID())
	if err := env.Disconnect(ctx, iso.ID()); err != nil {
		return res, fmt.Errorf("demo: disconnect %s: %w", iso.ID(), err)
	}
	fmt.Fprintf(out, "          %s can no longer reach central or its peers\n", iso.ID())

	// Step 4 -------------------------------------------------------------
	fmt.Fprintf(out, "\nstep 4/7  issuing %d units, one at a time, against the isolated node\n", OfflineOps)
	for i := 0; i < OfflineOps; i++ {
		if err := iso.Issue(ctx, SKU, iso.Location(), 1); err != nil {
			res.OfflineFailed++
			fmt.Fprintf(out, "          op %2d FAILED: %v\n", i+1, err)
			continue
		}
		res.OfflineOK++
	}
	fmt.Fprintf(out, "          %d succeeded, %d failed — an offline node is fully operational\n", res.OfflineOK, res.OfflineFailed)
	if res.OfflineFailed != 0 {
		return res, fmt.Errorf("%w: %d of %d failed", ErrOfflineWriteRejected, res.OfflineFailed, OfflineOps)
	}
	res.IsolatedLocal, err = iso.Balance(ctx, SKU, iso.Location())
	if err != nil {
		return res, fmt.Errorf("demo: balance %s: %w", iso.ID(), err)
	}
	fmt.Fprintf(out, "          %s reads its own balance at %s = %d, from its own log\n", iso.ID(), iso.Location(), res.IsolatedLocal)

	// Step 5 -------------------------------------------------------------
	fmt.Fprintln(out, "\nstep 5/7  central has not heard about any of them")
	// The isolated node's attempt is expected to get nowhere, so its failure is
	// not the scenario's failure.
	_ = syncAll(ctx, nodes)
	if res.SumWhileOffline, err = globalSum(ctx, env); err != nil {
		return res, err
	}
	fmt.Fprintf(out, "          central global sum = %d, still, while %d units have already been issued\n", res.SumWhileOffline, res.OfflineOK)
	fmt.Fprintln(out, "          this gap is not a bug: it is the price of accepting writes without coordination")
	if res.SumWhileOffline != res.SumAfterSeed {
		return res, fmt.Errorf("%w: %d, want %d", ErrLeakedWhileOffline, res.SumWhileOffline, res.SumAfterSeed)
	}

	// Step 6 -------------------------------------------------------------
	fmt.Fprintf(out, "\nstep 6/7  reconnecting %s\n", iso.ID())
	if err := env.Reconnect(ctx, iso.ID()); err != nil {
		return res, fmt.Errorf("demo: reconnect %s: %w", iso.ID(), err)
	}

	// Step 7 -------------------------------------------------------------
	fmt.Fprintln(out, "\nstep 7/7  syncing, then reading central again")
	if err := syncAll(ctx, nodes); err != nil {
		return res, err
	}
	if res.SumAfterRejoin, err = globalSum(ctx, env); err != nil {
		return res, err
	}
	want := res.SumAfterSeed - int64(res.OfflineOK)
	fmt.Fprintf(out, "          central global sum = %d (expected %d - %d = %d)\n", res.SumAfterRejoin, res.SumAfterSeed, res.OfflineOK, want)
	if res.SumAfterRejoin != want {
		return res, fmt.Errorf("%w: after rejoining it is %d, want %d", ErrDidNotConverge, res.SumAfterRejoin, want)
	}
	fmt.Fprintln(out, "          converged: every offline write is now in the global sum, in order, exactly once")

	return res, nil
}

// syncAll asks every node to reconcile, returning the first failure.
func syncAll(ctx context.Context, nodes []Node) error {
	for _, n := range nodes {
		if err := n.Sync(ctx); err != nil {
			return fmt.Errorf("demo: sync %s: %w", n.ID(), err)
		}
	}
	return nil
}

// globalSum reads central's total for the demo's single SKU. A SKU that is
// absent from the map sums to zero, which is the right answer for "nobody has
// any".
func globalSum(ctx context.Context, env Env) (int64, error) {
	sums, err := env.Central().GlobalSum(ctx)
	if err != nil {
		return 0, fmt.Errorf("demo: global sum: %w", err)
	}
	return sums[SKU], nil
}
```

- [ ] **Step 4: Run the integration test**

Run: `cd localfirst-go && make pg-up && LOCALFIRST_PG_DSN="postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable" go test ./demo/... -race -v -run TestScenarioConverges`
Expected: PASS, and the logged output shows the seven steps with 180, 10, 180, 160.

If `SumWhileOffline` comes back as 160 rather than 180, the partition is not
being honoured — check that `testNode.Sync` returns before touching the client.
If `SumAfterRejoin` stays 180, central is not merging node-3's records; run
`sync`'s own tests (Task 7) before suspecting the demo.

- [ ] **Step 5: Run the error-path tests**

Run: `cd localfirst-go && LOCALFIRST_PG_DSN="postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable" go test ./demo/... -race -cover`
Expected: PASS, `coverage: 100.0% of statements`. Every `return` in `Run` has a
named test above; if coverage is short, find the uncovered branch with
`go tool cover -func` and add the stub case rather than lowering the bar.

- [ ] **Step 6: Full suite, lint, commit**

Run: `cd localfirst-go && make test && golangci-lint run`
Expected: PASS everywhere, no lint output.

```bash
git add localfirst-go/demo
git commit -m "test(demo): seven-step offline-and-converge scenario with its integration test"
```

---

### Task 16: `make demo` — Docker Compose, a real network partition, real numbers

**Why:** Task 15 pinned the numbers in one process. This task makes them visible: five containers, a genuine `docker network disconnect` on node 3, and the seven steps printed to a terminal. That artifact is what makes local-first click for a reader in 60 seconds, and it is the only place in the repo where the partition is real rather than modelled.

Note on ordering: Task 9's `make pg-up` already needed the `postgres` service from `docker-compose.yml`. If you created a minimal file there, **replace it wholesale** with the one below — it keeps the same service name, credentials and host port `5433`, so `make test` keeps working.

**Files:**
- Create: `localfirst-go/Dockerfile`, `localfirst-go/docker-compose.yml`, `localfirst-go/demo/env.go`, `localfirst-go/demo/cmd/demo/main.go`
- Test: `localfirst-go/demo/env_test.go`, `localfirst-go/demo/env_internal_test.go`
- Modify: `localfirst-go/docker-compose.yml` (replacing the Task 9 stub, if any)

**Interfaces:**
- Consumes: `demo.Node`, `demo.Env`, `demo.Central`, `demo.Run`, `demo.SKU` (Task 15); `nodepb.NewNodeClient`, `nodepb.ReceiveRequest/IssueRequest/BalanceRequest/Empty` (Task 8); `central.OpenPG`, `*central.PGStore` (Task 9).
- Produces:
  - `type Exec func(ctx context.Context, name string, args ...string) error`
  - `type NodeAddr struct { ID, Location, Container, Addr string }`
  - `type ComposeConfig struct { Network, DSN string; Settle time.Duration; Exec Exec; Nodes []NodeAddr }`
  - `func NewComposeEnv(ctx context.Context, cfg ComposeConfig) (*ComposeEnv, func(), error)` — `*ComposeEnv` satisfies `Env`
  - `func (e *ComposeEnv) WaitReady(ctx context.Context, timeout time.Duration) error`
  - `func DefaultNodes() []NodeAddr` — the three nodes as `docker-compose.yml` declares them

- [ ] **Step 1: Write the failing environment test**

`localfirst-go/demo/env_test.go` (this file joins the `demo_test` package created in Task 15, so `dsn`, `wipe` and the imports there are already available — do not redeclare them):

```go
package demo_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/demo"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	transport "github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// serveNode stands up a real node service on loopback, so ComposeEnv can be
// tested against the same gRPC API the containers expose — no Docker required.
func serveNode(ctx context.Context, t *testing.T, id, loc string) string {
	t.Helper()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), id+".db"), id)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	inv, err := domain.NewInventory(ctx, store, clock.New(id, nil), []string{loc})
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	nodepb.RegisterNodeServer(srv, transport.NewNodeService(inv))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

// recorder captures the commands ComposeEnv would have run, so the partition
// step can be asserted exactly without touching a real network.
type recorder struct {
	calls []string
	err   error
}

func (r *recorder) exec(_ context.Context, name string, args ...string) error {
	r.calls = append(r.calls, name+" "+strings.Join(args, " "))
	return r.err
}

func newComposeEnv(ctx context.Context, t *testing.T, rec *recorder) *demo.ComposeEnv {
	t.Helper()
	url := dsn(t)
	wipe(ctx, t, url)

	var addrs []demo.NodeAddr
	for i, loc := range []string{"A", "B", "C"} {
		id := fmt.Sprintf("node-%d", i+1)
		addrs = append(addrs, demo.NodeAddr{
			ID:        id,
			Location:  loc,
			Container: "lf-" + id,
			Addr:      serveNode(ctx, t, id, loc),
		})
	}

	env, cleanup, err := demo.NewComposeEnv(ctx, demo.ComposeConfig{
		Network: "localfirst-go_lfnet",
		DSN:     url,
		Settle:  10 * time.Millisecond,
		Exec:    rec.exec,
		Nodes:   addrs,
	})
	if err != nil {
		t.Fatalf("NewComposeEnv: %v", err)
	}
	t.Cleanup(cleanup)
	return env
}

func TestComposeEnvTalksToNodesOverGRPC(t *testing.T) {
	ctx := context.Background()
	env := newComposeEnv(ctx, t, &recorder{})

	if err := env.WaitReady(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nodes := env.Nodes()
	if len(nodes) != 3 {
		t.Fatalf("Nodes() = %d, want 3", len(nodes))
	}
	if nodes[0].ID() != "node-1" || nodes[2].Location() != "C" {
		t.Fatalf("nodes are out of order: %s/%s", nodes[0].ID(), nodes[2].Location())
	}

	if err := nodes[0].Receive(ctx, demo.SKU, "A", 7); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := nodes[0].Issue(ctx, demo.SKU, "A", 2); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	got, err := nodes[0].Balance(ctx, demo.SKU, "A")
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 5 {
		t.Fatalf("balance = %d, want 5", got)
	}
	if err := nodes[0].Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

// A node that does not own a location must refuse the write, over gRPC, with the
// error surfacing through the transport rather than being swallowed.
func TestComposeEnvSurfacesDomainErrors(t *testing.T) {
	ctx := context.Background()
	env := newComposeEnv(ctx, t, &recorder{})
	if err := env.Nodes()[0].Receive(ctx, demo.SKU, "Z", 1); err == nil {
		t.Fatal("Receive into an unowned location must fail")
	}
}

func TestComposeEnvPartitionCommands(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{}
	env := newComposeEnv(ctx, t, rec)

	if err := env.Disconnect(ctx, "node-3"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := env.Reconnect(ctx, "node-3"); err != nil {
		t.Fatalf("Reconnect: %v", err)
	}

	want := []string{
		"docker network disconnect localfirst-go_lfnet lf-node-3",
		"docker network connect localfirst-go_lfnet lf-node-3",
	}
	if len(rec.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", rec.calls, want)
	}
	for i := range want {
		if rec.calls[i] != want[i] {
			t.Errorf("call %d = %q, want %q", i, rec.calls[i], want[i])
		}
	}
}

func TestComposeEnvRejectsAnUnknownNode(t *testing.T) {
	ctx := context.Background()
	env := newComposeEnv(ctx, t, &recorder{})
	if err := env.Disconnect(ctx, "node-9"); err == nil {
		t.Fatal("Disconnect must fail for a node it does not know")
	}
	if err := env.Reconnect(ctx, "node-9"); err == nil {
		t.Fatal("Reconnect must fail for a node it does not know")
	}
}

func TestComposeEnvReportsPartitionFailures(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{err: errBoom}
	env := newComposeEnv(ctx, t, rec)
	if err := env.Disconnect(ctx, "node-1"); err == nil {
		t.Fatal("Disconnect must report the exec failure")
	}
}

func TestComposeEnvCentralIsUsable(t *testing.T) {
	ctx := context.Background()
	env := newComposeEnv(ctx, t, &recorder{})
	if _, err := env.Central().GlobalSum(ctx); err != nil {
		t.Fatalf("GlobalSum: %v", err)
	}
}

func TestNewComposeEnvFailsOnABadDSN(t *testing.T) {
	_, _, err := demo.NewComposeEnv(context.Background(), demo.ComposeConfig{
		DSN:   "postgres://nobody@127.0.0.1:1/none",
		Nodes: demo.DefaultNodes(),
	})
	if err == nil {
		t.Fatal("NewComposeEnv must fail when Postgres is unreachable")
	}
}

func TestWaitReadyTimesOut(t *testing.T) {
	ctx := context.Background()
	url := dsn(t)
	wipe(ctx, t, url)

	// Port 1 on loopback refuses connections, so readiness can never arrive.
	env, cleanup, err := demo.NewComposeEnv(ctx, demo.ComposeConfig{
		DSN:    url,
		Settle: time.Millisecond,
		Exec:   (&recorder{}).exec,
		Nodes:  []demo.NodeAddr{{ID: "node-1", Location: "A", Container: "lf-node-1", Addr: "127.0.0.1:1"}},
	})
	if err != nil {
		t.Fatalf("NewComposeEnv: %v", err)
	}
	t.Cleanup(cleanup)

	if err := env.WaitReady(ctx, 150*time.Millisecond); err == nil {
		t.Fatal("WaitReady must time out against a dead address")
	}
}

func TestSyncStopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newComposeEnv(ctx, t, &recorder{})
	cancel()
	if err := env.Nodes()[0].Sync(ctx); err == nil {
		t.Fatal("Sync must return the context error")
	}
}

func TestDefaultNodesMatchTheComposeFile(t *testing.T) {
	nodes := demo.DefaultNodes()
	if len(nodes) != 3 {
		t.Fatalf("DefaultNodes() = %d, want 3", len(nodes))
	}
	want := []demo.NodeAddr{
		{ID: "node-1", Location: "A", Container: "lf-node-1", Addr: "127.0.0.1:8081"},
		{ID: "node-2", Location: "B", Container: "lf-node-2", Addr: "127.0.0.1:8082"},
		{ID: "node-3", Location: "C", Container: "lf-node-3", Addr: "127.0.0.1:8083"},
	}
	for i := range want {
		if nodes[i] != want[i] {
			t.Errorf("node %d = %+v, want %+v", i, nodes[i], want[i])
		}
	}
}
```

`localfirst-go/demo/env_internal_test.go` — the default `Exec` shells out, so it
gets its own internal test rather than an untested fallback:

```go
package demo

import (
	"context"
	"testing"
)

func TestDefaultExec(t *testing.T) {
	ctx := context.Background()
	if err := defaultExec(ctx, "go", "version"); err != nil {
		t.Fatalf("defaultExec(go version) = %v, want nil", err)
	}
	if err := defaultExec(ctx, "definitely-not-a-real-binary-9f3a"); err == nil {
		t.Fatal("defaultExec must report a failure to run")
	}
	// A command that runs and exits non-zero must also be an error, with its
	// output attached — a silent `docker network disconnect` failure would make
	// the demo lie.
	if err := defaultExec(ctx, "go", "definitely-not-a-subcommand"); err == nil {
		t.Fatal("defaultExec must report a non-zero exit")
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cd localfirst-go && LOCALFIRST_PG_DSN="postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable" go test ./demo/...`
Expected: FAIL to build — `undefined: demo.NewComposeEnv`, `undefined: demo.NodeAddr`, `undefined: defaultExec`.

- [ ] **Step 3: Implement the compose environment**

`localfirst-go/demo/env.go`:

```go
package demo

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/central"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// Exec runs an external command. ComposeEnv uses it only for
// `docker network (dis)connect`, which is the one thing it cannot do over gRPC —
// and injecting it is what lets the whole environment be tested without Docker.
type Exec func(ctx context.Context, name string, args ...string) error

func defaultExec(ctx context.Context, name string, args ...string) error {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("demo: %s %v: %w: %s", name, args, err, out)
	}
	return nil
}

// NodeAddr is one node as docker-compose.yml declares it: its logical identity,
// the location it owns, its container name (for the network commands) and its
// published address (for gRPC).
type NodeAddr struct {
	ID        string
	Location  string
	Container string
	Addr      string
}

// DefaultNodes is the three nodes from docker-compose.yml. It is a function, not
// a var, so a caller cannot accidentally mutate the shared slice.
func DefaultNodes() []NodeAddr {
	return []NodeAddr{
		{ID: "node-1", Location: "A", Container: "lf-node-1", Addr: "127.0.0.1:8081"},
		{ID: "node-2", Location: "B", Container: "lf-node-2", Addr: "127.0.0.1:8082"},
		{ID: "node-3", Location: "C", Container: "lf-node-3", Addr: "127.0.0.1:8083"},
	}
}

// ComposeConfig configures a ComposeEnv. Settle is how long to wait for the
// nodes' background sync loops to run; it must be comfortably longer than
// cmd/node's -sync-every.
type ComposeConfig struct {
	Network string
	DSN     string
	Settle  time.Duration
	Exec    Exec
	Nodes   []NodeAddr
}

// ComposeEnv drives real containers: commands go over gRPC to the node API, the
// global sum is read straight out of central's Postgres, and the partition is a
// real `docker network disconnect`.
type ComposeEnv struct {
	nodes   []Node
	pg      *central.PGStore
	network string
	run     Exec
	byID    map[string]NodeAddr
}

// NewComposeEnv dials every node and central. The returned func releases both.
func NewComposeEnv(ctx context.Context, cfg ComposeConfig) (*ComposeEnv, func(), error) {
	if cfg.Exec == nil {
		cfg.Exec = defaultExec
	}
	pg, err := central.OpenPG(ctx, cfg.DSN, "demo-reader")
	if err != nil {
		return nil, nil, fmt.Errorf("demo: open central: %w", err)
	}

	env := &ComposeEnv{
		pg:      pg,
		network: cfg.Network,
		run:     cfg.Exec,
		byID:    make(map[string]NodeAddr, len(cfg.Nodes)),
	}
	conns := make([]*grpc.ClientConn, 0, len(cfg.Nodes))
	cleanup := func() {
		for _, cc := range conns {
			_ = cc.Close()
		}
		pg.Close()
	}

	for _, n := range cfg.Nodes {
		cc, err := grpc.NewClient(n.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("demo: dial %s at %s: %w", n.ID, n.Addr, err)
		}
		conns = append(conns, cc)
		env.byID[n.ID] = n
		env.nodes = append(env.nodes, &grpcNode{
			id: n.ID, loc: n.Location,
			client: nodepb.NewNodeClient(cc),
			settle: cfg.Settle,
		})
	}
	return env, cleanup, nil
}

// Nodes returns the nodes in declaration order, which is the order demo.Seed
// applies to.
func (e *ComposeEnv) Nodes() []Node { return e.nodes }

// Central reads the global sum directly from Postgres. Central exposes no query
// RPC, and does not need one: the sum is a property of the records it already
// holds.
func (e *ComposeEnv) Central() Central { return e.pg }

// Disconnect performs a genuine network partition. This is the line the whole
// demo exists for.
func (e *ComposeEnv) Disconnect(ctx context.Context, nodeID string) error {
	return e.networkCmd(ctx, "disconnect", nodeID)
}

// Reconnect undoes it.
func (e *ComposeEnv) Reconnect(ctx context.Context, nodeID string) error {
	return e.networkCmd(ctx, "connect", nodeID)
}

func (e *ComposeEnv) networkCmd(ctx context.Context, verb, nodeID string) error {
	n, ok := e.byID[nodeID]
	if !ok {
		return fmt.Errorf("demo: unknown node %q", nodeID)
	}
	return e.run(ctx, "docker", "network", verb, e.network, n.Container)
}

// WaitReady blocks until every node answers a Balances call, or timeout
// elapses. Containers take a moment to boot, and a demo that races them prints
// a confusing failure instead of a story.
func (e *ComposeEnv) WaitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, n := range e.nodes {
		gn, ok := n.(*grpcNode)
		if !ok {
			continue
		}
		for {
			_, err := gn.client.Balances(ctx, &nodepb.Empty{})
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("demo: %s never became ready: %w", gn.id, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	return nil
}

// grpcNode is one containerised node, reached over its client API.
type grpcNode struct {
	id     string
	loc    string
	client nodepb.NodeClient
	settle time.Duration
}

func (n *grpcNode) ID() string       { return n.id }
func (n *grpcNode) Location() string { return n.loc }

func (n *grpcNode) Receive(ctx context.Context, sku, location string, qty int64) error {
	_, err := n.client.Receive(ctx, &nodepb.ReceiveRequest{Sku: sku, Location: location, Qty: qty})
	return err
}

func (n *grpcNode) Issue(ctx context.Context, sku, location string, qty int64) error {
	_, err := n.client.Issue(ctx, &nodepb.IssueRequest{Sku: sku, Location: location, Qty: qty})
	return err
}

func (n *grpcNode) Balance(ctx context.Context, sku, location string) (int64, error) {
	resp, err := n.client.Balance(ctx, &nodepb.BalanceRequest{Sku: sku, Location: location})
	if err != nil {
		return 0, err
	}
	return resp.GetQty(), nil
}

// Sync waits out the node's own background sync interval. A containerised node
// syncs on a timer with no RPC to trigger it, so "give replication a chance to
// run" is literally a wait — and for a partitioned node, waiting achieves
// nothing, which is exactly the behaviour the scenario needs.
func (n *grpcNode) Sync(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(n.settle):
		return nil
	}
}

// compile-time proof that ComposeEnv is a full Env.
var _ Env = (*ComposeEnv)(nil)
```

- [ ] **Step 4: Run the demo package tests**

Run: `cd localfirst-go && LOCALFIRST_PG_DSN="postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable" go test ./demo/... -race -cover`
Expected: PASS, `coverage: 100.0% of statements`. `grpcNode.Sync`'s timeout branch and its cancellation branch are both covered by `TestComposeEnvTalksToNodesOverGRPC` and `TestSyncStopsWhenTheContextIsCancelled` respectively.

- [ ] **Step 5: Write the Dockerfile**

`localfirst-go/Dockerfile`:

```dockerfile
# One image, both binaries. CGO_ENABLED=0 is not an optimisation here: the
# SQLite driver is pure Go (modernc.org/sqlite) precisely so this stays true.
FROM golang:1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/node ./cmd/node && \
    CGO_ENABLED=0 go build -o /out/central ./cmd/central

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/node /node
COPY --from=build /out/central /central
```

- [ ] **Step 6: Write the Compose file**

`localfirst-go/docker-compose.yml`:

```yaml
# Postgres, one central, three nodes. Container names are fixed because the demo
# addresses them by name when it partitions the network, and the network is
# named so that `docker network disconnect localfirst-go_lfnet lf-node-3` is
# predictable.
name: localfirst-go

networks:
  lfnet:

services:
  postgres:
    image: postgres:16-alpine
    container_name: lf-postgres
    networks: [lfnet]
    environment:
      POSTGRES_USER: localfirst
      POSTGRES_PASSWORD: localfirst
      POSTGRES_DB: localfirst
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U localfirst"]
      interval: 1s
      timeout: 3s
      retries: 30

  central:
    build: .
    container_name: lf-central
    networks: [lfnet]
    depends_on:
      postgres:
        condition: service_healthy
    command:
      - /central
      - -dsn=postgres://localfirst:localfirst@postgres:5432/localfirst?sslmode=disable
      - -listen=:9090
      - -id=central
    ports:
      - "9090:9090"

  node-1:
    build: .
    container_name: lf-node-1
    networks: [lfnet]
    depends_on: [central]
    command:
      - /node
      - -id=node-1
      - -db=/data/node-1.db
      - -listen=:8080
      - -central=central:9090
      - -locations=A
      - -sync-every=1s
    # The distroless image has no writable filesystem, and the log is meant to be
    # ephemeral: every `make demo` starts from an empty world.
    tmpfs:
      - /data
    ports:
      - "8081:8080"

  node-2:
    build: .
    container_name: lf-node-2
    networks: [lfnet]
    depends_on: [central]
    command:
      - /node
      - -id=node-2
      - -db=/data/node-2.db
      - -listen=:8080
      - -central=central:9090
      - -locations=B
      - -sync-every=1s
    # The distroless image has no writable filesystem, and the log is meant to be
    # ephemeral: every `make demo` starts from an empty world.
    tmpfs:
      - /data
    ports:
      - "8082:8080"

  node-3:
    build: .
    container_name: lf-node-3
    networks: [lfnet]
    depends_on: [central]
    command:
      - /node
      - -id=node-3
      - -db=/data/node-3.db
      - -listen=:8080
      - -central=central:9090
      - -locations=C
      - -sync-every=1s
    # The distroless image has no writable filesystem, and the log is meant to be
    # ephemeral: every `make demo` starts from an empty world.
    tmpfs:
      - /data
    ports:
      - "8083:8080"
```

Note there are no named volumes: every log is intentionally ephemeral, so
`make demo` starts from an empty world every time. That is also why the `demo`
target ends with `docker compose down -v`.

- [ ] **Step 7: Write the demo command**

`localfirst-go/demo/cmd/demo/main.go`:

```go
// Command demo runs the seven-step local-first demonstration against the
// containers from docker-compose.yml. Every step, and every number it prints,
// comes from package demo — the same code the integration test asserts on.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/demo"
)

func main() {
	network := flag.String("network", "localfirst-go_lfnet", "compose network to partition")
	dsn := flag.String("dsn", "postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable", "central's Postgres DSN")
	settle := flag.Duration("settle", 4*time.Second, "how long to let background sync run; must exceed the nodes' -sync-every")
	ready := flag.Duration("ready-timeout", 90*time.Second, "how long to wait for the containers to boot")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	env, cleanup, err := demo.NewComposeEnv(ctx, demo.ComposeConfig{
		Network: *network,
		DSN:     *dsn,
		Settle:  *settle,
		Nodes:   demo.DefaultNodes(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "demo: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	fmt.Println("waiting for the containers…")
	if err := env.WaitReady(ctx, *ready); err != nil {
		fmt.Fprintf(os.Stderr, "demo: %v\n", err)
		os.Exit(1)
	}

	if _, err := demo.Run(ctx, env, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "\ndemo FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\ndone. read docs/limitations.md for what this deliberately does not do.")
}
```

- [ ] **Step 8: Run the real thing**

Run: `cd localfirst-go && make demo`

Expected, on stdout, with these exact numbers:

```
step 1/7  three nodes and one central are up
          node-1 exclusively owns location A
          node-2 exclusively owns location B
          node-3 exclusively owns location C

step 2/7  receiving stock at each node (local writes; no network needed)
          node-1 received 100 SKU-1 at A
          node-2 received  50 SKU-1 at B
          node-3 received  30 SKU-1 at C
          central global sum for SKU-1 = 180 (expected 180)

step 3/7  cutting node-3 off the network
          node-3 can no longer reach central or its peers

step 4/7  issuing 20 units, one at a time, against the isolated node
          20 succeeded, 0 failed — an offline node is fully operational
          node-3 reads its own balance at C = 10, from its own log

step 5/7  central has not heard about any of them
          central global sum = 180, still, while 20 units have already been issued
          this gap is not a bug: it is the price of accepting writes without coordination

step 6/7  reconnecting node-3

step 7/7  syncing, then reading central again
          central global sum = 160 (expected 180 - 20 = 160)
          converged: every offline write is now in the global sum, in order, exactly once
```

Troubleshooting, in the order things actually go wrong:

- `network localfirst-go_lfnet not found` — the compose project name and the
  `-network` flag disagree. `docker network ls` and pass the real name.
- Step 4 ops fail with `unavailable` — the demo talks to node 3 through its
  **published host port**, not through the compose network, so the partition
  must not break that. It does not: `docker network disconnect` detaches the
  container from `lfnet` while the port publish survives. If it fails anyway,
  something else disconnected the container.
- Step 7 prints 180 — the nodes did not get long enough to sync. Raise
  `-settle` above the nodes' `-sync-every` (1s in the compose file).

- [ ] **Step 9: Full suite, lint, commit**

Run: `cd localfirst-go && make test && golangci-lint run`
Expected: PASS everywhere, `demo` at 100% except `main`, no lint output.

```bash
git add localfirst-go/Dockerfile localfirst-go/docker-compose.yml localfirst-go/demo
git commit -m "feat(demo): make demo — compose, a real partition, and the numbers printed"
```
