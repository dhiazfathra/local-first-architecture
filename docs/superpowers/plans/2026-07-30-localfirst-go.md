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
| `demo/demo_test.go`, `demo/scenario.go` | 7-step demo scenario, shared by `make demo` and the test |
| `docker-compose.yml` | Postgres, central, 3 nodes |
| `docs/…` | ADRs, architecture, limitations, swapping the domain |

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
	proj eventlog.Projector // nil on nodes; central passes its global projector
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
```

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
	syncpb.RegisterSyncServer(srv, lfsync.NewServer(store, nil))

	if cfg.CentralAddr != "" {
		cc, err := grpc.NewClient(cfg.CentralAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("node: dial central: %w", err)
		}
		defer func() { _ = cc.Close() }()
		go lfsync.NewClient(cc, store, nil, "central").Run(ctx, cfg.SyncEvery)
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
Expected: FAIL — `undefined: central.OpenPG`. (`make pg-up` needs `docker-compose.yml`; if it is not written yet, create just the `postgres` service now — Task 15 fills in the rest:

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
