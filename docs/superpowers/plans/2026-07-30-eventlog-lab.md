# eventlog-lab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local-first inventory node whose source of truth is a local append-only CRDT event log, plus a deterministic fault-injecting harness that proves convergence, no-lost-event, and order-independence under partitions, clock skew, duplication, reorder, crashes, and slow peers.

**Architecture:** Five library packages, each independently testable: `clock/` (hybrid logical clocks), `eventlog/` (SQLite append-only log + snapshots + compaction), `crdt/` (commutative merge semantics), `sync/` (bidirectional gRPC replication over an injectable transport), `node/` (in-process wiring). A sixth internal package `internal/jepsenlite/` is the deterministic simulator — it is the actual product of this project. A Postgres-backed log implementation makes the central store a merge participant running the *identical* `crdt.Apply` code, never an authority.

**Tech Stack:** Go (latest stable), `modernc.org/sqlite` (pure Go, no cgo), `google.golang.org/grpc` + `google.golang.org/protobuf`, `github.com/jackc/pgx/v5`, stdlib `testing` (table-driven), `golangci-lint`.

## Domain Primer (read this first — the plan assumes no CRDT background)

You do not need prior CRDT knowledge. Four ideas carry the whole project:

1. **Event log as truth.** Nodes never exchange *state*; they exchange *events*. Current state is always `fold(Apply, events)`. Two nodes holding the same set of events must compute the same state, regardless of the order they received them.

2. **PN-counter.** A counter that survives concurrent edits without locking. Instead of one number, keep two maps keyed by node: `Pos[node]` (total ever added by that node) and `Neg[node]` (total ever subtracted by that node). The value is `sum(Pos) - sum(Neg)`. Because each node only ever writes its own slot, and summation is commutative, two nodes that saw the same events get the same total. "PN" = positive/negative.

3. **LWW-register (Last-Writer-Wins).** For fields where addition makes no sense (a name), store `{Value, At}` where `At` is a timestamp with a *total* order. A write is accepted only if the incoming timestamp is strictly after the stored one. Total order means no ties, so every replica makes the same accept/reject decision.

4. **Hybrid Logical Clock (HLC).** Wall-clock timestamps alone break LWW: a node with a clock 10 minutes fast wins every conflict forever. An HLC is `{Wall, Logical, NodeID}`. `Wall` is unix millis, `Logical` is a tiebreak counter, `NodeID` breaks remaining ties so the order is total. Crucially, when a node *receives* a remote event, it calls `Observe(remoteHLC)`, which pulls its own clock up to at least the remote's. That means any write a node makes *after* seeing your event is guaranteed to sort after your event — causality is preserved even with drifting clocks.

5. **Version vector.** A map `NodeID -> highest Seq of that node's events I hold`. Comparing two version vectors tells each side exactly what to send. `A dominates B` means A holds everything B holds.

## Global Constraints

Copied verbatim from the spec and the repo standard; every task's requirements implicitly include this section.

- Location: `eventlog-lab/` at the repo top level, its own `go.mod`, module path `github.com/dhiazfathra/local-first-architecture/eventlog-lab`. The repo currently contains only a Go `.gitignore` and `docs/` — no Go code exists yet.
- Go: latest stable release. SQLite via `modernc.org/sqlite` — pure Go, **no cgo**. Postgres via `github.com/jackc/pgx/v5`. Sync via gRPC + protobuf.
- **Minimal boilerplate. SOLID. DRY.** No interface with a single implementation unless a test or the harness supplies a second one.
- **100% test coverage** — every function, branch, and edge case tested. The harness under `internal/jepsenlite/` counts as tests, not as covered code.
- Every task runs `go test ./... -cover` and `golangci-lint run` from `eventlog-lab/` **before** its commit step. No task may conclude with failing or skipped tests.
- Central store is a merge participant, not an authority: it cannot reject an event, only merge it. There is exactly one `crdt.Apply` implementation, shared by nodes and central.
- Non-goals — do not build: authentication, multi-tenancy, UI, HTTP API, tombstone GC beyond `Compact`, real ERP domain rules, performance tuning, or any reusable-library packaging.
- Negative quantity is **accepted, not rejected**. Concurrent over-picking converging to a negative quantity is correct behavior and must be documented, not guarded against.

## File Structure

All paths relative to `eventlog-lab/`.

| File | Responsibility |
|---|---|
| `go.mod`, `go.sum` | Module definition, module path per Global Constraints. |
| `.golangci.yml` | Linter config. |
| `README.md` | What this is, how to run it, the documented negative-stock limitation. |
| `clock/clock.go` | `NodeID`, `HLC`, `Clock`, `Now`, `Observe`, `Before`. Injected wall source. |
| `clock/clock_test.go` | Monotonicity incl. backwards jumps, total order, `Observe`. |
| `eventlog/event.go` | `Seq`, `EventID`, `Kind`, `MetaSet`, `Event`, payload JSON codec, `VersionVector`. |
| `eventlog/log.go` | `Log` interface; shared SQL text; errors. |
| `eventlog/sqlite.go` | `SQLiteLog`: schema, `Append`, `Since`, `VersionVector`, `NextSeq`, snapshots, `Compact`. |
| `eventlog/postgres.go` | `PostgresLog`: same interface, same schema, pgx. Central store. |
| `eventlog/*_test.go` | Idempotence, crash-mid-append, iteration order, compaction safety. |
| `crdt/lww.go` | `LWW[T]` generic register. |
| `crdt/item.go` | `ItemState`, `Apply`, `Quantity`, JSON (de)serialization for snapshots. |
| `crdt/*_test.go` | Commutativity, associativity, idempotence-via-log, LWW rejection. |
| `sync/sync.proto`, `sync/syncpb/*.pb.go` | Wire protocol. |
| `sync/transport.go` | `Stream` + `Dialer` interfaces — the seam the harness injects into. |
| `sync/server.go` | `Server`: serves `Replicate`, merges, acks. Never rejects. |
| `sync/client.go` | `Client`: one session — Hello/Events/Ack, resumable via cursors, jittered backoff. |
| `sync/grpc.go` | gRPC-backed `Dialer` and server registration. |
| `sync/*_test.go` | Protocol happy path, malformed frames, VV regression, resume. |
| `node/node.go` | `Node`: log + clock + projection cache + sync client. `Receive/Pick/SetMeta/Delete/Get`. |
| `node/node_test.go` | Op semantics, projection cache invalidation, append-failure not acked. |
| `internal/jepsenlite/transport.go` | In-memory transport + all seven faults. |
| `internal/jepsenlite/schedule.go` | Seeded op + fault schedule generation. |
| `internal/jepsenlite/harness.go` | Run to quiescence, check the three properties, report. |
| `internal/jepsenlite/*_test.go` | Seeded runs; targeted non-random tests. |
| `cmd/lab/main.go` | `lab node|op|state|sim`. |

---

### Task 1: Module bootstrap, shared identifiers, and lint gate

Creates the module and the primitive types every later package imports. Ends with a real (if tiny) tested unit so the coverage gate is meaningful from task one.

**Files:**
- Create: `eventlog-lab/go.mod`
- Create: `eventlog-lab/.golangci.yml`
- Create: `eventlog-lab/clock/clock.go` (types only in this task)
- Test: `eventlog-lab/clock/clock_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type NodeID string` in package `clock`.
  - `type HLC struct { Wall int64; Logical uint32; NodeID NodeID }`
  - `func (a HLC) Before(b HLC) bool` — strict total order over (Wall, Logical, NodeID).
  - `func (a HLC) Compare(b HLC) int` — -1/0/+1, the primitive `Before` is built on.

- [ ] **Step 1: Create the module**

```bash
mkdir -p eventlog-lab/clock
cd eventlog-lab
go mod init github.com/dhiazfathra/local-first-architecture/eventlog-lab
```

Confirm the `go` directive in `go.mod` names your installed stable version (`go version`). Do not hand-edit it lower — `iter.Seq2` used later requires Go 1.23+.

- [ ] **Step 2: Create the linter config**

`eventlog-lab/.golangci.yml`:

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
    - errorlint
    - bodyclose
    - sqlclosecheck
    - rowserrcheck
formatters:
  enable:
    - gofmt
```

Verify the tool runs: `golangci-lint run` (it will pass trivially — no Go files yet).

- [ ] **Step 3: Write the failing test for HLC ordering**

`eventlog-lab/clock/clock_test.go`:

```go
package clock

import "testing"

func TestHLCCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b HLC
		want int
	}{
		{"wall dominates", HLC{Wall: 1}, HLC{Wall: 2}, -1},
		{"wall dominates reversed", HLC{Wall: 2}, HLC{Wall: 1}, 1},
		{"logical breaks wall tie", HLC{Wall: 1, Logical: 1}, HLC{Wall: 1, Logical: 2}, -1},
		{"logical tie reversed", HLC{Wall: 1, Logical: 2}, HLC{Wall: 1, Logical: 1}, 1},
		{"node breaks logical tie", HLC{Wall: 1, Logical: 1, NodeID: "A"}, HLC{Wall: 1, Logical: 1, NodeID: "B"}, -1},
		{"node tie reversed", HLC{Wall: 1, Logical: 1, NodeID: "B"}, HLC{Wall: 1, Logical: 1, NodeID: "A"}, 1},
		{"fully equal", HLC{Wall: 1, Logical: 1, NodeID: "A"}, HLC{Wall: 1, Logical: 1, NodeID: "A"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Fatalf("Compare() = %d, want %d", got, tt.want)
			}
			wantBefore := tt.want < 0
			if got := tt.a.Before(tt.b); got != wantBefore {
				t.Fatalf("Before() = %v, want %v", got, wantBefore)
			}
		})
	}
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd eventlog-lab && go test ./clock/ -run TestHLCCompare -v`
Expected: FAIL — build error, `undefined: HLC`.

- [ ] **Step 5: Write the minimal implementation**

`eventlog-lab/clock/clock.go`:

```go
// Package clock implements hybrid logical clocks (HLCs).
//
// An HLC timestamp combines a wall-clock reading with a logical counter and a
// node identifier. The triple has a strict total order, which is what makes
// last-writer-wins registers deterministic across replicas: every replica
// reaches the same verdict on which of two writes is later, with no ties.
package clock

import "cmp"

// NodeID identifies a replica. It is the final tiebreak in HLC ordering, so it
// must be stable for the lifetime of a node's data.
type NodeID string

// HLC is a hybrid logical clock timestamp.
type HLC struct {
	Wall    int64  // unix milliseconds
	Logical uint32 // tiebreak counter within the same Wall value
	NodeID  NodeID // final tiebreak, yields a total order
}

// Compare orders a against b by Wall, then Logical, then NodeID.
// It returns -1, 0, or +1.
func (a HLC) Compare(b HLC) int {
	if c := cmp.Compare(a.Wall, b.Wall); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Logical, b.Logical); c != 0 {
		return c
	}
	return cmp.Compare(a.NodeID, b.NodeID)
}

// Before reports whether a strictly precedes b in the total order.
func (a HLC) Before(b HLC) bool { return a.Compare(b) < 0 }
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd eventlog-lab && go test ./clock/ -run TestHLCCompare -v`
Expected: PASS, all seven subtests.

- [ ] **Step 7: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover
golangci-lint run
```
Expected: `clock` at 100.0% of statements; linter clean. Fix anything reported before continuing.

- [ ] **Step 8: Commit**

```bash
cd eventlog-lab
git add go.mod .golangci.yml clock/clock.go clock/clock_test.go
git commit -m "feat(eventlog-lab): bootstrap module with HLC total ordering"
```

---

### Task 2: Hybrid logical clock — `Now` and `Observe`

**Files:**
- Modify: `eventlog-lab/clock/clock.go`
- Modify: `eventlog-lab/clock/clock_test.go`

**Interfaces:**
- Consumes: `NodeID`, `HLC`, `HLC.Before`, `HLC.Compare` from Task 1.
- Produces:
  - `type WallFunc func() int64` — injected millisecond wall source.
  - `func New(id NodeID, wall WallFunc) *Clock` — `wall` may be nil, meaning real time.
  - `func (c *Clock) Now() HLC` — never regresses, even if `wall` jumps backwards.
  - `func (c *Clock) Observe(remote HLC) HLC` — merges a remote timestamp into the local clock and returns the new local timestamp.
  - `func (c *Clock) Last() HLC` — last issued timestamp, for assertions.
  - `*Clock` is safe for concurrent use.

**Domain note for the implementer:** `Now()` must be monotonic. If the injected wall clock jumps backwards (NTP correction, a fault the harness injects deliberately), clamp to the last issued `Wall` and bump `Logical` instead. `Observe(remote)` exists so that a local write made *after* receiving a remote event always sorts *after* that event: take the max of (local wall, last issued, remote wall), and when the winner ties an existing value, bump `Logical` past it. Without `Observe`, a node with a slow clock would keep losing LWW conflicts to a fast peer forever and causality would invert.

- [ ] **Step 1: Write the failing tests**

Append to `eventlog-lab/clock/clock_test.go`:

```go
// fakeWall returns a WallFunc reading from a mutable slice of readings; the
// last reading repeats once exhausted.
func fakeWall(readings ...int64) (WallFunc, *int) {
	i := 0
	return func() int64 {
		v := readings[i]
		if i < len(readings)-1 {
			i++
		}
		return v
	}, &i
}

func TestClockNowMonotonic(t *testing.T) {
	tests := []struct {
		name     string
		readings []int64
		calls    int
		want     []HLC
	}{
		{
			name:     "advancing wall resets logical",
			readings: []int64{100, 101, 102},
			calls:    3,
			want: []HLC{
				{Wall: 100, Logical: 0, NodeID: "A"},
				{Wall: 101, Logical: 0, NodeID: "A"},
				{Wall: 102, Logical: 0, NodeID: "A"},
			},
		},
		{
			name:     "stalled wall bumps logical",
			readings: []int64{100, 100, 100},
			calls:    3,
			want: []HLC{
				{Wall: 100, Logical: 0, NodeID: "A"},
				{Wall: 100, Logical: 1, NodeID: "A"},
				{Wall: 100, Logical: 2, NodeID: "A"},
			},
		},
		{
			name:     "backwards jump clamps and bumps logical",
			readings: []int64{100, 50, 50},
			calls:    3,
			want: []HLC{
				{Wall: 100, Logical: 0, NodeID: "A"},
				{Wall: 100, Logical: 1, NodeID: "A"},
				{Wall: 100, Logical: 2, NodeID: "A"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wall, _ := fakeWall(tt.readings...)
			c := New("A", wall)
			var prev HLC
			for i := 0; i < tt.calls; i++ {
				got := c.Now()
				if got != tt.want[i] {
					t.Fatalf("call %d: Now() = %+v, want %+v", i, got, tt.want[i])
				}
				if i > 0 && !prev.Before(got) {
					t.Fatalf("call %d: %+v did not advance past %+v", i, got, prev)
				}
				prev = got
			}
			if c.Last() != tt.want[tt.calls-1] {
				t.Fatalf("Last() = %+v, want %+v", c.Last(), tt.want[tt.calls-1])
			}
		})
	}
}

func TestClockObserve(t *testing.T) {
	tests := []struct {
		name   string
		wall   int64
		remote HLC
		want   HLC
	}{
		{
			name:   "remote in the future pulls local forward",
			wall:   100,
			remote: HLC{Wall: 500, Logical: 3, NodeID: "B"},
			want:   HLC{Wall: 500, Logical: 4, NodeID: "A"},
		},
		{
			name:   "remote in the past leaves local wall, uses wall reading",
			wall:   100,
			remote: HLC{Wall: 50, Logical: 9, NodeID: "B"},
			want:   HLC{Wall: 100, Logical: 0, NodeID: "A"},
		},
		{
			name:   "remote equal wall bumps logical past remote",
			wall:   100,
			remote: HLC{Wall: 100, Logical: 7, NodeID: "B"},
			want:   HLC{Wall: 100, Logical: 8, NodeID: "A"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wall, _ := fakeWall(tt.wall)
			c := New("A", wall)
			got := c.Observe(tt.remote)
			if got != tt.want {
				t.Fatalf("Observe() = %+v, want %+v", got, tt.want)
			}
			if !tt.remote.Before(got) && tt.remote.Wall >= tt.wall {
				t.Fatalf("Observe() = %+v does not sort after remote %+v", got, tt.remote)
			}
			next := c.Now()
			if !got.Before(next) {
				t.Fatalf("Now() = %+v did not advance past Observe() = %+v", next, got)
			}
		})
	}
}

func TestClockDefaultWallUsesRealTime(t *testing.T) {
	c := New("A", nil)
	before := time.Now().UnixMilli()
	got := c.Now()
	after := time.Now().UnixMilli()
	if got.Wall < before || got.Wall > after {
		t.Fatalf("Now().Wall = %d, want within [%d, %d]", got.Wall, before, after)
	}
}
```

Add `"time"` to the test file's imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./clock/ -v`
Expected: FAIL — `undefined: WallFunc`, `undefined: New`.

- [ ] **Step 3: Write the implementation**

Append to `eventlog-lab/clock/clock.go`:

```go
// WallFunc reads a wall clock in unix milliseconds. Injected so tests and the
// fault harness can skew, stall, or reverse time.
type WallFunc func() int64

// Clock issues monotonically increasing HLC timestamps for one node.
type Clock struct {
	mu   sync.Mutex
	id   NodeID
	wall WallFunc
	last HLC
}

// New returns a Clock for id. A nil wall means real system time.
func New(id NodeID, wall WallFunc) *Clock {
	if wall == nil {
		wall = func() int64 { return time.Now().UnixMilli() }
	}
	return &Clock{id: id, wall: wall, last: HLC{NodeID: id}}
}

// Now returns the next local timestamp. It never regresses: if the wall clock
// has not advanced past the last issued timestamp -- including a jump
// backwards -- the wall value is clamped and Logical is bumped instead.
func (c *Clock) Now() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tick(HLC{NodeID: c.id})
}

// Observe merges a remote timestamp into the local clock and returns the new
// local timestamp, which is guaranteed to sort after remote. Calling it on
// every received event is what preserves causality across drifting clocks.
func (c *Clock) Observe(remote HLC) HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tick(remote)
}

// Last returns the most recently issued timestamp.
func (c *Clock) Last() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// tick advances the clock past both c.last and floor, using the wall reading
// when it is ahead of both. Caller holds c.mu.
func (c *Clock) tick(floor HLC) HLC {
	next := HLC{Wall: c.wall(), NodeID: c.id}
	for _, prior := range [...]HLC{c.last, floor} {
		if next.Wall < prior.Wall {
			next.Wall = prior.Wall
			next.Logical = prior.Logical + 1
		} else if next.Wall == prior.Wall && next.Logical <= prior.Logical {
			next.Logical = prior.Logical + 1
		}
	}
	c.last = next
	return next
}
```

Add `"sync"` and `"time"` to the imports in `clock.go`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd eventlog-lab && go test ./clock/ -v`
Expected: PASS.

- [ ] **Step 5: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover
golangci-lint run
```
Expected: `clock` at 100.0%. If `tick`'s `else if` branch is uncovered, the "remote equal wall" subtest covers it — check the test ran.

- [ ] **Step 6: Commit**

```bash
cd eventlog-lab
git add clock/
git commit -m "feat(clock): monotonic HLC Now and causal Observe"
```

---

### Task 3: Event types, payload codec, and version vectors

Pure data and pure functions. No storage yet — this task exists separately because both storage backends and the CRDT package depend on these types, and a reviewer can reject the wire/JSON shape independently of any SQL.

**Files:**
- Create: `eventlog-lab/eventlog/event.go`
- Test: `eventlog-lab/eventlog/event_test.go`

**Interfaces:**
- Consumes: `clock.NodeID`, `clock.HLC` from Tasks 1-2.
- Produces:
  - `type Seq uint64`
  - `type EventID struct { NodeID clock.NodeID; Seq Seq }`
  - `type Kind int` with `KindQuantityDelta Kind = 1`, `KindMetaSet = 2`, `KindDeleteSet = 3`
  - `func (k Kind) Valid() bool`
  - `type MetaSet struct { Name *string; ReorderPoint *int64 }`
  - `type Event struct { ID EventID; HLC clock.HLC; SKU string; Kind Kind; Delta int64; Meta *MetaSet; DeletedTo *bool }`
  - `func (e Event) MarshalPayload() ([]byte, error)`
  - `func (e *Event) UnmarshalPayload(b []byte) error`
  - `func (e Event) Validate() error`
  - `var ErrMalformedEvent error`
  - `type VersionVector map[clock.NodeID]Seq`
  - `func (vv VersionVector) Clone() VersionVector`
  - `func (vv VersionVector) Observe(id EventID)`
  - `func (vv VersionVector) Contains(id EventID) bool`
  - `func (vv VersionVector) Dominates(other VersionVector) bool`
  - `func (vv VersionVector) Merge(other VersionVector) VersionVector`

**Domain notes for the implementer:**
- `EventID` is `{NodeID, Seq}`. A node numbers its own events 1, 2, 3, … so IDs are globally unique with no UUIDs and no coordination. A gap-free per-node sequence is also what lets a version vector be a single number per node.
- `KindQuantityDelta` carries a **signed delta**, never an absolute quantity. `Delta: -3` means "three were picked". Deltas commute; absolute values do not — two nodes setting `Quantity = 7` and `Quantity = 4` concurrently have no correct merge, whereas `-3` and `-4` obviously sum.
- `Validate` is the trust boundary for remote events. The spec is explicit: *never persist an event the local `crdt` package cannot apply*, or convergence breaks silently. Reject unknown kinds, empty SKUs, zero `Seq`, empty `NodeID`, and kind/field mismatches.

- [ ] **Step 1: Write the failing tests**

`eventlog-lab/eventlog/event_test.go`:

```go
package eventlog

import (
	"errors"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

func ptr[T any](v T) *T { return &v }

func TestEventPayloadRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   Event
	}{
		{"quantity delta positive", Event{Kind: KindQuantityDelta, Delta: 10}},
		{"quantity delta negative", Event{Kind: KindQuantityDelta, Delta: -3}},
		{"meta both fields", Event{Kind: KindMetaSet, Meta: &MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(5))}}},
		{"meta name only", Event{Kind: KindMetaSet, Meta: &MetaSet{Name: ptr("Widget")}}},
		{"meta reorder only", Event{Kind: KindMetaSet, Meta: &MetaSet{ReorderPoint: ptr(int64(5))}}},
		{"delete true", Event{Kind: KindDeleteSet, DeletedTo: ptr(true)}},
		{"delete false", Event{Kind: KindDeleteSet, DeletedTo: ptr(false)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := tt.in.MarshalPayload()
			if err != nil {
				t.Fatalf("MarshalPayload() error = %v", err)
			}
			got := Event{Kind: tt.in.Kind}
			if err := got.UnmarshalPayload(b); err != nil {
				t.Fatalf("UnmarshalPayload() error = %v", err)
			}
			if got.Delta != tt.in.Delta {
				t.Errorf("Delta = %d, want %d", got.Delta, tt.in.Delta)
			}
			switch {
			case tt.in.Meta == nil && got.Meta != nil:
				t.Errorf("Meta = %+v, want nil", got.Meta)
			case tt.in.Meta != nil:
				if got.Meta == nil {
					t.Fatalf("Meta = nil, want %+v", tt.in.Meta)
				}
				if !eqPtr(got.Meta.Name, tt.in.Meta.Name) || !eqPtr(got.Meta.ReorderPoint, tt.in.Meta.ReorderPoint) {
					t.Errorf("Meta = %+v/%+v, want %+v/%+v", got.Meta.Name, got.Meta.ReorderPoint, tt.in.Meta.Name, tt.in.Meta.ReorderPoint)
				}
			}
			if !eqPtr(got.DeletedTo, tt.in.DeletedTo) {
				t.Errorf("DeletedTo = %v, want %v", got.DeletedTo, tt.in.DeletedTo)
			}
		})
	}
}

func eqPtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func TestEventUnmarshalPayloadRejectsGarbage(t *testing.T) {
	e := Event{Kind: KindQuantityDelta}
	if err := e.UnmarshalPayload([]byte("{not json")); !errors.Is(err, ErrMalformedEvent) {
		t.Fatalf("UnmarshalPayload() error = %v, want ErrMalformedEvent", err)
	}
}

func TestEventValidate(t *testing.T) {
	base := func(mut func(*Event)) Event {
		e := Event{
			ID:   EventID{NodeID: "A", Seq: 1},
			HLC:  clock.HLC{Wall: 1, NodeID: "A"},
			SKU:  "SKU-1",
			Kind: KindQuantityDelta,
			Delta: 1,
		}
		mut(&e)
		return e
	}
	tests := []struct {
		name    string
		event   Event
		wantErr bool
	}{
		{"valid quantity delta", base(func(*Event) {}), false},
		{"valid meta", base(func(e *Event) { e.Kind = KindMetaSet; e.Delta = 0; e.Meta = &MetaSet{Name: ptr("W")} }), false},
		{"valid delete", base(func(e *Event) { e.Kind = KindDeleteSet; e.Delta = 0; e.DeletedTo = ptr(true) }), false},
		{"empty node id", base(func(e *Event) { e.ID.NodeID = "" }), true},
		{"zero seq", base(func(e *Event) { e.ID.Seq = 0 }), true},
		{"empty sku", base(func(e *Event) { e.SKU = "" }), true},
		{"unknown kind", base(func(e *Event) { e.Kind = 99 }), true},
		{"zero delta on quantity kind", base(func(e *Event) { e.Delta = 0 }), true},
		{"meta kind without meta", base(func(e *Event) { e.Kind = KindMetaSet; e.Delta = 0 }), true},
		{"meta kind with empty meta", base(func(e *Event) { e.Kind = KindMetaSet; e.Delta = 0; e.Meta = &MetaSet{} }), true},
		{"delete kind without value", base(func(e *Event) { e.Kind = KindDeleteSet; e.Delta = 0 }), true},
		{"delta set on meta kind", base(func(e *Event) { e.Kind = KindMetaSet; e.Meta = &MetaSet{Name: ptr("W")} }), true},
		{"hlc node mismatch", base(func(e *Event) { e.HLC.NodeID = "B" }), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrMalformedEvent) {
				t.Fatalf("Validate() error = %v, want wrapped ErrMalformedEvent", err)
			}
		})
	}
}

func TestKindValid(t *testing.T) {
	for _, k := range []Kind{KindQuantityDelta, KindMetaSet, KindDeleteSet} {
		if !k.Valid() {
			t.Errorf("Kind(%d).Valid() = false, want true", k)
		}
	}
	for _, k := range []Kind{0, 4, -1} {
		if k.Valid() {
			t.Errorf("Kind(%d).Valid() = true, want false", k)
		}
	}
}

func TestVersionVector(t *testing.T) {
	t.Run("observe keeps the highest seq per node", func(t *testing.T) {
		vv := VersionVector{}
		vv.Observe(EventID{NodeID: "A", Seq: 5})
		vv.Observe(EventID{NodeID: "A", Seq: 3})
		vv.Observe(EventID{NodeID: "B", Seq: 1})
		if vv["A"] != 5 || vv["B"] != 1 {
			t.Fatalf("vv = %v, want {A:5, B:1}", vv)
		}
	})

	t.Run("contains", func(t *testing.T) {
		vv := VersionVector{"A": 5}
		cases := []struct {
			id   EventID
			want bool
		}{
			{EventID{NodeID: "A", Seq: 5}, true},
			{EventID{NodeID: "A", Seq: 4}, true},
			{EventID{NodeID: "A", Seq: 6}, false},
			{EventID{NodeID: "B", Seq: 1}, false},
		}
		for _, c := range cases {
			if got := vv.Contains(c.id); got != c.want {
				t.Errorf("Contains(%+v) = %v, want %v", c.id, got, c.want)
			}
		}
	})

	t.Run("dominates and merge", func(t *testing.T) {
		tests := []struct {
			name          string
			a, b          VersionVector
			wantDominates bool
			wantMerge     VersionVector
		}{
			{"equal", VersionVector{"A": 1}, VersionVector{"A": 1}, true, VersionVector{"A": 1}},
			{"strictly ahead", VersionVector{"A": 2}, VersionVector{"A": 1}, true, VersionVector{"A": 2}},
			{"strictly behind", VersionVector{"A": 1}, VersionVector{"A": 2}, false, VersionVector{"A": 2}},
			{"missing node", VersionVector{"A": 1}, VersionVector{"B": 1}, false, VersionVector{"A": 1, "B": 1}},
			{"superset", VersionVector{"A": 1, "B": 1}, VersionVector{"A": 1}, true, VersionVector{"A": 1, "B": 1}},
			{"empty dominated by anything", VersionVector{"A": 1}, VersionVector{}, true, VersionVector{"A": 1}},
			{"nil receiver", nil, VersionVector{"A": 1}, false, VersionVector{"A": 1}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := tt.a.Dominates(tt.b); got != tt.wantDominates {
					t.Errorf("Dominates() = %v, want %v", got, tt.wantDominates)
				}
				got := tt.a.Merge(tt.b)
				if len(got) != len(tt.wantMerge) {
					t.Fatalf("Merge() = %v, want %v", got, tt.wantMerge)
				}
				for k, v := range tt.wantMerge {
					if got[k] != v {
						t.Fatalf("Merge()[%q] = %d, want %d", k, got[k], v)
					}
				}
			})
		}
	})

	t.Run("clone is independent", func(t *testing.T) {
		vv := VersionVector{"A": 1}
		c := vv.Clone()
		c["A"] = 9
		c["B"] = 2
		if vv["A"] != 1 || len(vv) != 1 {
			t.Fatalf("original mutated: %v", vv)
		}
		if got := VersionVector(nil).Clone(); got == nil || len(got) != 0 {
			t.Fatalf("nil.Clone() = %v, want empty non-nil", got)
		}
	})
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./eventlog/ -v`
Expected: FAIL — build error, `undefined: Event`.

- [ ] **Step 3: Write the implementation**

`eventlog-lab/eventlog/event.go`:

```go
// Package eventlog defines the append-only event log that is a node's source
// of truth, plus the event types replicated between nodes.
package eventlog

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// ErrMalformedEvent marks an event that must never be persisted: the local
// crdt package could not apply it, and silently storing it would break
// convergence. Callers reject the frame, log loudly, and continue.
var ErrMalformedEvent = errors.New("malformed event")

// Seq is a per-node monotonic sequence number starting at 1. Gap-free
// numbering is what lets a version vector summarise a node with one integer.
type Seq uint64

// EventID globally identifies an event without a UUID: a node only ever mints
// IDs bearing its own NodeID, so no coordination is needed.
type EventID struct {
	NodeID clock.NodeID
	Seq    Seq
}

// Kind discriminates the event payload.
type Kind int

const (
	KindQuantityDelta Kind = 1 // signed Delta, commutative
	KindMetaSet       Kind = 2 // LWW Name and/or ReorderPoint
	KindDeleteSet     Kind = 3 // LWW tombstone flag
)

// Valid reports whether k is a kind this build can apply.
func (k Kind) Valid() bool {
	return k == KindQuantityDelta || k == KindMetaSet || k == KindDeleteSet
}

// MetaSet carries the last-writer-wins metadata fields. Nil fields are not
// written, which lets one event set Name without clobbering ReorderPoint.
type MetaSet struct {
	Name         *string `json:"name,omitempty"`
	ReorderPoint *int64  `json:"reorder_point,omitempty"`
}

// Empty reports whether the MetaSet would write nothing.
func (m *MetaSet) Empty() bool { return m == nil || (m.Name == nil && m.ReorderPoint == nil) }

// Event is one immutable fact about a StockItem.
type Event struct {
	ID        EventID
	HLC       clock.HLC
	SKU       string
	Kind      Kind
	Delta     int64    // KindQuantityDelta only; sign carries increment/decrement
	Meta      *MetaSet // KindMetaSet only
	DeletedTo *bool    // KindDeleteSet only
}

// payload is the kind-specific JSON blob stored in the events table. Keeping
// the identity and ordering columns out of it means indexes work and the blob
// stays a pure payload.
type payload struct {
	Delta     int64    `json:"delta,omitempty"`
	Meta      *MetaSet `json:"meta,omitempty"`
	DeletedTo *bool    `json:"deleted_to,omitempty"`
}

// MarshalPayload encodes the kind-specific fields.
func (e Event) MarshalPayload() ([]byte, error) {
	b, err := json.Marshal(payload{Delta: e.Delta, Meta: e.Meta, DeletedTo: e.DeletedTo})
	if err != nil {
		return nil, fmt.Errorf("marshal payload for %v: %w", e.ID, err)
	}
	return b, nil
}

// UnmarshalPayload decodes the kind-specific fields into e.
func (e *Event) UnmarshalPayload(b []byte) error {
	var p payload
	if err := json.Unmarshal(b, &p); err != nil {
		return fmt.Errorf("%w: unparseable payload for %v: %v", ErrMalformedEvent, e.ID, err)
	}
	e.Delta, e.Meta, e.DeletedTo = p.Delta, p.Meta, p.DeletedTo
	return nil
}

// Validate is the trust boundary for remote events: anything it rejects must
// never reach storage.
func (e Event) Validate() error {
	switch {
	case e.ID.NodeID == "":
		return fmt.Errorf("%w: empty node id", ErrMalformedEvent)
	case e.ID.Seq == 0:
		return fmt.Errorf("%w: seq must start at 1", ErrMalformedEvent)
	case e.SKU == "":
		return fmt.Errorf("%w: empty sku", ErrMalformedEvent)
	case e.HLC.NodeID != e.ID.NodeID:
		return fmt.Errorf("%w: hlc node %q != event node %q", ErrMalformedEvent, e.HLC.NodeID, e.ID.NodeID)
	case !e.Kind.Valid():
		return fmt.Errorf("%w: unknown kind %d", ErrMalformedEvent, e.Kind)
	}
	if e.Kind != KindQuantityDelta && e.Delta != 0 {
		return fmt.Errorf("%w: delta set on kind %d", ErrMalformedEvent, e.Kind)
	}
	switch e.Kind {
	case KindQuantityDelta:
		if e.Delta == 0 {
			return fmt.Errorf("%w: zero quantity delta", ErrMalformedEvent)
		}
	case KindMetaSet:
		if e.Meta.Empty() {
			return fmt.Errorf("%w: meta event writes nothing", ErrMalformedEvent)
		}
	case KindDeleteSet:
		if e.DeletedTo == nil {
			return fmt.Errorf("%w: delete event without value", ErrMalformedEvent)
		}
	}
	return nil
}

// VersionVector maps each node to the highest Seq of its events we hold.
type VersionVector map[clock.NodeID]Seq

// Clone returns an independent copy, never nil.
func (vv VersionVector) Clone() VersionVector {
	out := make(VersionVector, len(vv))
	for k, v := range vv {
		out[k] = v
	}
	return out
}

// Observe records that we now hold id.
func (vv VersionVector) Observe(id EventID) {
	if vv[id.NodeID] < id.Seq {
		vv[id.NodeID] = id.Seq
	}
}

// Contains reports whether id is already covered.
func (vv VersionVector) Contains(id EventID) bool { return vv[id.NodeID] >= id.Seq }

// Dominates reports whether vv holds everything other holds.
func (vv VersionVector) Dominates(other VersionVector) bool {
	for k, v := range other {
		if vv[k] < v {
			return false
		}
	}
	return true
}

// Merge returns the pointwise maximum of vv and other.
func (vv VersionVector) Merge(other VersionVector) VersionVector {
	out := vv.Clone()
	for k, v := range other {
		if out[k] < v {
			out[k] = v
		}
	}
	return out
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd eventlog-lab && go test ./eventlog/ -v`
Expected: PASS.

- [ ] **Step 5: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover
golangci-lint run
```
Expected: both packages 100.0%.

- [ ] **Step 6: Commit**

```bash
cd eventlog-lab
git add eventlog/
git commit -m "feat(eventlog): event types, payload codec, and version vectors"
```

---

### Task 4: SQLite event log — schema, idempotent append, `Since`, `VersionVector`

**Files:**
- Create: `eventlog-lab/eventlog/log.go` (interface + shared schema text)
- Create: `eventlog-lab/eventlog/sqlite.go`
- Test: `eventlog-lab/eventlog/sqlite_test.go`
- Modify: `eventlog-lab/go.mod` (adds `modernc.org/sqlite`)

**Interfaces:**
- Consumes: `Event`, `EventID`, `Seq`, `VersionVector`, `ErrMalformedEvent`, `clock.HLC` from Task 3.
- Produces:
  - ```go
    type Log interface {
        Append(ctx context.Context, e Event) error
        AppendLocal(ctx context.Context, mint func(Seq) Event) (Event, error)
        Since(ctx context.Context, vv VersionVector) iter.Seq2[Event, error]
        VersionVector(ctx context.Context) (VersionVector, error)
        LoadSnapshot(ctx context.Context, sku string) ([]byte, VersionVector, error)
        SaveSnapshot(ctx context.Context, sku string, state []byte, covers VersionVector) error
        Cursor(ctx context.Context, peer clock.NodeID) (Seq, error)
        SetCursor(ctx context.Context, peer clock.NodeID, last Seq) error
        Compact(ctx context.Context, upTo VersionVector) error
        Close() error
    }
    ```
  - `func OpenSQLite(dsn string) (*SQLiteLog, error)` — applies the schema; `dsn` may be `":memory:"`.
  - `func (l *SQLiteLog) EventsForSKU(ctx context.Context, sku string, after VersionVector) iter.Seq2[Event, error]`
  - `var ErrNoSnapshot error`
- Note: `LoadSnapshot`, `SaveSnapshot`, `Cursor`, `SetCursor`, and `Compact` are declared on the interface here but implemented in Task 6 (snapshots/compaction) and Task 8 (cursors). **In this task, implement them as working methods** — they are small, and stubbing them would leave uncovered code that the coverage gate rejects. The steps below include their code.

**Domain notes for the implementer:**
- `Append` is idempotent by primary key `(node_id, seq)`. `INSERT ... ON CONFLICT DO NOTHING` **is the entire duplicate-delivery defense** — there is no dedup table and no bloom filter. Every "duplicate delivery" fault in the harness is absorbed here.
- `AppendLocal` takes a *minting function* rather than an event, because `Seq` must be allocated inside the same transaction as the insert. If allocation and insert were separate transactions, a crash between them would burn a sequence number and leave a permanent gap, which breaks the single-integer version-vector summary.
- `Since` returns events in HLC order. That is a convenience for humans reading logs, not a correctness requirement — `crdt.Apply` is order-independent by construction. Do not let any later code depend on the order.
- Use `iter.Seq2[Event, error]` (Go 1.23 range-over-func) so callers can stream a large log without materialising it, and so an error mid-iteration is delivered to the consumer rather than swallowed.

- [ ] **Step 1: Add the driver dependency**

```bash
cd eventlog-lab
go get modernc.org/sqlite
```

Confirm no cgo is involved: `go list -deps ./... | grep -c '^C$'` prints `0`.

- [ ] **Step 2: Write the failing tests**

`eventlog-lab/eventlog/sqlite_test.go`:

```go
package eventlog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// newTestLog returns an isolated on-disk log; on-disk rather than :memory: so
// tests can reopen it and prove durability.
func newTestLog(t *testing.T) *SQLiteLog {
	t.Helper()
	l, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return l
}

func qty(node clock.NodeID, seq Seq, wall int64, sku string, delta int64) Event {
	return Event{
		ID:    EventID{NodeID: node, Seq: seq},
		HLC:   clock.HLC{Wall: wall, NodeID: node},
		SKU:   sku,
		Kind:  KindQuantityDelta,
		Delta: delta,
	}
}

func collect(t *testing.T, seq func() (Event, error), it func(func(Event, error) bool)) []Event {
	t.Helper()
	var out []Event
	for e, err := range it {
		if err != nil {
			t.Fatalf("iteration error = %v", err)
		}
		out = append(out, e)
	}
	return out
}

func TestAppendIsIdempotent(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	e := qty("A", 1, 100, "SKU-1", 5)

	for i := 0; i < 3; i++ {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() call %d error = %v", i, err)
		}
	}

	var got []Event
	for ev, err := range l.Since(ctx, VersionVector{}) {
		if err != nil {
			t.Fatalf("Since() error = %v", err)
		}
		got = append(got, ev)
	}
	if len(got) != 1 {
		t.Fatalf("len(events) = %d after 3 identical appends, want 1", len(got))
	}
	if got[0].ID != e.ID || got[0].Delta != e.Delta || got[0].HLC != e.HLC || got[0].SKU != e.SKU {
		t.Fatalf("round trip = %+v, want %+v", got[0], e)
	}
}

func TestAppendRejectsMalformed(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	tests := []struct {
		name  string
		event Event
	}{
		{"unknown kind", Event{ID: EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{NodeID: "A"}, SKU: "S", Kind: 42}},
		{"empty sku", Event{ID: EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{NodeID: "A"}, Kind: KindQuantityDelta, Delta: 1}},
		{"zero seq", Event{ID: EventID{NodeID: "A"}, HLC: clock.HLC{NodeID: "A"}, SKU: "S", Kind: KindQuantityDelta, Delta: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := l.Append(ctx, tt.event); !errors.Is(err, ErrMalformedEvent) {
				t.Fatalf("Append() error = %v, want ErrMalformedEvent", err)
			}
		})
	}
	vv, err := l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if len(vv) != 0 {
		t.Fatalf("malformed events reached storage: %v", vv)
	}
}

func TestAppendLocalAllocatesGapFreeSeq(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	for i := Seq(1); i <= 3; i++ {
		got, err := l.AppendLocal(ctx, func(s Seq) Event {
			return qty("A", s, int64(100+i), "SKU-1", 1)
		})
		if err != nil {
			t.Fatalf("AppendLocal() error = %v", err)
		}
		if got.ID.Seq != i {
			t.Fatalf("AppendLocal() seq = %d, want %d", got.ID.Seq, i)
		}
	}
	vv, err := l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["A"] != 3 {
		t.Fatalf("vv[A] = %d, want 3", vv["A"])
	}
}

func TestAppendLocalRejectsMalformedWithoutBurningSeq(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if _, err := l.AppendLocal(ctx, func(s Seq) Event {
		return Event{ID: EventID{NodeID: "A", Seq: s}, HLC: clock.HLC{NodeID: "A"}, SKU: "S", Kind: 42}
	}); !errors.Is(err, ErrMalformedEvent) {
		t.Fatalf("AppendLocal() error = %v, want ErrMalformedEvent", err)
	}
	got, err := l.AppendLocal(ctx, func(s Seq) Event { return qty("A", s, 100, "SKU-1", 1) })
	if err != nil {
		t.Fatalf("AppendLocal() error = %v", err)
	}
	if got.ID.Seq != 1 {
		t.Fatalf("seq = %d after a rejected append, want 1 (no gap)", got.ID.Seq)
	}
}

func TestSinceFiltersByVersionVectorAndOrdersByHLC(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	all := []Event{
		qty("A", 1, 300, "SKU-1", 1),
		qty("A", 2, 100, "SKU-1", 2),
		qty("B", 1, 200, "SKU-2", 3),
	}
	for _, e := range all {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append(%+v) error = %v", e.ID, err)
		}
	}

	tests := []struct {
		name string
		vv   VersionVector
		want []EventID
	}{
		{"empty vector yields all in HLC order", VersionVector{}, []EventID{
			{NodeID: "A", Seq: 2}, {NodeID: "B", Seq: 1}, {NodeID: "A", Seq: 1},
		}},
		{"partial vector skips covered", VersionVector{"A": 2}, []EventID{{NodeID: "B", Seq: 1}}},
		{"full vector yields nothing", VersionVector{"A": 2, "B": 1}, nil},
		{"unknown peer in vector is ignored", VersionVector{"Z": 9}, []EventID{
			{NodeID: "A", Seq: 2}, {NodeID: "B", Seq: 1}, {NodeID: "A", Seq: 1},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []EventID
			for e, err := range l.Since(ctx, tt.vv) {
				if err != nil {
					t.Fatalf("Since() error = %v", err)
				}
				got = append(got, e.ID)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ids = %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("ids = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestSinceBreaksEarly(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	for i := Seq(1); i <= 5; i++ {
		if err := l.Append(ctx, qty("A", i, int64(i), "SKU-1", 1)); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	n := 0
	for range l.Since(ctx, VersionVector{}) {
		n++
		break
	}
	if n != 1 {
		t.Fatalf("visited %d events, want 1 (yield=false must stop iteration)", n)
	}
}

func TestSinceSurfacesDecodeError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if err := l.Append(ctx, qty("A", 1, 100, "SKU-1", 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := l.db.ExecContext(ctx, `UPDATE events SET payload = ? WHERE node_id = 'A'`, []byte("{bad")); err != nil {
		t.Fatalf("corrupting payload: %v", err)
	}
	var sawErr error
	for _, err := range l.Since(ctx, VersionVector{}) {
		sawErr = err
	}
	if !errors.Is(sawErr, ErrMalformedEvent) {
		t.Fatalf("Since() error = %v, want ErrMalformedEvent", sawErr)
	}
}

func TestEventsForSKU(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	for _, e := range []Event{
		qty("A", 1, 100, "SKU-1", 1),
		qty("A", 2, 200, "SKU-2", 1),
		qty("B", 1, 300, "SKU-1", 1),
	} {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	tests := []struct {
		name  string
		sku   string
		after VersionVector
		want  int
	}{
		{"all for sku", "SKU-1", VersionVector{}, 2},
		{"excludes covered", "SKU-1", VersionVector{"A": 1}, 1},
		{"other sku", "SKU-2", VersionVector{}, 1},
		{"unknown sku", "SKU-9", VersionVector{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := 0
			for _, err := range l.EventsForSKU(ctx, tt.sku, tt.after) {
				if err != nil {
					t.Fatalf("EventsForSKU() error = %v", err)
				}
				n++
			}
			if n != tt.want {
				t.Fatalf("count = %d, want %d", n, tt.want)
			}
		})
	}
}

func TestOpenSQLiteFailsOnBadPath(t *testing.T) {
	if _, err := OpenSQLite(filepath.Join(t.TempDir(), "no-such-dir", "x.db")); err == nil {
		t.Fatal("OpenSQLite() error = nil, want a failure for an unwritable path")
	}
}

func TestCursorRoundTrip(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	got, err := l.Cursor(ctx, "B")
	if err != nil || got != 0 {
		t.Fatalf("Cursor() = %d, %v; want 0, nil for an unknown peer", got, err)
	}
	if err := l.SetCursor(ctx, "B", 7); err != nil {
		t.Fatalf("SetCursor() error = %v", err)
	}
	if err := l.SetCursor(ctx, "B", 9); err != nil {
		t.Fatalf("SetCursor() error = %v", err)
	}
	if got, err = l.Cursor(ctx, "B"); err != nil || got != 9 {
		t.Fatalf("Cursor() = %d, %v; want 9, nil", got, err)
	}
}

func TestClosedLogReturnsErrors(t *testing.T) {
	ctx := context.Background()
	l, err := OpenSQLite(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := l.Append(ctx, qty("A", 1, 100, "SKU-1", 1)); err == nil {
		t.Error("Append() on a closed log error = nil, want an error")
	}
	if _, err := l.AppendLocal(ctx, func(s Seq) Event { return qty("A", s, 100, "SKU-1", 1) }); err == nil {
		t.Error("AppendLocal() on a closed log error = nil, want an error")
	}
	if _, err := l.VersionVector(ctx); err == nil {
		t.Error("VersionVector() on a closed log error = nil, want an error")
	}
	if _, err := l.Cursor(ctx, "B"); err == nil {
		t.Error("Cursor() on a closed log error = nil, want an error")
	}
	if err := l.SetCursor(ctx, "B", 1); err == nil {
		t.Error("SetCursor() on a closed log error = nil, want an error")
	}
	var sawErr error
	for _, err := range l.Since(ctx, VersionVector{}) {
		sawErr = err
	}
	if sawErr == nil {
		t.Error("Since() on a closed log yielded no error, want one")
	}
}

// SQLiteLog must satisfy Log.
var _ Log = (*SQLiteLog)(nil)
```

Delete the unused `collect` helper if `golangci-lint` flags it — it is only kept here if you find it useful.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./eventlog/ -v`
Expected: FAIL — build error, `undefined: OpenSQLite`.

- [ ] **Step 4: Write the `Log` interface and shared schema**

`eventlog-lab/eventlog/log.go`:

```go
package eventlog

import (
	"context"
	"errors"
	"iter"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// ErrNoSnapshot means no snapshot exists for a SKU yet; the caller folds from
// the start of the log.
var ErrNoSnapshot = errors.New("no snapshot")

// Log is a node's append-only source of truth. Two implementations exist and
// share this interface so central (Postgres) is a merge participant running the
// same code path as a node (SQLite), never an authority.
type Log interface {
	// Append stores a remote or replayed event. It is idempotent on
	// (NodeID, Seq): re-appending an event already held is a no-op. This is
	// the whole duplicate-delivery defense.
	Append(ctx context.Context, e Event) error

	// AppendLocal allocates the next local Seq and inserts the event mint
	// produces in a single transaction, so a crash can never leave a gap.
	AppendLocal(ctx context.Context, mint func(Seq) Event) (Event, error)

	// Since streams every held event not covered by vv, in HLC order.
	Since(ctx context.Context, vv VersionVector) iter.Seq2[Event, error]

	// VersionVector reports the highest Seq held per node.
	VersionVector(ctx context.Context) (VersionVector, error)

	// LoadSnapshot returns the serialized state for sku and the version
	// vector it folds in, or ErrNoSnapshot.
	LoadSnapshot(ctx context.Context, sku string) ([]byte, VersionVector, error)

	// SaveSnapshot replaces the snapshot for sku.
	SaveSnapshot(ctx context.Context, sku string, state []byte, covers VersionVector) error

	// Cursor returns the highest Seq of peer's events we have applied, 0 if
	// we have never synced with peer. Persisting it makes sync resumable.
	Cursor(ctx context.Context, peer clock.NodeID) (Seq, error)

	// SetCursor records progress against peer.
	SetCursor(ctx context.Context, peer clock.NodeID, last Seq) error

	// Compact deletes events dominated by upTo and covered by a local
	// snapshot. Never call it with a vector a peer has not acked.
	Compact(ctx context.Context, upTo VersionVector) error

	Close() error
}

// schema is applied verbatim by both backends. Kept in one place so the two
// implementations cannot drift.
const schema = `
CREATE TABLE IF NOT EXISTS events (
  node_id     TEXT    NOT NULL,
  seq         INTEGER NOT NULL,
  hlc_wall    INTEGER NOT NULL,
  hlc_logical INTEGER NOT NULL,
  sku         TEXT    NOT NULL,
  kind        INTEGER NOT NULL,
  payload     BLOB    NOT NULL,
  PRIMARY KEY (node_id, seq)
);
CREATE INDEX IF NOT EXISTS events_by_hlc ON events (hlc_wall, hlc_logical, node_id);
CREATE INDEX IF NOT EXISTS events_by_sku ON events (sku);

CREATE TABLE IF NOT EXISTS snapshots (
  sku        TEXT    NOT NULL PRIMARY KEY,
  state      BLOB    NOT NULL,
  covers     BLOB    NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_cursors (
  peer_node_id TEXT NOT NULL PRIMARY KEY,
  last_seq     INTEGER NOT NULL
);
`

const selectEvents = `SELECT node_id, seq, hlc_wall, hlc_logical, sku, kind, payload FROM events`
```

- [ ] **Step 5: Write the SQLite implementation**

`eventlog-lab/eventlog/sqlite.go`:

```go
package eventlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	_ "modernc.org/sqlite" // pure-Go driver, no cgo

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// SQLiteLog is the per-node local log.
type SQLiteLog struct {
	dsn string
	db  *sql.DB
}

// OpenSQLite opens (creating if needed) a log at dsn and applies the schema.
// dsn may be ":memory:" for a throwaway log.
func OpenSQLite(dsn string) (*SQLiteLog, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", dsn, err)
	}
	// One writer at a time: SQLite serialises writes anyway, and a single
	// connection keeps ":memory:" from becoming several distinct databases.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema to %q: %w", dsn, err)
	}
	return &SQLiteLog{dsn: dsn, db: db}, nil
}

// DSN reports where this log is stored, so a crash fault can reopen it.
func (l *SQLiteLog) DSN() string { return l.dsn }

// Close releases the database handle.
func (l *SQLiteLog) Close() error {
	if err := l.db.Close(); err != nil {
		return fmt.Errorf("close sqlite %q: %w", l.dsn, err)
	}
	return nil
}

const insertEvent = `INSERT INTO events
  (node_id, seq, hlc_wall, hlc_logical, sku, kind, payload)
  VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT (node_id, seq) DO NOTHING`

// Append is idempotent on (NodeID, Seq).
func (l *SQLiteLog) Append(ctx context.Context, e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	payload, err := e.MarshalPayload()
	if err != nil {
		return err
	}
	if _, err := l.db.ExecContext(ctx, insertEvent,
		string(e.ID.NodeID), int64(e.ID.Seq), e.HLC.Wall, int64(e.HLC.Logical),
		e.SKU, int(e.Kind), payload); err != nil {
		return fmt.Errorf("append %v: %w", e.ID, err)
	}
	return nil
}

// AppendLocal allocates the next Seq for the node mint stamps and inserts in
// the same transaction, so no crash can leave a sequence gap.
func (l *SQLiteLog) AppendLocal(ctx context.Context, mint func(Seq) Event) (Event, error) {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, fmt.Errorf("begin append-local tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	probe := mint(0)
	var next int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE node_id = ?`,
		string(probe.ID.NodeID)).Scan(&next); err != nil {
		return Event{}, fmt.Errorf("allocate seq for %q: %w", probe.ID.NodeID, err)
	}

	e := mint(Seq(next))
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	payload, err := e.MarshalPayload()
	if err != nil {
		return Event{}, err
	}
	if _, err := tx.ExecContext(ctx, insertEvent,
		string(e.ID.NodeID), int64(e.ID.Seq), e.HLC.Wall, int64(e.HLC.Logical),
		e.SKU, int(e.Kind), payload); err != nil {
		return Event{}, fmt.Errorf("insert %v: %w", e.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return Event{}, fmt.Errorf("commit %v: %w", e.ID, err)
	}
	return e, nil
}

// Since streams events not covered by vv, ordered by HLC. Ordering is a
// readability convenience: crdt.Apply is order-independent by construction.
func (l *SQLiteLog) Since(ctx context.Context, vv VersionVector) iter.Seq2[Event, error] {
	return l.stream(ctx, vv, selectEvents+` ORDER BY hlc_wall, hlc_logical, node_id`)
}

// EventsForSKU streams the events for one SKU not covered by after. This is
// the projection read path.
func (l *SQLiteLog) EventsForSKU(ctx context.Context, sku string, after VersionVector) iter.Seq2[Event, error] {
	return l.stream(ctx, after, selectEvents+` WHERE sku = ? ORDER BY hlc_wall, hlc_logical, node_id`, sku)
}

// stream runs query and yields every decoded event not covered by skip.
// Filtering in Go rather than SQL keeps one code path for both queries; the
// harness never grows a log large enough for that to matter.
func (l *SQLiteLog) stream(ctx context.Context, skip VersionVector, query string, args ...any) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		rows, err := l.db.QueryContext(ctx, query, args...)
		if err != nil {
			yield(Event{}, fmt.Errorf("query events: %w", err))
			return
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			e, err := scanEvent(rows)
			if err != nil {
				yield(Event{}, err)
				return
			}
			if skip.Contains(e.ID) {
				continue
			}
			if !yield(e, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(Event{}, fmt.Errorf("iterate events: %w", err))
		}
	}
}

// scanner is the subset of *sql.Row / *sql.Rows scanEvent needs, so the same
// decode path serves both backends.
type scanner interface{ Scan(dest ...any) error }

func scanEvent(s scanner) (Event, error) {
	var (
		e       Event
		node    string
		seq     int64
		logical int64
		kind    int
		payload []byte
	)
	if err := s.Scan(&node, &seq, &e.HLC.Wall, &logical, &e.SKU, &kind, &payload); err != nil {
		return Event{}, fmt.Errorf("scan event row: %w", err)
	}
	e.ID = EventID{NodeID: clock.NodeID(node), Seq: Seq(seq)}
	e.HLC.Logical = uint32(logical)
	e.HLC.NodeID = e.ID.NodeID
	e.Kind = Kind(kind)
	if err := e.UnmarshalPayload(payload); err != nil {
		return Event{}, err
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}

// VersionVector reports the highest Seq held per node.
func (l *SQLiteLog) VersionVector(ctx context.Context) (VersionVector, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT node_id, MAX(seq) FROM events GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("query version vector: %w", err)
	}
	defer func() { _ = rows.Close() }()
	vv := VersionVector{}
	for rows.Next() {
		var (
			node string
			max  int64
		)
		if err := rows.Scan(&node, &max); err != nil {
			return nil, fmt.Errorf("scan version vector row: %w", err)
		}
		vv[clock.NodeID(node)] = Seq(max)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate version vector: %w", err)
	}
	return vv, nil
}

// Cursor returns the highest Seq of peer's events we have applied.
func (l *SQLiteLog) Cursor(ctx context.Context, peer clock.NodeID) (Seq, error) {
	var last int64
	err := l.db.QueryRowContext(ctx,
		`SELECT last_seq FROM sync_cursors WHERE peer_node_id = ?`, string(peer)).Scan(&last)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read cursor for %q: %w", peer, err)
	}
	return Seq(last), nil
}

// SetCursor records progress against peer. It never moves a cursor backwards:
// a peer that reports a regressed version vector must not shrink our record.
func (l *SQLiteLog) SetCursor(ctx context.Context, peer clock.NodeID, last Seq) error {
	if _, err := l.db.ExecContext(ctx,
		`INSERT INTO sync_cursors (peer_node_id, last_seq) VALUES (?, ?)
		 ON CONFLICT (peer_node_id) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)`,
		string(peer), int64(last)); err != nil {
		return fmt.Errorf("set cursor for %q: %w", peer, err)
	}
	return nil
}

// LoadSnapshot returns the stored state for sku, or ErrNoSnapshot.
func (l *SQLiteLog) LoadSnapshot(ctx context.Context, sku string) ([]byte, VersionVector, error) {
	var state, covers []byte
	err := l.db.QueryRowContext(ctx,
		`SELECT state, covers FROM snapshots WHERE sku = ?`, sku).Scan(&state, &covers)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil, fmt.Errorf("%w for %q", ErrNoSnapshot, sku)
	case err != nil:
		return nil, nil, fmt.Errorf("load snapshot %q: %w", sku, err)
	}
	vv := VersionVector{}
	if err := json.Unmarshal(covers, &vv); err != nil {
		return nil, nil, fmt.Errorf("decode snapshot covers for %q: %w", sku, err)
	}
	return state, vv, nil
}

// SaveSnapshot replaces the snapshot for sku.
func (l *SQLiteLog) SaveSnapshot(ctx context.Context, sku string, state []byte, covers VersionVector) error {
	blob, err := json.Marshal(covers)
	if err != nil {
		return fmt.Errorf("encode snapshot covers for %q: %w", sku, err)
	}
	if _, err := l.db.ExecContext(ctx,
		`INSERT INTO snapshots (sku, state, covers) VALUES (?, ?, ?)
		 ON CONFLICT (sku) DO UPDATE SET state = excluded.state, covers = excluded.covers`,
		sku, state, blob); err != nil {
		return fmt.Errorf("save snapshot %q: %w", sku, err)
	}
	return nil
}
```

`Compact` is deliberately absent here — Task 6 adds it together with its safety tests. Until then `SQLiteLog` does not satisfy `Log`, so **comment out** the `var _ Log = (*SQLiteLog)(nil)` assertion at the bottom of `sqlite_test.go` with the note `// uncommented in Task 6, once Compact exists` and uncomment it there.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd eventlog-lab && go test ./eventlog/ -v`
Expected: PASS. If `TestSinceSurfacesDecodeError` fails with a nil error, check that `scanEvent` calls `UnmarshalPayload` *and* propagates its error.

- [ ] **Step 7: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover
golangci-lint run
```
Expected: 100.0% for `clock`; `eventlog` will be short only of `Compact`, which does not exist yet. Every other branch must be covered — check with `go test ./eventlog/ -coverprofile=/tmp/c.out && go tool cover -func=/tmp/c.out` and add table rows for anything at less than 100%.

- [ ] **Step 8: Commit**

```bash
cd eventlog-lab
git add go.mod go.sum eventlog/
git commit -m "feat(eventlog): SQLite log with idempotent append and version vectors"
```

---

### Task 5: CRDT merge semantics — `LWW[T]`, `ItemState`, `Apply`

**Files:**
- Create: `eventlog-lab/crdt/lww.go`
- Create: `eventlog-lab/crdt/item.go`
- Test: `eventlog-lab/crdt/lww_test.go`
- Test: `eventlog-lab/crdt/item_test.go`

**Interfaces:**
- Consumes: `clock.HLC`, `clock.NodeID`, `eventlog.Event`, `eventlog.Kind` constants from Tasks 1-4.
- Produces:
  - `type LWW[T any] struct { Value T; At clock.HLC }`
  - `func (r *LWW[T]) Set(v T, at clock.HLC) bool` — reports whether the write was accepted.
  - `type ItemState struct { Pos, Neg map[clock.NodeID]int64; Name LWW[string]; ReorderPoint LWW[int64]; Deleted LWW[bool] }`
  - `func NewItemState() *ItemState`
  - `func (s *ItemState) Apply(e eventlog.Event)`
  - `func (s ItemState) Quantity() int64`
  - `func (s *ItemState) Fold(events iter.Seq2[eventlog.Event, error]) error`
  - `func Marshal(s *ItemState) ([]byte, error)` / `func Unmarshal(b []byte) (*ItemState, error)`
  - `func (s *ItemState) Equal(other *ItemState) bool`

**Domain notes for the implementer — read before writing code:**

`Apply` must be **commutative** (order does not matter), **associative**, and **idempotent when driven through the log** — and the last qualifier is the subtle one that the spec calls out explicitly.

For `KindQuantityDelta`, `Apply` adds `|Delta|` into `Pos[e.ID.NodeID]` or `Neg[e.ID.NodeID]` depending on the sign. A naive `+=` is **not idempotent**: applying the same event twice double-counts. The resolution is architectural, not algorithmic — `Apply` is only ever driven by a projection over a **deduplicated** event set, and the deduplication comes from `eventlog.Append`'s primary key on `(NodeID, Seq)`. **Idempotence is a property of the log, not of `Apply`.** State this in a code comment on `Apply`, and prove it with the test in Step 1 that feeds a duplicate through both the direct path (double-counts — wrong) and the log path (absorbed — right).

For LWW fields, `Set` accepts only if `existing.At.Before(new.At)`. Because HLC ordering is total (Task 1), two distinct events can never tie, so every replica reaches the same verdict independently and no tiebreak policy is needed.

`Deleted` is an LWW-register, not an add-wins set: an undelete with a later HLC genuinely resurrects the item. That is a deliberate spec choice — last writer decides.

- [ ] **Step 1: Write the failing tests**

`eventlog-lab/crdt/lww_test.go`:

```go
package crdt

import (
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

func TestLWWSet(t *testing.T) {
	tests := []struct {
		name       string
		startAt    clock.HLC
		startValue string
		writeAt    clock.HLC
		writeValue string
		wantOK     bool
		wantValue  string
	}{
		{"first write always wins", clock.HLC{}, "", clock.HLC{Wall: 1, NodeID: "A"}, "new", true, "new"},
		{"later wall wins", clock.HLC{Wall: 1, NodeID: "A"}, "old", clock.HLC{Wall: 2, NodeID: "A"}, "new", true, "new"},
		{"earlier wall loses", clock.HLC{Wall: 2, NodeID: "A"}, "old", clock.HLC{Wall: 1, NodeID: "A"}, "new", false, "old"},
		{"same stamp loses (idempotent replay)", clock.HLC{Wall: 1, NodeID: "A"}, "old", clock.HLC{Wall: 1, NodeID: "A"}, "new", false, "old"},
		{"logical breaks wall tie", clock.HLC{Wall: 1, NodeID: "A"}, "old", clock.HLC{Wall: 1, Logical: 1, NodeID: "A"}, "new", true, "new"},
		{"node id breaks full tie, higher wins", clock.HLC{Wall: 1, NodeID: "A"}, "old", clock.HLC{Wall: 1, NodeID: "B"}, "new", true, "new"},
		{"node id breaks full tie, lower loses", clock.HLC{Wall: 1, NodeID: "B"}, "old", clock.HLC{Wall: 1, NodeID: "A"}, "new", false, "old"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := LWW[string]{Value: tt.startValue, At: tt.startAt}
			if got := r.Set(tt.writeValue, tt.writeAt); got != tt.wantOK {
				t.Fatalf("Set() = %v, want %v", got, tt.wantOK)
			}
			if r.Value != tt.wantValue {
				t.Fatalf("Value = %q, want %q", r.Value, tt.wantValue)
			}
		})
	}
}
```

`eventlog-lab/crdt/item_test.go`:

```go
package crdt

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

func ptr[T any](v T) *T { return &v }

func qty(node clock.NodeID, seq eventlog.Seq, wall int64, delta int64) eventlog.Event {
	return eventlog.Event{
		ID:    eventlog.EventID{NodeID: node, Seq: seq},
		HLC:   clock.HLC{Wall: wall, NodeID: node},
		SKU:   "SKU-1",
		Kind:  eventlog.KindQuantityDelta,
		Delta: delta,
	}
}

func meta(node clock.NodeID, seq eventlog.Seq, wall int64, m *eventlog.MetaSet) eventlog.Event {
	return eventlog.Event{
		ID:   eventlog.EventID{NodeID: node, Seq: seq},
		HLC:  clock.HLC{Wall: wall, NodeID: node},
		SKU:  "SKU-1",
		Kind: eventlog.KindMetaSet,
		Meta: m,
	}
}

func del(node clock.NodeID, seq eventlog.Seq, wall int64, to bool) eventlog.Event {
	return eventlog.Event{
		ID:        eventlog.EventID{NodeID: node, Seq: seq},
		HLC:       clock.HLC{Wall: wall, NodeID: node},
		SKU:       "SKU-1",
		Kind:      eventlog.KindDeleteSet,
		DeletedTo: ptr(to),
	}
}

func applyAll(events []eventlog.Event) *ItemState {
	s := NewItemState()
	for _, e := range events {
		s.Apply(e)
	}
	return s
}

func TestApplyQuantity(t *testing.T) {
	tests := []struct {
		name     string
		events   []eventlog.Event
		wantQty  int64
		wantPos  map[clock.NodeID]int64
		wantNeg  map[clock.NodeID]int64
	}{
		{"single increment", []eventlog.Event{qty("A", 1, 1, 10)}, 10,
			map[clock.NodeID]int64{"A": 10}, map[clock.NodeID]int64{}},
		{"single decrement", []eventlog.Event{qty("A", 1, 1, -4)}, -4,
			map[clock.NodeID]int64{}, map[clock.NodeID]int64{"A": 4}},
		{"per-node halves accumulate", []eventlog.Event{
			qty("A", 1, 1, 10), qty("A", 2, 2, -3), qty("B", 1, 3, -8),
		}, -1, map[clock.NodeID]int64{"A": 10}, map[clock.NodeID]int64{"A": 3, "B": 8}},
		{"concurrent overdraw goes negative and that is correct", []eventlog.Event{
			qty("C", 1, 1, 10), qty("A", 1, 2, -8), qty("B", 1, 2, -8),
		}, -6, map[clock.NodeID]int64{"C": 10}, map[clock.NodeID]int64{"A": 8, "B": 8}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := applyAll(tt.events)
			if got := s.Quantity(); got != tt.wantQty {
				t.Fatalf("Quantity() = %d, want %d", got, tt.wantQty)
			}
			assertCounter(t, "Pos", s.Pos, tt.wantPos)
			assertCounter(t, "Neg", s.Neg, tt.wantNeg)
		})
	}
}

func assertCounter(t *testing.T, name string, got, want map[clock.NodeID]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s[%q] = %d, want %d", name, k, got[k], v)
		}
	}
}

func TestApplyMetaAndDelete(t *testing.T) {
	tests := []struct {
		name             string
		events           []eventlog.Event
		wantName         string
		wantReorderPoint int64
		wantDeleted      bool
	}{
		{"meta sets both", []eventlog.Event{
			meta("A", 1, 1, &eventlog.MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(5))}),
		}, "Widget", 5, false},
		{"nil field does not clobber", []eventlog.Event{
			meta("A", 1, 1, &eventlog.MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(5))}),
			meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("Gadget")}),
		}, "Gadget", 5, false},
		{"later wall wins regardless of arrival order", []eventlog.Event{
			meta("B", 1, 9, &eventlog.MetaSet{Name: ptr("Late")}),
			meta("A", 1, 1, &eventlog.MetaSet{Name: ptr("Early")}),
		}, "Late", 0, false},
		{"delete sets tombstone", []eventlog.Event{del("A", 1, 1, true)}, "", 0, true},
		{"later undelete resurrects (LWW, not add-wins)", []eventlog.Event{
			del("A", 1, 1, true), del("B", 1, 2, false),
		}, "", 0, false},
		{"earlier undelete loses", []eventlog.Event{
			del("A", 1, 2, true), del("B", 1, 1, false),
		}, "", 0, true},
		{"nil meta payload is ignored, not a panic", []eventlog.Event{
			{ID: eventlog.EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{Wall: 1, NodeID: "A"}, SKU: "SKU-1", Kind: eventlog.KindMetaSet},
		}, "", 0, false},
		{"nil delete payload is ignored, not a panic", []eventlog.Event{
			{ID: eventlog.EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{Wall: 1, NodeID: "A"}, SKU: "SKU-1", Kind: eventlog.KindDeleteSet},
		}, "", 0, false},
		{"unknown kind is ignored, not a panic", []eventlog.Event{
			{ID: eventlog.EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{Wall: 1, NodeID: "A"}, SKU: "SKU-1", Kind: 99},
		}, "", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := applyAll(tt.events)
			if s.Name.Value != tt.wantName {
				t.Errorf("Name = %q, want %q", s.Name.Value, tt.wantName)
			}
			if s.ReorderPoint.Value != tt.wantReorderPoint {
				t.Errorf("ReorderPoint = %d, want %d", s.ReorderPoint.Value, tt.wantReorderPoint)
			}
			if s.Deleted.Value != tt.wantDeleted {
				t.Errorf("Deleted = %v, want %v", s.Deleted.Value, tt.wantDeleted)
			}
		})
	}
}

// TestApplyIsNotIdempotentButTheLogPathIs is the test the spec demands: it
// shows the direct path double-counts a duplicate, and the log path does not,
// because idempotence comes from Append's primary key, never from Apply.
func TestApplyIsNotIdempotentButTheLogPathIs(t *testing.T) {
	ctx := context.Background()
	e := qty("A", 1, 100, 7)

	direct := NewItemState()
	direct.Apply(e)
	direct.Apply(e)
	if direct.Quantity() != 14 {
		t.Fatalf("direct double-apply Quantity() = %d, want 14 -- Apply is documented as NOT idempotent", direct.Quantity())
	}

	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "dup.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	for i := 0; i < 2; i++ {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	viaLog := NewItemState()
	if err := viaLog.Fold(l.EventsForSKU(ctx, "SKU-1", eventlog.VersionVector{})); err != nil {
		t.Fatalf("Fold() error = %v", err)
	}
	if viaLog.Quantity() != 7 {
		t.Fatalf("log-path Quantity() = %d after duplicate delivery, want 7", viaLog.Quantity())
	}
}

func TestFoldSurfacesIterationError(t *testing.T) {
	wantErr := context.Canceled
	s := NewItemState()
	err := s.Fold(func(yield func(eventlog.Event, error) bool) {
		yield(eventlog.Event{}, wantErr)
	})
	if err == nil {
		t.Fatal("Fold() error = nil, want the iteration error")
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	s := applyAll([]eventlog.Event{
		qty("A", 1, 1, 10),
		qty("B", 1, 2, -3),
		meta("A", 2, 3, &eventlog.MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(4))}),
		del("A", 3, 4, true),
	})
	b, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !got.Equal(s) {
		t.Fatalf("round trip = %+v, want %+v", got, s)
	}
	if _, err := Unmarshal([]byte("{bad")); err == nil {
		t.Fatal("Unmarshal(garbage) error = nil, want an error")
	}
}

func TestEqual(t *testing.T) {
	base := func() *ItemState {
		return applyAll([]eventlog.Event{qty("A", 1, 1, 5), meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("W")})})
	}
	tests := []struct {
		name  string
		other *ItemState
		want  bool
	}{
		{"identical", base(), true},
		{"different quantity", applyAll([]eventlog.Event{qty("A", 1, 1, 6), meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("W")})}), false},
		{"different node in counter", applyAll([]eventlog.Event{qty("B", 1, 1, 5), meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("W")})}), false},
		{"different name", applyAll([]eventlog.Event{qty("A", 1, 1, 5), meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("X")})}), false},
		{"empty", NewItemState(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := base().Equal(tt.other); got != tt.want {
				t.Fatalf("Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./crdt/ -v`
Expected: FAIL — build error, `undefined: LWW`, `undefined: NewItemState`.

- [ ] **Step 3: Write `LWW[T]`**

`eventlog-lab/crdt/lww.go`:

```go
// Package crdt holds the merge semantics for a StockItem. Every replica --
// including the central store -- runs exactly this code, so there is only one
// definition of "merged".
package crdt

import "github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"

// LWW is a last-writer-wins register. Because clock.HLC has a strict total
// order, two distinct writes can never tie, so every replica independently
// reaches the same verdict and no tiebreak policy is needed.
type LWW[T any] struct {
	Value T         `json:"value"`
	At    clock.HLC `json:"at"`
}

// Set accepts v only if at strictly follows the stored stamp. It reports
// whether the write was accepted. Re-delivering the same write is a no-op,
// which is what makes LWW fields safe under replay.
func (r *LWW[T]) Set(v T, at clock.HLC) bool {
	if !r.At.Before(at) {
		return false
	}
	r.Value, r.At = v, at
	return true
}
```

- [ ] **Step 4: Write `ItemState`**

`eventlog-lab/crdt/item.go`:

```go
package crdt

import (
	"encoding/json"
	"fmt"
	"iter"
	"maps"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

// ItemState is the merged state of one StockItem.
//
// Quantity is a PN-counter: two maps keyed by node, holding everything that
// node ever added (Pos) and subtracted (Neg). Each node writes only its own
// slot and the value is a sum, so the result is independent of the order events
// arrive in. The metadata fields are LWW-registers.
type ItemState struct {
	Pos          map[clock.NodeID]int64 `json:"pos"`
	Neg          map[clock.NodeID]int64 `json:"neg"`
	Name         LWW[string]            `json:"name"`
	ReorderPoint LWW[int64]             `json:"reorder_point"`
	Deleted      LWW[bool]              `json:"deleted"`
}

// NewItemState returns the identity state: quantity zero, no metadata.
func NewItemState() *ItemState {
	return &ItemState{Pos: map[clock.NodeID]int64{}, Neg: map[clock.NodeID]int64{}}
}

// Apply folds one event into s. It is commutative and associative, so any
// order of the same event set yields the same state.
//
// Apply is deliberately NOT idempotent: applying a KindQuantityDelta event
// twice double-counts it. Idempotence is provided by eventlog.Append's primary
// key on (NodeID, Seq), which makes duplicate delivery a storage no-op. Never
// drive Apply from anything but a projection over the log; driving it straight
// from a network frame reintroduces double-counting.
//
// Unapplicable events (unknown kind, missing payload) are ignored rather than
// panicking, but they can never reach here in practice: eventlog.Event.Validate
// rejects them before storage.
func (s *ItemState) Apply(e eventlog.Event) {
	switch e.Kind {
	case eventlog.KindQuantityDelta:
		if e.Delta >= 0 {
			s.Pos[e.ID.NodeID] += e.Delta
		} else {
			s.Neg[e.ID.NodeID] += -e.Delta
		}
	case eventlog.KindMetaSet:
		if e.Meta == nil {
			return
		}
		if e.Meta.Name != nil {
			s.Name.Set(*e.Meta.Name, e.HLC)
		}
		if e.Meta.ReorderPoint != nil {
			s.ReorderPoint.Set(*e.Meta.ReorderPoint, e.HLC)
		}
	case eventlog.KindDeleteSet:
		if e.DeletedTo != nil {
			s.Deleted.Set(*e.DeletedTo, e.HLC)
		}
	}
}

// Fold applies every event the iterator yields, stopping at the first error.
func (s *ItemState) Fold(events iter.Seq2[eventlog.Event, error]) error {
	for e, err := range events {
		if err != nil {
			return fmt.Errorf("fold events: %w", err)
		}
		s.Apply(e)
	}
	return nil
}

// Quantity is sum(Pos) - sum(Neg). It may legitimately be negative when two
// nodes overdraw concurrently while partitioned; see README.
func (s ItemState) Quantity() int64 {
	var total int64
	for _, v := range s.Pos {
		total += v
	}
	for _, v := range s.Neg {
		total -= v
	}
	return total
}

// Equal reports whether two states are identical. This is the convergence
// predicate the harness asserts with.
func (s *ItemState) Equal(other *ItemState) bool {
	if other == nil {
		return false
	}
	return maps.Equal(s.Pos, other.Pos) &&
		maps.Equal(s.Neg, other.Neg) &&
		s.Name == other.Name &&
		s.ReorderPoint == other.ReorderPoint &&
		s.Deleted == other.Deleted
}

// Marshal serialises s for a snapshot row, per-node counters included.
func Marshal(s *ItemState) ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("marshal item state: %w", err)
	}
	return b, nil
}

// Unmarshal restores a snapshotted state, normalising nil maps so Apply can
// write into them.
func Unmarshal(b []byte) (*ItemState, error) {
	s := NewItemState()
	if err := json.Unmarshal(b, s); err != nil {
		return nil, fmt.Errorf("unmarshal item state: %w", err)
	}
	if s.Pos == nil {
		s.Pos = map[clock.NodeID]int64{}
	}
	if s.Neg == nil {
		s.Neg = map[clock.NodeID]int64{}
	}
	return s, nil
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd eventlog-lab && go test ./crdt/ -v`
Expected: PASS. `TestApplyIsNotIdempotentButTheLogPathIs` passing in both halves is the important one — it is the executable form of the spec's warning.

- [ ] **Step 6: Add the exhaustive commutativity test**

Append to `eventlog-lab/crdt/item_test.go`. This is the spec's "targeted, not left to random search" commutativity check: exhaustive permutations for small sets, sampled above that.

```go
// permute calls fn with every permutation of events when the count is small
// enough to enumerate (<= 6! = 720), otherwise with `samples` random shuffles.
func permute(t *testing.T, events []eventlog.Event, samples int, fn func([]eventlog.Event)) {
	t.Helper()
	if len(events) <= 6 {
		buf := make([]eventlog.Event, len(events))
		copy(buf, events)
		var rec func(k int)
		rec = func(k int) {
			if k == len(buf) {
				out := make([]eventlog.Event, len(buf))
				copy(out, buf)
				fn(out)
				return
			}
			for i := k; i < len(buf); i++ {
				buf[k], buf[i] = buf[i], buf[k]
				rec(k + 1)
				buf[k], buf[i] = buf[i], buf[k]
			}
		}
		rec(0)
		return
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < samples; i++ {
		out := make([]eventlog.Event, len(events))
		copy(out, events)
		rng.Shuffle(len(out), func(a, b int) { out[a], out[b] = out[b], out[a] })
		fn(out)
	}
}

func TestApplyIsCommutative(t *testing.T) {
	tests := []struct {
		name   string
		events []eventlog.Event
	}{
		{"small mixed set, exhaustive", []eventlog.Event{
			qty("A", 1, 10, 10),
			qty("B", 1, 20, -3),
			meta("A", 2, 30, &eventlog.MetaSet{Name: ptr("Widget")}),
			del("B", 2, 40, true),
		}},
		{"conflicting lww writes, exhaustive", []eventlog.Event{
			meta("A", 1, 10, &eventlog.MetaSet{Name: ptr("A1"), ReorderPoint: ptr(int64(1))}),
			meta("B", 1, 10, &eventlog.MetaSet{Name: ptr("B1"), ReorderPoint: ptr(int64(2))}),
			meta("A", 2, 11, &eventlog.MetaSet{Name: ptr("A2")}),
			del("A", 3, 12, true),
			del("B", 2, 12, false),
		}},
		{"large set, sampled", func() []eventlog.Event {
			var out []eventlog.Event
			for i := 1; i <= 12; i++ {
				node := clock.NodeID("A")
				if i%3 == 0 {
					node = "B"
				}
				out = append(out, qty(node, eventlog.Seq(i), int64(i), int64(i%5)-2+1))
			}
			out = append(out, meta("A", 20, 50, &eventlog.MetaSet{Name: ptr("Z")}))
			return out
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := applyAll(tt.events)
			n := 0
			permute(t, tt.events, 200, func(perm []eventlog.Event) {
				n++
				if got := applyAll(perm); !got.Equal(want) {
					t.Fatalf("permutation %d diverged:\n got %+v\nwant %+v\nperm %v", n, got, want, ids(perm))
				}
			})
			if n < 2 {
				t.Fatalf("only %d permutations exercised", n)
			}
		})
	}
}

func ids(events []eventlog.Event) []eventlog.EventID {
	out := make([]eventlog.EventID, len(events))
	for i, e := range events {
		out[i] = e.ID
	}
	return out
}

func TestApplyIsAssociative(t *testing.T) {
	events := []eventlog.Event{
		qty("A", 1, 10, 10),
		qty("B", 1, 20, -3),
		meta("A", 2, 30, &eventlog.MetaSet{Name: ptr("Widget")}),
		qty("C", 1, 40, 5),
	}
	whole := applyAll(events)
	for split := 0; split <= len(events); split++ {
		t.Run(fmt.Sprintf("split at %d", split), func(t *testing.T) {
			left := applyAll(events[:split])
			for _, e := range events[split:] {
				left.Apply(e)
			}
			if !left.Equal(whole) {
				t.Fatalf("split %d = %+v, want %+v", split, left, whole)
			}
		})
	}
}
```

Add `"fmt"` and `"math/rand/v2"` to the test imports.

- [ ] **Step 7: Run the new tests**

Run: `cd eventlog-lab && go test ./crdt/ -run 'TestApplyIs' -v`
Expected: PASS. All 24 permutations of the 4-event set and all 120 of the 5-event set converge.

- [ ] **Step 8: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover
golangci-lint run
```
Expected: `crdt` at 100.0%.

- [ ] **Step 9: Commit**

```bash
cd eventlog-lab
git add crdt/
git commit -m "feat(crdt): PN-counter and LWW merge with commutativity proofs"
```

---

### Task 6: Snapshots and compaction

Closes the `Log` interface and adds the read path the projection cache uses.

**Files:**
- Modify: `eventlog-lab/eventlog/sqlite.go` (adds `Compact`)
- Create: `eventlog-lab/eventlog/snapshot.go` (backend-agnostic projection helpers)
- Test: `eventlog-lab/eventlog/snapshot_test.go`
- Modify: `eventlog-lab/eventlog/sqlite_test.go` (uncomment the `var _ Log` assertion)

**Interfaces:**
- Consumes: `Log`, `SQLiteLog`, `ErrNoSnapshot`, `EventsForSKU` from Task 4; `crdt.ItemState`, `crdt.Marshal`, `crdt.Unmarshal`, `crdt.NewItemState` from Task 5.
- Produces:
  - `func (l *SQLiteLog) Compact(ctx context.Context, upTo VersionVector) error`
  - `func (l *SQLiteLog) CountForSKU(ctx context.Context, sku string) (int, error)`
  - `type Projector struct { Log SQLLog; SnapshotEvery int }` where
    `type SQLLog interface { Log; EventsForSKU(ctx context.Context, sku string, after VersionVector) iter.Seq2[Event, error]; CountForSKU(ctx context.Context, sku string) (int, error) }`
  - `func (p *Projector) Project(ctx context.Context, sku string) (*crdt.ItemState, error)`
  - `func (p *Projector) MaybeSnapshot(ctx context.Context, sku string) error`
- Import direction note: `eventlog` imports `crdt` here. `crdt` must **not** import `eventlog`'s storage — it only uses the event types, which live in the same package. Since `crdt` already imports `eventlog`, putting `Projector` in `eventlog` would create a cycle. **Put `snapshot.go` in package `crdt` instead**, file `eventlog-lab/crdt/projector.go`, and depend on `eventlog` one-way. The interface names above are unchanged; only the package is `crdt`.

**Files (corrected):**
- Modify: `eventlog-lab/eventlog/sqlite.go` (adds `Compact`, `CountForSKU`)
- Create: `eventlog-lab/crdt/projector.go`
- Test: `eventlog-lab/eventlog/compact_test.go`
- Test: `eventlog-lab/crdt/projector_test.go`

**Domain notes for the implementer:**
- A projection is `fold(Apply, events for SKU)`. Replaying the whole log on every read is O(history). A **snapshot** stores the serialized `ItemState` plus the **version vector it folds in**; the read path loads the snapshot and applies only the events that vector does not cover. Because `Apply` is commutative, "snapshot plus the remainder" equals "fold everything" exactly.
- `Compact` deletes an event only when **both** conditions hold: it is dominated by `upTo` (every peer has acked holding it — so nobody will ever ask for it again) **and** it is covered by a local snapshot (so our own read path does not need it). Deleting an event a peer has not seen loses data permanently. There is no recovery, so the `AND` is not optional.

- [ ] **Step 1: Write the failing compaction tests**

`eventlog-lab/eventlog/compact_test.go`:

```go
package eventlog

import (
	"context"
	"encoding/json"
	"testing"
)

func TestCompactRequiresBothDominanceAndSnapshotCoverage(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		snapshotFor  string // "" means save no snapshot
		snapshotVV   VersionVector
		upTo         VersionVector
		wantRemainVV VersionVector
	}{
		{
			name:         "no snapshot deletes nothing",
			upTo:         VersionVector{"A": 2, "B": 1},
			wantRemainVV: VersionVector{"A": 2, "B": 1},
		},
		{
			name:         "snapshot covers all and peers acked all deletes all",
			snapshotFor:  "SKU-1",
			snapshotVV:   VersionVector{"A": 2, "B": 1},
			upTo:         VersionVector{"A": 2, "B": 1},
			wantRemainVV: VersionVector{},
		},
		{
			name:         "peer behind keeps its uncovered events",
			snapshotFor:  "SKU-1",
			snapshotVV:   VersionVector{"A": 2, "B": 1},
			upTo:         VersionVector{"A": 1, "B": 1},
			wantRemainVV: VersionVector{"A": 2},
		},
		{
			name:         "snapshot behind keeps events it does not fold in",
			snapshotFor:  "SKU-1",
			snapshotVV:   VersionVector{"A": 1},
			upTo:         VersionVector{"A": 2, "B": 1},
			wantRemainVV: VersionVector{"A": 2, "B": 1},
		},
		{
			name:         "empty upTo deletes nothing",
			snapshotFor:  "SKU-1",
			snapshotVV:   VersionVector{"A": 2, "B": 1},
			upTo:         VersionVector{},
			wantRemainVV: VersionVector{"A": 2, "B": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newTestLog(t)
			for _, e := range []Event{
				qty("A", 1, 100, "SKU-1", 5),
				qty("A", 2, 200, "SKU-1", -1),
				qty("B", 1, 300, "SKU-1", 2),
			} {
				if err := l.Append(ctx, e); err != nil {
					t.Fatalf("Append() error = %v", err)
				}
			}
			if tt.snapshotFor != "" {
				if err := l.SaveSnapshot(ctx, tt.snapshotFor, []byte(`{}`), tt.snapshotVV); err != nil {
					t.Fatalf("SaveSnapshot() error = %v", err)
				}
			}
			if err := l.Compact(ctx, tt.upTo); err != nil {
				t.Fatalf("Compact() error = %v", err)
			}
			got, err := l.VersionVector(ctx)
			if err != nil {
				t.Fatalf("VersionVector() error = %v", err)
			}
			if len(got) != len(tt.wantRemainVV) {
				t.Fatalf("remaining vv = %v, want %v", got, tt.wantRemainVV)
			}
			for k, v := range tt.wantRemainVV {
				if got[k] != v {
					t.Fatalf("remaining vv = %v, want %v", got, tt.wantRemainVV)
				}
			}
		})
	}
}

func TestCompactOnlyConsidersSnapshottedSKUs(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	for _, e := range []Event{
		qty("A", 1, 100, "SKU-1", 5),
		qty("A", 2, 200, "SKU-2", 5),
	} {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	// Only SKU-1 has a snapshot, so SKU-2's event must survive even though the
	// peer vector dominates it.
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{}`), VersionVector{"A": 2}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if err := l.Compact(ctx, VersionVector{"A": 2}); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	n, err := l.CountForSKU(ctx, "SKU-2")
	if err != nil {
		t.Fatalf("CountForSKU() error = %v", err)
	}
	if n != 1 {
		t.Fatalf("SKU-2 events after compaction = %d, want 1", n)
	}
	if n, err = l.CountForSKU(ctx, "SKU-1"); err != nil || n != 0 {
		t.Fatalf("SKU-1 events after compaction = %d, %v; want 0, nil", n, err)
	}
}

func TestCompactRejectsCorruptSnapshotCovers(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if _, err := l.db.ExecContext(ctx,
		`INSERT INTO snapshots (sku, state, covers) VALUES ('SKU-1', ?, ?)`,
		[]byte(`{}`), []byte("{bad")); err != nil {
		t.Fatalf("seeding bad snapshot: %v", err)
	}
	if err := l.Compact(ctx, VersionVector{"A": 1}); err == nil {
		t.Fatal("Compact() error = nil, want a decode failure rather than a wrong deletion")
	}
}

func TestSnapshotRoundTripAndErrors(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)

	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); !errorsIsNoSnapshot(err) {
		t.Fatalf("LoadSnapshot() error = %v, want ErrNoSnapshot", err)
	}
	state, _ := json.Marshal(map[string]int{"x": 1})
	if err := l.SaveSnapshot(ctx, "SKU-1", state, VersionVector{"A": 3}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	// Overwrite -- SaveSnapshot must replace, not fail.
	if err := l.SaveSnapshot(ctx, "SKU-1", state, VersionVector{"A": 4}); err != nil {
		t.Fatalf("SaveSnapshot() overwrite error = %v", err)
	}
	gotState, covers, err := l.LoadSnapshot(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if string(gotState) != string(state) || covers["A"] != 4 {
		t.Fatalf("LoadSnapshot() = %s, %v; want %s, {A:4}", gotState, covers, state)
	}
	if _, err := l.db.ExecContext(ctx, `UPDATE snapshots SET covers = ? WHERE sku = 'SKU-1'`, []byte("{bad")); err != nil {
		t.Fatalf("corrupting covers: %v", err)
	}
	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); err == nil {
		t.Fatal("LoadSnapshot() error = nil for corrupt covers, want an error")
	}
}

func errorsIsNoSnapshot(err error) bool { return err != nil && errors.Is(err, ErrNoSnapshot) }
```

Add `"errors"` to the test imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./eventlog/ -run 'TestCompact|TestSnapshot' -v`
Expected: FAIL — `l.Compact undefined`, `l.CountForSKU undefined`.

- [ ] **Step 3: Implement `Compact` and `CountForSKU`**

Append to `eventlog-lab/eventlog/sqlite.go`:

```go
// CountForSKU reports how many events remain for sku. Used by the snapshot
// trigger and by compaction tests.
func (l *SQLiteLog) CountForSKU(ctx context.Context, sku string) (int, error) {
	var n int
	if err := l.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM events WHERE sku = ?`, sku).Scan(&n); err != nil {
		return 0, fmt.Errorf("count events for %q: %w", sku, err)
	}
	return n, nil
}

// Compact deletes an event only when BOTH hold:
//
//  1. upTo dominates it -- every peer has acked holding it, so nobody will ask
//     for it again; and
//  2. a snapshot for its SKU already folds it in, so our own read path does not
//     need it.
//
// Both conditions are mandatory. Deleting an event a peer has not yet seen
// loses it permanently: there is no recovery path.
func (l *SQLiteLog) Compact(ctx context.Context, upTo VersionVector) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin compact tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	covered, err := snapshotCoverage(ctx, tx)
	if err != nil {
		return err
	}
	for sku, snapVV := range covered {
		// Intersect: an event survives if either gate says keep.
		safe := VersionVector{}
		for node, seq := range snapVV {
			if peerSeq, ok := upTo[node]; ok {
				safe[node] = min(seq, peerSeq)
			}
		}
		for node, seq := range safe {
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM events WHERE sku = ? AND node_id = ? AND seq <= ?`,
				sku, string(node), int64(seq)); err != nil {
				return fmt.Errorf("compact %q/%q: %w", sku, node, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit compact: %w", err)
	}
	return nil
}

// snapshotCoverage reads every snapshot's covered version vector.
func snapshotCoverage(ctx context.Context, tx *sql.Tx) (map[string]VersionVector, error) {
	rows, err := tx.QueryContext(ctx, `SELECT sku, covers FROM snapshots`)
	if err != nil {
		return nil, fmt.Errorf("query snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]VersionVector{}
	for rows.Next() {
		var (
			sku    string
			covers []byte
		)
		if err := rows.Scan(&sku, &covers); err != nil {
			return nil, fmt.Errorf("scan snapshot row: %w", err)
		}
		vv := VersionVector{}
		if err := json.Unmarshal(covers, &vv); err != nil {
			return nil, fmt.Errorf("decode covers for %q: %w", sku, err)
		}
		out[sku] = vv
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshots: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run the compaction tests**

Run: `cd eventlog-lab && go test ./eventlog/ -run 'TestCompact|TestSnapshot' -v`
Expected: PASS. Then uncomment `var _ Log = (*SQLiteLog)(nil)` in `sqlite_test.go` and re-run — the interface is now satisfied.

- [ ] **Step 5: Write the failing projector tests**

`eventlog-lab/crdt/projector_test.go`:

```go
package crdt

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

func newProjector(t *testing.T, every int) (*Projector, *eventlog.SQLiteLog) {
	t.Helper()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "p.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return &Projector{Log: l, SnapshotEvery: every}, l
}

func TestProjectFoldsWholeLogWithoutSnapshot(t *testing.T) {
	ctx := context.Background()
	p, l := newProjector(t, 0)
	for _, e := range []eventlog.Event{qty("A", 1, 10, 10), qty("B", 1, 20, -4)} {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	got, err := p.Project(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if got.Quantity() != 6 {
		t.Fatalf("Quantity() = %d, want 6", got.Quantity())
	}
}

func TestProjectSnapshotPlusRemainderEqualsFullFold(t *testing.T) {
	ctx := context.Background()
	p, l := newProjector(t, 2)

	events := []eventlog.Event{
		qty("A", 1, 10, 10),
		qty("A", 2, 20, -1),
		meta("A", 3, 30, &eventlog.MetaSet{Name: ptr("Widget")}),
		qty("B", 1, 40, 5),
		del("B", 2, 50, true),
	}
	for i, e := range events {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		// Snapshot midway so later reads take the snapshot path.
		if i == 1 {
			if err := p.MaybeSnapshot(ctx, "SKU-1"); err != nil {
				t.Fatalf("MaybeSnapshot() error = %v", err)
			}
		}
	}
	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); err != nil {
		t.Fatalf("expected a snapshot to exist: %v", err)
	}

	got, err := p.Project(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if want := applyAll(events); !got.Equal(want) {
		t.Fatalf("snapshot path = %+v, want %+v", got, want)
	}
}

func TestMaybeSnapshotHonoursThreshold(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		every        int
		events       int
		wantSnapshot bool
	}{
		{"disabled", 0, 5, false},
		{"below threshold", 4, 3, false},
		{"at threshold", 3, 3, true},
		{"above threshold", 2, 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, l := newProjector(t, tt.every)
			for i := 1; i <= tt.events; i++ {
				if err := l.Append(ctx, qty("A", eventlog.Seq(i), int64(i*10), 1)); err != nil {
					t.Fatalf("Append() error = %v", err)
				}
			}
			if err := p.MaybeSnapshot(ctx, "SKU-1"); err != nil {
				t.Fatalf("MaybeSnapshot() error = %v", err)
			}
			_, _, err := l.LoadSnapshot(ctx, "SKU-1")
			if got := err == nil; got != tt.wantSnapshot {
				t.Fatalf("snapshot exists = %v, want %v (err = %v)", got, tt.wantSnapshot, err)
			}
		})
	}
}

func TestProjectRejectsCorruptSnapshotState(t *testing.T) {
	ctx := context.Background()
	p, l := newProjector(t, 0)
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte("{bad"), eventlog.VersionVector{}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if _, err := p.Project(ctx, "SKU-1"); err == nil {
		t.Fatal("Project() error = nil for a corrupt snapshot, want an error")
	}
}

func TestProjectPropagatesLogErrors(t *testing.T) {
	ctx := context.Background()
	p, l := newProjector(t, 1)
	if err := l.Append(ctx, qty("A", 1, 10, 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := p.Project(ctx, "SKU-1"); err == nil {
		t.Error("Project() on a closed log error = nil, want an error")
	}
	if err := p.MaybeSnapshot(ctx, "SKU-1"); err == nil {
		t.Error("MaybeSnapshot() on a closed log error = nil, want an error")
	}
}
```

- [ ] **Step 6: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./crdt/ -run 'TestProject|TestMaybeSnapshot' -v`
Expected: FAIL — `undefined: Projector`.

- [ ] **Step 7: Implement the projector**

`eventlog-lab/crdt/projector.go`:

```go
package crdt

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

// SQLLog is the slice of a log the projector needs. Both eventlog.SQLiteLog and
// eventlog.PostgresLog satisfy it, which is how central runs this identical
// projection code.
type SQLLog interface {
	eventlog.Log
	EventsForSKU(ctx context.Context, sku string, after eventlog.VersionVector) iter.Seq2[eventlog.Event, error]
	CountForSKU(ctx context.Context, sku string) (int, error)
}

// Projector computes ItemState for a SKU as fold(Apply, events), using
// snapshots to avoid replaying history on every read.
type Projector struct {
	Log SQLLog
	// SnapshotEvery is the per-SKU event count at or above which
	// MaybeSnapshot writes a snapshot. Zero disables snapshotting.
	SnapshotEvery int
}

// Project returns the merged state for sku: load the snapshot if one exists,
// then apply only the events its version vector does not already fold in.
// Because Apply is commutative, this equals folding the whole log.
func (p *Projector) Project(ctx context.Context, sku string) (*ItemState, error) {
	state := NewItemState()
	covered := eventlog.VersionVector{}

	blob, snapVV, err := p.Log.LoadSnapshot(ctx, sku)
	switch {
	case err == nil:
		if state, err = Unmarshal(blob); err != nil {
			return nil, fmt.Errorf("project %q: %w", sku, err)
		}
		covered = snapVV
	case !errors.Is(err, eventlog.ErrNoSnapshot):
		return nil, fmt.Errorf("project %q: %w", sku, err)
	}

	if err := state.Fold(p.Log.EventsForSKU(ctx, sku, covered)); err != nil {
		return nil, fmt.Errorf("project %q: %w", sku, err)
	}
	return state, nil
}

// MaybeSnapshot writes a snapshot for sku once its event count reaches
// SnapshotEvery, recording the version vector the snapshot folds in. Compaction
// later uses that vector as one of its two safety gates.
func (p *Projector) MaybeSnapshot(ctx context.Context, sku string) error {
	if p.SnapshotEvery <= 0 {
		return nil
	}
	n, err := p.Log.CountForSKU(ctx, sku)
	if err != nil {
		return err
	}
	if n < p.SnapshotEvery {
		return nil
	}

	state := NewItemState()
	covers := eventlog.VersionVector{}
	for e, err := range p.Log.EventsForSKU(ctx, sku, eventlog.VersionVector{}) {
		if err != nil {
			return fmt.Errorf("snapshot %q: %w", sku, err)
		}
		state.Apply(e)
		covers.Observe(e.ID)
	}
	blob, err := Marshal(state)
	if err != nil {
		return err
	}
	return p.Log.SaveSnapshot(ctx, sku, blob, covers)
}
```

Note that `MaybeSnapshot` folds from scratch rather than from the previous snapshot. That is deliberate: a snapshot must cover a contiguous prefix per node, and re-folding is trivially correct. If it ever shows up in a profile, fold from the existing snapshot instead — but do not do it speculatively.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cd eventlog-lab && go test ./crdt/ ./eventlog/ -v`
Expected: PASS.

- [ ] **Step 9: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover
golangci-lint run
```
Expected: all four packages at 100.0%.

- [ ] **Step 10: Commit**

```bash
cd eventlog-lab
git add eventlog/ crdt/
git commit -m "feat(eventlog): snapshots, safe compaction, and the projection read path"
```

---

### Task 7: `node/` — wiring and the in-process op API

**Files:**
- Create: `eventlog-lab/node/node.go`
- Test: `eventlog-lab/node/node_test.go`

**Interfaces:**
- Consumes: `clock.New`, `clock.NodeID`, `clock.HLC`, `clock.WallFunc`; `eventlog.OpenSQLite`, `eventlog.Log`, `eventlog.Event`, `eventlog.EventID`, `eventlog.Seq`, `eventlog.MetaSet`, `eventlog.VersionVector`, `eventlog.Kind` constants; `crdt.Projector`, `crdt.SQLLog`, `crdt.ItemState`.
- Produces:
  - `type Config struct { ID clock.NodeID; Log crdt.SQLLog; Wall clock.WallFunc; SnapshotEvery int }`
  - `func New(cfg Config) (*Node, error)`
  - `func (n *Node) ID() clock.NodeID`
  - `func (n *Node) Log() crdt.SQLLog`
  - `func (n *Node) Clock() *clock.Clock`
  - `func (n *Node) Receive(ctx context.Context, sku string, qty int64) (eventlog.EventID, error)`
  - `func (n *Node) Pick(ctx context.Context, sku string, qty int64) (eventlog.EventID, error)`
  - `func (n *Node) SetMeta(ctx context.Context, sku string, name *string, reorderPoint *int64) (eventlog.EventID, error)`
  - `func (n *Node) Delete(ctx context.Context, sku string, deleted bool) (eventlog.EventID, error)`
  - `func (n *Node) Get(ctx context.Context, sku string) (*crdt.ItemState, error)`
  - `func (n *Node) Merge(ctx context.Context, events []eventlog.Event) (int, error)` — appends remote events, calls `clock.Observe` for each, returns how many were accepted; skips and reports malformed ones without aborting the batch.
  - `func (n *Node) VersionVector(ctx context.Context) (eventlog.VersionVector, error)`

**Domain notes for the implementer:**
- `Receive`/`Pick` are the same event kind with opposite signs: `Receive(10)` emits `Delta: +10`, `Pick(3)` emits `Delta: -3`. Both reject a non-positive argument — a "pick of -3" would be an obfuscated receive, and the CLI should not be able to smuggle one through.
- The op API is **in-process only**. No HTTP. The CLI and the harness are both direct callers.
- Per the spec's error handling: *local `Append` failure is returned to the caller and the op is not acknowledged.* So an op returns `(EventID, error)` and the "no lost event" property only covers ops that returned a nil error. Do not add a retry loop inside the op — the caller decides.
- `Merge` must call `n.clock.Observe(e.HLC)` for every accepted remote event. Skipping this is the classic local-first bug: your next local write would carry a stamp *earlier* than an event you already hold, and LWW would resolve backwards.
- Per the spec: a malformed remote event is rejected, logged loudly, and the session continues. So `Merge` skips it and keeps going rather than failing the batch.

- [ ] **Step 1: Write the failing tests**

`eventlog-lab/node/node_test.go`:

```go
package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

func ptr[T any](v T) *T { return &v }

func newTestNode(t *testing.T, id clock.NodeID) *Node {
	t.Helper()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), string(id)+".db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	wall := int64(1000)
	n, err := New(Config{
		ID:            id,
		Log:           l,
		Wall:          func() int64 { wall++; return wall },
		SnapshotEvery: 3,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return n
}

func TestNewValidatesConfig(t *testing.T) {
	l, err := eventlog.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"valid", Config{ID: "A", Log: l}, false},
		{"empty id", Config{Log: l}, true},
		{"nil log", Config{ID: "A"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if err == nil && got.ID() != tt.cfg.ID {
				t.Fatalf("ID() = %q, want %q", got.ID(), tt.cfg.ID)
			}
		})
	}
}

func TestOpsProjectToState(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")

	if _, err := n.Receive(ctx, "SKU-1", 10); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if _, err := n.Pick(ctx, "SKU-1", 3); err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if _, err := n.SetMeta(ctx, "SKU-1", ptr("Widget"), ptr(int64(4))); err != nil {
		t.Fatalf("SetMeta() error = %v", err)
	}
	if _, err := n.Delete(ctx, "SKU-1", true); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, err := n.Get(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Quantity() != 7 {
		t.Errorf("Quantity() = %d, want 7", got.Quantity())
	}
	if got.Name.Value != "Widget" {
		t.Errorf("Name = %q, want %q", got.Name.Value, "Widget")
	}
	if got.ReorderPoint.Value != 4 {
		t.Errorf("ReorderPoint = %d, want 4", got.ReorderPoint.Value)
	}
	if !got.Deleted.Value {
		t.Error("Deleted = false, want true")
	}
}

func TestOpsAllocateSequentialIDs(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")
	for i := eventlog.Seq(1); i <= 3; i++ {
		id, err := n.Receive(ctx, "SKU-1", 1)
		if err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
		if id != (eventlog.EventID{NodeID: "A", Seq: i}) {
			t.Fatalf("Receive() id = %+v, want {A %d}", id, i)
		}
	}
}

func TestOpsRejectBadArguments(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")
	tests := []struct {
		name string
		call func() error
	}{
		{"receive zero", func() error { _, err := n.Receive(ctx, "SKU-1", 0); return err }},
		{"receive negative", func() error { _, err := n.Receive(ctx, "SKU-1", -1); return err }},
		{"pick zero", func() error { _, err := n.Pick(ctx, "SKU-1", 0); return err }},
		{"pick negative", func() error { _, err := n.Pick(ctx, "SKU-1", -1); return err }},
		{"empty sku on receive", func() error { _, err := n.Receive(ctx, "", 1); return err }},
		{"empty sku on setmeta", func() error { _, err := n.SetMeta(ctx, "", ptr("x"), nil); return err }},
		{"empty sku on delete", func() error { _, err := n.Delete(ctx, "", true); return err }},
		{"empty sku on get", func() error { _, err := n.Get(ctx, ""); return err }},
		{"setmeta with nothing to set", func() error { _, err := n.SetMeta(ctx, "SKU-1", nil, nil); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); err == nil {
				t.Fatal("error = nil, want a rejection")
			}
		})
	}
	vv, err := n.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if len(vv) != 0 {
		t.Fatalf("rejected ops reached the log: %v", vv)
	}
}

func TestOpsAdvanceTheClock(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")
	var prev clock.HLC
	for i := 0; i < 3; i++ {
		if _, err := n.Receive(ctx, "SKU-1", 1); err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
		got := n.Clock().Last()
		if i > 0 && !prev.Before(got) {
			t.Fatalf("stamp %+v did not advance past %+v", got, prev)
		}
		prev = got
	}
}

func TestMergeObservesRemoteClockAndSkipsMalformed(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")

	good := eventlog.Event{
		ID:    eventlog.EventID{NodeID: "B", Seq: 1},
		HLC:   clock.HLC{Wall: 999_000, NodeID: "B"},
		SKU:   "SKU-1",
		Kind:  eventlog.KindQuantityDelta,
		Delta: 5,
	}
	bad := eventlog.Event{
		ID:   eventlog.EventID{NodeID: "B", Seq: 2},
		HLC:  clock.HLC{Wall: 999_001, NodeID: "B"},
		SKU:  "SKU-1",
		Kind: 99, // unknown kind: must be rejected, never persisted
	}

	accepted, err := n.Merge(ctx, []eventlog.Event{good, bad, good})
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if accepted != 2 {
		t.Fatalf("Merge() accepted = %d, want 2 (duplicate counts as accepted, malformed does not)", accepted)
	}

	vv, err := n.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["B"] != 1 {
		t.Fatalf("vv = %v; the malformed event must not be stored", vv)
	}

	// A local write after the merge must sort AFTER the remote event, even
	// though the remote wall clock is far ahead of ours.
	if _, err := n.Receive(ctx, "SKU-1", 1); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if !good.HLC.Before(n.Clock().Last()) {
		t.Fatalf("local stamp %+v does not follow observed remote %+v", n.Clock().Last(), good.HLC)
	}

	state, err := n.Get(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if state.Quantity() != 6 {
		t.Fatalf("Quantity() = %d, want 6", state.Quantity())
	}
}

func TestMergeEmptyBatch(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")
	got, err := n.Merge(ctx, nil)
	if err != nil || got != 0 {
		t.Fatalf("Merge(nil) = %d, %v; want 0, nil", got, err)
	}
}

func TestOpsPropagateAppendFailure(t *testing.T) {
	ctx := context.Background()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	n, err := New(Config{ID: "A", Log: l})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	// Every op must surface the failure so the caller does not acknowledge it.
	if _, err := n.Receive(ctx, "SKU-1", 1); err == nil {
		t.Error("Receive() error = nil on a broken log, want an error")
	}
	if _, err := n.Pick(ctx, "SKU-1", 1); err == nil {
		t.Error("Pick() error = nil on a broken log, want an error")
	}
	if _, err := n.SetMeta(ctx, "SKU-1", ptr("x"), nil); err == nil {
		t.Error("SetMeta() error = nil on a broken log, want an error")
	}
	if _, err := n.Delete(ctx, "SKU-1", true); err == nil {
		t.Error("Delete() error = nil on a broken log, want an error")
	}
	if _, err := n.Get(ctx, "SKU-1"); err == nil {
		t.Error("Get() error = nil on a broken log, want an error")
	}
	if _, err := n.VersionVector(ctx); err == nil {
		t.Error("VersionVector() error = nil on a broken log, want an error")
	}
	if _, err := n.Merge(ctx, []eventlog.Event{{
		ID: eventlog.EventID{NodeID: "B", Seq: 1}, HLC: clock.HLC{Wall: 1, NodeID: "B"},
		SKU: "SKU-1", Kind: eventlog.KindQuantityDelta, Delta: 1,
	}}); err == nil {
		t.Error("Merge() error = nil on a broken log, want an error")
	}
	if n.Log() == nil {
		t.Error("Log() = nil, want the configured log")
	}
	if !errors.Is(errors.Unwrap(errors.New("x")), nil) { // keep errors import used
		_ = err
	}
}
```

Drop that last contrived `errors` check and instead remove `"errors"` from the imports if the linter flags it as unused.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./node/ -v`
Expected: FAIL — build error, `undefined: New`.

- [ ] **Step 3: Write the implementation**

`eventlog-lab/node/node.go`:

```go
// Package node wires one replica together: local log, hybrid logical clock, and
// a projection over the log. It exposes an in-process op API that both the CLI
// and the fault harness drive directly. There is deliberately no HTTP layer.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

// Config constructs a Node.
type Config struct {
	ID   clock.NodeID
	Log  crdt.SQLLog
	Wall clock.WallFunc // nil means real time
	// SnapshotEvery is the per-SKU event count at which a snapshot is
	// written. Zero disables snapshotting.
	SnapshotEvery int
}

// Node is one replica.
type Node struct {
	id        clock.NodeID
	log       crdt.SQLLog
	clk       *clock.Clock
	projector *crdt.Projector
}

// New validates cfg and returns a ready Node.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("node: empty node id")
	}
	if cfg.Log == nil {
		return nil, errors.New("node: nil log")
	}
	return &Node{
		id:        cfg.ID,
		log:       cfg.Log,
		clk:       clock.New(cfg.ID, cfg.Wall),
		projector: &crdt.Projector{Log: cfg.Log, SnapshotEvery: cfg.SnapshotEvery},
	}, nil
}

// ID reports this replica's identity.
func (n *Node) ID() clock.NodeID { return n.id }

// Log exposes the underlying log for the sync layer and the harness.
func (n *Node) Log() crdt.SQLLog { return n.log }

// Clock exposes the HLC so the sync layer can Observe remote stamps.
func (n *Node) Clock() *clock.Clock { return n.clk }

// Receive records an inbound quantity as a positive delta.
func (n *Node) Receive(ctx context.Context, sku string, qty int64) (eventlog.EventID, error) {
	if qty <= 0 {
		return eventlog.EventID{}, fmt.Errorf("node: receive quantity must be positive, got %d", qty)
	}
	return n.emit(ctx, sku, func(e *eventlog.Event) {
		e.Kind, e.Delta = eventlog.KindQuantityDelta, qty
	})
}

// Pick records an outbound quantity as a negative delta. Picking more than is
// on hand is accepted: quantity is a CRDT counter, and a negative result is a
// reportable anomaly at central, not a rejected write. See README.
func (n *Node) Pick(ctx context.Context, sku string, qty int64) (eventlog.EventID, error) {
	if qty <= 0 {
		return eventlog.EventID{}, fmt.Errorf("node: pick quantity must be positive, got %d", qty)
	}
	return n.emit(ctx, sku, func(e *eventlog.Event) {
		e.Kind, e.Delta = eventlog.KindQuantityDelta, -qty
	})
}

// SetMeta writes the last-writer-wins metadata fields. Nil arguments are left
// untouched rather than cleared.
func (n *Node) SetMeta(ctx context.Context, sku string, name *string, reorderPoint *int64) (eventlog.EventID, error) {
	meta := &eventlog.MetaSet{Name: name, ReorderPoint: reorderPoint}
	if meta.Empty() {
		return eventlog.EventID{}, errors.New("node: set-meta writes nothing")
	}
	return n.emit(ctx, sku, func(e *eventlog.Event) {
		e.Kind, e.Meta = eventlog.KindMetaSet, meta
	})
}

// Delete sets or clears the tombstone. It is an LWW-register, so a later
// Delete(false) genuinely resurrects the item.
func (n *Node) Delete(ctx context.Context, sku string, deleted bool) (eventlog.EventID, error) {
	return n.emit(ctx, sku, func(e *eventlog.Event) {
		e.Kind, e.DeletedTo = eventlog.KindDeleteSet, &deleted
	})
}

// emit stamps, appends, and snapshots one locally-originated event. An append
// failure is returned to the caller and the op is NOT acknowledged -- the
// "no lost event" property only covers acknowledged ops.
func (n *Node) emit(ctx context.Context, sku string, fill func(*eventlog.Event)) (eventlog.EventID, error) {
	if sku == "" {
		return eventlog.EventID{}, errors.New("node: empty sku")
	}
	stamp := n.clk.Now()
	e, err := n.log.AppendLocal(ctx, func(s eventlog.Seq) eventlog.Event {
		ev := eventlog.Event{ID: eventlog.EventID{NodeID: n.id, Seq: s}, HLC: stamp, SKU: sku}
		fill(&ev)
		return ev
	})
	if err != nil {
		return eventlog.EventID{}, fmt.Errorf("node %q: %w", n.id, err)
	}
	if err := n.projector.MaybeSnapshot(ctx, sku); err != nil {
		return eventlog.EventID{}, fmt.Errorf("node %q: %w", n.id, err)
	}
	return e.ID, nil
}

// Merge appends remote events and returns how many were accepted. It calls
// Observe on every accepted event so a subsequent local write sorts after
// anything we have already seen -- without that, LWW resolves backwards.
//
// A malformed event is rejected, logged loudly, and skipped; the session
// continues. Storing an event the crdt package cannot apply would break
// convergence silently, which is far worse than dropping a frame.
func (n *Node) Merge(ctx context.Context, events []eventlog.Event) (int, error) {
	accepted := 0
	for _, e := range events {
		if err := n.log.Append(ctx, e); err != nil {
			if errors.Is(err, eventlog.ErrMalformedEvent) {
				slog.Error("rejecting malformed remote event",
					"node", n.id, "event", e.ID, "kind", e.Kind, "err", err)
				continue
			}
			return accepted, fmt.Errorf("node %q merge: %w", n.id, err)
		}
		n.clk.Observe(e.HLC)
		accepted++
		if err := n.projector.MaybeSnapshot(ctx, e.SKU); err != nil {
			return accepted, fmt.Errorf("node %q merge: %w", n.id, err)
		}
	}
	return accepted, nil
}

// Get returns the merged state for sku.
func (n *Node) Get(ctx context.Context, sku string) (*crdt.ItemState, error) {
	if sku == "" {
		return nil, errors.New("node: empty sku")
	}
	return n.projector.Project(ctx, sku)
}

// VersionVector reports what this node holds.
func (n *Node) VersionVector(ctx context.Context) (eventlog.VersionVector, error) {
	return n.log.VersionVector(ctx)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd eventlog-lab && go test ./node/ -v`
Expected: PASS.

- [ ] **Step 5: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover
golangci-lint run
```
Expected: `node` at 100.0%. If `Merge`'s `MaybeSnapshot` error branch is uncovered, add a subtest that merges into a node whose log was closed after the first successful append.

- [ ] **Step 6: Commit**

```bash
cd eventlog-lab
git add node/
git commit -m "feat(node): in-process op API over log, clock, and projection"
```

---

### Task 8: `sync/` — protocol, wire codec, and the injectable transport seam

Protocol definition plus the frame/event codec. Split from Task 9 (the session logic) so a reviewer can reject the wire shape without re-reviewing the state machine.

**Files:**
- Create: `eventlog-lab/sync/sync.proto`
- Create: `eventlog-lab/sync/syncpb/` (generated: `sync.pb.go`, `sync_grpc.pb.go`)
- Create: `eventlog-lab/sync/codec.go`
- Create: `eventlog-lab/sync/transport.go`
- Test: `eventlog-lab/sync/codec_test.go`
- Modify: `eventlog-lab/go.mod`

**Interfaces:**
- Consumes: `eventlog.Event`, `eventlog.EventID`, `eventlog.Seq`, `eventlog.Kind` constants, `eventlog.MetaSet`, `eventlog.VersionVector`, `eventlog.ErrMalformedEvent`, `clock.HLC`, `clock.NodeID`.
- Produces:
  - Generated Go for the messages below in package `syncpb`.
  - `func encodeEvent(e eventlog.Event) (*syncpb.Event, error)`
  - `func decodeEvent(pe *syncpb.Event) (eventlog.Event, error)`
  - `func encodeVV(vv eventlog.VersionVector) map[string]uint64`
  - `func decodeVV(m map[string]uint64) eventlog.VersionVector`
  - `type Stream interface { Send(*syncpb.ClientFrame) error; Recv() (*syncpb.ServerFrame, error); CloseSend() error }`
  - `type ServerStream interface { Send(*syncpb.ServerFrame) error; Recv() (*syncpb.ClientFrame, error) }`
  - `type Dialer interface { Dial(ctx context.Context, addr string) (Stream, error) }`
  - `type Replica interface { ID() clock.NodeID; VersionVector(context.Context) (eventlog.VersionVector, error); Merge(context.Context, []eventlog.Event) (int, error); Log() crdt.SQLLog }` — satisfied by `*node.Node` as written in Task 7, with no changes to it.

**Design note:** `Stream`/`ServerStream`/`Dialer` exist *only* because the fault harness needs to substitute a deterministic in-memory transport for gRPC. That is the one legitimate reason to introduce an interface here — two real implementations exist.

- [ ] **Step 1: Install the protobuf toolchain and dependencies**

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
cd eventlog-lab
go get google.golang.org/grpc google.golang.org/protobuf
```

`protoc` itself must be on `PATH` (`brew install protobuf` on macOS). Verify: `protoc --version` and `protoc-gen-go --version`.

- [ ] **Step 2: Write the proto file**

`eventlog-lab/sync/sync.proto`:

```proto
syntax = "proto3";

package eventloglab.sync.v1;

option go_package = "github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb";

service Sync {
  rpc Replicate(stream ClientFrame) returns (stream ServerFrame);
}

message ClientFrame {
  oneof body {
    Hello  hello  = 1; // my node id + my version vector
    Events events = 2; // batch of my events the peer lacks
    Ack    ack    = 3; // version vector after applying peer's batch
  }
}

message ServerFrame {
  oneof body {
    Welcome welcome = 1; // central's version vector
    Events  events  = 2;
    Ack     ack     = 3;
  }
}

message Hello {
  string node_id = 1;
  map<string, uint64> version_vector = 2;
}

message Welcome {
  string node_id = 1;
  map<string, uint64> version_vector = 2;
}

message Events {
  repeated Event events = 1;
}

message Ack {
  map<string, uint64> version_vector = 1;
}

message Event {
  string node_id     = 1;
  uint64 seq         = 2;
  int64  hlc_wall    = 3;
  uint32 hlc_logical = 4;
  string sku         = 5;
  int32  kind        = 6;
  int64  delta       = 7;  // kind == 1 only
  optional string name          = 8;  // kind == 2 only
  optional int64  reorder_point = 9;  // kind == 2 only
  optional bool   deleted_to    = 10; // kind == 3 only
}
```

Note there are deliberately **no arbitration frames**: central cannot reject an event, only merge it.

- [ ] **Step 3: Generate the Go code**

```bash
cd eventlog-lab
mkdir -p sync/syncpb
protoc --proto_path=sync \
  --go_out=. --go_opt=module=github.com/dhiazfathra/local-first-architecture/eventlog-lab \
  --go-grpc_out=. --go-grpc_opt=module=github.com/dhiazfathra/local-first-architecture/eventlog-lab \
  sync/sync.proto
go mod tidy
```

Confirm `sync/syncpb/sync.pb.go` and `sync/syncpb/sync_grpc.pb.go` exist and `go build ./...` succeeds. Add the invocation as a `//go:generate` line at the top of `codec.go` in the next step so it is reproducible.

Exclude generated code from coverage expectations — add to `.golangci.yml`:

```yaml
issues:
  exclude-rules:
    - path: sync/syncpb/
      linters: [revive, staticcheck]
```

- [ ] **Step 4: Write the failing codec tests**

`eventlog-lab/sync/codec_test.go`:

```go
package sync

import (
	"errors"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

func ptr[T any](v T) *T { return &v }

func TestEventCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   eventlog.Event
	}{
		{"quantity delta", eventlog.Event{
			ID: eventlog.EventID{NodeID: "A", Seq: 7}, HLC: clock.HLC{Wall: 100, Logical: 2, NodeID: "A"},
			SKU: "SKU-1", Kind: eventlog.KindQuantityDelta, Delta: -3,
		}},
		{"meta both", eventlog.Event{
			ID: eventlog.EventID{NodeID: "B", Seq: 1}, HLC: clock.HLC{Wall: 100, NodeID: "B"},
			SKU: "SKU-1", Kind: eventlog.KindMetaSet,
			Meta: &eventlog.MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(5))},
		}},
		{"meta name only", eventlog.Event{
			ID: eventlog.EventID{NodeID: "B", Seq: 2}, HLC: clock.HLC{Wall: 101, NodeID: "B"},
			SKU: "SKU-1", Kind: eventlog.KindMetaSet, Meta: &eventlog.MetaSet{Name: ptr("Widget")},
		}},
		{"meta reorder only", eventlog.Event{
			ID: eventlog.EventID{NodeID: "B", Seq: 3}, HLC: clock.HLC{Wall: 102, NodeID: "B"},
			SKU: "SKU-1", Kind: eventlog.KindMetaSet, Meta: &eventlog.MetaSet{ReorderPoint: ptr(int64(9))},
		}},
		{"delete", eventlog.Event{
			ID: eventlog.EventID{NodeID: "C", Seq: 1}, HLC: clock.HLC{Wall: 103, NodeID: "C"},
			SKU: "SKU-2", Kind: eventlog.KindDeleteSet, DeletedTo: ptr(true),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pe, err := encodeEvent(tt.in)
			if err != nil {
				t.Fatalf("encodeEvent() error = %v", err)
			}
			got, err := decodeEvent(pe)
			if err != nil {
				t.Fatalf("decodeEvent() error = %v", err)
			}
			if got.ID != tt.in.ID || got.HLC != tt.in.HLC || got.SKU != tt.in.SKU ||
				got.Kind != tt.in.Kind || got.Delta != tt.in.Delta {
				t.Fatalf("round trip = %+v, want %+v", got, tt.in)
			}
			switch tt.in.Kind {
			case eventlog.KindMetaSet:
				if got.Meta == nil {
					t.Fatalf("Meta = nil, want %+v", tt.in.Meta)
				}
				if !eqPtr(got.Meta.Name, tt.in.Meta.Name) || !eqPtr(got.Meta.ReorderPoint, tt.in.Meta.ReorderPoint) {
					t.Fatalf("Meta = %+v, want %+v", got.Meta, tt.in.Meta)
				}
			case eventlog.KindDeleteSet:
				if !eqPtr(got.DeletedTo, tt.in.DeletedTo) {
					t.Fatalf("DeletedTo = %v, want %v", got.DeletedTo, tt.in.DeletedTo)
				}
			}
		})
	}
}

func eqPtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func TestEncodeEventRejectsMalformed(t *testing.T) {
	if _, err := encodeEvent(eventlog.Event{ID: eventlog.EventID{NodeID: "A", Seq: 1}, Kind: 42}); !errors.Is(err, eventlog.ErrMalformedEvent) {
		t.Fatalf("encodeEvent() error = %v, want ErrMalformedEvent", err)
	}
}

func TestDecodeEventRejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		in   *syncpb.Event
	}{
		{"nil frame", nil},
		{"unknown kind", &syncpb.Event{NodeId: "A", Seq: 1, Sku: "S", Kind: 42}},
		{"empty sku", &syncpb.Event{NodeId: "A", Seq: 1, Kind: 1, Delta: 1}},
		{"zero seq", &syncpb.Event{NodeId: "A", Kind: 1, Sku: "S", Delta: 1}},
		{"meta kind with no fields", &syncpb.Event{NodeId: "A", Seq: 1, Sku: "S", Kind: 2}},
		{"delete kind with no value", &syncpb.Event{NodeId: "A", Seq: 1, Sku: "S", Kind: 3}},
		{"delta on wrong kind", &syncpb.Event{NodeId: "A", Seq: 1, Sku: "S", Kind: 3, Delta: 1, DeletedTo: ptr(true)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeEvent(tt.in); !errors.Is(err, eventlog.ErrMalformedEvent) {
				t.Fatalf("decodeEvent() error = %v, want ErrMalformedEvent", err)
			}
		})
	}
}

func TestVersionVectorCodec(t *testing.T) {
	tests := []struct {
		name string
		vv   eventlog.VersionVector
	}{
		{"empty", eventlog.VersionVector{}},
		{"populated", eventlog.VersionVector{"A": 3, "B": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decodeVV(encodeVV(tt.vv))
			if len(got) != len(tt.vv) {
				t.Fatalf("round trip = %v, want %v", got, tt.vv)
			}
			for k, v := range tt.vv {
				if got[k] != v {
					t.Fatalf("round trip = %v, want %v", got, tt.vv)
				}
			}
		})
	}
	if got := decodeVV(nil); got == nil || len(got) != 0 {
		t.Fatalf("decodeVV(nil) = %v, want empty non-nil", got)
	}
}
```

- [ ] **Step 5: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./sync/ -v`
Expected: FAIL — `undefined: encodeEvent`.

- [ ] **Step 6: Write the codec and the transport seam**

`eventlog-lab/sync/codec.go`:

```go
// Package sync replicates events between two logs over a bidirectional stream.
// Both sides merge; neither arbitrates. The central store runs the same code
// with the same crdt.Apply, so it is a merge participant, not an authority.
package sync

//go:generate protoc --proto_path=. --go_out=.. --go_opt=module=github.com/dhiazfathra/local-first-architecture/eventlog-lab --go-grpc_out=.. --go-grpc_opt=module=github.com/dhiazfathra/local-first-architecture/eventlog-lab sync.proto

import (
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// encodeEvent converts a stored event to its wire form. It validates first so a
// node can never emit something its peer would have to reject.
func encodeEvent(e eventlog.Event) (*syncpb.Event, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	pe := &syncpb.Event{
		NodeId:     string(e.ID.NodeID),
		Seq:        uint64(e.ID.Seq),
		HlcWall:    e.HLC.Wall,
		HlcLogical: e.HLC.Logical,
		Sku:        e.SKU,
		Kind:       int32(e.Kind),
		Delta:      e.Delta,
	}
	if e.Meta != nil {
		pe.Name, pe.ReorderPoint = e.Meta.Name, e.Meta.ReorderPoint
	}
	pe.DeletedTo = e.DeletedTo
	return pe, nil
}

// decodeEvent converts a wire event to its stored form, validating it. This is
// the trust boundary for remote input: an event that fails here is never
// persisted, because storing something crdt cannot apply breaks convergence
// silently.
func decodeEvent(pe *syncpb.Event) (eventlog.Event, error) {
	if pe == nil {
		return eventlog.Event{}, fmt.Errorf("%w: nil event frame", eventlog.ErrMalformedEvent)
	}
	node := clock.NodeID(pe.GetNodeId())
	e := eventlog.Event{
		ID:        eventlog.EventID{NodeID: node, Seq: eventlog.Seq(pe.GetSeq())},
		HLC:       clock.HLC{Wall: pe.GetHlcWall(), Logical: pe.GetHlcLogical(), NodeID: node},
		SKU:       pe.GetSku(),
		Kind:      eventlog.Kind(pe.GetKind()),
		Delta:     pe.GetDelta(),
		DeletedTo: pe.DeletedTo,
	}
	if pe.Name != nil || pe.ReorderPoint != nil {
		e.Meta = &eventlog.MetaSet{Name: pe.Name, ReorderPoint: pe.ReorderPoint}
	}
	if err := e.Validate(); err != nil {
		return eventlog.Event{}, err
	}
	return e, nil
}

// encodeVV converts a version vector to its wire form.
func encodeVV(vv eventlog.VersionVector) map[string]uint64 {
	out := make(map[string]uint64, len(vv))
	for k, v := range vv {
		out[string(k)] = uint64(v)
	}
	return out
}

// decodeVV converts a wire version vector back, never returning nil.
func decodeVV(m map[string]uint64) eventlog.VersionVector {
	out := make(eventlog.VersionVector, len(m))
	for k, v := range m {
		out[clock.NodeID(k)] = eventlog.Seq(v)
	}
	return out
}
```

`eventlog-lab/sync/transport.go`:

```go
package sync

import (
	"context"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// Stream is the client half of a replication session. gRPC's generated client
// stream satisfies it directly; the fault harness supplies a deterministic
// in-memory implementation. That second implementation is the only reason this
// interface exists.
type Stream interface {
	Send(*syncpb.ClientFrame) error
	Recv() (*syncpb.ServerFrame, error)
	CloseSend() error
}

// ServerStream is the server half of a session.
type ServerStream interface {
	Send(*syncpb.ServerFrame) error
	Recv() (*syncpb.ClientFrame, error)
}

// Dialer opens a session to a peer address.
type Dialer interface {
	Dial(ctx context.Context, addr string) (Stream, error)
}

// Replica is what a session needs from either end. *node.Node satisfies it, for
// both a SQLite-backed node and the Postgres-backed central store.
type Replica interface {
	ID() clock.NodeID
	VersionVector(ctx context.Context) (eventlog.VersionVector, error)
	Merge(ctx context.Context, events []eventlog.Event) (int, error)
	Log() crdt.SQLLog
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd eventlog-lab && go test ./sync/ -v`
Expected: PASS.

- [ ] **Step 8: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover
golangci-lint run
```
Expected: `sync` at 100.0% excluding `sync/syncpb` (generated). Check with `go tool cover -func` that `codec.go` is fully covered; `transport.go` declares only types, so it contributes no statements.

- [ ] **Step 9: Commit**

```bash
cd eventlog-lab
git add go.mod go.sum sync/ .golangci.yml
git commit -m "feat(sync): replication protocol, wire codec, and transport seam"
```

---

### Task 9: `sync/` — session state machine, gRPC transport, in-memory transport

**Files:**
- Create: `eventlog-lab/sync/server.go`
- Create: `eventlog-lab/sync/client.go`
- Create: `eventlog-lab/sync/grpc.go`
- Create: `eventlog-lab/sync/memory.go`
- Test: `eventlog-lab/sync/session_test.go`
- Test: `eventlog-lab/sync/grpc_test.go`

**Interfaces:**
- Consumes: everything from Task 8, plus `node.New`, `node.Config`, `*node.Node`.
- Produces:
  - `func NewServer(r Replica, batchSize int) *Server`
  - `func (s *Server) Session(ctx context.Context, st ServerStream) error`
  - `func (s *Server) Replicate(gs syncpb.Sync_ReplicateServer) error`
  - `func NewClient(r Replica, d Dialer, batchSize int) *Client`
  - `func (c *Client) SyncOnce(ctx context.Context, addr string) (Report, error)`
  - `func (c *Client) SyncWithBackoff(ctx context.Context, addr string, attempts int, base time.Duration, jitter func(time.Duration) time.Duration) (Report, error)`
  - `type Report struct { Sent, Received int; PeerID clock.NodeID; PeerVV eventlog.VersionVector }`
  - `func NewGRPCDialer(opts ...grpc.DialOption) Dialer`
  - `func Register(s *grpc.Server, srv *Server)`
  - `func NewMemoryTransport() *MemoryTransport`
  - `func (m *MemoryTransport) Serve(addr string, srv *Server)`
  - `func (m *MemoryTransport) Dial(ctx context.Context, addr string) (Stream, error)`
  - `func (m *MemoryTransport) SetFilter(f FrameFilter)` where `type FrameFilter func(from, to clock.NodeID, frame any) []any` — Task 10 uses this hook to inject drops, duplicates, and reorderings. Default filter passes each frame through unchanged.

**Protocol, exactly as the session must implement it.** Half-duplex turn-taking, so the harness is deterministic:

1. Client → `Hello{node_id, version_vector}`.
2. Server → `Welcome{node_id, version_vector}`, then `Events` batches covering everything the client's vector lacks, then `Ack{version_vector}`.
3. Client merges each `Events` batch as it arrives. On `Ack`, client → `Events` batches covering everything the server's vector lacks, then `Ack{version_vector}`, then `CloseSend`.
4. Server merges, persists a cursor for the client, and returns.

Both sides persist a cursor (`SetCursor`) so a dropped stream resumes from the last ack rather than restarting. Per the spec, a **version-vector regression** from a peer (it claims less than it acked before) is logged and re-sent from the lower point — harmless, because `Append` is idempotent.

- [ ] **Step 1: Write the failing session tests**

`eventlog-lab/sync/session_test.go`:

```go
package sync

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/node"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

func newNode(t *testing.T, id clock.NodeID) *node.Node {
	t.Helper()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), string(id)+".db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	wall := int64(1000)
	n, err := New2(t, id, l, func() int64 { wall += 10; return wall })
	if err != nil {
		t.Fatalf("node.New() error = %v", err)
	}
	return n
}

// New2 keeps the node construction in one place for these tests.
func New2(t *testing.T, id clock.NodeID, l *eventlog.SQLiteLog, wall clock.WallFunc) (*node.Node, error) {
	t.Helper()
	return node.New(node.Config{ID: id, Log: l, Wall: wall, SnapshotEvery: 4})
}

func syncPair(t *testing.T, client, server *node.Node, batch int) Report {
	t.Helper()
	tr := NewMemoryTransport()
	tr.Serve("central", NewServer(server, batch))
	rep, err := NewClient(client, tr, batch).SyncOnce(context.Background(), "central")
	if err != nil {
		t.Fatalf("SyncOnce() error = %v", err)
	}
	return rep
}

func TestSyncOnceExchangesBothDirections(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")

	if _, err := a.Receive(ctx, "SKU-1", 10); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if _, err := b.Pick(ctx, "SKU-1", 4); err != nil {
		t.Fatalf("Pick() error = %v", err)
	}

	rep := syncPair(t, a, b, 8)
	if rep.Sent != 1 || rep.Received != 1 {
		t.Fatalf("Report = %+v, want Sent 1 / Received 1", rep)
	}
	if rep.PeerID != "B" {
		t.Fatalf("PeerID = %q, want B", rep.PeerID)
	}

	for _, n := range []*node.Node{a, b} {
		state, err := n.Get(ctx, "SKU-1")
		if err != nil {
			t.Fatalf("Get() on %q error = %v", n.ID(), err)
		}
		if state.Quantity() != 6 {
			t.Fatalf("node %q Quantity() = %d, want 6", n.ID(), state.Quantity())
		}
	}
}

func TestSyncOnceIsIdempotentAndBatchesCorrectly(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		batch     int
		aEvents   int
		bEvents   int
		wantSent  int
		wantRecvd int
	}{
		{"single batch", 8, 2, 3, 2, 3},
		{"batch size one forces many frames", 1, 3, 2, 3, 2},
		{"nothing to exchange", 8, 0, 0, 0, 0},
		{"one-sided", 8, 3, 0, 3, 0},
		{"exact multiple of batch size", 2, 4, 2, 4, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := newNode(t, "A"), newNode(t, "B")
			for i := 0; i < tt.aEvents; i++ {
				if _, err := a.Receive(ctx, "SKU-1", 1); err != nil {
					t.Fatalf("Receive() error = %v", err)
				}
			}
			for i := 0; i < tt.bEvents; i++ {
				if _, err := b.Receive(ctx, "SKU-1", 1); err != nil {
					t.Fatalf("Receive() error = %v", err)
				}
			}

			rep := syncPair(t, a, b, tt.batch)
			if rep.Sent != tt.wantSent || rep.Received != tt.wantRecvd {
				t.Fatalf("Report = %+v, want Sent %d / Received %d", rep, tt.wantSent, tt.wantRecvd)
			}

			// A second session must exchange nothing: both sides converged.
			again := syncPair(t, a, b, tt.batch)
			if again.Sent != 0 || again.Received != 0 {
				t.Fatalf("second session = %+v, want zero in both directions", again)
			}

			av, err := a.VersionVector(ctx)
			if err != nil {
				t.Fatalf("VersionVector() error = %v", err)
			}
			bv, err := b.VersionVector(ctx)
			if err != nil {
				t.Fatalf("VersionVector() error = %v", err)
			}
			if !av.Dominates(bv) || !bv.Dominates(av) {
				t.Fatalf("vectors diverged: A = %v, B = %v", av, bv)
			}
		})
	}
}

func TestSyncPersistsCursorsForResume(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")
	for i := 0; i < 3; i++ {
		if _, err := a.Receive(ctx, "SKU-1", 1); err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
	}
	syncPair(t, a, b, 8)

	got, err := b.Log().Cursor(ctx, "A")
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	if got != 3 {
		t.Fatalf("server cursor for A = %d, want 3", got)
	}
	if got, err = a.Log().Cursor(ctx, "B"); err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	if got != 0 {
		t.Fatalf("client cursor for B = %d, want 0 (B emitted nothing)", got)
	}
}

func TestSyncToleratesPeerVersionVectorRegression(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")
	for i := 0; i < 3; i++ {
		if _, err := a.Receive(ctx, "SKU-1", 1); err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
	}
	syncPair(t, a, b, 8)

	// B forgets everything it learned; its Welcome now regresses below the
	// cursor A holds. A must re-send from the lower point, and idempotent
	// Append must absorb it.
	if _, err := b.Log().(*eventlog.SQLiteLog).VersionVector(ctx); err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	forgetful := newNode(t, "B2")
	rep := syncPair(t, a, forgetful, 8)
	if rep.Sent != 3 {
		t.Fatalf("re-send after regression Sent = %d, want 3", rep.Sent)
	}
	state, err := forgetful.Get(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if state.Quantity() != 3 {
		t.Fatalf("Quantity() = %d, want 3", state.Quantity())
	}
}

func TestServerRejectsMalformedFrameAndContinuesSession(t *testing.T) {
	ctx := context.Background()
	server := newNode(t, "B")
	srv := NewServer(server, 8)

	valid := &syncpb.Event{
		NodeId: "A", Seq: 1, HlcWall: 10, Sku: "SKU-1", Kind: int32(eventlog.KindQuantityDelta), Delta: 5,
	}
	broken := &syncpb.Event{NodeId: "A", Seq: 2, HlcWall: 20, Sku: "SKU-1", Kind: 99}

	st := &scriptedServerStream{in: []*syncpb.ClientFrame{
		{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}},
		{Body: &syncpb.ClientFrame_Events{Events: &syncpb.Events{Events: []*syncpb.Event{valid, broken}}}},
		{Body: &syncpb.ClientFrame_Ack{Ack: &syncpb.Ack{VersionVector: map[string]uint64{"A": 2}}}},
	}}
	if err := srv.Session(ctx, st); err != nil {
		t.Fatalf("Session() error = %v, want the malformed frame to be skipped, not fatal", err)
	}
	vv, err := server.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["A"] != 1 {
		t.Fatalf("vv = %v; the malformed event must not be stored, the valid one must be", vv)
	}
}

func TestServerRejectsOutOfOrderHandshake(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(newNode(t, "B"), 8)
	tests := []struct {
		name string
		in   []*syncpb.ClientFrame
	}{
		{"events before hello", []*syncpb.ClientFrame{
			{Body: &syncpb.ClientFrame_Events{Events: &syncpb.Events{}}},
		}},
		{"empty hello node id", []*syncpb.ClientFrame{
			{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{}}},
		}},
		{"second hello", []*syncpb.ClientFrame{
			{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}},
			{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}},
		}},
		{"unknown body", []*syncpb.ClientFrame{{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := srv.Session(ctx, &scriptedServerStream{in: tt.in}); err == nil {
				t.Fatal("Session() error = nil, want a protocol error")
			}
		})
	}
}

func TestClientSurfacesTransportErrors(t *testing.T) {
	ctx := context.Background()
	a := newNode(t, "A")
	tests := []struct {
		name   string
		dialer Dialer
	}{
		{"dial fails", failingDialer{dialErr: errors.New("boom")}},
		{"send fails", failingDialer{sendErr: errors.New("boom")}},
		{"recv fails", failingDialer{recvErr: errors.New("boom")}},
		{"welcome missing", failingDialer{frames: []*syncpb.ServerFrame{
			{Body: &syncpb.ServerFrame_Ack{Ack: &syncpb.Ack{}}},
		}}},
		{"server sends malformed event", failingDialer{frames: []*syncpb.ServerFrame{
			{Body: &syncpb.ServerFrame_Welcome{Welcome: &syncpb.Welcome{NodeId: "B"}}},
			{Body: &syncpb.ServerFrame_Events{Events: &syncpb.Events{Events: []*syncpb.Event{{NodeId: "B", Seq: 1, Sku: "S", Kind: 99}}}}},
			{Body: &syncpb.ServerFrame_Ack{Ack: &syncpb.Ack{}}},
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewClient(a, tt.dialer, 8).SyncOnce(ctx, "peer"); err == nil {
				t.Fatal("SyncOnce() error = nil, want an error")
			}
		})
	}
}

func TestSyncWithBackoffRetriesThenSucceeds(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")
	if _, err := a.Receive(ctx, "SKU-1", 5); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	tr := NewMemoryTransport()
	flaky := &flakyDialer{inner: tr, failFirst: 2}
	tr.Serve("central", NewServer(b, 8))

	var sleeps []time.Duration
	rep, err := NewClient(a, flaky, 8).SyncWithBackoff(ctx, "central", 5, time.Millisecond,
		func(d time.Duration) time.Duration { sleeps = append(sleeps, d); return 0 })
	if err != nil {
		t.Fatalf("SyncWithBackoff() error = %v", err)
	}
	if rep.Sent != 1 {
		t.Fatalf("Report = %+v, want Sent 1", rep)
	}
	if len(sleeps) != 2 {
		t.Fatalf("backoff sleeps = %v, want 2 entries", sleeps)
	}
	if sleeps[1] <= sleeps[0] {
		t.Fatalf("backoff did not grow: %v", sleeps)
	}
}

func TestSyncWithBackoffGivesUp(t *testing.T) {
	a := newNode(t, "A")
	_, err := NewClient(a, failingDialer{dialErr: errors.New("boom")}, 8).
		SyncWithBackoff(context.Background(), "central", 2, time.Nanosecond, func(time.Duration) time.Duration { return 0 })
	if err == nil {
		t.Fatal("SyncWithBackoff() error = nil, want the final failure")
	}
}

func TestSyncWithBackoffHonoursContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := newNode(t, "A")
	_, err := NewClient(a, failingDialer{dialErr: errors.New("boom")}, 8).
		SyncWithBackoff(ctx, "central", 5, time.Hour, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SyncWithBackoff() error = %v, want context.Canceled", err)
	}
}

func TestMemoryTransportUnknownAddress(t *testing.T) {
	if _, err := NewMemoryTransport().Dial(context.Background(), "nope"); err == nil {
		t.Fatal("Dial() error = nil for an unserved address, want an error")
	}
}

// --- test doubles ---

// scriptedServerStream feeds a fixed sequence of client frames to a Server and
// records what the Server sent back.
type scriptedServerStream struct {
	in   []*syncpb.ClientFrame
	i    int
	sent []*syncpb.ServerFrame
}

func (s *scriptedServerStream) Recv() (*syncpb.ClientFrame, error) {
	if s.i >= len(s.in) {
		return nil, io.EOF
	}
	f := s.in[s.i]
	s.i++
	return f, nil
}

func (s *scriptedServerStream) Send(f *syncpb.ServerFrame) error {
	s.sent = append(s.sent, f)
	return nil
}

// failingDialer injects failures at each stage of a session.
type failingDialer struct {
	dialErr, sendErr, recvErr error
	frames                    []*syncpb.ServerFrame
}

func (d failingDialer) Dial(context.Context, string) (Stream, error) {
	if d.dialErr != nil {
		return nil, d.dialErr
	}
	return &failingStream{d: d}, nil
}

type failingStream struct {
	d failingDialer
	i int
}

func (s *failingStream) Send(*syncpb.ClientFrame) error { return s.d.sendErr }
func (s *failingStream) CloseSend() error               { return nil }
func (s *failingStream) Recv() (*syncpb.ServerFrame, error) {
	if s.d.recvErr != nil {
		return nil, s.d.recvErr
	}
	if s.i >= len(s.d.frames) {
		return nil, io.EOF
	}
	f := s.d.frames[s.i]
	s.i++
	return f, nil
}

// flakyDialer fails the first failFirst dials, then delegates.
type flakyDialer struct {
	inner     Dialer
	failFirst int
	calls     int
}

func (d *flakyDialer) Dial(ctx context.Context, addr string) (Stream, error) {
	d.calls++
	if d.calls <= d.failFirst {
		return nil, errors.New("transport unavailable")
	}
	return d.inner.Dial(ctx, addr)
}
```

Add `"io"` to the test imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./sync/ -v`
Expected: FAIL — `undefined: NewServer`, `undefined: NewClient`, `undefined: NewMemoryTransport`.

- [ ] **Step 3: Write the server session**

`eventlog-lab/sync/server.go`:

```go
package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// defaultBatchSize is used when a caller passes a non-positive size.
const defaultBatchSize = 64

// Server serves replication sessions. It merges everything valid it is given
// and never rejects an event on policy grounds -- there are no arbitration
// frames in the protocol, by design.
type Server struct {
	syncpb.UnimplementedSyncServer
	replica   Replica
	batchSize int
}

// NewServer wraps a replica as the serving end of replication.
func NewServer(r Replica, batchSize int) *Server {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return &Server{replica: r, batchSize: batchSize}
}

// Replicate adapts the gRPC stream to Session.
func (s *Server) Replicate(gs syncpb.Sync_ReplicateServer) error {
	return s.Session(gs.Context(), gs)
}

// Session runs one replication session: read Hello, reply Welcome, stream what
// the peer lacks, ack, then merge what the peer streams and record a cursor.
func (s *Server) Session(ctx context.Context, st ServerStream) error {
	hello, err := s.recvHello(st)
	if err != nil {
		return err
	}
	peer := clock.NodeID(hello.GetNodeId())
	peerVV := decodeVV(hello.GetVersionVector())

	if err := s.warnOnRegression(ctx, peer, peerVV); err != nil {
		return err
	}

	mine, err := s.replica.VersionVector(ctx)
	if err != nil {
		return fmt.Errorf("sync server: %w", err)
	}
	if err := st.Send(&syncpb.ServerFrame{Body: &syncpb.ServerFrame_Welcome{
		Welcome: &syncpb.Welcome{NodeId: string(s.replica.ID()), VersionVector: encodeVV(mine)},
	}}); err != nil {
		return fmt.Errorf("sync server: send welcome: %w", err)
	}

	if err := streamEvents(ctx, s.replica, peerVV, s.batchSize, func(batch []*syncpb.Event) error {
		return st.Send(&syncpb.ServerFrame{Body: &syncpb.ServerFrame_Events{Events: &syncpb.Events{Events: batch}}})
	}); err != nil {
		return err
	}
	if err := st.Send(&syncpb.ServerFrame{Body: &syncpb.ServerFrame_Ack{
		Ack: &syncpb.Ack{VersionVector: encodeVV(mine)},
	}}); err != nil {
		return fmt.Errorf("sync server: send ack: %w", err)
	}

	return s.consume(ctx, st, peer)
}

// recvHello reads the mandatory opening frame.
func (s *Server) recvHello(st ServerStream) (*syncpb.Hello, error) {
	frame, err := st.Recv()
	if err != nil {
		return nil, fmt.Errorf("sync server: recv hello: %w", err)
	}
	hello, ok := frame.GetBody().(*syncpb.ClientFrame_Hello)
	if !ok {
		return nil, fmt.Errorf("sync server: expected hello, got %T", frame.GetBody())
	}
	if hello.Hello.GetNodeId() == "" {
		return nil, errors.New("sync server: hello with empty node id")
	}
	return hello.Hello, nil
}

// warnOnRegression logs when a peer claims less than it previously acked. The
// session continues from the peer's lower point: re-sending is harmless because
// Append is idempotent.
func (s *Server) warnOnRegression(ctx context.Context, peer clock.NodeID, peerVV eventlog.VersionVector) error {
	cursor, err := s.replica.Log().Cursor(ctx, peer)
	if err != nil {
		return fmt.Errorf("sync server: %w", err)
	}
	if peerVV[peer] < cursor {
		slog.Warn("peer version vector regressed; re-sending from the lower point",
			"peer", peer, "acked", cursor, "claimed", peerVV[peer])
	}
	return nil
}

// consume merges the peer's event batches until its Ack, then records a cursor.
func (s *Server) consume(ctx context.Context, st ServerStream, peer clock.NodeID) error {
	for {
		frame, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("sync server: recv from %q: %w", peer, err)
		}
		switch body := frame.GetBody().(type) {
		case *syncpb.ClientFrame_Events:
			if _, err := mergeFrame(ctx, s.replica, body.Events.GetEvents()); err != nil {
				return fmt.Errorf("sync server: %w", err)
			}
		case *syncpb.ClientFrame_Ack:
			acked := decodeVV(body.Ack.GetVersionVector())
			if err := s.replica.Log().SetCursor(ctx, peer, acked[peer]); err != nil {
				return fmt.Errorf("sync server: %w", err)
			}
			return nil
		case *syncpb.ClientFrame_Hello:
			return errors.New("sync server: duplicate hello mid-session")
		default:
			return fmt.Errorf("sync server: unexpected frame %T", frame.GetBody())
		}
	}
}

// streamEvents sends everything the replica holds that peerVV lacks, in batches.
func streamEvents(ctx context.Context, r Replica, peerVV eventlog.VersionVector, batchSize int,
	send func([]*syncpb.Event) error) error {
	batch := make([]*syncpb.Event, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := send(batch); err != nil {
			return fmt.Errorf("send events: %w", err)
		}
		batch = batch[:0]
		return nil
	}
	for e, err := range r.Log().Since(ctx, peerVV) {
		if err != nil {
			return fmt.Errorf("read local events: %w", err)
		}
		pe, err := encodeEvent(e)
		if err != nil {
			return err
		}
		batch = append(batch, pe)
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// mergeFrame decodes and merges one Events frame, skipping malformed events.
// Per the spec: reject the bad event, log loudly, continue the session. Never
// persist something crdt cannot apply.
func mergeFrame(ctx context.Context, r Replica, pes []*syncpb.Event) (int, error) {
	events := make([]eventlog.Event, 0, len(pes))
	for _, pe := range pes {
		e, err := decodeEvent(pe)
		if err != nil {
			slog.Error("rejecting malformed remote event frame",
				"replica", r.ID(), "node", pe.GetNodeId(), "seq", pe.GetSeq(), "err", err)
			continue
		}
		events = append(events, e)
	}
	return r.Merge(ctx, events)
}
```

- [ ] **Step 4: Write the client session**

`eventlog-lab/sync/client.go`:

```go
package sync

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// Report summarises one session.
type Report struct {
	Sent     int
	Received int
	PeerID   clock.NodeID
	PeerVV   eventlog.VersionVector
}

// Client drives replication sessions from a replica to a peer address.
type Client struct {
	replica   Replica
	dialer    Dialer
	batchSize int
}

// NewClient wraps a replica as the dialing end of replication.
func NewClient(r Replica, d Dialer, batchSize int) *Client {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return &Client{replica: r, dialer: d, batchSize: batchSize}
}

// SyncOnce runs a single session: Hello, consume the peer's events until its
// Ack, then stream ours and Ack. A node is fully usable offline, so a failure
// here is never fatal to the node -- the caller decides whether to retry.
func (c *Client) SyncOnce(ctx context.Context, addr string) (Report, error) {
	mine, err := c.replica.VersionVector(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("sync client: %w", err)
	}
	st, err := c.dialer.Dial(ctx, addr)
	if err != nil {
		return Report{}, fmt.Errorf("sync client: dial %q: %w", addr, err)
	}
	if err := st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Hello{
		Hello: &syncpb.Hello{NodeId: string(c.replica.ID()), VersionVector: encodeVV(mine)},
	}}); err != nil {
		return Report{}, fmt.Errorf("sync client: send hello: %w", err)
	}

	rep, err := c.consume(ctx, st)
	if err != nil {
		return Report{}, err
	}

	if err := streamEvents(ctx, c.replica, rep.PeerVV, c.batchSize, func(batch []*syncpb.Event) error {
		rep.Sent += len(batch)
		return st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Events{Events: &syncpb.Events{Events: batch}}})
	}); err != nil {
		return Report{}, fmt.Errorf("sync client: %w", err)
	}

	after, err := c.replica.VersionVector(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("sync client: %w", err)
	}
	if err := st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Ack{
		Ack: &syncpb.Ack{VersionVector: encodeVV(after)},
	}}); err != nil {
		return Report{}, fmt.Errorf("sync client: send ack: %w", err)
	}
	if err := st.CloseSend(); err != nil {
		return Report{}, fmt.Errorf("sync client: close send: %w", err)
	}
	if err := c.replica.Log().SetCursor(ctx, rep.PeerID, rep.PeerVV[rep.PeerID]); err != nil {
		return Report{}, fmt.Errorf("sync client: %w", err)
	}
	return rep, nil
}

// consume reads Welcome, then the peer's event batches, until the peer's Ack.
func (c *Client) consume(ctx context.Context, st Stream) (Report, error) {
	frame, err := st.Recv()
	if err != nil {
		return Report{}, fmt.Errorf("sync client: recv welcome: %w", err)
	}
	welcome, ok := frame.GetBody().(*syncpb.ServerFrame_Welcome)
	if !ok {
		return Report{}, fmt.Errorf("sync client: expected welcome, got %T", frame.GetBody())
	}
	rep := Report{
		PeerID: clock.NodeID(welcome.Welcome.GetNodeId()),
		PeerVV: decodeVV(welcome.Welcome.GetVersionVector()),
	}

	for {
		frame, err := st.Recv()
		if err != nil {
			return Report{}, fmt.Errorf("sync client: recv from %q: %w", rep.PeerID, err)
		}
		switch body := frame.GetBody().(type) {
		case *syncpb.ServerFrame_Events:
			n, err := mergeFrame(ctx, c.replica, body.Events.GetEvents())
			if err != nil {
				return Report{}, fmt.Errorf("sync client: %w", err)
			}
			if n != len(body.Events.GetEvents()) {
				return Report{}, fmt.Errorf("sync client: peer %q sent an unapplicable event", rep.PeerID)
			}
			rep.Received += n
		case *syncpb.ServerFrame_Ack:
			return rep, nil
		default:
			return Report{}, fmt.Errorf("sync client: unexpected frame %T", frame.GetBody())
		}
	}
}

// SyncWithBackoff retries SyncOnce with exponential backoff and jitter. sleep
// may be nil, meaning time.Sleep with full jitter; tests pass a recorder.
// A node stays fully usable offline indefinitely, so exhausting attempts is a
// reportable outcome, not a crash.
func (c *Client) SyncWithBackoff(ctx context.Context, addr string, attempts int, base time.Duration,
	sleep func(time.Duration) time.Duration) (Report, error) {
	if sleep == nil {
		sleep = func(d time.Duration) time.Duration {
			jittered := time.Duration(rand.Int64N(int64(d) + 1))
			time.Sleep(jittered)
			return jittered
		}
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Report{}, fmt.Errorf("sync client: %w", err)
		}
		rep, err := c.SyncOnce(ctx, addr)
		if err == nil {
			return rep, nil
		}
		lastErr = err
		if attempt < attempts-1 {
			sleep(base << attempt)
		}
	}
	return Report{}, fmt.Errorf("sync client: gave up after %d attempts: %w", attempts, lastErr)
}

var _ = errors.Is // retained for readability of the error wrapping above
```

Delete that trailing `var _ = errors.Is` line and the `"errors"` import if the linter flags them.

- [ ] **Step 5: Write the gRPC and in-memory transports**

`eventlog-lab/sync/grpc.go`:

```go
package sync

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// Register attaches srv to a gRPC server.
func Register(s *grpc.Server, srv *Server) { syncpb.RegisterSyncServer(s, srv) }

// grpcDialer dials real gRPC peers.
type grpcDialer struct{ opts []grpc.DialOption }

// NewGRPCDialer returns a Dialer backed by gRPC.
func NewGRPCDialer(opts ...grpc.DialOption) Dialer { return &grpcDialer{opts: opts} }

func (d *grpcDialer) Dial(ctx context.Context, addr string) (Stream, error) {
	conn, err := grpc.NewClient(addr, d.opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %q: %w", addr, err)
	}
	st, err := syncpb.NewSyncClient(conn).Replicate(ctx)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("grpc replicate %q: %w", addr, err)
	}
	return &grpcStream{Sync_ReplicateClient: st, conn: conn}, nil
}

// grpcStream closes the connection when the client stops sending.
type grpcStream struct {
	syncpb.Sync_ReplicateClient
	conn *grpc.ClientConn
}

func (s *grpcStream) CloseSend() error {
	err := s.Sync_ReplicateClient.CloseSend()
	if cerr := s.conn.Close(); err == nil && cerr != nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("grpc close send: %w", err)
	}
	return nil
}
```

`eventlog-lab/sync/memory.go`:

```go
package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// FrameFilter intercepts one frame in flight and returns the frames to actually
// deliver: none (a drop), the same one (pass-through), or several (a duplicate).
// The fault harness installs a filter to inject partitions, duplication, and
// reordering; the default filter passes everything through.
type FrameFilter func(from, to clock.NodeID, frame any) []any

// MemoryTransport wires clients and servers together in-process with no real
// networking, so a whole cluster runs deterministically inside one test.
type MemoryTransport struct {
	mu      sync.Mutex
	servers map[string]*Server
	filter  FrameFilter
}

// NewMemoryTransport returns an empty transport with a pass-through filter.
func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{servers: map[string]*Server{}}
}

// Serve registers srv at addr.
func (m *MemoryTransport) Serve(addr string, srv *Server) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.servers[addr] = srv
}

// SetFilter installs a frame filter. Passing nil restores pass-through.
func (m *MemoryTransport) SetFilter(f FrameFilter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.filter = f
}

// Dial starts a session goroutine for the server at addr and returns the client
// half of a pair of channels.
func (m *MemoryTransport) Dial(ctx context.Context, addr string) (Stream, error) {
	m.mu.Lock()
	srv, ok := m.servers[addr]
	filter := m.filter
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("memory transport: no server at %q", addr)
	}

	p := &memPipe{
		toServer: make(chan *syncpb.ClientFrame, 1024),
		toClient: make(chan *syncpb.ServerFrame, 1024),
		done:     make(chan struct{}),
		filter:   filter,
		server:   srv.replica.ID(),
	}
	go func() {
		defer close(p.done)
		defer close(p.toClient)
		p.serverErr = srv.Session(ctx, (*memServerSide)(p))
	}()
	return (*memClientSide)(p), nil
}

// memPipe is a bidirectional in-memory frame pipe.
type memPipe struct {
	toServer  chan *syncpb.ClientFrame
	toClient  chan *syncpb.ServerFrame
	done      chan struct{}
	filter    FrameFilter
	client    clock.NodeID
	server    clock.NodeID
	serverErr error
}

// deliver applies the filter and returns the frames to enqueue.
func (p *memPipe) deliver(from, to clock.NodeID, frame any) []any {
	if p.filter == nil {
		return []any{frame}
	}
	return p.filter(from, to, frame)
}

type memClientSide memPipe

func (s *memClientSide) Send(f *syncpb.ClientFrame) error {
	p := (*memPipe)(s)
	if hello, ok := f.GetBody().(*syncpb.ClientFrame_Hello); ok {
		p.client = clock.NodeID(hello.Hello.GetNodeId())
	}
	for _, out := range p.deliver(p.client, p.server, f) {
		cf, ok := out.(*syncpb.ClientFrame)
		if !ok {
			return fmt.Errorf("memory transport: filter returned %T on the client side", out)
		}
		select {
		case p.toServer <- cf:
		case <-p.done:
			return errors.New("memory transport: session ended")
		}
	}
	return nil
}

func (s *memClientSide) Recv() (*syncpb.ServerFrame, error) {
	p := (*memPipe)(s)
	f, ok := <-p.toClient
	if !ok {
		if p.serverErr != nil {
			return nil, fmt.Errorf("memory transport: server session failed: %w", p.serverErr)
		}
		return nil, io.EOF
	}
	return f, nil
}

func (s *memClientSide) CloseSend() error {
	close((*memPipe)(s).toServer)
	<-(*memPipe)(s).done
	if err := (*memPipe)(s).serverErr; err != nil {
		return fmt.Errorf("memory transport: server session failed: %w", err)
	}
	return nil
}

type memServerSide memPipe

func (s *memServerSide) Recv() (*syncpb.ClientFrame, error) {
	f, ok := <-(*memPipe)(s).toServer
	if !ok {
		return nil, io.EOF
	}
	return f, nil
}

func (s *memServerSide) Send(f *syncpb.ServerFrame) error {
	p := (*memPipe)(s)
	for _, out := range p.deliver(p.server, p.client, f) {
		sf, ok := out.(*syncpb.ServerFrame)
		if !ok {
			return fmt.Errorf("memory transport: filter returned %T on the server side", out)
		}
		p.toClient <- sf
	}
	return nil
}
```

- [ ] **Step 6: Run the session tests**

Run: `cd eventlog-lab && go test ./sync/ -v -race`
Expected: PASS, no data races.

- [ ] **Step 7: Add the real-gRPC end-to-end test**

`eventlog-lab/sync/grpc_test.go`:

```go
package sync

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCEndToEnd(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")
	if _, err := a.Receive(ctx, "SKU-1", 10); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if _, err := b.Pick(ctx, "SKU-1", 4); err != nil {
		t.Fatalf("Pick() error = %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	gs := grpc.NewServer()
	Register(gs, NewServer(b, 4))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	dialer := NewGRPCDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	rep, err := NewClient(a, dialer, 4).SyncOnce(ctx, lis.Addr().String())
	if err != nil {
		t.Fatalf("SyncOnce() error = %v", err)
	}
	if rep.Sent != 1 || rep.Received != 1 {
		t.Fatalf("Report = %+v, want Sent 1 / Received 1", rep)
	}
	for _, n := range []interface{ Get(context.Context, string) (*crdtState, error) }{} {
		_ = n
	}
	for _, n := range []*nodeAlias{{a}, {b}} {
		state, err := n.Get(ctx, "SKU-1")
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if state.Quantity() != 6 {
			t.Fatalf("node %q Quantity() = %d, want 6", n.ID(), state.Quantity())
		}
	}
}

func TestGRPCDialerRejectsBadTarget(t *testing.T) {
	if _, err := NewGRPCDialer().Dial(context.Background(), "!!!not a target!!!"); err == nil {
		t.Fatal("Dial() error = nil for an invalid target, want an error")
	}
}
```

Simplify that loop before running — replace the two placeholder loops with a direct pair, and delete the `crdtState`/`nodeAlias` names:

```go
	for _, n := range []*node.Node{a, b} {
		state, err := n.Get(ctx, "SKU-1")
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if state.Quantity() != 6 {
			t.Fatalf("node %q Quantity() = %d, want 6", n.ID(), state.Quantity())
		}
	}
```

adding `"github.com/dhiazfathra/local-first-architecture/eventlog-lab/node"` to the imports.

- [ ] **Step 8: Run the gRPC test**

Run: `cd eventlog-lab && go test ./sync/ -run TestGRPC -v`
Expected: PASS. Same convergence result as the in-memory transport — that equivalence is the point of the seam.

- [ ] **Step 9: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover -race
golangci-lint run
```
Expected: `sync` at 100.0% excluding `syncpb`. Use `go tool cover -html` to find any uncovered error branch and add a table row for it.

- [ ] **Step 10: Commit**

```bash
cd eventlog-lab
git add sync/
git commit -m "feat(sync): session state machine over gRPC and in-memory transports"
```

---

### Task 10: Postgres central store — same interface, same `Apply`, plus a reporting projection

**Files:**
- Create: `eventlog-lab/eventlog/postgres.go`
- Create: `eventlog-lab/eventlog/postgres_test.go`
- Modify: `eventlog-lab/go.mod`
- Modify: `eventlog-lab/README.md` (created here if it does not exist yet)

**Interfaces:**
- Consumes: `Log`, `schema`, `selectEvents`, `scanEvent`, `Event`, `VersionVector`, `ErrNoSnapshot`, `ErrMalformedEvent`, `clock.NodeID`; `crdt.SQLLog` (which `*PostgresLog` must satisfy).
- Produces:
  - `func OpenPostgres(ctx context.Context, dsn string) (*PostgresLog, error)`
  - `*PostgresLog` implements every method of `crdt.SQLLog`: `Append`, `AppendLocal`, `Since`, `EventsForSKU`, `CountForSKU`, `VersionVector`, `LoadSnapshot`, `SaveSnapshot`, `Cursor`, `SetCursor`, `Compact`, `Close`.
  - `func (l *PostgresLog) UpsertProjection(ctx context.Context, sku string, s *crdt.ItemState) error`
  - `func (l *PostgresLog) Anomalies(ctx context.Context) ([]Anomaly, error)` where `type Anomaly struct { SKU string; Quantity int64 }`
  - `var pgSchema string` — the Postgres translation of `schema` plus the projection table.

**Domain notes for the implementer:**
- Central is a **merge participant, not an authority**. It has no rejection path: it runs the same `sync.Server` code, the same `node.Node`, and the same `crdt.Apply` as every other replica. The *only* thing it has extra is a reporting projection table.
- Negative quantity is the anomaly the projection reports. Central does not and must not refuse the events that produced it. Rejection lives in a different project (`warehouse-node`); documenting the limitation honestly is the deliverable here.
- Postgres uses `$1` placeholders, not `?`, and `BYTEA` rather than `BLOB` — so the schema text and the SQL cannot literally be shared with SQLite. Keep the *column definitions identical* to the spec's schema so the two backends cannot drift semantically.
- These tests require a live Postgres. Gate them on an env var and **skip cleanly when absent** — but note the Global Constraint that no task concludes with skipped tests: run them at least once against a real database (`docker run --rm -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:16`) and record the passing output before committing.

- [ ] **Step 1: Add the driver**

```bash
cd eventlog-lab
go get github.com/jackc/pgx/v5
```

- [ ] **Step 2: Start a Postgres and export the DSN**

```bash
docker run --rm -d --name eventlog-lab-pg -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:16
export EVENTLOG_LAB_PG_DSN='postgres://postgres:pg@127.0.0.1:5432/postgres?sslmode=disable'
```

- [ ] **Step 3: Write the failing tests**

`eventlog-lab/eventlog/postgres_test.go`:

```go
package eventlog

import (
	"context"
	"os"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// newPGLog returns a Postgres log with empty tables, or skips when no DSN is
// configured. Run these at least once against a real database -- see the plan.
func newPGLog(t *testing.T) *PostgresLog {
	t.Helper()
	dsn := os.Getenv("EVENTLOG_LAB_PG_DSN")
	if dsn == "" {
		t.Skip("EVENTLOG_LAB_PG_DSN not set; start Postgres and export it")
	}
	ctx := context.Background()
	l, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgres() error = %v", err)
	}
	if err := l.truncateAll(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestPostgresSatisfiesTheSameContractAsSQLite(t *testing.T) {
	ctx := context.Background()
	l := newPGLog(t)

	e := qty("A", 1, 100, "SKU-1", 5)
	for i := 0; i < 3; i++ {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() call %d error = %v", i, err)
		}
	}
	vv, err := l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["A"] != 1 {
		t.Fatalf("vv = %v, want {A:1} -- Append must be idempotent on (node_id, seq)", vv)
	}

	if err := l.Append(ctx, Event{ID: EventID{NodeID: "A", Seq: 2}, HLC: clock.HLC{NodeID: "A"}, SKU: "S", Kind: 99}); err == nil {
		t.Fatal("Append() error = nil for an unknown kind, want ErrMalformedEvent")
	}

	local, err := l.AppendLocal(ctx, func(s Seq) Event { return qty("C", s, 200, "SKU-1", 2) })
	if err != nil {
		t.Fatalf("AppendLocal() error = %v", err)
	}
	if local.ID.Seq != 1 {
		t.Fatalf("AppendLocal() seq = %d, want 1", local.ID.Seq)
	}

	if err := l.Append(ctx, qty("B", 1, 50, "SKU-2", 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	var ids []EventID
	for ev, err := range l.Since(ctx, VersionVector{}) {
		if err != nil {
			t.Fatalf("Since() error = %v", err)
		}
		ids = append(ids, ev.ID)
	}
	want := []EventID{{NodeID: "B", Seq: 1}, {NodeID: "A", Seq: 1}, {NodeID: "C", Seq: 1}}
	if len(ids) != len(want) {
		t.Fatalf("Since() ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("Since() ids = %v, want %v", ids, want)
		}
	}

	n, err := l.CountForSKU(ctx, "SKU-1")
	if err != nil || n != 2 {
		t.Fatalf("CountForSKU() = %d, %v; want 2, nil", n, err)
	}

	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); err == nil {
		t.Fatal("LoadSnapshot() error = nil with no snapshot, want ErrNoSnapshot")
	}
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{"pos":{}}`), VersionVector{"A": 1, "C": 1}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{"pos":{}}`), VersionVector{"A": 1, "C": 1}); err != nil {
		t.Fatalf("SaveSnapshot() overwrite error = %v", err)
	}
	blob, covers, err := l.LoadSnapshot(ctx, "SKU-1")
	if err != nil || string(blob) != `{"pos":{}}` || covers["A"] != 1 {
		t.Fatalf("LoadSnapshot() = %s, %v, %v", blob, covers, err)
	}

	if got, err := l.Cursor(ctx, "A"); err != nil || got != 0 {
		t.Fatalf("Cursor() = %d, %v; want 0, nil", got, err)
	}
	if err := l.SetCursor(ctx, "A", 1); err != nil {
		t.Fatalf("SetCursor() error = %v", err)
	}
	if got, err := l.Cursor(ctx, "A"); err != nil || got != 1 {
		t.Fatalf("Cursor() = %d, %v; want 1, nil", got, err)
	}

	// Compaction: A and C are covered by both the snapshot and the peer
	// vector; B's event has no snapshot and must survive.
	if err := l.Compact(ctx, VersionVector{"A": 1, "C": 1}); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if n, err = l.CountForSKU(ctx, "SKU-1"); err != nil || n != 0 {
		t.Fatalf("SKU-1 count after compaction = %d, %v; want 0, nil", n, err)
	}
	if n, err = l.CountForSKU(ctx, "SKU-2"); err != nil || n != 1 {
		t.Fatalf("SKU-2 count after compaction = %d, %v; want 1, nil", n, err)
	}
}

func TestPostgresEventsForSKU(t *testing.T) {
	ctx := context.Background()
	l := newPGLog(t)
	for _, e := range []Event{qty("A", 1, 10, "SKU-1", 1), qty("A", 2, 20, "SKU-2", 1)} {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	tests := []struct {
		name  string
		sku   string
		after VersionVector
		want  int
	}{
		{"all", "SKU-1", VersionVector{}, 1},
		{"covered", "SKU-1", VersionVector{"A": 1}, 0},
		{"unknown", "SKU-9", VersionVector{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := 0
			for _, err := range l.EventsForSKU(ctx, tt.sku, tt.after) {
				if err != nil {
					t.Fatalf("EventsForSKU() error = %v", err)
				}
				n++
			}
			if n != tt.want {
				t.Fatalf("count = %d, want %d", n, tt.want)
			}
		})
	}
}

func TestPostgresOpenRejectsBadDSN(t *testing.T) {
	if _, err := OpenPostgres(context.Background(), "not-a-dsn"); err == nil {
		t.Fatal("OpenPostgres() error = nil for a bad DSN, want an error")
	}
}

func TestPostgresAnomaliesReportNegativeStockWithoutRejectingIt(t *testing.T) {
	ctx := context.Background()
	l := newPGLog(t)

	// Two nodes each pick 8 of 10 while partitioned. Central merges both --
	// it has no rejection path -- and reports the negative result.
	events := []Event{
		qty("C", 1, 10, "SKU-1", 10),
		qty("A", 1, 20, "SKU-1", -8),
		qty("B", 1, 20, "SKU-1", -8),
		qty("C", 2, 30, "SKU-2", 5),
	}
	for _, e := range events {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append(%v) error = %v -- central must never reject", e.ID, err)
		}
	}

	p := &crdt.Projector{Log: l}
	for _, sku := range []string{"SKU-1", "SKU-2"} {
		state, err := p.Project(ctx, sku)
		if err != nil {
			t.Fatalf("Project(%q) error = %v", sku, err)
		}
		if err := l.UpsertProjection(ctx, sku, state); err != nil {
			t.Fatalf("UpsertProjection(%q) error = %v", sku, err)
		}
	}

	got, err := l.Anomalies(ctx)
	if err != nil {
		t.Fatalf("Anomalies() error = %v", err)
	}
	if len(got) != 1 || got[0].SKU != "SKU-1" || got[0].Quantity != -6 {
		t.Fatalf("Anomalies() = %+v, want exactly [{SKU-1 -6}]", got)
	}
}
```

Add `"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"` to the imports — and note this makes `eventlog`'s **test** depend on `crdt`, which is fine (test-only edges cannot form an import cycle in Go as long as the test stays in package `eventlog` and `crdt` does not import `eventlog`'s tests). If the compiler complains about a cycle, move `TestPostgresAnomalies...` into `crdt/central_test.go` unchanged.

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cd eventlog-lab && go test ./eventlog/ -run TestPostgres -v`
Expected: FAIL — `undefined: OpenPostgres`.

- [ ] **Step 5: Write the implementation**

`eventlog-lab/eventlog/postgres.go`:

```go
package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"
)

// pgSchema mirrors the SQLite schema column-for-column, translated to Postgres
// types, plus a projection table that exists only for reporting. Central is a
// merge participant, not an authority: nothing here can reject an event.
const pgSchema = `
CREATE TABLE IF NOT EXISTS events (
  node_id     TEXT     NOT NULL,
  seq         BIGINT   NOT NULL,
  hlc_wall    BIGINT   NOT NULL,
  hlc_logical BIGINT   NOT NULL,
  sku         TEXT     NOT NULL,
  kind        INTEGER  NOT NULL,
  payload     BYTEA    NOT NULL,
  PRIMARY KEY (node_id, seq)
);
CREATE INDEX IF NOT EXISTS events_by_hlc ON events (hlc_wall, hlc_logical, node_id);
CREATE INDEX IF NOT EXISTS events_by_sku ON events (sku);

CREATE TABLE IF NOT EXISTS snapshots (
  sku        TEXT   NOT NULL PRIMARY KEY,
  state      BYTEA  NOT NULL,
  covers     BYTEA  NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_cursors (
  peer_node_id TEXT   NOT NULL PRIMARY KEY,
  last_seq     BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS projections (
  sku            TEXT   NOT NULL PRIMARY KEY,
  quantity       BIGINT NOT NULL,
  name           TEXT   NOT NULL,
  reorder_point  BIGINT NOT NULL,
  deleted        BOOLEAN NOT NULL
);
`

// PostgresLog is the central store. It implements exactly the same interface as
// the per-node SQLite log and runs the same crdt.Apply, so there is no second
// merge implementation anywhere in this project.
type PostgresLog struct {
	pool *pgxpool.Pool
}

// OpenPostgres connects to dsn and applies the schema.
func OpenPostgres(ctx context.Context, dsn string) (*PostgresLog, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, pgSchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply postgres schema: %w", err)
	}
	return &PostgresLog{pool: pool}, nil
}

// Close releases the pool.
func (l *PostgresLog) Close() error {
	l.pool.Close()
	return nil
}

// truncateAll empties every table. Test helper, exported to the package only.
func (l *PostgresLog) truncateAll(ctx context.Context) error {
	_, err := l.pool.Exec(ctx, `TRUNCATE events, snapshots, sync_cursors, projections`)
	if err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	return nil
}

const pgInsertEvent = `INSERT INTO events
  (node_id, seq, hlc_wall, hlc_logical, sku, kind, payload)
  VALUES ($1, $2, $3, $4, $5, $6, $7)
  ON CONFLICT (node_id, seq) DO NOTHING`

// Append is idempotent on (node_id, seq), exactly as in SQLite.
func (l *PostgresLog) Append(ctx context.Context, e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	payload, err := e.MarshalPayload()
	if err != nil {
		return err
	}
	if _, err := l.pool.Exec(ctx, pgInsertEvent,
		string(e.ID.NodeID), int64(e.ID.Seq), e.HLC.Wall, int64(e.HLC.Logical),
		e.SKU, int(e.Kind), payload); err != nil {
		return fmt.Errorf("append %v: %w", e.ID, err)
	}
	return nil
}

// AppendLocal allocates and inserts in one transaction, so no crash can leave a
// sequence gap.
func (l *PostgresLog) AppendLocal(ctx context.Context, mint func(Seq) Event) (Event, error) {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return Event{}, fmt.Errorf("begin append-local tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	probe := mint(0)
	var next int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE node_id = $1`,
		string(probe.ID.NodeID)).Scan(&next); err != nil {
		return Event{}, fmt.Errorf("allocate seq for %q: %w", probe.ID.NodeID, err)
	}
	e := mint(Seq(next))
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	payload, err := e.MarshalPayload()
	if err != nil {
		return Event{}, err
	}
	if _, err := tx.Exec(ctx, pgInsertEvent,
		string(e.ID.NodeID), int64(e.ID.Seq), e.HLC.Wall, int64(e.HLC.Logical),
		e.SKU, int(e.Kind), payload); err != nil {
		return Event{}, fmt.Errorf("insert %v: %w", e.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Event{}, fmt.Errorf("commit %v: %w", e.ID, err)
	}
	return e, nil
}

// Since streams events not covered by vv, in HLC order.
func (l *PostgresLog) Since(ctx context.Context, vv VersionVector) iter.Seq2[Event, error] {
	return l.stream(ctx, vv, selectEvents+` ORDER BY hlc_wall, hlc_logical, node_id`)
}

// EventsForSKU streams one SKU's events not covered by after.
func (l *PostgresLog) EventsForSKU(ctx context.Context, sku string, after VersionVector) iter.Seq2[Event, error] {
	return l.stream(ctx, after,
		selectEvents+` WHERE sku = $1 ORDER BY hlc_wall, hlc_logical, node_id`, sku)
}

func (l *PostgresLog) stream(ctx context.Context, skip VersionVector, query string, args ...any) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		rows, err := l.pool.Query(ctx, query, args...)
		if err != nil {
			yield(Event{}, fmt.Errorf("query events: %w", err))
			return
		}
		defer rows.Close()
		for rows.Next() {
			e, err := scanEvent(rows)
			if err != nil {
				yield(Event{}, err)
				return
			}
			if skip.Contains(e.ID) {
				continue
			}
			if !yield(e, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(Event{}, fmt.Errorf("iterate events: %w", err))
		}
	}
}

// CountForSKU reports how many events remain for sku.
func (l *PostgresLog) CountForSKU(ctx context.Context, sku string) (int, error) {
	var n int
	if err := l.pool.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE sku = $1`, sku).Scan(&n); err != nil {
		return 0, fmt.Errorf("count events for %q: %w", sku, err)
	}
	return n, nil
}

// VersionVector reports the highest seq held per node.
func (l *PostgresLog) VersionVector(ctx context.Context) (VersionVector, error) {
	rows, err := l.pool.Query(ctx, `SELECT node_id, MAX(seq) FROM events GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("query version vector: %w", err)
	}
	defer rows.Close()
	vv := VersionVector{}
	for rows.Next() {
		var (
			node string
			max  int64
		)
		if err := rows.Scan(&node, &max); err != nil {
			return nil, fmt.Errorf("scan version vector row: %w", err)
		}
		vv[clock.NodeID(node)] = Seq(max)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate version vector: %w", err)
	}
	return vv, nil
}

// Cursor returns the highest seq of peer's events we have applied.
func (l *PostgresLog) Cursor(ctx context.Context, peer clock.NodeID) (Seq, error) {
	var last int64
	err := l.pool.QueryRow(ctx,
		`SELECT last_seq FROM sync_cursors WHERE peer_node_id = $1`, string(peer)).Scan(&last)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read cursor for %q: %w", peer, err)
	}
	return Seq(last), nil
}

// SetCursor records progress against peer, never moving backwards.
func (l *PostgresLog) SetCursor(ctx context.Context, peer clock.NodeID, last Seq) error {
	if _, err := l.pool.Exec(ctx,
		`INSERT INTO sync_cursors (peer_node_id, last_seq) VALUES ($1, $2)
		 ON CONFLICT (peer_node_id) DO UPDATE
		 SET last_seq = GREATEST(sync_cursors.last_seq, excluded.last_seq)`,
		string(peer), int64(last)); err != nil {
		return fmt.Errorf("set cursor for %q: %w", peer, err)
	}
	return nil
}

// LoadSnapshot returns the stored state for sku, or ErrNoSnapshot.
func (l *PostgresLog) LoadSnapshot(ctx context.Context, sku string) ([]byte, VersionVector, error) {
	var state, covers []byte
	err := l.pool.QueryRow(ctx,
		`SELECT state, covers FROM snapshots WHERE sku = $1`, sku).Scan(&state, &covers)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil, fmt.Errorf("%w for %q", ErrNoSnapshot, sku)
	case err != nil:
		return nil, nil, fmt.Errorf("load snapshot %q: %w", sku, err)
	}
	vv := VersionVector{}
	if err := json.Unmarshal(covers, &vv); err != nil {
		return nil, nil, fmt.Errorf("decode snapshot covers for %q: %w", sku, err)
	}
	return state, vv, nil
}

// SaveSnapshot replaces the snapshot for sku.
func (l *PostgresLog) SaveSnapshot(ctx context.Context, sku string, state []byte, covers VersionVector) error {
	blob, err := json.Marshal(covers)
	if err != nil {
		return fmt.Errorf("encode snapshot covers for %q: %w", sku, err)
	}
	if _, err := l.pool.Exec(ctx,
		`INSERT INTO snapshots (sku, state, covers) VALUES ($1, $2, $3)
		 ON CONFLICT (sku) DO UPDATE SET state = excluded.state, covers = excluded.covers`,
		sku, state, blob); err != nil {
		return fmt.Errorf("save snapshot %q: %w", sku, err)
	}
	return nil
}

// Compact applies the same two mandatory gates as SQLite: dominated by upTo AND
// folded into a local snapshot.
func (l *PostgresLog) Compact(ctx context.Context, upTo VersionVector) error {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin compact tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `SELECT sku, covers FROM snapshots`)
	if err != nil {
		return fmt.Errorf("query snapshots: %w", err)
	}
	covered := map[string]VersionVector{}
	for rows.Next() {
		var (
			sku    string
			blob   []byte
		)
		if err := rows.Scan(&sku, &blob); err != nil {
			rows.Close()
			return fmt.Errorf("scan snapshot row: %w", err)
		}
		vv := VersionVector{}
		if err := json.Unmarshal(blob, &vv); err != nil {
			rows.Close()
			return fmt.Errorf("decode covers for %q: %w", sku, err)
		}
		covered[sku] = vv
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate snapshots: %w", err)
	}

	for sku, snapVV := range covered {
		for node, seq := range snapVV {
			peerSeq, ok := upTo[node]
			if !ok {
				continue
			}
			if _, err := tx.Exec(ctx,
				`DELETE FROM events WHERE sku = $1 AND node_id = $2 AND seq <= $3`,
				sku, string(node), int64(min(seq, peerSeq))); err != nil {
				return fmt.Errorf("compact %q/%q: %w", sku, node, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit compact: %w", err)
	}
	return nil
}

// Anomaly is a reportable condition in the central projection. Negative stock
// is the canonical one: it is correct CRDT behavior, not a bug, and central
// reports it rather than rejecting the events that caused it.
type Anomaly struct {
	SKU      string
	Quantity int64
}

// UpsertProjection writes the merged state for sku into the reporting table.
func (l *PostgresLog) UpsertProjection(ctx context.Context, sku string, s *crdt.ItemState) error {
	if _, err := l.pool.Exec(ctx,
		`INSERT INTO projections (sku, quantity, name, reorder_point, deleted)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (sku) DO UPDATE SET quantity = excluded.quantity, name = excluded.name,
		   reorder_point = excluded.reorder_point, deleted = excluded.deleted`,
		sku, s.Quantity(), s.Name.Value, s.ReorderPoint.Value, s.Deleted.Value); err != nil {
		return fmt.Errorf("upsert projection %q: %w", sku, err)
	}
	return nil
}

// Anomalies lists SKUs whose merged quantity is negative.
func (l *PostgresLog) Anomalies(ctx context.Context) ([]Anomaly, error) {
	rows, err := l.pool.Query(ctx,
		`SELECT sku, quantity FROM projections WHERE quantity < 0 ORDER BY sku`)
	if err != nil {
		return nil, fmt.Errorf("query anomalies: %w", err)
	}
	defer rows.Close()
	var out []Anomaly
	for rows.Next() {
		var a Anomaly
		if err := rows.Scan(&a.SKU, &a.Quantity); err != nil {
			return nil, fmt.Errorf("scan anomaly row: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate anomalies: %w", err)
	}
	return out, nil
}
```

Note `eventlog` now imports `crdt` for `UpsertProjection`, while `crdt` imports `eventlog` — **that is an import cycle and will not compile.** Fix it by taking the four scalars instead of the struct:

```go
// UpsertProjection writes a merged state for sku into the reporting table.
func (l *PostgresLog) UpsertProjection(ctx context.Context, sku string,
	quantity int64, name string, reorderPoint int64, deleted bool) error {
```

with the body using those parameters directly, and drop the `crdt` import. In the test, call it as
`l.UpsertProjection(ctx, sku, state.Quantity(), state.Name.Value, state.ReorderPoint.Value, state.Deleted.Value)`.
The anomaly test then belongs in `crdt/central_test.go` (package `crdt`), since it needs both packages.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd eventlog-lab && go test ./eventlog/ ./crdt/ -run 'Postgres|Anomal' -v`
Expected: PASS with `EVENTLOG_LAB_PG_DSN` exported. Record the output — this is the "ran at least once against a real database" evidence the Global Constraints require.

- [ ] **Step 7: Write the README**

`eventlog-lab/README.md`:

```markdown
# eventlog-lab

A CRDT event-log laboratory. A local-first inventory node whose source of truth
is a local append-only event log, where concurrent edits across nodes merge
mathematically with no central arbitration. **The test harness is the product.**

This is a learning vehicle, not a reusable library.

## What it proves

A property-based harness partitions nodes, skews clocks, duplicates and reorders
deliveries, and crashes nodes mid-append, then asserts:

1. **Convergence** — nodes that exchanged the same event set compute identical state.
2. **No lost event** — every event durably acknowledged to a caller appears in
   every replica's log after sync.
3. **Order independence** — replaying a merged log in any causally-valid order
   yields identical state.

## Layout

| Package | Role |
|---|---|
| `clock/` | Hybrid logical clocks: monotonic `Now`, causal `Observe`, total order. |
| `eventlog/` | Append-only log on SQLite (per node) and Postgres (central). Snapshots, compaction. |
| `crdt/` | Merge semantics: PN-counter quantity, LWW metadata. One implementation, shared by every replica. |
| `sync/` | Bidirectional gRPC replication over an injectable transport. |
| `node/` | Wiring and the in-process op API. |
| `internal/jepsenlite/` | The deterministic fault simulator. |
| `cmd/lab/` | CLI. |

## Running

```bash
go test ./... -cover          # unit and property tests
go run ./cmd/lab sim --seed 42 --nodes 3 --ops 500 --faults partition,skew,dup
```

Central-store tests need Postgres:

```bash
docker run --rm -d -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:16
export EVENTLOG_LAB_PG_DSN='postgres://postgres:pg@127.0.0.1:5432/postgres?sslmode=disable'
```

## Known and accepted limitation: negative stock

Two nodes, partitioned, each pick 8 units of an item holding 10. Both writes are
valid locally. After sync, every replica converges on a quantity of **−6**.

This is correct CRDT behavior and this project accepts it. A commutative counter
cannot enforce a global invariant like "quantity >= 0" without exactly the
coordination that local-first architecture exists to avoid. Negative stock is a
**reportable anomaly** at central (`projections.quantity < 0`), never a rejected
write — central is a merge participant, not an authority.

Rejection semantics belong in a system that accepts the coordination cost. That
is not this project.

## Non-goals

Authentication, multi-tenancy, UI, HTTP API, tombstone garbage collection beyond
`Compact`, real ERP domain rules, performance tuning, reusable-library packaging.
```

- [ ] **Step 8: Run the full gate**

```bash
cd eventlog-lab
go test ./... -cover -race
golangci-lint run
```
Expected: all packages at 100.0% (with the Postgres DSN exported, so nothing skips).

- [ ] **Step 9: Commit**

```bash
cd eventlog-lab
git add go.mod go.sum eventlog/ crdt/ README.md
git commit -m "feat(eventlog): Postgres central store with reporting projection"
```

---
