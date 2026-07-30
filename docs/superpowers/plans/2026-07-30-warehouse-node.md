# warehouse-node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an offline-first warehouse service whose source of truth is a local event log, with a central server that arbitrates cross-node invariants and pushes compensating events back down.

**Architecture:** Each warehouse node owns a SQLite append-only event log. Operator commands go through a pure `internal/domain` package that either returns events or a named rule violation — nothing is appended when a node-enforceable invariant fails. Events replicate to a central Postgres server over a resumable gRPC bidirectional stream; central re-validates each event against cross-node state it alone can see, and when it rejects one it keeps the original (history is immutable) and emits a compensating event carrying `CausationID` pointing at the rejected event. Nodes apply compensations unconditionally and surface them in an exceptions projection.

**Tech Stack:** Go (latest stable), `modernc.org/sqlite` (pure Go, no cgo) for node storage, `github.com/jackc/pgx/v5` for the central store, gRPC + protobuf (`google.golang.org/grpc`, `google.golang.org/protobuf`) for the node API and sync, `google.golang.org/protobuf/types/known/timestamppb`.

## Domain vocabulary (read this first)

This plan assumes zero warehouse/ERP background. These terms appear throughout:

- **SKU** — Stock Keeping Unit. The identifier of a product ("WIDGET-BLUE-M"). The **item master** is the catalogue of SKUs and their attributes; it is owned by central and replicated read-only to every node.
- **UoM** — Unit of Measure. A SKU has one **base UoM** (say `EA`, each) and optional **alternate UoMs** with conversion factors (`CASE` = 12 EA). All stock is stored in base UoM; operator input is converted on the way in. "UoM conversion valid for item" means: the UoM the operator typed is either the base UoM or one of that SKU's declared alternates.
- **Lot** — a batch of a SKU produced/received together, with an expiry date. A **lot-tracked** SKU may not move without a lot ID. Expiry matters at pick time: you must not ship expired goods.
- **Location** — a physical place inside one warehouse, with a type: `receiving` (dock), `bulk` (reserve racking), `pick` (face shelves), `staging` (outbound), `quarantine` (held stock). `external` is a sentinel location meaning "outside this warehouse" — the supplier, the customer, another warehouse.
- **Receiving** — recording that a supplier delivery arrived. Goods move `external → receiving`. The delivery is identified by a **delivery note** number printed on the supplier's paperwork, and references a **PO** (purchase order) — the order central placed, which has an ordered quantity.
- **Putaway** — moving received goods from the receiving dock into a storage location (`receiving → bulk`, `bulk → pick`). Nothing enters or leaves the warehouse; it is a pure internal move.
- **Picking** — taking goods off a shelf to fulfil an outbound order: `pick → external`.
- **Reservation** — a soft hold on stock at one key so two orders don't promise the same units. Reserved stock is still physically present but no longer **available**. `available = on_hand - active_reservations`.
- **Cycle count / stock count** — an operator physically recounts a location and records what they see. Where the count differs from the book quantity, the difference is a **variance**, written as a `StockAdjusted` event with a reason code. This is the only sanctioned way to make the book match reality.
- **Transfer** — moving stock between two *warehouses*, i.e. between two nodes. It is **two-phase** because the truck takes time: the source dispatches (its stock goes down now) and the destination receives later (its stock goes up then). Between those two moments the goods belong to neither node — they sit in an **in-transit balance** held at central. In-transit is a real balance, not an accounting fudge: dispatched-minus-received must equal it at all times, and it must be zero once the transfer closes.
- **Compensating event** — a normal event, emitted by central, that undoes the *effect* of an event central decided was invalid. It does not delete the original. It carries `CausationID = <rejected event's ID>` so the audit trail says which event it answers. Compensation is not rollback: downstream events that already consumed the bad stock are left alone, which is why compensation can drive a balance negative. That is accepted, flagged, and resolved by a human doing a stock count.
- **HLC** — Hybrid Logical Clock. A timestamp of `(wall_millis, counter, node_id)` that gives a total order across nodes without a synchronised clock: it advances with the wall clock when the wall clock moves forward, and otherwise bumps the counter; on receiving a remote HLC it takes the max of both and bumps. Ties break on node ID. Used for deterministic ordering of events from different nodes; `RecordedAt` wall clock is audit-only and never ordered on.

## Global Constraints

- Module lives in the top-level directory `warehouse-node/` with its own `go.mod`, module path `github.com/dhiazfathra/local-first-architecture/warehouse-node`. The repository currently contains only a Go `.gitignore` and `docs/` — no Go code exists. Task 1 creates the module.
- Go latest stable. Node storage is SQLite via `modernc.org/sqlite` — pure Go, **no cgo**. Central storage is Postgres via `github.com/jackc/pgx/v5`.
- `internal/domain` is pure: commands in, events or a validation error out. No database, no network, no `time.Now()` inside it (callers pass the instant). Its tests need no fixtures beyond a `State` struct.
- Node-enforceable invariants are validated **before** any append. On violation the command is rejected, nothing is written, and the error names the violated rule.
- Central-enforceable invariants are validated **after** the fact. The original event is persisted regardless; rejection produces a compensating event with `CausationID` set.
- Coding standards, hard requirements: minimal boilerplate, SOLID, DRY, **100% test coverage** — every function, branch and edge case.
- Every task runs `go test ./... -cover` and `golangci-lint run` from `warehouse-node/` before its commit step. No task may conclude with failing or skipped tests.
- Movement events always carry explicit `From` and `To` locations, with `external` as the sentinel for the outside world. Every movement is a balanced pair, so the stock projection is one code path and reconciliation is a sum that must come to zero.
- Out of scope (do not build): UI, auth (single trusted operator per node), PO lifecycle beyond a stub reference, costing/financials, multi-tenancy, barcode hardware, reporting beyond the listed projections, cascading compensation through downstream events.

## File Structure

```
warehouse-node/
  go.mod
  .golangci.yml
  Makefile
  internal/domain/          pure: no I/O, no DB, no clock
    ids.go                  NodeID, EventID, LocationCode, StockKey, LocationType
    hlc.go                  HLC type, Tick, Merge, Compare
    event.go                Event, Envelope, payload structs, Decode/Encode
    errors.go               RuleError + rule-name constants
    item.go                 Item, UoM conversion, lot/expiry helpers
    state.go                State, Apply — the single movement code path
    stock.go                Receive/PutAway/Pick commands + stock invariants
    reservation.go          Reserve/Release/Consume commands + availability invariant
    receipt.go              Receipt aggregate lifecycle
    transfer.go             Dispatch/Receive transfer commands (node half)
    count.go                StockCount lifecycle + variance adjustments
  internal/eventlog/
    schema.go               DDL, projection_version bookkeeping
    log.go                  Open, Append (idempotent), Read, cursors, version vector
  internal/projection/
    registry.go             Projection interface, Runner, rebuild-on-version-bump
    stock.go                stock_on_hand
    reservation.go          reservations
    exceptions.go           exceptions (compensations + negative balances)
    transfer.go             transfers (node-side view incl. status)
  internal/node/
    service.go              the seam over log + state + projections (package node)
  internal/transport/grpc/
    node_api.go             operator gRPC service (package grpctransport)
  internal/sync/            package syncrepl, so it does not shadow stdlib sync
    codec.go                domain.Envelope <-> proto conversion, batch size caps
    client.go               node side of the bidi stream, cursor resume, backoff
    server.go               central side of the bidi stream, batching, fan-out
  internal/central/
    store.go                the Store interface, Decision, ReceiptFact, InTransitRow
    memory.go               in-memory Store for the arbiter and integration tests
    schema.sql              Postgres DDL, events partitioned by list on node_id
    postgres.go             pgx-backed Store: events, in-transit, decisions, cursors
  internal/arbiter/
    arbiter.go              Validator chain, Decision, compensation emission
    validators.go           the five rejection rules
  cmd/node/main.go          node server: gRPC node API + sync client
  cmd/central/main.go       central server: gRPC sync server + arbitration
  proto/sync.proto          -> generated package proto/syncpb
  proto/node_api.proto      -> generated package proto/nodeapi
  internal/integration/     two nodes + central, in-process, injectable transport
    harness_test.go
    transfer_test.go
    scenarios_test.go
    determinism_test.go
  README.md
```

---

### Task 1: Module bootstrap, identifiers, and the hybrid logical clock

Creates the Go module and the two files every later task imports: identifier types and the HLC.

**Files:**
- Create: `warehouse-node/go.mod`
- Create: `warehouse-node/.golangci.yml`
- Create: `warehouse-node/Makefile`
- Create: `warehouse-node/internal/domain/ids.go`
- Create: `warehouse-node/internal/domain/hlc.go`
- Test: `warehouse-node/internal/domain/hlc_test.go`
- Test: `warehouse-node/internal/domain/ids_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `domain.NodeID` (string), `domain.EventID{NodeID NodeID; Seq uint64}`, `func (EventID) String() string`
  - `domain.LocationCode` (string), `domain.External LocationCode = "external"`
  - `domain.LocationType` (string) with `LocReceiving`, `LocBulk`, `LocPick`, `LocStaging`, `LocQuarantine`
  - `domain.StockKey{SKU string; Location LocationCode; LotID string}`
  - `domain.HLC{Wall int64; Counter uint32; Node NodeID}`
  - `func (HLC) Compare(other HLC) int`
  - `func Tick(prev HLC, nowMillis int64, node NodeID) HLC`
  - `func Merge(local, remote HLC, nowMillis int64, node NodeID) HLC`

- [ ] **Step 1: Create the module and tooling**

```bash
mkdir -p warehouse-node/internal/domain
cd warehouse-node
go mod init github.com/dhiazfathra/local-first-architecture/warehouse-node
```

`warehouse-node/.golangci.yml`:

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
    - gocritic
    - errorlint
    - misspell
formatters:
  enable:
    - gofmt
    - goimports
```

`warehouse-node/Makefile`:

```make
.PHONY: test lint proto
test:
	go test ./... -cover

lint:
	golangci-lint run

proto:
	protoc --go_out=. --go_opt=module=github.com/dhiazfathra/local-first-architecture/warehouse-node \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/dhiazfathra/local-first-architecture/warehouse-node \
	       proto/sync.proto proto/node_api.proto
```

- [ ] **Step 2: Write the failing tests**

`warehouse-node/internal/domain/ids_test.go`:

```go
package domain

import "testing"

func TestEventIDString(t *testing.T) {
	got := EventID{NodeID: "wh-a", Seq: 42}.String()
	if got != "wh-a/42" {
		t.Fatalf("got %q, want %q", got, "wh-a/42")
	}
}

func TestLocationTypeValid(t *testing.T) {
	tests := []struct {
		name string
		in   LocationType
		want bool
	}{
		{"receiving", LocReceiving, true},
		{"bulk", LocBulk, true},
		{"pick", LocPick, true},
		{"staging", LocStaging, true},
		{"quarantine", LocQuarantine, true},
		{"empty", LocationType(""), false},
		{"nonsense", LocationType("mezzanine"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.Valid(); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}
```

`warehouse-node/internal/domain/hlc_test.go`:

```go
package domain

import "testing"

func TestTick(t *testing.T) {
	tests := []struct {
		name string
		prev HLC
		now  int64
		want HLC
	}{
		{
			name: "wall clock moved forward resets counter",
			prev: HLC{Wall: 100, Counter: 7, Node: "a"},
			now:  200,
			want: HLC{Wall: 200, Counter: 0, Node: "a"},
		},
		{
			name: "wall clock equal bumps counter",
			prev: HLC{Wall: 100, Counter: 7, Node: "a"},
			now:  100,
			want: HLC{Wall: 100, Counter: 8, Node: "a"},
		},
		{
			name: "wall clock went backwards keeps prev wall and bumps counter",
			prev: HLC{Wall: 100, Counter: 7, Node: "a"},
			now:  50,
			want: HLC{Wall: 100, Counter: 8, Node: "a"},
		},
		{
			name: "zero prev adopts now",
			prev: HLC{},
			now:  10,
			want: HLC{Wall: 10, Counter: 0, Node: "a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Tick(tt.prev, tt.now, "a"); got != tt.want {
				t.Fatalf("Tick() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name           string
		local, remote  HLC
		now            int64
		want           HLC
	}{
		{
			name:   "now dominates both",
			local:  HLC{Wall: 100, Counter: 3, Node: "a"},
			remote: HLC{Wall: 120, Counter: 9, Node: "b"},
			now:    200,
			want:   HLC{Wall: 200, Counter: 0, Node: "a"},
		},
		{
			name:   "remote ahead of local and now",
			local:  HLC{Wall: 100, Counter: 3, Node: "a"},
			remote: HLC{Wall: 300, Counter: 9, Node: "b"},
			now:    200,
			want:   HLC{Wall: 300, Counter: 10, Node: "a"},
		},
		{
			name:   "local ahead of remote and now",
			local:  HLC{Wall: 400, Counter: 3, Node: "a"},
			remote: HLC{Wall: 300, Counter: 9, Node: "b"},
			now:    200,
			want:   HLC{Wall: 400, Counter: 4, Node: "a"},
		},
		{
			name:   "equal walls takes max counter plus one",
			local:  HLC{Wall: 300, Counter: 3, Node: "a"},
			remote: HLC{Wall: 300, Counter: 9, Node: "b"},
			now:    200,
			want:   HLC{Wall: 300, Counter: 10, Node: "a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Merge(tt.local, tt.remote, tt.now, "a"); got != tt.want {
				t.Fatalf("Merge() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestHLCCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b HLC
		want int
	}{
		{"wall less", HLC{Wall: 1}, HLC{Wall: 2}, -1},
		{"wall greater", HLC{Wall: 3}, HLC{Wall: 2}, 1},
		{"counter less", HLC{Wall: 1, Counter: 1}, HLC{Wall: 1, Counter: 2}, -1},
		{"counter greater", HLC{Wall: 1, Counter: 5}, HLC{Wall: 1, Counter: 2}, 1},
		{"node breaks tie less", HLC{Wall: 1, Counter: 1, Node: "a"}, HLC{Wall: 1, Counter: 1, Node: "b"}, -1},
		{"node breaks tie greater", HLC{Wall: 1, Counter: 1, Node: "c"}, HLC{Wall: 1, Counter: 1, Node: "b"}, 1},
		{"fully equal", HLC{Wall: 1, Counter: 1, Node: "a"}, HLC{Wall: 1, Counter: 1, Node: "a"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Fatalf("Compare() = %d, want %d", got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd warehouse-node && go test ./internal/domain/ -run 'TestTick|TestMerge|TestHLCCompare|TestEventIDString|TestLocationTypeValid' -v`
Expected: FAIL — build error, `undefined: EventID`, `undefined: Tick`, etc.

- [ ] **Step 4: Write `ids.go`**

```go
// Package domain holds the pure warehouse domain: commands in, events or a
// named rule violation out. It performs no I/O, touches no database, and never
// reads the clock — callers pass the instant.
package domain

import "strconv"

// NodeID identifies one warehouse node. It is also the tiebreaker in HLC
// ordering, so it must be unique and stable for the life of the node.
type NodeID string

// EventID is the globally unique identity of an event: the node that emitted it
// plus that node's monotonic sequence number. Central never renumbers events.
type EventID struct {
	NodeID NodeID `json:"node_id"`
	Seq    uint64 `json:"seq"`
}

// String renders an EventID as "<node>/<seq>" for logs and audit records.
func (e EventID) String() string {
	return string(e.NodeID) + "/" + strconv.FormatUint(e.Seq, 10)
}

// LocationCode identifies a physical place within one warehouse.
type LocationCode string

// External is the sentinel location standing for everything outside this
// warehouse: suppliers, customers, other nodes. Every movement names a From and
// a To, so receiving is external->receiving and picking is pick->external. That
// keeps the stock projection a single code path and makes reconciliation a sum
// that must come to zero.
const External LocationCode = "external"

// LocationType classifies what a location is for.
type LocationType string

// The five location types a warehouse node models.
const (
	LocReceiving  LocationType = "receiving"
	LocBulk       LocationType = "bulk"
	LocPick       LocationType = "pick"
	LocStaging    LocationType = "staging"
	LocQuarantine LocationType = "quarantine"
)

// Valid reports whether t is one of the known location types.
func (t LocationType) Valid() bool {
	switch t {
	case LocReceiving, LocBulk, LocPick, LocStaging, LocQuarantine:
		return true
	default:
		return false
	}
}

// StockKey is the grain of every stock balance: one SKU, in one location, of one
// lot. LotID is empty for SKUs that are not lot-tracked.
type StockKey struct {
	SKU      string       `json:"sku"`
	Location LocationCode `json:"location"`
	LotID    string       `json:"lot_id"`
}
```

- [ ] **Step 5: Write `hlc.go`**

```go
package domain

// HLC is a hybrid logical clock reading. Wall is Unix milliseconds, Counter
// disambiguates events sharing a wall reading, and Node breaks remaining ties so
// that ordering is total across nodes without requiring synchronised clocks.
//
// Ordering events on HLC rather than on wall time is what lets two nodes that
// have never spoken produce a log that replays identically everywhere.
type HLC struct {
	Wall    int64  `json:"wall"`
	Counter uint32 `json:"counter"`
	Node    NodeID `json:"node"`
}

// Compare returns -1, 0 or 1 ordering a before, equal to, or after other.
func (h HLC) Compare(other HLC) int {
	switch {
	case h.Wall != other.Wall:
		return sign(h.Wall - other.Wall)
	case h.Counter != other.Counter:
		return sign(int64(h.Counter) - int64(other.Counter))
	case h.Node != other.Node:
		if h.Node < other.Node {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func sign(d int64) int {
	if d < 0 {
		return -1
	}
	return 1
}

// Tick produces the next local HLC for an event emitted now. If the wall clock
// has advanced past the previous reading it is adopted and the counter resets;
// otherwise (equal, or the clock jumped backwards) the previous wall reading is
// kept and the counter bumps, so the clock never goes backwards.
func Tick(prev HLC, nowMillis int64, node NodeID) HLC {
	if nowMillis > prev.Wall {
		return HLC{Wall: nowMillis, Counter: 0, Node: node}
	}
	return HLC{Wall: prev.Wall, Counter: prev.Counter + 1, Node: node}
}

// Merge produces the next local HLC after observing a remote reading, which is
// how causality crosses the sync stream: anything this node emits after seeing
// remote sorts after remote.
func Merge(local, remote HLC, nowMillis int64, node NodeID) HLC {
	maxWall := max64(max64(local.Wall, remote.Wall), nowMillis)
	switch {
	case maxWall == nowMillis && nowMillis > local.Wall && nowMillis > remote.Wall:
		return HLC{Wall: nowMillis, Counter: 0, Node: node}
	case local.Wall == remote.Wall && local.Wall == maxWall:
		return HLC{Wall: maxWall, Counter: maxU32(local.Counter, remote.Counter) + 1, Node: node}
	case local.Wall == maxWall:
		return HLC{Wall: maxWall, Counter: local.Counter + 1, Node: node}
	default:
		return HLC{Wall: maxWall, Counter: remote.Counter + 1, Node: node}
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxU32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/domain/ -cover -v`
Expected: PASS, coverage 100.0% of statements.

- [ ] **Step 7: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean. Fix every warning before continuing.

- [ ] **Step 8: Commit**

```bash
git add warehouse-node
git commit -m "feat(warehouse-node): bootstrap module with identifiers and hybrid logical clock

- create go.mod, golangci config and Makefile targets for test/lint/proto
- add NodeID, EventID, LocationCode/Type, StockKey identifier types
- add HLC with Tick, Merge and total-order Compare"
```

---

### Task 2: Event envelope, payload types, rule errors, and the item master

Defines the wire/storage shape of every event, the error type that names a violated
invariant, and the replicated read-only item master with UoM conversion and lot expiry.

**Files:**
- Create: `warehouse-node/internal/domain/event.go`
- Create: `warehouse-node/internal/domain/errors.go`
- Create: `warehouse-node/internal/domain/item.go`
- Test: `warehouse-node/internal/domain/event_test.go`
- Test: `warehouse-node/internal/domain/errors_test.go`
- Test: `warehouse-node/internal/domain/item_test.go`

**Interfaces:**
- Consumes: `domain.EventID`, `domain.HLC`, `domain.LocationCode`, `domain.StockKey` (Task 1).
- Produces:
  - `domain.Event{Type string; AggregateID string; Payload any}` — what commands return, before the log assigns identity.
  - `domain.Envelope{ID EventID; AggregateID string; Type string; HLC HLC; RecordedAt time.Time; CausationID *EventID; Payload json.RawMessage}`
  - Type-name constants: `TypeGoodsReceived`, `TypePutAway`, `TypePicked`, `TypeStockAdjusted`, `TypeStockReserved`, `TypeReservationReleased`, `TypeReservationConsumed`, `TypeTransferDispatched`, `TypeTransferReceived`, `TypeReceiptOpened`, `TypeReceiptLineRecorded`, `TypeReceiptClosed`, `TypeCountStarted`, `TypeCountLineCounted`, `TypeCountClosed`, `TypeItemUpserted`, `TypeLocationRegistered`
  - `domain.Movement{SKU, LotID string; From, To LocationCode; Qty float64}`
  - Payload structs: `GoodsReceived`, `PutAway`, `Picked`, `StockAdjusted`, `StockReserved`, `ReservationReleased`, `ReservationConsumed`, `TransferDispatched`, `TransferReceived`, `ReceiptOpened`, `ReceiptLineRecorded`, `ReceiptClosed`, `CountStarted`, `CountLineCounted`, `CountClosed`, `ItemUpserted`, `LocationRegistered`
  - `func NewEnvelope(id EventID, hlc HLC, recordedAt time.Time, causation *EventID, e Event) (Envelope, error)`
  - `func DecodePayload(env Envelope) (any, error)`
  - `func (m Movement) FromKey() StockKey`, `func (m Movement) ToKey() StockKey`
  - `domain.RuleError{Rule, Detail string}` + `func (RuleError) Error() string`, `func Violation(rule, format string, args ...any) error`, `func IsViolation(err error, rule string) bool`
  - Rule constants: `RuleStockNonNegative`, `RuleReservationAvailable`, `RuleLotNotExpired`, `RuleUoMValid`, `RuleSKUExists`, `RuleLocationExists`, `RuleLotRequired`, `RuleAggregateState`, `RuleQtyPositive`
  - Reason-code constants: `ReasonCountVariance`, `ReasonPOOverReceipt`, `ReasonDuplicateReceipt`, `ReasonUnknownSKU`, `ReasonTransferRejected`, `ReasonTransferOverReceipt`
  - `domain.Item{SKU, Description string; BaseUoM UoM; AltUoM map[UoM]float64; LotTracked bool; ShelfLifeDays int; Deleted bool}`
  - `func (Item) ToBase(qty float64, u UoM) (float64, error)`
  - `domain.Lot{ID, SKU string; ExpiresOn time.Time}`, `func (Lot) ExpiredAt(t time.Time) bool`

- [ ] **Step 1: Write the failing tests for errors**

`warehouse-node/internal/domain/errors_test.go`:

```go
package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestRuleErrorMessageNamesTheRule(t *testing.T) {
	err := Violation(RuleStockNonNegative, "location %s would go to %v", LocationCode("PICK-01"), -3.0)
	want := "invariant violated [stock_non_negative]: location PICK-01 would go to -3"
	if err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestIsViolation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		rule string
		want bool
	}{
		{"matching rule", Violation(RuleUoMValid, "x"), RuleUoMValid, true},
		{"different rule", Violation(RuleUoMValid, "x"), RuleSKUExists, false},
		{"wrapped matching rule", fmt.Errorf("wrapped: %w", Violation(RuleSKUExists, "x")), RuleSKUExists, true},
		{"foreign error", errors.New("boom"), RuleSKUExists, false},
		{"nil error", nil, RuleSKUExists, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsViolation(tt.err, tt.rule); got != tt.want {
				t.Fatalf("IsViolation() = %v, want %v", got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Write the failing tests for the item master**

`warehouse-node/internal/domain/item_test.go`:

```go
package domain

import (
	"testing"
	"time"
)

func widget() Item {
	return Item{
		SKU:           "WIDGET",
		Description:   "Blue widget",
		BaseUoM:       "EA",
		AltUoM:        map[UoM]float64{"CASE": 12, "PALLET": 480},
		LotTracked:    true,
		ShelfLifeDays: 30,
	}
}

func TestItemToBase(t *testing.T) {
	tests := []struct {
		name    string
		item    Item
		qty     float64
		uom     UoM
		want    float64
		wantErr string // "" means no error
	}{
		{name: "base uom passes through", item: widget(), qty: 5, uom: "EA", want: 5},
		{name: "alternate uom multiplies by factor", item: widget(), qty: 3, uom: "CASE", want: 36},
		{name: "second alternate uom", item: widget(), qty: 2, uom: "PALLET", want: 960},
		{name: "unknown uom is rejected", item: widget(), qty: 1, uom: "TONNE", wantErr: RuleUoMValid},
		{name: "empty uom is rejected", item: widget(), qty: 1, uom: "", wantErr: RuleUoMValid},
		{
			name:    "non-positive factor in master is rejected",
			item:    Item{SKU: "X", BaseUoM: "EA", AltUoM: map[UoM]float64{"BAD": 0}},
			qty:     1,
			uom:     "BAD",
			wantErr: RuleUoMValid,
		},
		{name: "zero quantity is rejected", item: widget(), qty: 0, uom: "EA", wantErr: RuleQtyPositive},
		{name: "negative quantity is rejected", item: widget(), qty: -1, uom: "EA", wantErr: RuleQtyPositive},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.item.ToBase(tt.qty, tt.uom)
			if tt.wantErr != "" {
				if !IsViolation(err, tt.wantErr) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("ToBase() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLotExpiredAt(t *testing.T) {
	expiry := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	lot := Lot{ID: "L1", SKU: "WIDGET", ExpiresOn: expiry}
	tests := []struct {
		name string
		lot  Lot
		at   time.Time
		want bool
	}{
		{"day before is fine", lot, expiry.AddDate(0, 0, -1), false},
		{"expiry day itself is still usable", lot, expiry, false},
		{"later that same day is still usable", lot, expiry.Add(23 * time.Hour), false},
		{"day after is expired", lot, expiry.AddDate(0, 0, 1), true},
		{"no expiry date never expires", Lot{ID: "L2", SKU: "WIDGET"}, expiry.AddDate(9, 0, 0), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lot.ExpiredAt(tt.at); got != tt.want {
				t.Fatalf("ExpiredAt() = %v, want %v", got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 3: Write the failing tests for envelopes and payload decoding**

`warehouse-node/internal/domain/event_test.go`:

```go
package domain

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestMovementKeys(t *testing.T) {
	m := Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: 10}
	if got, want := m.FromKey(), (StockKey{SKU: "WIDGET", Location: External, LotID: "L1"}); got != want {
		t.Fatalf("FromKey() = %+v, want %+v", got, want)
	}
	if got, want := m.ToKey(), (StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}); got != want {
		t.Fatalf("ToKey() = %+v, want %+v", got, want)
	}
}

func TestNewEnvelopeRoundTripsEveryPayloadType(t *testing.T) {
	at := time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)
	mv := Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: 10}
	tests := []struct {
		typ     string
		payload any
	}{
		{TypeGoodsReceived, GoodsReceived{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1", Move: mv}},
		{TypePutAway, PutAway{Move: mv}},
		{TypePicked, Picked{Move: mv, OrderRef: "SO-1"}},
		{TypeStockAdjusted, StockAdjusted{Move: mv, Reason: ReasonCountVariance}},
		{TypeStockReserved, StockReserved{ReservationID: "RS1", Key: mv.ToKey(), Qty: 4}},
		{TypeReservationReleased, ReservationReleased{ReservationID: "RS1"}},
		{TypeReservationConsumed, ReservationConsumed{ReservationID: "RS1"}},
		{TypeTransferDispatched, TransferDispatched{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", Lines: []Movement{mv}}},
		{TypeTransferReceived, TransferReceived{TransferID: "T1", Lines: []Movement{mv}}},
		{TypeReceiptOpened, ReceiptOpened{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1"}},
		{TypeReceiptLineRecorded, ReceiptLineRecorded{ReceiptID: "R1", LineNo: 1, SKU: "WIDGET", LotID: "L1", QtyBase: 10}},
		{TypeReceiptClosed, ReceiptClosed{ReceiptID: "R1"}},
		{TypeCountStarted, CountStarted{CountID: "C1", Location: "PICK-01"}},
		{TypeCountLineCounted, CountLineCounted{CountID: "C1", Key: mv.ToKey(), CountedQty: 9}},
		{TypeCountClosed, CountClosed{CountID: "C1"}},
		{TypeItemUpserted, ItemUpserted{Item: widget()}},
		{TypeLocationRegistered, LocationRegistered{Code: "PICK-01", Type: LocPick}},
	}
	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			env, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: 1}, HLC{Wall: 1, Node: "wh-a"}, at, nil,
				Event{Type: tt.typ, AggregateID: "agg", Payload: tt.payload})
			if err != nil {
				t.Fatalf("NewEnvelope: %v", err)
			}
			if env.Type != tt.typ || env.AggregateID != "agg" || !env.RecordedAt.Equal(at) {
				t.Fatalf("envelope header wrong: %+v", env)
			}
			got, err := DecodePayload(env)
			if err != nil {
				t.Fatalf("DecodePayload: %v", err)
			}
			if !reflect.DeepEqual(got, tt.payload) {
				t.Fatalf("round trip mismatch:\n got %#v\nwant %#v", got, tt.payload)
			}
		})
	}
}

func TestNewEnvelopeCarriesCausationID(t *testing.T) {
	cause := EventID{NodeID: "wh-a", Seq: 3}
	env, err := NewEnvelope(EventID{NodeID: "central", Seq: 1}, HLC{Wall: 9, Node: "central"}, time.Unix(0, 0), &cause,
		Event{Type: TypeStockAdjusted, AggregateID: "WIDGET", Payload: StockAdjusted{Reason: ReasonPOOverReceipt}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if env.CausationID == nil || *env.CausationID != cause {
		t.Fatalf("CausationID = %v, want %v", env.CausationID, cause)
	}
}

func TestNewEnvelopeRejectsUnmarshalablePayload(t *testing.T) {
	_, err := NewEnvelope(EventID{NodeID: "a", Seq: 1}, HLC{}, time.Unix(0, 0), nil,
		Event{Type: TypePutAway, AggregateID: "x", Payload: math.Inf(1)})
	if err == nil {
		t.Fatal("expected an error encoding a non-JSON payload")
	}
}

func TestDecodePayloadErrors(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
	}{
		{"unknown type", Envelope{Type: "NopeHappened", Payload: []byte(`{}`)}},
		{"malformed json for known type", Envelope{Type: TypePutAway, Payload: []byte(`{"move":`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodePayload(tt.env); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cd warehouse-node && go test ./internal/domain/ -v`
Expected: FAIL — `undefined: Violation`, `undefined: Item`, `undefined: NewEnvelope`, etc.

- [ ] **Step 5: Write `errors.go`**

```go
package domain

import (
	"errors"
	"fmt"
)

// Rule names. Every node-enforced invariant failure carries one of these so the
// operator sees exactly which rule stopped the command, and so tests can assert
// on the rule rather than on message text.
const (
	RuleStockNonNegative     = "stock_non_negative"
	RuleReservationAvailable = "reservation_within_available"
	RuleLotNotExpired        = "lot_not_expired"
	RuleUoMValid             = "uom_valid_for_item"
	RuleSKUExists            = "sku_exists_in_item_master"
	RuleLocationExists       = "location_exists"
	RuleLotRequired          = "lot_required_for_tracked_item"
	RuleAggregateState       = "aggregate_state"
	RuleQtyPositive          = "quantity_positive"
)

// Adjustment reason codes. The first is produced by a node closing a stock
// count; the rest are produced only by central when it compensates an event.
const (
	ReasonCountVariance       = "count_variance"
	ReasonPOOverReceipt       = "po_overreceipt"
	ReasonDuplicateReceipt    = "duplicate_receipt"
	ReasonUnknownSKU          = "unknown_sku"
	ReasonTransferRejected    = "transfer_rejected"
	ReasonTransferOverReceipt = "transfer_overreceipt"
)

// RuleError reports that a command violated a named invariant. Nothing was
// appended to the log when one of these is returned.
type RuleError struct {
	Rule   string
	Detail string
}

// Error names the violated rule so the message is actionable on its own.
func (e RuleError) Error() string {
	return fmt.Sprintf("invariant violated [%s]: %s", e.Rule, e.Detail)
}

// Violation builds a RuleError for the named rule.
func Violation(rule, format string, args ...any) error {
	return RuleError{Rule: rule, Detail: fmt.Sprintf(format, args...)}
}

// IsViolation reports whether err is (or wraps) a violation of the named rule.
func IsViolation(err error, rule string) bool {
	var re RuleError
	return errors.As(err, &re) && re.Rule == rule
}
```

- [ ] **Step 6: Write `item.go`**

```go
package domain

import "time"

// UoM is a unit of measure code, such as "EA" or "CASE".
type UoM string

// Item is one row of the item master. The master is owned by central and
// replicated read-only to every node: a node never emits Item events, it applies
// ItemUpserted events arriving on the sync stream. A node's copy can therefore be
// stale, which is why central re-checks SKU existence after the fact.
type Item struct {
	SKU string `json:"sku"`
	// Description is free text shown to the operator.
	Description string `json:"description"`
	// BaseUoM is the unit every stock balance is stored in.
	BaseUoM UoM `json:"base_uom"`
	// AltUoM maps an alternate unit to how many base units it contains.
	AltUoM map[UoM]float64 `json:"alt_uom,omitempty"`
	// LotTracked requires every movement of this SKU to name a lot.
	LotTracked bool `json:"lot_tracked"`
	// ShelfLifeDays is how long after receipt a new lot stays usable.
	ShelfLifeDays int `json:"shelf_life_days"`
	// Deleted marks a SKU withdrawn at central. Nodes with a stale master may
	// still transact against it; central compensates those events.
	Deleted bool `json:"deleted"`
}

// ToBase converts an operator-entered quantity into base units, enforcing the
// node-side "UoM conversion valid for item" invariant: the unit must be the base
// unit or one of this item's declared alternates, with a usable factor.
func (i Item) ToBase(qty float64, u UoM) (float64, error) {
	if qty <= 0 {
		return 0, Violation(RuleQtyPositive, "quantity %v must be greater than zero", qty)
	}
	if u == i.BaseUoM {
		return qty, nil
	}
	factor, ok := i.AltUoM[u]
	if !ok {
		return 0, Violation(RuleUoMValid, "uom %q is not base %q nor an alternate of sku %s", u, i.BaseUoM, i.SKU)
	}
	if factor <= 0 {
		return 0, Violation(RuleUoMValid, "uom %q of sku %s has non-positive conversion factor %v", u, i.SKU, factor)
	}
	return qty * factor, nil
}

// Lot is a received batch of one SKU with an expiry date. A zero ExpiresOn means
// the lot never expires.
type Lot struct {
	ID        string    `json:"id"`
	SKU       string    `json:"sku"`
	ExpiresOn time.Time `json:"expires_on"`
}

// ExpiredAt reports whether the lot is past expiry at instant t. Expiry is a
// date, not a timestamp: the whole expiry day is still usable, so only the day
// after counts as expired. That coarseness is why the node's local clock is good
// enough to enforce this invariant without consulting central.
func (l Lot) ExpiredAt(t time.Time) bool {
	if l.ExpiresOn.IsZero() {
		return false
	}
	last := l.ExpiresOn.AddDate(0, 0, 1).Truncate(24 * time.Hour)
	return !t.UTC().Before(last)
}
```

- [ ] **Step 7: Write `event.go`**

```go
package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event is what a command returns: a type name, the aggregate it belongs to, and
// a payload struct. Identity, HLC and timestamps are assigned by the event log,
// keeping the domain free of clocks and sequence state.
type Event struct {
	Type        string
	AggregateID string
	Payload     any
}

// Envelope is the stored and replicated form of an event.
type Envelope struct {
	ID          EventID         `json:"id"`
	AggregateID string          `json:"aggregate_id"`
	Type        string          `json:"type"`
	HLC         HLC             `json:"hlc"`
	// RecordedAt is the emitting wall clock, kept for audit only. Ordering uses
	// HLC, never this.
	RecordedAt time.Time `json:"recorded_at"`
	// CausationID is set only on compensating events emitted by central, and
	// points at the event being compensated.
	CausationID *EventID        `json:"causation_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

// Event type names, used as the discriminator in Envelope.Type.
const (
	TypeGoodsReceived        = "GoodsReceived"
	TypePutAway              = "PutAway"
	TypePicked               = "Picked"
	TypeStockAdjusted        = "StockAdjusted"
	TypeStockReserved        = "StockReserved"
	TypeReservationReleased  = "ReservationReleased"
	TypeReservationConsumed  = "ReservationConsumed"
	TypeTransferDispatched   = "TransferDispatched"
	TypeTransferReceived     = "TransferReceived"
	TypeReceiptOpened        = "ReceiptOpened"
	TypeReceiptLineRecorded  = "ReceiptLineRecorded"
	TypeReceiptClosed        = "ReceiptClosed"
	TypeCountStarted         = "CountStarted"
	TypeCountLineCounted     = "CountLineCounted"
	TypeCountClosed          = "CountClosed"
	TypeItemUpserted         = "ItemUpserted"
	TypeLocationRegistered   = "LocationRegistered"
)

// Movement is a balanced stock move: Qty leaves From and arrives at To. Using
// External as one side models the outside world, so receiving, picking,
// putaway and transfers are all the same shape and the stock projection is one
// code path whose totals must sum to zero.
type Movement struct {
	SKU   string       `json:"sku"`
	LotID string       `json:"lot_id,omitempty"`
	From  LocationCode `json:"from"`
	To    LocationCode `json:"to"`
	Qty   float64      `json:"qty"`
}

// FromKey is the stock key the movement debits.
func (m Movement) FromKey() StockKey {
	return StockKey{SKU: m.SKU, Location: m.From, LotID: m.LotID}
}

// ToKey is the stock key the movement credits.
func (m Movement) ToKey() StockKey {
	return StockKey{SKU: m.SKU, Location: m.To, LotID: m.LotID}
}

// Payload types.
type (
	// GoodsReceived records a supplier delivery line arriving: external -> receiving.
	GoodsReceived struct {
		ReceiptID    string   `json:"receipt_id"`
		DeliveryNote string   `json:"delivery_note"`
		PORef        string   `json:"po_ref"`
		Move         Movement `json:"move"`
	}
	// PutAway records an internal move from the dock into storage.
	PutAway struct {
		Move Movement `json:"move"`
	}
	// Picked records stock leaving for a customer: pick -> external.
	Picked struct {
		Move     Movement `json:"move"`
		OrderRef string   `json:"order_ref,omitempty"`
	}
	// StockAdjusted corrects a balance, either from a count variance on the node
	// or from a compensation emitted by central. Reason is one of the Reason*
	// constants.
	StockAdjusted struct {
		Move   Movement `json:"move"`
		Reason string   `json:"reason"`
	}
	// StockReserved places a soft hold on stock at one key.
	StockReserved struct {
		ReservationID string   `json:"reservation_id"`
		Key           StockKey `json:"key"`
		Qty           float64  `json:"qty"`
	}
	// ReservationReleased cancels a hold, returning the quantity to available.
	ReservationReleased struct {
		ReservationID string `json:"reservation_id"`
	}
	// ReservationConsumed closes a hold because the stock was picked against it.
	ReservationConsumed struct {
		ReservationID string `json:"reservation_id"`
	}
	// TransferDispatched is the first half of an inter-node transfer. Source
	// stock decrements immediately; central holds the quantity in transit.
	TransferDispatched struct {
		TransferID string     `json:"transfer_id"`
		FromNode   NodeID     `json:"from_node"`
		ToNode     NodeID     `json:"to_node"`
		Lines      []Movement `json:"lines"`
	}
	// TransferReceived is the second half, emitted by the destination node.
	TransferReceived struct {
		TransferID string     `json:"transfer_id"`
		Lines      []Movement `json:"lines"`
	}
	// ReceiptOpened starts a receipt against a supplier delivery note and PO.
	ReceiptOpened struct {
		ReceiptID    string `json:"receipt_id"`
		DeliveryNote string `json:"delivery_note"`
		PORef        string `json:"po_ref"`
	}
	// ReceiptLineRecorded records one line of a receipt in base units. A negative
	// QtyBase is a reversal emitted by central when it compensates a receipt.
	ReceiptLineRecorded struct {
		ReceiptID string  `json:"receipt_id"`
		LineNo    int     `json:"line_no"`
		SKU       string  `json:"sku"`
		LotID     string  `json:"lot_id,omitempty"`
		QtyBase   float64 `json:"qty_base"`
	}
	// ReceiptClosed ends the receipt; no further lines may be recorded.
	ReceiptClosed struct {
		ReceiptID string `json:"receipt_id"`
	}
	// CountStarted opens a physical recount of one location.
	CountStarted struct {
		CountID  string       `json:"count_id"`
		Location LocationCode `json:"location"`
	}
	// CountLineCounted records what the operator physically counted at one key.
	CountLineCounted struct {
		CountID    string   `json:"count_id"`
		Key        StockKey `json:"key"`
		CountedQty float64  `json:"counted_qty"`
	}
	// CountClosed ends the count. Variance adjustments are emitted alongside it.
	CountClosed struct {
		CountID string `json:"count_id"`
	}
	// ItemUpserted replicates one item-master row down from central.
	ItemUpserted struct {
		Item Item `json:"item"`
	}
	// LocationRegistered declares a node-owned stock location.
	LocationRegistered struct {
		Code LocationCode `json:"code"`
		Type LocationType `json:"type"`
	}
)

// payloadFactories maps each event type to a constructor for its payload, so
// decoding is a table lookup rather than a switch repeated per call site.
var payloadFactories = map[string]func() any{
	TypeGoodsReceived:       func() any { return new(GoodsReceived) },
	TypePutAway:             func() any { return new(PutAway) },
	TypePicked:              func() any { return new(Picked) },
	TypeStockAdjusted:       func() any { return new(StockAdjusted) },
	TypeStockReserved:       func() any { return new(StockReserved) },
	TypeReservationReleased: func() any { return new(ReservationReleased) },
	TypeReservationConsumed: func() any { return new(ReservationConsumed) },
	TypeTransferDispatched:  func() any { return new(TransferDispatched) },
	TypeTransferReceived:    func() any { return new(TransferReceived) },
	TypeReceiptOpened:       func() any { return new(ReceiptOpened) },
	TypeReceiptLineRecorded: func() any { return new(ReceiptLineRecorded) },
	TypeReceiptClosed:       func() any { return new(ReceiptClosed) },
	TypeCountStarted:        func() any { return new(CountStarted) },
	TypeCountLineCounted:    func() any { return new(CountLineCounted) },
	TypeCountClosed:         func() any { return new(CountClosed) },
	TypeItemUpserted:        func() any { return new(ItemUpserted) },
	TypeLocationRegistered:  func() any { return new(LocationRegistered) },
}

// NewEnvelope seals an Event into its stored form with the given identity, clock
// reading and optional causation link.
func NewEnvelope(id EventID, hlc HLC, recordedAt time.Time, causation *EventID, e Event) (Envelope, error) {
	raw, err := json.Marshal(e.Payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode payload of %s: %w", e.Type, err)
	}
	return Envelope{
		ID:          id,
		AggregateID: e.AggregateID,
		Type:        e.Type,
		HLC:         hlc,
		RecordedAt:  recordedAt,
		CausationID: causation,
		Payload:     raw,
	}, nil
}

// DecodePayload returns the concrete payload value for an envelope. An unknown
// type is an error, never a skip: silently ignoring an event from the authority
// would fork state.
func DecodePayload(env Envelope) (any, error) {
	factory, ok := payloadFactories[env.Type]
	if !ok {
		return nil, fmt.Errorf("unknown event type %q", env.Type)
	}
	target := factory()
	if err := json.Unmarshal(env.Payload, target); err != nil {
		return nil, fmt.Errorf("decode %s payload: %w", env.Type, err)
	}
	// Return the value, not the pointer, so callers type-switch on payload
	// structs and comparisons in tests are by value.
	return derefPayload(target), nil
}

func derefPayload(p any) any {
	switch v := p.(type) {
	case *GoodsReceived:
		return *v
	case *PutAway:
		return *v
	case *Picked:
		return *v
	case *StockAdjusted:
		return *v
	case *StockReserved:
		return *v
	case *ReservationReleased:
		return *v
	case *ReservationConsumed:
		return *v
	case *TransferDispatched:
		return *v
	case *TransferReceived:
		return *v
	case *ReceiptOpened:
		return *v
	case *ReceiptLineRecorded:
		return *v
	case *ReceiptClosed:
		return *v
	case *CountStarted:
		return *v
	case *CountLineCounted:
		return *v
	case *CountClosed:
		return *v
	case *ItemUpserted:
		return *v
	default:
		return *(p.(*LocationRegistered))
	}
}
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/domain/ -cover -v`
Expected: PASS, coverage 100.0%. If `derefPayload`'s default branch is uncovered, the
`TypeLocationRegistered` case in `TestNewEnvelopeRoundTripsEveryPayloadType` covers it —
confirm with `go test ./internal/domain/ -coverprofile=/tmp/c.out && go tool cover -func=/tmp/c.out`.

- [ ] **Step 9: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 10: Commit**

```bash
git add warehouse-node/internal/domain
git commit -m "feat(domain): add event envelope, payload types, rule errors and item master

- Event/Envelope with HLC ordering, audit-only RecordedAt and CausationID
- all fifteen domain payload types plus ItemUpserted and LocationRegistered
- Movement with explicit from/to and the external sentinel
- RuleError naming the violated invariant, plus adjustment reason codes
- Item.ToBase enforcing UoM validity; Lot.ExpiredAt treating expiry as a date"
```

---

### Task 3: Domain state and the single Apply code path

`State` is the in-memory fold of the log that commands validate against. `Apply` is the
only place events change state, and because every movement is a balanced pair it is one
code path: debit `FromKey`, credit `ToKey`. `Apply` deliberately permits negative
balances — a compensation from central can drive a location negative and the spec
requires that be recorded and flagged, not hidden. Prevention lives in the command
functions (Tasks 4-8), never here.

**Files:**
- Create: `warehouse-node/internal/domain/state.go`
- Test: `warehouse-node/internal/domain/state_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1-2.
- Produces:
  - `domain.ReservationStatus` (string) with `ResActive`, `ResReleased`, `ResConsumed`
  - `domain.Reservation{ID string; Key StockKey; Qty float64; Status ReservationStatus}`
  - `domain.ReceiptStatus` (string) with `ReceiptOpen`, `ReceiptClosedStatus`
  - `domain.ReceiptState{ID, DeliveryNote, PORef string; Status ReceiptStatus; Lines map[int]ReceiptLineRecorded; NextLineNo int}`
  - `domain.TransferStatus` (string) with `TransferInFlight`, `TransferComplete`, `TransferFailed`
  - `domain.TransferState{ID string; FromNode, ToNode NodeID; Dispatched, Received map[StockKey]float64; Status TransferStatus}`
  - `domain.CountStatus` (string) with `CountOpen`, `CountClosedStatus`
  - `domain.CountState{ID string; Location LocationCode; Status CountStatus; Counted map[StockKey]float64}`
  - `domain.NewState() *State` and `State` with fields `Items map[string]Item`, `Locations map[LocationCode]LocationType`, `Lots map[string]Lot`, `Stock map[StockKey]float64`, `Reservations map[string]Reservation`, `Receipts map[string]*ReceiptState`, `Transfers map[string]*TransferState`, `Counts map[string]*CountState`, `Compensations []Envelope`
  - `func (*State) Apply(env Envelope) error`
  - `func (*State) OnHand(k StockKey) float64`
  - `func (*State) Available(k StockKey) float64`
  - `func (*State) Item(sku string) (Item, error)` — enforces `RuleSKUExists` against the replicated master, including the deleted case

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/domain/state_test.go`:

```go
package domain

import (
	"testing"
	"time"
)

// env is a test helper sealing a payload into an envelope with sequential
// identity. It keeps the table-driven tests free of clock and identity noise.
func env(t *testing.T, seq uint64, typ, agg string, payload any) Envelope {
	t.Helper()
	e, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: seq}, HLC{Wall: int64(seq), Node: "wh-a"},
		time.Unix(int64(seq), 0).UTC(), nil, Event{Type: typ, AggregateID: agg, Payload: payload})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	return e
}

// applyAll folds a sequence of envelopes into a fresh state, failing on error.
func applyAll(t *testing.T, envs ...Envelope) *State {
	t.Helper()
	s := NewState()
	for _, e := range envs {
		if err := s.Apply(e); err != nil {
			t.Fatalf("Apply(%s): %v", e.Type, err)
		}
	}
	return s
}

func TestApplyMovementsAreBalanced(t *testing.T) {
	mv := Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: 10}
	s := applyAll(t,
		env(t, 1, TypeGoodsReceived, "R1", GoodsReceived{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1", Move: mv}),
		env(t, 2, TypePutAway, "WIDGET", PutAway{Move: Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: "PICK-01", Qty: 4}}),
		env(t, 3, TypePicked, "WIDGET", Picked{Move: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 1}, OrderRef: "SO-1"}),
		env(t, 4, TypeStockAdjusted, "WIDGET", StockAdjusted{Move: Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "PICK-01", Qty: 2}, Reason: ReasonCountVariance}),
	)
	want := map[StockKey]float64{
		{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}: 6,
		{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}: 5,
		{SKU: "WIDGET", Location: External, LotID: "L1"}:  -11,
	}
	for k, v := range want {
		if got := s.OnHand(k); got != v {
			t.Errorf("OnHand(%+v) = %v, want %v", k, got, v)
		}
	}
	// Every movement is balanced, so all balances including the external
	// sentinel must sum to zero. This is the reconciliation property.
	var total float64
	for _, v := range s.Stock {
		total += v
	}
	if total != 0 {
		t.Fatalf("balances sum to %v, want 0", total)
	}
}

func TestApplyAllowsNegativeBalanceFromCompensation(t *testing.T) {
	// Received 5, picked all 5, then central compensates the receipt. The node
	// kept working, so compensation drives the location negative. That must be
	// recorded, not clamped.
	cause := EventID{NodeID: "wh-a", Seq: 1}
	comp, err := NewEnvelope(EventID{NodeID: "central", Seq: 1}, HLC{Wall: 99, Node: "central"}, time.Unix(99, 0).UTC(), &cause,
		Event{Type: TypeStockAdjusted, AggregateID: "WIDGET", Payload: StockAdjusted{
			Move:   Movement{SKU: "WIDGET", From: "RECV-01", To: External, Qty: 5},
			Reason: ReasonDuplicateReceipt,
		}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	s := applyAll(t,
		env(t, 1, TypeGoodsReceived, "R1", GoodsReceived{ReceiptID: "R1", Move: Movement{SKU: "WIDGET", From: External, To: "RECV-01", Qty: 5}}),
		env(t, 2, TypePicked, "WIDGET", Picked{Move: Movement{SKU: "WIDGET", From: "RECV-01", To: External, Qty: 5}}),
		comp,
	)
	if got := s.OnHand(StockKey{SKU: "WIDGET", Location: "RECV-01"}); got != -5 {
		t.Fatalf("OnHand = %v, want -5", got)
	}
	if len(s.Compensations) != 1 || s.Compensations[0].ID != comp.ID {
		t.Fatalf("Compensations = %+v, want the one compensating envelope", s.Compensations)
	}
}

func TestApplyReservationLifecycleAffectsAvailable(t *testing.T) {
	key := StockKey{SKU: "WIDGET", Location: "PICK-01"}
	base := []Envelope{
		env(t, 1, TypeGoodsReceived, "R1", GoodsReceived{ReceiptID: "R1", Move: Movement{SKU: "WIDGET", From: External, To: "PICK-01", Qty: 10}}),
		env(t, 2, TypeStockReserved, "RS1", StockReserved{ReservationID: "RS1", Key: key, Qty: 4}),
	}
	tests := []struct {
		name          string
		extra         []Envelope
		wantAvailable float64
		wantStatus    ReservationStatus
	}{
		{name: "active reservation reduces available", wantAvailable: 6, wantStatus: ResActive},
		{
			name:          "released reservation returns to available",
			extra:         []Envelope{env(t, 3, TypeReservationReleased, "RS1", ReservationReleased{ReservationID: "RS1"})},
			wantAvailable: 10,
			wantStatus:    ResReleased,
		},
		{
			name: "consumed reservation stops holding but stock left with the pick",
			extra: []Envelope{
				env(t, 3, TypePicked, "WIDGET", Picked{Move: Movement{SKU: "WIDGET", From: "PICK-01", To: External, Qty: 4}}),
				env(t, 4, TypeReservationConsumed, "RS1", ReservationConsumed{ReservationID: "RS1"}),
			},
			wantAvailable: 6,
			wantStatus:    ResConsumed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := applyAll(t, append(append([]Envelope{}, base...), tt.extra...)...)
			if got := s.Available(key); got != tt.wantAvailable {
				t.Fatalf("Available() = %v, want %v", got, tt.wantAvailable)
			}
			if got := s.Reservations["RS1"].Status; got != tt.wantStatus {
				t.Fatalf("status = %q, want %q", got, tt.wantStatus)
			}
		})
	}
}

func TestApplyAggregateLifecycles(t *testing.T) {
	s := applyAll(t,
		env(t, 1, TypeItemUpserted, "WIDGET", ItemUpserted{Item: widget()}),
		env(t, 2, TypeLocationRegistered, "PICK-01", LocationRegistered{Code: "PICK-01", Type: LocPick}),
		env(t, 3, TypeReceiptOpened, "R1", ReceiptOpened{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1"}),
		env(t, 4, TypeReceiptLineRecorded, "R1", ReceiptLineRecorded{ReceiptID: "R1", LineNo: 1, SKU: "WIDGET", LotID: "L1", QtyBase: 10}),
		env(t, 5, TypeReceiptClosed, "R1", ReceiptClosed{ReceiptID: "R1"}),
		env(t, 6, TypeTransferDispatched, "T1", TransferDispatched{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
			Lines: []Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 3}}}),
		env(t, 7, TypeTransferReceived, "T1", TransferReceived{TransferID: "T1",
			Lines: []Movement{{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: 3}}}),
		env(t, 8, TypeCountStarted, "C1", CountStarted{CountID: "C1", Location: "PICK-01"}),
		env(t, 9, TypeCountLineCounted, "C1", CountLineCounted{CountID: "C1", Key: StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}, CountedQty: 7}),
		env(t, 10, TypeCountClosed, "C1", CountClosed{CountID: "C1"}),
	)

	if _, err := s.Item("WIDGET"); err != nil {
		t.Fatalf("Item: %v", err)
	}
	if s.Locations["PICK-01"] != LocPick {
		t.Fatalf("location type = %q", s.Locations["PICK-01"])
	}
	r := s.Receipts["R1"]
	if r.Status != ReceiptClosedStatus || r.NextLineNo != 2 || r.Lines[1].QtyBase != 10 || r.DeliveryNote != "DN-1" || r.PORef != "PO-1" {
		t.Fatalf("receipt state = %+v", r)
	}
	// Receiving a lot-tracked SKU creates the lot with expiry = receipt day plus
	// shelf life, so the node can enforce expiry at pick time with no help.
	lot, ok := s.Lots["L1"]
	if !ok || !lot.ExpiresOn.Equal(time.Unix(4, 0).UTC().AddDate(0, 0, 30)) {
		t.Fatalf("lot = %+v ok=%v", lot, ok)
	}
	tr := s.Transfers["T1"]
	if tr.Status != TransferComplete || tr.FromNode != "wh-a" || tr.ToNode != "wh-b" {
		t.Fatalf("transfer state = %+v", tr)
	}
	c := s.Counts["C1"]
	if c.Status != CountClosedStatus || c.Counted[StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}] != 7 {
		t.Fatalf("count state = %+v", c)
	}
}

func TestTransferStaysInFlightUntilReceived(t *testing.T) {
	s := applyAll(t, env(t, 1, TypeTransferDispatched, "T1", TransferDispatched{
		TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
		Lines: []Movement{{SKU: "WIDGET", From: "PICK-01", To: External, Qty: 3}},
	}))
	if s.Transfers["T1"].Status != TransferInFlight {
		t.Fatalf("status = %q, want %q", s.Transfers["T1"].Status, TransferInFlight)
	}
}

func TestApplyIgnoresEventsForUnknownAggregates(t *testing.T) {
	// Events can arrive out of order across a sync boundary; Apply must not panic
	// or error on a line for a receipt/count/transfer it has never seen. Later
	// events are folded in when they arrive.
	tests := []struct {
		name string
		e    Envelope
	}{
		{"line for unknown receipt", env(t, 1, TypeReceiptLineRecorded, "R9", ReceiptLineRecorded{ReceiptID: "R9", LineNo: 1, SKU: "WIDGET", QtyBase: 1})},
		{"close of unknown receipt", env(t, 2, TypeReceiptClosed, "R9", ReceiptClosed{ReceiptID: "R9"})},
		{"line for unknown count", env(t, 3, TypeCountLineCounted, "C9", CountLineCounted{CountID: "C9", CountedQty: 1})},
		{"close of unknown count", env(t, 4, TypeCountClosed, "C9", CountClosed{CountID: "C9"})},
		{"receive of unknown transfer", env(t, 5, TypeTransferReceived, "T9", TransferReceived{TransferID: "T9"})},
		{"release of unknown reservation", env(t, 6, TypeReservationReleased, "RS9", ReservationReleased{ReservationID: "RS9"})},
		{"consume of unknown reservation", env(t, 7, TypeReservationConsumed, "RS9", ReservationConsumed{ReservationID: "RS9"})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewState().Apply(tt.e); err != nil {
				t.Fatalf("Apply: %v", err)
			}
		})
	}
}

func TestApplyRejectsUndecodableEvent(t *testing.T) {
	if err := NewState().Apply(Envelope{Type: "NopeHappened", Payload: []byte(`{}`)}); err == nil {
		t.Fatal("expected an error for an unknown event type")
	}
}

func TestStateItem(t *testing.T) {
	s := NewState()
	deleted := widget()
	deleted.SKU = "GONE"
	deleted.Deleted = true
	if err := s.Apply(env(t, 1, TypeItemUpserted, "WIDGET", ItemUpserted{Item: widget()})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := s.Apply(env(t, 2, TypeItemUpserted, "GONE", ItemUpserted{Item: deleted})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	tests := []struct {
		name    string
		sku     string
		wantErr bool
	}{
		{"known sku", "WIDGET", false},
		{"absent from stale master", "NEVERHEARDOFIT", true},
		{"deleted at central", "GONE", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Item(tt.sku)
			if tt.wantErr != IsViolation(err, RuleSKUExists) {
				t.Fatalf("Item(%q) err = %v, wantErr = %v", tt.sku, err, tt.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/domain/ -run 'TestApply|TestState|TestTransferStays' -v`
Expected: FAIL — `undefined: NewState`.

- [ ] **Step 3: Write `state.go`**

```go
package domain

import "time"

// ReservationStatus tracks whether a soft hold is still holding stock.
type ReservationStatus string

// Reservation statuses.
const (
	ResActive   ReservationStatus = "active"
	ResReleased ReservationStatus = "released"
	ResConsumed ReservationStatus = "consumed"
)

// Reservation is a soft hold on stock at one key. Only active reservations
// subtract from available.
type Reservation struct {
	ID     string
	Key    StockKey
	Qty    float64
	Status ReservationStatus
}

// ReceiptStatus is the lifecycle position of a receipt.
type ReceiptStatus string

// Receipt statuses.
const (
	ReceiptOpen         ReceiptStatus = "open"
	ReceiptClosedStatus ReceiptStatus = "closed"
)

// ReceiptState folds one receipt's events: the supplier delivery note it records,
// the PO it is against, and its lines by line number.
type ReceiptState struct {
	ID           string
	DeliveryNote string
	PORef        string
	Status       ReceiptStatus
	Lines        map[int]ReceiptLineRecorded
	NextLineNo   int
}

// TransferStatus is the lifecycle position of an inter-node transfer as this node
// understands it.
type TransferStatus string

// Transfer statuses. InFlight means dispatched but not yet received: the goods are
// in transit and belong to neither node.
const (
	TransferInFlight TransferStatus = "in_flight"
	TransferComplete TransferStatus = "complete"
	TransferFailed   TransferStatus = "failed"
)

// TransferState folds both halves of a transfer keyed by stock key, so the
// in-transit quantity for any key is Dispatched minus Received.
type TransferState struct {
	ID         string
	FromNode   NodeID
	ToNode     NodeID
	Dispatched map[StockKey]float64
	Received   map[StockKey]float64
	Status     TransferStatus
}

// CountStatus is the lifecycle position of a stock count.
type CountStatus string

// Count statuses.
const (
	CountOpen         CountStatus = "open"
	CountClosedStatus CountStatus = "closed"
)

// CountState folds one physical count: which location, and what the operator
// counted at each key.
type CountState struct {
	ID       string
	Location LocationCode
	Status   CountStatus
	Counted  map[StockKey]float64
}

// State is the in-memory fold of a node's log. Commands validate against it and
// never mutate it; only Apply mutates.
type State struct {
	// Items is the replicated read-only item master. It may be stale.
	Items     map[string]Item
	Locations map[LocationCode]LocationType
	Lots      map[string]Lot
	// Stock may hold negative values: a compensation from central can drive a
	// location negative after the node has already shipped the goods. That is
	// recorded and flagged, never clamped.
	Stock        map[StockKey]float64
	Reservations map[string]Reservation
	Receipts     map[string]*ReceiptState
	Transfers    map[string]*TransferState
	Counts       map[string]*CountState
	// Compensations collects every envelope carrying a CausationID, i.e. every
	// event central emitted to undo one of ours. The exceptions projection is
	// built from these.
	Compensations []Envelope
}

// NewState returns an empty state with every map ready to use.
func NewState() *State {
	return &State{
		Items:        map[string]Item{},
		Locations:    map[LocationCode]LocationType{},
		Lots:         map[string]Lot{},
		Stock:        map[StockKey]float64{},
		Reservations: map[string]Reservation{},
		Receipts:     map[string]*ReceiptState{},
		Transfers:    map[string]*TransferState{},
		Counts:       map[string]*CountState{},
	}
}

// OnHand is the book quantity at one key.
func (s *State) OnHand(k StockKey) float64 { return s.Stock[k] }

// Available is on-hand minus every active reservation against the same key. This
// is the quantity a new command may promise.
func (s *State) Available(k StockKey) float64 {
	avail := s.Stock[k]
	for _, r := range s.Reservations {
		if r.Status == ResActive && r.Key == k {
			avail -= r.Qty
		}
	}
	return avail
}

// Item looks a SKU up in the replicated master, enforcing the node-side
// "SKU exists in item master" invariant. The node's master may be stale, so
// central re-checks this after the fact and compensates what it disagrees with.
func (s *State) Item(sku string) (Item, error) {
	it, ok := s.Items[sku]
	if !ok {
		return Item{}, Violation(RuleSKUExists, "sku %q is not in the replicated item master", sku)
	}
	if it.Deleted {
		return Item{}, Violation(RuleSKUExists, "sku %q was deleted at central", sku)
	}
	return it, nil
}

// Apply folds one envelope into the state. It is the only mutator, and every
// movement goes through move(), so stock arithmetic exists exactly once.
func (s *State) Apply(envelope Envelope) error {
	payload, err := DecodePayload(envelope)
	if err != nil {
		return err
	}
	if envelope.CausationID != nil {
		s.Compensations = append(s.Compensations, envelope)
	}
	switch p := payload.(type) {
	case ItemUpserted:
		s.Items[p.Item.SKU] = p.Item
	case LocationRegistered:
		s.Locations[p.Code] = p.Type
	case GoodsReceived:
		s.registerLot(p.Move, envelope.RecordedAt)
		s.move(p.Move)
	case PutAway:
		s.move(p.Move)
	case Picked:
		s.move(p.Move)
	case StockAdjusted:
		s.move(p.Move)
	case StockReserved:
		s.Reservations[p.ReservationID] = Reservation{ID: p.ReservationID, Key: p.Key, Qty: p.Qty, Status: ResActive}
	case ReservationReleased:
		s.setReservationStatus(p.ReservationID, ResReleased)
	case ReservationConsumed:
		s.setReservationStatus(p.ReservationID, ResConsumed)
	case ReceiptOpened:
		s.Receipts[p.ReceiptID] = &ReceiptState{
			ID: p.ReceiptID, DeliveryNote: p.DeliveryNote, PORef: p.PORef,
			Status: ReceiptOpen, Lines: map[int]ReceiptLineRecorded{}, NextLineNo: 1,
		}
	case ReceiptLineRecorded:
		if r, ok := s.Receipts[p.ReceiptID]; ok {
			r.Lines[p.LineNo] = p
			if p.LineNo >= r.NextLineNo {
				r.NextLineNo = p.LineNo + 1
			}
		}
	case ReceiptClosed:
		if r, ok := s.Receipts[p.ReceiptID]; ok {
			r.Status = ReceiptClosedStatus
		}
	case TransferDispatched:
		t := &TransferState{
			ID: p.TransferID, FromNode: p.FromNode, ToNode: p.ToNode,
			Dispatched: map[StockKey]float64{}, Received: map[StockKey]float64{},
			Status: TransferInFlight,
		}
		for _, line := range p.Lines {
			t.Dispatched[StockKey{SKU: line.SKU, LotID: line.LotID}] += line.Qty
			s.move(line)
		}
		s.Transfers[p.TransferID] = t
	case TransferReceived:
		t, ok := s.Transfers[p.TransferID]
		if !ok {
			// The destination node learns of the dispatch only via central. If
			// the receive is folded first, ignore it; the dispatch fills in the
			// rest when it arrives.
			return nil
		}
		for _, line := range p.Lines {
			t.Received[StockKey{SKU: line.SKU, LotID: line.LotID}] += line.Qty
			s.move(line)
		}
		if t.Status == TransferInFlight {
			t.Status = TransferComplete
		}
	case CountStarted:
		s.Counts[p.CountID] = &CountState{ID: p.CountID, Location: p.Location, Status: CountOpen, Counted: map[StockKey]float64{}}
	case CountLineCounted:
		if c, ok := s.Counts[p.CountID]; ok {
			c.Counted[p.Key] = p.CountedQty
		}
	case CountClosed:
		if c, ok := s.Counts[p.CountID]; ok {
			c.Status = CountClosedStatus
		}
	}
	return nil
}

// move applies one balanced movement: the quantity leaves From and arrives at To.
// Zero-valued keys are pruned so replays produce byte-identical maps regardless of
// the path taken to get there, which is what makes determinism testable.
func (s *State) move(m Movement) {
	s.add(m.FromKey(), -m.Qty)
	s.add(m.ToKey(), m.Qty)
}

func (s *State) add(k StockKey, delta float64) {
	v := s.Stock[k] + delta
	if v == 0 {
		delete(s.Stock, k)
		return
	}
	s.Stock[k] = v
}

// registerLot creates the lot record on first receipt of a lot-tracked SKU,
// dating expiry from the receipt instant plus the item's shelf life.
func (s *State) registerLot(m Movement, at time.Time) {
	if m.LotID == "" {
		return
	}
	if _, exists := s.Lots[m.LotID]; exists {
		return
	}
	lot := Lot{ID: m.LotID, SKU: m.SKU}
	if it, ok := s.Items[m.SKU]; ok && it.ShelfLifeDays > 0 {
		lot.ExpiresOn = at.UTC().AddDate(0, 0, it.ShelfLifeDays)
	}
	s.Lots[m.LotID] = lot
}

func (s *State) setReservationStatus(id string, status ReservationStatus) {
	if r, ok := s.Reservations[id]; ok {
		r.Status = status
		s.Reservations[id] = r
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/domain/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 5: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/internal/domain
git commit -m "feat(domain): add State and the single balanced-movement Apply path

- fold every event type into State; movements share one debit/credit code path
- permit negative balances so compensations are recorded, not hidden
- track reservations, receipts, transfers, counts, lots and compensations
- Available subtracts active reservations; Item enforces sku_exists on the
  replicated read-only master including the deleted case"
```

---

### Task 4: Putaway and pick commands — the node-enforced stock invariants

The first two node-owned commands, and the four node-enforced invariants they share:
stock never negative, lot not expired at pick time, UoM valid for the item, SKU exists in
the (possibly stale) replicated master. All four are checked **before** any event is
produced — the command returns a `RuleError` naming the rule and nothing is appended.

Putaway is a purely internal move (`receiving → bulk`, `bulk → pick`). Picking sends stock
to a customer (`pick → external`) and is the only command that cares about lot expiry,
because shipping expired goods is the failure mode expiry exists to prevent.

**Files:**
- Create: `warehouse-node/internal/domain/stock.go`
- Test: `warehouse-node/internal/domain/stock_test.go`

**Interfaces:**
- Consumes: `domain.State`, `domain.Item`, `domain.Movement`, `domain.Violation`, rule constants (Tasks 1-3).
- Produces:
  - `domain.Line{SKU string; LotID string; Qty float64; UoM UoM}` — an operator-entered quantity before conversion, reused by every command that takes stock quantities.
  - `func (*State) ResolveLine(l Line) (Movement, Item, error)` — validates SKU + UoM + lot requirement and returns a partially filled `Movement` (From/To unset) in base units.
  - `func (*State) RequireLocation(code LocationCode) error`
  - `func (*State) RequireOnHand(k StockKey, qty float64) error`
  - `domain.PutAwayCmd{Line Line; From, To LocationCode}` and `func DoPutAway(s *State, c PutAwayCmd) ([]Event, error)`
  - `domain.PickCmd{Line Line; From LocationCode; OrderRef string; At time.Time}` and `func DoPick(s *State, c PickCmd) ([]Event, error)`

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/domain/stock_test.go`:

```go
package domain

import (
	"testing"
	"time"
)

// stocked builds a state with the widget item master, two locations, and 10 units
// of lot L1 in PICK-01 received on the given day. It is the only fixture the pure
// domain tests need.
func stocked(t *testing.T, receivedOn time.Time) *State {
	t.Helper()
	s := NewState()
	seq := uint64(1)
	push := func(typ, agg string, payload any) {
		e, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: seq}, HLC{Wall: int64(seq), Node: "wh-a"},
			receivedOn, nil, Event{Type: typ, AggregateID: agg, Payload: payload})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		if err := s.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		seq++
	}
	push(TypeItemUpserted, "WIDGET", ItemUpserted{Item: widget()})
	push(TypeItemUpserted, "BOLT", ItemUpserted{Item: Item{SKU: "BOLT", BaseUoM: "EA", AltUoM: map[UoM]float64{"BOX": 100}}})
	push(TypeLocationRegistered, "RECV-01", LocationRegistered{Code: "RECV-01", Type: LocReceiving})
	push(TypeLocationRegistered, "PICK-01", LocationRegistered{Code: "PICK-01", Type: LocPick})
	push(TypeLocationRegistered, "BULK-01", LocationRegistered{Code: "BULK-01", Type: LocBulk})
	push(TypeGoodsReceived, "R0", GoodsReceived{ReceiptID: "R0", Move: Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "PICK-01", Qty: 10}})
	return s
}

func TestDoPutAway(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		cmd      PutAwayCmd
		wantRule string // "" means success
		wantMove Movement
	}{
		{
			name:     "moves stock in base units",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 4, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantMove: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: "BULK-01", Qty: 4},
		},
		{
			name:     "converts an alternate uom before checking stock",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0.5, UoM: "CASE"}, From: "PICK-01", To: "BULK-01"},
			wantMove: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: "BULK-01", Qty: 6},
		},
		{
			name:     "would drive the source location negative",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 11, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleStockNonNegative,
		},
		{
			name:     "alternate uom conversion pushes it over the balance",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "CASE"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleStockNonNegative,
		},
		{
			name:     "sku missing from the stale replicated master",
			cmd:      PutAwayCmd{Line: Line{SKU: "GHOST", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleSKUExists,
		},
		{
			name:     "uom not declared for this item",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "BOX"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleUoMValid,
		},
		{
			name:     "lot-tracked sku with no lot",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", Qty: 1, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleLotRequired,
		},
		{
			name:     "unknown source location",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "NOWHERE", To: "BULK-01"},
			wantRule: RuleLocationExists,
		},
		{
			name:     "unknown destination location",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", To: "NOWHERE"},
			wantRule: RuleLocationExists,
		},
		{
			name:     "external is never a putaway endpoint",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", To: External},
			wantRule: RuleLocationExists,
		},
		{
			name:     "non-positive quantity",
			cmd:      PutAwayCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0, UoM: "EA"}, From: "PICK-01", To: "BULK-01"},
			wantRule: RuleQtyPositive,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			before := s.OnHand(StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"})
			events, err := DoPutAway(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				if s.OnHand(StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}) != before {
					t.Fatal("rejected command mutated state")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(events) != 1 || events[0].Type != TypePutAway || events[0].AggregateID != "WIDGET" {
				t.Fatalf("events = %+v", events)
			}
			if got := events[0].Payload.(PutAway).Move; got != tt.wantMove {
				t.Fatalf("move = %+v, want %+v", got, tt.wantMove)
			}
		})
	}
}

func TestDoPick(t *testing.T) {
	// widget() has ShelfLifeDays 30, so lot L1 received 2026-07-01 expires
	// 2026-07-31 and is usable through that whole day.
	received := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		cmd      PickCmd
		wantRule string
		wantQty  float64
	}{
		{
			name:    "picks to external",
			cmd:     PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 3, UoM: "EA"}, From: "PICK-01", OrderRef: "SO-1", At: received.AddDate(0, 0, 5)},
			wantQty: 3,
		},
		{
			name:    "picks on the expiry day itself",
			cmd:     PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", At: time.Date(2026, 7, 31, 23, 0, 0, 0, time.UTC)},
			wantQty: 1,
		},
		{
			name:     "refuses an expired lot",
			cmd:      PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", At: time.Date(2026, 8, 1, 0, 1, 0, 0, time.UTC)},
			wantRule: RuleLotNotExpired,
		},
		{
			name:     "refuses to drive the location negative",
			cmd:      PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 11, UoM: "EA"}, From: "PICK-01", At: received},
			wantRule: RuleStockNonNegative,
		},
		{
			name:     "refuses an unknown sku",
			cmd:      PickCmd{Line: Line{SKU: "GHOST", LotID: "L1", Qty: 1, UoM: "EA"}, From: "PICK-01", At: received},
			wantRule: RuleSKUExists,
		},
		{
			name:     "refuses an unknown location",
			cmd:      PickCmd{Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, From: "NOWHERE", At: received},
			wantRule: RuleLocationExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, received)
			events, err := DoPick(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			p := events[0].Payload.(Picked)
			want := Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: tt.wantQty}
			if p.Move != want || p.OrderRef != tt.cmd.OrderRef {
				t.Fatalf("payload = %+v, want move %+v", p, want)
			}
		})
	}
}

func TestDoPickAllowsUntrackedLotForNonLotItem(t *testing.T) {
	s := stocked(t, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC))
	// Seed BOLT stock, which is not lot-tracked, so no lot is required and no
	// expiry check applies.
	e, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: 99}, HLC{Wall: 99, Node: "wh-a"}, time.Unix(99, 0).UTC(), nil,
		Event{Type: TypeGoodsReceived, AggregateID: "R0", Payload: GoodsReceived{ReceiptID: "R0",
			Move: Movement{SKU: "BOLT", From: External, To: "PICK-01", Qty: 500}}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if err := s.Apply(e); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	events, err := DoPick(s, PickCmd{Line: Line{SKU: "BOLT", Qty: 2, UoM: "BOX"}, From: "PICK-01", At: time.Now()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := events[0].Payload.(Picked).Move.Qty; got != 200 {
		t.Fatalf("qty = %v, want 200 (2 boxes of 100)", got)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/domain/ -run 'TestDoPutAway|TestDoPick' -v`
Expected: FAIL — `undefined: PutAwayCmd`, `undefined: DoPutAway`, `undefined: DoPick`.

- [ ] **Step 3: Write `stock.go`**

```go
package domain

import "time"

// Line is a quantity as the operator entered it: a SKU, an optional lot, an
// amount and the unit that amount is in. Conversion to base units happens in
// ResolveLine, so no command duplicates it.
type Line struct {
	SKU   string  `json:"sku"`
	LotID string  `json:"lot_id,omitempty"`
	Qty   float64 `json:"qty"`
	UoM   UoM     `json:"uom"`
}

// ResolveLine enforces the three node-side line invariants — SKU exists in the
// replicated master, the UoM is valid for that item, and a lot-tracked item names
// a lot — and returns the movement in base units with From/To left for the caller
// to fill in.
func (s *State) ResolveLine(l Line) (Movement, Item, error) {
	item, err := s.Item(l.SKU)
	if err != nil {
		return Movement{}, Item{}, err
	}
	if item.LotTracked && l.LotID == "" {
		return Movement{}, Item{}, Violation(RuleLotRequired, "sku %s is lot-tracked; a lot id is required", l.SKU)
	}
	base, err := item.ToBase(l.Qty, l.UoM)
	if err != nil {
		return Movement{}, Item{}, err
	}
	return Movement{SKU: l.SKU, LotID: l.LotID, Qty: base}, item, nil
}

// RequireLocation enforces that a location is one this node owns. External is
// rejected: it is a sentinel for the outside world, valid only where a command
// explicitly puts it (receiving, picking, transfers), never as operator input.
func (s *State) RequireLocation(code LocationCode) error {
	if _, ok := s.Locations[code]; !ok {
		return Violation(RuleLocationExists, "location %q is not registered on this node", code)
	}
	return nil
}

// RequireOnHand enforces the "stock at a location never negative" invariant for a
// command about to remove qty from k. The node owns its own locations outright, so
// it can decide this alone, synchronously, before appending anything.
func (s *State) RequireOnHand(k StockKey, qty float64) error {
	if onHand := s.OnHand(k); onHand < qty {
		return Violation(RuleStockNonNegative,
			"location %s holds %v of sku %s lot %q; cannot remove %v", k.Location, onHand, k.SKU, k.LotID, qty)
	}
	return nil
}

// RequireLotUsable enforces "lot not expired" for a lot-tracked item at instant at.
func (s *State) RequireLotUsable(lotID string, at time.Time) error {
	lot, ok := s.Lots[lotID]
	if !ok {
		return nil // Unknown lots carry no expiry information to check.
	}
	if lot.ExpiredAt(at) {
		return Violation(RuleLotNotExpired, "lot %s of sku %s expired on %s", lot.ID, lot.SKU, lot.ExpiresOn.Format(time.DateOnly))
	}
	return nil
}

// PutAwayCmd moves stock between two locations inside this warehouse.
type PutAwayCmd struct {
	Line Line
	From LocationCode
	To   LocationCode
}

// DoPutAway validates and returns the PutAway event, or a named rule violation
// with no events at all.
func DoPutAway(s *State, c PutAwayCmd) ([]Event, error) {
	move, _, err := s.ResolveLine(c.Line)
	if err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.From); err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.To); err != nil {
		return nil, err
	}
	move.From, move.To = c.From, c.To
	if err := s.RequireOnHand(move.FromKey(), move.Qty); err != nil {
		return nil, err
	}
	return []Event{{Type: TypePutAway, AggregateID: c.Line.SKU, Payload: PutAway{Move: move}}}, nil
}

// PickCmd takes stock off a shelf for an outbound order: From -> external.
type PickCmd struct {
	Line     Line
	From     LocationCode
	OrderRef string
	At       time.Time
}

// DoPick validates and returns the Picked event. It is the only command enforcing
// lot expiry, because expiry exists to stop expired goods being shipped.
func DoPick(s *State, c PickCmd) ([]Event, error) {
	move, _, err := s.ResolveLine(c.Line)
	if err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.From); err != nil {
		return nil, err
	}
	if err := s.RequireLotUsable(c.Line.LotID, c.At); err != nil {
		return nil, err
	}
	move.From, move.To = c.From, External
	if err := s.RequireOnHand(move.FromKey(), move.Qty); err != nil {
		return nil, err
	}
	return []Event{{Type: TypePicked, AggregateID: c.Line.SKU, Payload: Picked{Move: move, OrderRef: c.OrderRef}}}, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/domain/ -cover -v`
Expected: PASS, coverage 100.0%. `RequireLotUsable`'s unknown-lot branch is covered by
`TestDoPickAllowsUntrackedLotForNonLotItem`; the expired branch by `TestDoPick`.

- [ ] **Step 5: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/internal/domain
git commit -m "feat(domain): add putaway and pick commands with node-enforced invariants

- Line + ResolveLine centralise sku existence, uom conversion and lot requirement
- RequireLocation, RequireOnHand, RequireLotUsable as reusable guards
- commands reject before producing any event, naming the violated rule"
```

---

### Task 5: Receipts — the receipt aggregate and goods receipt

A receipt is the record that a supplier delivery arrived. Its lifecycle is
`ReceiptOpened → ReceiptLineRecorded* → ReceiptClosed`. It carries the supplier's
**delivery note** number (printed on their paperwork) and a **PO reference** (the order
central placed). Both are what central later arbitrates on: it alone can see that two
nodes received against the same PO past its quantity, or keyed the same delivery note
twice. The node cannot check either, so it does not try.

Each line produces two events: `ReceiptLineRecorded` (the paperwork) and `GoodsReceived`
(the stock movement `external → receiving`).

**Files:**
- Create: `warehouse-node/internal/domain/receipt.go`
- Test: `warehouse-node/internal/domain/receipt_test.go`

**Interfaces:**
- Consumes: `domain.State`, `domain.Line`, `domain.ResolveLine`, `domain.RequireLocation` (Tasks 3-4).
- Produces:
  - `domain.ReceiveCmd{ReceiptID, DeliveryNote, PORef string; Line Line; To LocationCode}` and `func DoReceive(s *State, c ReceiveCmd) ([]Event, error)`
  - `domain.CloseReceiptCmd{ReceiptID string}` and `func DoCloseReceipt(s *State, c CloseReceiptCmd) ([]Event, error)`

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/domain/receipt_test.go`:

```go
package domain

import (
	"testing"
	"time"
)

func TestDoReceiveOpensTheReceiptOnFirstLine(t *testing.T) {
	s := stocked(t, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC))
	events, err := DoReceive(s, ReceiveCmd{
		ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
		Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 2, UoM: "CASE"}, To: "RECV-01",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3 (opened, line, goods received)", len(events))
	}
	if got, want := events[0].Payload, (ReceiptOpened{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1"}); got != want {
		t.Fatalf("event 0 = %+v, want %+v", got, want)
	}
	if got, want := events[1].Payload, (ReceiptLineRecorded{ReceiptID: "R1", LineNo: 1, SKU: "WIDGET", LotID: "L9", QtyBase: 24}); got != want {
		t.Fatalf("event 1 = %+v, want %+v", got, want)
	}
	if got, want := events[2].Payload, (GoodsReceived{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
		Move: Movement{SKU: "WIDGET", LotID: "L9", From: External, To: "RECV-01", Qty: 24}}); got != want {
		t.Fatalf("event 2 = %+v, want %+v", got, want)
	}
	for i, e := range events {
		if e.AggregateID != "R1" {
			t.Fatalf("event %d aggregate = %q, want R1", i, e.AggregateID)
		}
	}
}

func TestDoReceiveSecondLineDoesNotReopen(t *testing.T) {
	s := stocked(t, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC))
	first, err := DoReceive(s, ReceiveCmd{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
		Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 1, UoM: "EA"}, To: "RECV-01"})
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}
	applyEvents(t, s, 100, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), first...)

	second, err := DoReceive(s, ReceiveCmd{ReceiptID: "R1",
		Line: Line{SKU: "BOLT", Qty: 1, UoM: "BOX"}, To: "RECV-01"})
	if err != nil {
		t.Fatalf("second receive: %v", err)
	}
	if len(second) != 2 {
		t.Fatalf("got %d events, want 2 (line, goods received)", len(second))
	}
	line := second[0].Payload.(ReceiptLineRecorded)
	if line.LineNo != 2 || line.QtyBase != 100 {
		t.Fatalf("line = %+v, want LineNo 2 and QtyBase 100", line)
	}
	// The delivery note and PO come from the already-open receipt, not the command,
	// so central sees one consistent note per receipt no matter what the operator
	// retypes on later lines.
	if gr := second[1].Payload.(GoodsReceived); gr.DeliveryNote != "DN-1" || gr.PORef != "PO-1" {
		t.Fatalf("goods received = %+v, want DN-1/PO-1 carried from the open receipt", gr)
	}
}

func TestDoReceiveRejections(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		setup    func(t *testing.T, s *State)
		cmd      ReceiveCmd
		wantRule string
	}{
		{
			name:     "unknown sku against a stale master",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "GHOST", Qty: 1, UoM: "EA"}, To: "RECV-01"},
			wantRule: RuleSKUExists,
		},
		{
			name:     "uom not valid for the item",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 1, UoM: "TONNE"}, To: "RECV-01"},
			wantRule: RuleUoMValid,
		},
		{
			name:     "lot-tracked item with no lot",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", Qty: 1, UoM: "EA"}, To: "RECV-01"},
			wantRule: RuleLotRequired,
		},
		{
			name:     "unregistered destination",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 1, UoM: "EA"}, To: "NOWHERE"},
			wantRule: RuleLocationExists,
		},
		{
			name:     "zero quantity",
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 0, UoM: "EA"}, To: "RECV-01"},
			wantRule: RuleQtyPositive,
		},
		{
			name: "line on a closed receipt",
			setup: func(t *testing.T, s *State) {
				t.Helper()
				applyEvents(t, s, 200, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
					Event{Type: TypeReceiptOpened, AggregateID: "R1", Payload: ReceiptOpened{ReceiptID: "R1", DeliveryNote: "DN-1"}},
					Event{Type: TypeReceiptClosed, AggregateID: "R1", Payload: ReceiptClosed{ReceiptID: "R1"}},
				)
			},
			cmd:      ReceiveCmd{ReceiptID: "R1", Line: Line{SKU: "WIDGET", LotID: "L9", Qty: 1, UoM: "EA"}, To: "RECV-01"},
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			if tt.setup != nil {
				tt.setup(t, s)
			}
			events, err := DoReceive(s, tt.cmd)
			if !IsViolation(err, tt.wantRule) {
				t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
			}
			if len(events) != 0 {
				t.Fatalf("rejected command produced %d events, want 0", len(events))
			}
		})
	}
}

func TestDoCloseReceipt(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		setup    func(t *testing.T, s *State)
		wantRule string
	}{
		{
			name: "closes an open receipt",
			setup: func(t *testing.T, s *State) {
				t.Helper()
				applyEvents(t, s, 300, day, Event{Type: TypeReceiptOpened, AggregateID: "R1", Payload: ReceiptOpened{ReceiptID: "R1"}})
			},
		},
		{
			name:     "unknown receipt",
			wantRule: RuleAggregateState,
		},
		{
			name: "already closed receipt",
			setup: func(t *testing.T, s *State) {
				t.Helper()
				applyEvents(t, s, 400, day,
					Event{Type: TypeReceiptOpened, AggregateID: "R1", Payload: ReceiptOpened{ReceiptID: "R1"}},
					Event{Type: TypeReceiptClosed, AggregateID: "R1", Payload: ReceiptClosed{ReceiptID: "R1"}},
				)
			},
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			if tt.setup != nil {
				tt.setup(t, s)
			}
			events, err := DoCloseReceipt(s, CloseReceiptCmd{ReceiptID: "R1"})
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(events) != 1 || events[0].Payload != (ReceiptClosed{ReceiptID: "R1"}) {
				t.Fatalf("events = %+v", events)
			}
		})
	}
}
```

Add this shared helper to `warehouse-node/internal/domain/state_test.go` (it seals and
folds command output back into state, which several later tasks need too):

```go
// applyEvents seals command output into envelopes and folds it into s, which is
// what a real caller does after a command succeeds.
func applyEvents(t *testing.T, s *State, startSeq uint64, at time.Time, events ...Event) {
	t.Helper()
	for i, e := range events {
		env, err := NewEnvelope(EventID{NodeID: "wh-a", Seq: startSeq + uint64(i)},
			HLC{Wall: int64(startSeq) + int64(i), Node: "wh-a"}, at, nil, e)
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		if err := s.Apply(env); err != nil {
			t.Fatalf("Apply(%s): %v", e.Type, err)
		}
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/domain/ -run 'TestDoReceive|TestDoCloseReceipt' -v`
Expected: FAIL — `undefined: ReceiveCmd`, `undefined: DoReceive`, `undefined: DoCloseReceipt`.

- [ ] **Step 3: Write `receipt.go`**

```go
package domain

// ReceiveCmd records one line of a supplier delivery. DeliveryNote and PORef are
// used only when the command opens the receipt; later lines inherit them from the
// open receipt so central always sees one note per receipt.
//
// The node cannot check either of the two invariants that make receipts
// interesting — "does this exceed the open PO quantity" and "have we seen this
// delivery note elsewhere" — because both need to see other nodes' receipts.
// Central arbitrates them after the fact.
type ReceiveCmd struct {
	ReceiptID    string
	DeliveryNote string
	PORef        string
	Line         Line
	To           LocationCode
}

// DoReceive validates the line and returns ReceiptOpened (first line only),
// ReceiptLineRecorded and GoodsReceived.
func DoReceive(s *State, c ReceiveCmd) ([]Event, error) {
	move, _, err := s.ResolveLine(c.Line)
	if err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.To); err != nil {
		return nil, err
	}
	move.From, move.To = External, c.To

	var events []Event
	receipt, open := s.Receipts[c.ReceiptID]
	note, po, lineNo := c.DeliveryNote, c.PORef, 1
	switch {
	case !open:
		events = append(events, Event{Type: TypeReceiptOpened, AggregateID: c.ReceiptID,
			Payload: ReceiptOpened{ReceiptID: c.ReceiptID, DeliveryNote: note, PORef: po}})
	case receipt.Status != ReceiptOpen:
		return nil, Violation(RuleAggregateState, "receipt %s is %s; cannot record more lines", c.ReceiptID, receipt.Status)
	default:
		note, po, lineNo = receipt.DeliveryNote, receipt.PORef, receipt.NextLineNo
	}

	return append(events,
		Event{Type: TypeReceiptLineRecorded, AggregateID: c.ReceiptID, Payload: ReceiptLineRecorded{
			ReceiptID: c.ReceiptID, LineNo: lineNo, SKU: c.Line.SKU, LotID: c.Line.LotID, QtyBase: move.Qty,
		}},
		Event{Type: TypeGoodsReceived, AggregateID: c.ReceiptID, Payload: GoodsReceived{
			ReceiptID: c.ReceiptID, DeliveryNote: note, PORef: po, Move: move,
		}},
	), nil
}

// CloseReceiptCmd ends a receipt so no further lines can be recorded.
type CloseReceiptCmd struct {
	ReceiptID string
}

// DoCloseReceipt validates and returns the ReceiptClosed event.
func DoCloseReceipt(s *State, c CloseReceiptCmd) ([]Event, error) {
	receipt, ok := s.Receipts[c.ReceiptID]
	if !ok {
		return nil, Violation(RuleAggregateState, "receipt %s does not exist", c.ReceiptID)
	}
	if receipt.Status != ReceiptOpen {
		return nil, Violation(RuleAggregateState, "receipt %s is already %s", c.ReceiptID, receipt.Status)
	}
	return []Event{{Type: TypeReceiptClosed, AggregateID: c.ReceiptID, Payload: ReceiptClosed{ReceiptID: c.ReceiptID}}}, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/domain/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 5: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/internal/domain
git commit -m "feat(domain): add receipt aggregate and goods receipt command

- DoReceive opens the receipt on the first line and inherits note/PO after that
- each line emits ReceiptLineRecorded plus a GoodsReceived external->receiving move
- DoCloseReceipt guards unknown and already-closed receipts"
```

---

### Task 6: Reservations — the availability invariant

A reservation is a soft hold so two outbound orders cannot promise the same units.
The stock stays physically where it is; what shrinks is **available** =
`on_hand - active_reservations`. "Reservation cannot exceed available" is node-enforced
for the same reason negative stock is: the node exclusively owns its own locations, so it
can decide alone. Reservations are never sync-relevant to *other* nodes — no other node
consults them — but they do replicate up to central for visibility.

**Files:**
- Create: `warehouse-node/internal/domain/reservation.go`
- Test: `warehouse-node/internal/domain/reservation_test.go`

**Interfaces:**
- Consumes: `domain.State`, `domain.Available`, `domain.Line`, `domain.ResolveLine` (Tasks 3-4).
- Produces:
  - `domain.ReserveCmd{ReservationID string; Line Line; Location LocationCode}` and `func DoReserve(s *State, c ReserveCmd) ([]Event, error)`
  - `domain.ReleaseReservationCmd{ReservationID string}` and `func DoReleaseReservation(s *State, c ReleaseReservationCmd) ([]Event, error)`
  - `domain.ConsumeReservationCmd{ReservationID string}` and `func DoConsumeReservation(s *State, c ConsumeReservationCmd) ([]Event, error)`

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/domain/reservation_test.go`:

```go
package domain

import (
	"testing"
	"time"
)

func TestDoReserve(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	key := StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}
	tests := []struct {
		name     string
		existing []Event // reservations already in place
		cmd      ReserveCmd
		wantRule string
		wantQty  float64
	}{
		{
			name:    "reserves within available",
			cmd:     ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 4, UoM: "EA"}, Location: "PICK-01"},
			wantQty: 4,
		},
		{
			name:    "reserves exactly all of available",
			cmd:     ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 10, UoM: "EA"}, Location: "PICK-01"},
			wantQty: 10,
		},
		{
			name:    "converts alternate uom before checking",
			cmd:     ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0.5, UoM: "CASE"}, Location: "PICK-01"},
			wantQty: 6,
		},
		{
			name:     "exceeds available with nothing reserved",
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 11, UoM: "EA"}, Location: "PICK-01"},
			wantRule: RuleReservationAvailable,
		},
		{
			name: "exceeds available because of an existing active hold",
			existing: []Event{{Type: TypeStockReserved, AggregateID: "RS0",
				Payload: StockReserved{ReservationID: "RS0", Key: key, Qty: 7}}},
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 4, UoM: "EA"}, Location: "PICK-01"},
			wantRule: RuleReservationAvailable,
		},
		{
			name: "a released hold no longer blocks",
			existing: []Event{
				{Type: TypeStockReserved, AggregateID: "RS0", Payload: StockReserved{ReservationID: "RS0", Key: key, Qty: 7}},
				{Type: TypeReservationReleased, AggregateID: "RS0", Payload: ReservationReleased{ReservationID: "RS0"}},
			},
			cmd:     ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 10, UoM: "EA"}, Location: "PICK-01"},
			wantQty: 10,
		},
		{
			name: "duplicate reservation id",
			existing: []Event{{Type: TypeStockReserved, AggregateID: "RS1",
				Payload: StockReserved{ReservationID: "RS1", Key: key, Qty: 1}}},
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, Location: "PICK-01"},
			wantRule: RuleAggregateState,
		},
		{
			name:     "unknown sku",
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "GHOST", Qty: 1, UoM: "EA"}, Location: "PICK-01"},
			wantRule: RuleSKUExists,
		},
		{
			name:     "bad uom",
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "TONNE"}, Location: "PICK-01"},
			wantRule: RuleUoMValid,
		},
		{
			name:     "unknown location",
			cmd:      ReserveCmd{ReservationID: "RS1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, Location: "NOWHERE"},
			wantRule: RuleLocationExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 500, day, tt.existing...)
			events, err := DoReserve(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := StockReserved{ReservationID: "RS1", Key: key, Qty: tt.wantQty}
			if len(events) != 1 || events[0].Payload != want || events[0].AggregateID != "RS1" {
				t.Fatalf("events = %+v, want payload %+v", events, want)
			}
		})
	}
}

func TestDoReleaseAndConsumeReservation(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	key := StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}
	active := []Event{{Type: TypeStockReserved, AggregateID: "RS1",
		Payload: StockReserved{ReservationID: "RS1", Key: key, Qty: 3}}}
	released := append(append([]Event{}, active...),
		Event{Type: TypeReservationReleased, AggregateID: "RS1", Payload: ReservationReleased{ReservationID: "RS1"}})

	tests := []struct {
		name     string
		existing []Event
		release  bool // true: DoReleaseReservation, false: DoConsumeReservation
		wantType string
		wantRule string
	}{
		{name: "release an active hold", existing: active, release: true, wantType: TypeReservationReleased},
		{name: "consume an active hold", existing: active, wantType: TypeReservationConsumed},
		{name: "release an unknown hold", release: true, wantRule: RuleAggregateState},
		{name: "consume an unknown hold", wantRule: RuleAggregateState},
		{name: "release an already-released hold", existing: released, release: true, wantRule: RuleAggregateState},
		{name: "consume an already-released hold", existing: released, wantRule: RuleAggregateState},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 600, day, tt.existing...)
			var (
				events []Event
				err    error
			)
			if tt.release {
				events, err = DoReleaseReservation(s, ReleaseReservationCmd{ReservationID: "RS1"})
			} else {
				events, err = DoConsumeReservation(s, ConsumeReservationCmd{ReservationID: "RS1"})
			}
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(events) != 1 || events[0].Type != tt.wantType || events[0].AggregateID != "RS1" {
				t.Fatalf("events = %+v, want one %s", events, tt.wantType)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/domain/ -run 'TestDoReserve|TestDoReleaseAndConsume' -v`
Expected: FAIL — `undefined: ReserveCmd`, `undefined: DoReserve`.

- [ ] **Step 3: Write `reservation.go`**

```go
package domain

// ReserveCmd places a soft hold on stock at one location so a second order cannot
// promise the same units.
type ReserveCmd struct {
	ReservationID string
	Line          Line
	Location      LocationCode
}

// DoReserve enforces "reservation cannot exceed available", where available is
// on-hand minus every other active hold on the same key. The node owns its own
// locations, so this needs no coordination with central.
func DoReserve(s *State, c ReserveCmd) ([]Event, error) {
	move, _, err := s.ResolveLine(c.Line)
	if err != nil {
		return nil, err
	}
	if err := s.RequireLocation(c.Location); err != nil {
		return nil, err
	}
	if _, exists := s.Reservations[c.ReservationID]; exists {
		return nil, Violation(RuleAggregateState, "reservation %s already exists", c.ReservationID)
	}
	key := StockKey{SKU: c.Line.SKU, Location: c.Location, LotID: c.Line.LotID}
	if avail := s.Available(key); avail < move.Qty {
		return nil, Violation(RuleReservationAvailable,
			"only %v of sku %s lot %q available at %s; cannot reserve %v", avail, key.SKU, key.LotID, key.Location, move.Qty)
	}
	return []Event{{Type: TypeStockReserved, AggregateID: c.ReservationID,
		Payload: StockReserved{ReservationID: c.ReservationID, Key: key, Qty: move.Qty}}}, nil
}

// ReleaseReservationCmd cancels a hold, returning its quantity to available.
type ReleaseReservationCmd struct {
	ReservationID string
}

// DoReleaseReservation validates and returns the ReservationReleased event.
func DoReleaseReservation(s *State, c ReleaseReservationCmd) ([]Event, error) {
	if err := s.requireActiveReservation(c.ReservationID); err != nil {
		return nil, err
	}
	return []Event{{Type: TypeReservationReleased, AggregateID: c.ReservationID,
		Payload: ReservationReleased{ReservationID: c.ReservationID}}}, nil
}

// ConsumeReservationCmd closes a hold because the stock was picked against it.
type ConsumeReservationCmd struct {
	ReservationID string
}

// DoConsumeReservation validates and returns the ReservationConsumed event.
func DoConsumeReservation(s *State, c ConsumeReservationCmd) ([]Event, error) {
	if err := s.requireActiveReservation(c.ReservationID); err != nil {
		return nil, err
	}
	return []Event{{Type: TypeReservationConsumed, AggregateID: c.ReservationID,
		Payload: ReservationConsumed{ReservationID: c.ReservationID}}}, nil
}

func (s *State) requireActiveReservation(id string) error {
	r, ok := s.Reservations[id]
	if !ok {
		return Violation(RuleAggregateState, "reservation %s does not exist", id)
	}
	if r.Status != ResActive {
		return Violation(RuleAggregateState, "reservation %s is already %s", id, r.Status)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/domain/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 5: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/internal/domain
git commit -m "feat(domain): add reservation commands enforcing the availability invariant

- DoReserve rejects holds exceeding on-hand minus other active holds
- release and consume guard unknown and already-settled reservations"
```

---

### Task 7: Transfers — the node half of the two-phase inter-node move

A transfer moves stock between two *warehouses*, i.e. two nodes, and takes two events
because the truck takes time. The **source** emits `TransferDispatched` and decrements its
own stock immediately (`FROM-location → external`). The **destination** emits
`TransferReceived` when the truck arrives and increments its own stock
(`external → TO-location`). Between the two, the goods belong to neither node: central
holds them in an **in-transit** balance (Task 13).

Two of this feature's invariants are structurally impossible for a node to check and so
belong to central: "transfer destination node exists and accepts the item" (no node has
authority over another node's configuration) and "received quantity ≤ dispatched quantity"
(the two halves live on different nodes). The node checks only what it owns: its own
stock, its own locations, its own item master, and — on the receiving side — that it has
actually been told about the dispatch, which reaches it via central on the sync stream.

**Files:**
- Create: `warehouse-node/internal/domain/transfer.go`
- Test: `warehouse-node/internal/domain/transfer_test.go`

**Interfaces:**
- Consumes: `domain.State`, `domain.TransferState`, `domain.Line`, `domain.ResolveLine` (Tasks 3-4).
- Produces:
  - `domain.DispatchTransferCmd{TransferID string; FromNode, ToNode NodeID; From LocationCode; Lines []Line; At time.Time}` and `func DoDispatchTransfer(s *State, c DispatchTransferCmd) ([]Event, error)`
  - `domain.ReceiveTransferCmd{TransferID string; To LocationCode; Lines []Line}` and `func DoReceiveTransfer(s *State, c ReceiveTransferCmd) ([]Event, error)`
  - `func (*TransferState) InTransit(k StockKey) float64` — dispatched minus received for one SKU/lot, keyed with an empty `Location` because in-transit stock is at no location.

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/domain/transfer_test.go`:

```go
package domain

import (
	"testing"
	"time"
)

func TestDoDispatchTransfer(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		cmd       DispatchTransferCmd
		wantRule  string
		wantLines []Movement
	}{
		{
			name: "dispatches all lines out to external",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 3, UoM: "EA"}}},
			wantLines: []Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 3}},
		},
		{
			name: "converts alternate uom per line",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 0.5, UoM: "CASE"}}},
			wantLines: []Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 6}},
		},
		{
			name: "rejects when the sum of lines exceeds on hand even though each line fits",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
				Lines: []Line{
					{SKU: "WIDGET", LotID: "L1", Qty: 6, UoM: "EA"},
					{SKU: "WIDGET", LotID: "L1", Qty: 6, UoM: "EA"},
				}},
			wantRule: RuleStockNonNegative,
		},
		{
			name: "rejects an expired lot: it must not leave the building",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01",
				At:    time.Date(2026, 8, 1, 0, 1, 0, 0, time.UTC),
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}}},
			wantRule: RuleLotNotExpired,
		},
		{
			name: "rejects an unknown sku",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
				Lines: []Line{{SKU: "GHOST", Qty: 1, UoM: "EA"}}},
			wantRule: RuleSKUExists,
		},
		{
			name: "rejects an unknown source location",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "NOWHERE", At: day,
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}}},
			wantRule: RuleLocationExists,
		},
		{
			name:     "rejects an empty transfer",
			cmd:      DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day},
			wantRule: RuleQtyPositive,
		},
		{
			name: "rejects a transfer to this same node",
			cmd: DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-a", From: "PICK-01", At: day,
				Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}}},
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			events, err := DoDispatchTransfer(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			p := events[0].Payload.(TransferDispatched)
			if p.TransferID != "T1" || p.FromNode != "wh-a" || p.ToNode != "wh-b" {
				t.Fatalf("header = %+v", p)
			}
			if len(p.Lines) != len(tt.wantLines) {
				t.Fatalf("got %d lines, want %d", len(p.Lines), len(tt.wantLines))
			}
			for i := range p.Lines {
				if p.Lines[i] != tt.wantLines[i] {
					t.Fatalf("line %d = %+v, want %+v", i, p.Lines[i], tt.wantLines[i])
				}
			}
		})
	}
}

func TestDoDispatchTransferRejectsDuplicateID(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := stocked(t, day)
	cmd := DispatchTransferCmd{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b", From: "PICK-01", At: day,
		Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}}}
	events, err := DoDispatchTransfer(s, cmd)
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	applyEvents(t, s, 700, day, events...)
	if _, err := DoDispatchTransfer(s, cmd); !IsViolation(err, RuleAggregateState) {
		t.Fatalf("err = %v, want violation of %s", err, RuleAggregateState)
	}
}

// dispatchedInto seeds a destination node's state with the TransferDispatched
// event it learned about from central, without any of the source node's stock.
func dispatchedInto(t *testing.T, day time.Time, qty float64) *State {
	t.Helper()
	s := NewState()
	applyEvents(t, s, 1, day,
		Event{Type: TypeItemUpserted, AggregateID: "WIDGET", Payload: ItemUpserted{Item: widget()}},
		Event{Type: TypeLocationRegistered, AggregateID: "RECV-01", Payload: LocationRegistered{Code: "RECV-01", Type: LocReceiving}},
		Event{Type: TypeTransferDispatched, AggregateID: "T1", Payload: TransferDispatched{
			TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
			Lines:      []Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: qty}},
		}},
	)
	return s
}

func TestTransferStateInTransit(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := dispatchedInto(t, day, 5)
	key := StockKey{SKU: "WIDGET", LotID: "L1"}
	if got := s.Transfers["T1"].InTransit(key); got != 5 {
		t.Fatalf("in transit after dispatch = %v, want 5", got)
	}
	events, err := DoReceiveTransfer(s, ReceiveTransferCmd{TransferID: "T1", To: "RECV-01",
		Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	applyEvents(t, s, 800, day, events...)
	if got := s.Transfers["T1"].InTransit(key); got != 0 {
		t.Fatalf("in transit after receipt = %v, want 0", got)
	}
	if s.Transfers["T1"].Status != TransferComplete {
		t.Fatalf("status = %q, want %q", s.Transfers["T1"].Status, TransferComplete)
	}
	if got := s.OnHand(StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}); got != 5 {
		t.Fatalf("destination on hand = %v, want 5", got)
	}
}

func TestDoReceiveTransfer(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		known    bool // has the dispatch reached this node yet?
		cmd      ReceiveTransferCmd
		wantRule string
		wantQty  float64
	}{
		{
			name:    "receives the dispatched quantity",
			known:   true,
			cmd:     ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}},
			wantQty: 5,
		},
		{
			name:    "receives a short shipment",
			known:   true,
			cmd:     ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 4, UoM: "EA"}}},
			wantQty: 4,
		},
		{
			name: "an over-receipt is accepted locally and left for central to arbitrate",
			// The node cannot know the true dispatched quantity is authoritative
			// -- it may hold a stale copy of the dispatch -- so it records what
			// the operator counted off the truck. Central compares both halves
			// and compensates the excess with reason transfer_overreceipt.
			known:   true,
			cmd:     ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 9, UoM: "EA"}}},
			wantQty: 9,
		},
		{
			name:     "rejects a receipt with no matching dispatch",
			known:    false,
			cmd:      ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}},
			wantRule: RuleAggregateState,
		},
		{
			name:     "rejects an unknown destination location",
			known:    true,
			cmd:      ReceiveTransferCmd{TransferID: "T1", To: "NOWHERE", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}},
			wantRule: RuleLocationExists,
		},
		{
			name:     "rejects an unknown sku",
			known:    true,
			cmd:      ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "GHOST", Qty: 1, UoM: "EA"}}},
			wantRule: RuleSKUExists,
		},
		{
			name:     "rejects an empty receipt",
			known:    true,
			cmd:      ReceiveTransferCmd{TransferID: "T1", To: "RECV-01"},
			wantRule: RuleQtyPositive,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewState()
			if tt.known {
				s = dispatchedInto(t, day, 5)
			} else {
				applyEvents(t, s, 1, day,
					Event{Type: TypeItemUpserted, AggregateID: "WIDGET", Payload: ItemUpserted{Item: widget()}},
					Event{Type: TypeLocationRegistered, AggregateID: "RECV-01", Payload: LocationRegistered{Code: "RECV-01", Type: LocReceiving}},
				)
			}
			events, err := DoReceiveTransfer(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			p := events[0].Payload.(TransferReceived)
			want := Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "RECV-01", Qty: tt.wantQty}
			if p.TransferID != "T1" || len(p.Lines) != 1 || p.Lines[0] != want {
				t.Fatalf("payload = %+v, want line %+v", p, want)
			}
		})
	}
}

func TestDoReceiveTransferRejectsSecondReceipt(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := dispatchedInto(t, day, 5)
	cmd := ReceiveTransferCmd{TransferID: "T1", To: "RECV-01", Lines: []Line{{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}}}
	events, err := DoReceiveTransfer(s, cmd)
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}
	applyEvents(t, s, 900, day, events...)
	if _, err := DoReceiveTransfer(s, cmd); !IsViolation(err, RuleAggregateState) {
		t.Fatalf("err = %v, want violation of %s", err, RuleAggregateState)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/domain/ -run 'Transfer' -v`
Expected: FAIL — `undefined: DispatchTransferCmd`, `undefined: DoDispatchTransfer`, `undefined: DoReceiveTransfer`.

- [ ] **Step 3: Write `transfer.go`**

```go
package domain

import "time"

// InTransit is the quantity of one SKU/lot dispatched but not yet received. The
// key's Location is empty because in-transit stock is at no location: it is on a
// truck, owned by neither node, and held as a first-class balance at central.
func (t *TransferState) InTransit(k StockKey) float64 {
	bare := StockKey{SKU: k.SKU, LotID: k.LotID}
	return t.Dispatched[bare] - t.Received[bare]
}

// DispatchTransferCmd sends stock from this node to another. Only the source half
// is emitted here; the destination emits its own TransferReceived when the truck
// arrives.
//
// Two invariants are deliberately absent: whether ToNode exists and accepts the
// item, and whether the eventual receipt exceeds this dispatch. Neither is
// knowable here, so central arbitrates both and compensates what it rejects.
type DispatchTransferCmd struct {
	TransferID string
	FromNode   NodeID
	ToNode     NodeID
	From       LocationCode
	Lines      []Line
	At         time.Time
}

// DoDispatchTransfer validates every line against this node's own stock and
// returns a single TransferDispatched event.
func DoDispatchTransfer(s *State, c DispatchTransferCmd) ([]Event, error) {
	if len(c.Lines) == 0 {
		return nil, Violation(RuleQtyPositive, "transfer %s has no lines", c.TransferID)
	}
	if c.ToNode == c.FromNode {
		return nil, Violation(RuleAggregateState, "transfer %s targets its own node %s", c.TransferID, c.ToNode)
	}
	if _, exists := s.Transfers[c.TransferID]; exists {
		return nil, Violation(RuleAggregateState, "transfer %s already exists", c.TransferID)
	}
	if err := s.RequireLocation(c.From); err != nil {
		return nil, err
	}
	// Accumulate per key so several lines drawing on the same key are checked
	// against the balance in aggregate, not one at a time.
	running := map[StockKey]float64{}
	moves := make([]Movement, 0, len(c.Lines))
	for _, l := range c.Lines {
		move, _, err := s.ResolveLine(l)
		if err != nil {
			return nil, err
		}
		if err := s.RequireLotUsable(l.LotID, c.At); err != nil {
			return nil, err
		}
		move.From, move.To = c.From, External
		running[move.FromKey()] += move.Qty
		if err := s.RequireOnHand(move.FromKey(), running[move.FromKey()]); err != nil {
			return nil, err
		}
		moves = append(moves, move)
	}
	return []Event{{Type: TypeTransferDispatched, AggregateID: c.TransferID, Payload: TransferDispatched{
		TransferID: c.TransferID, FromNode: c.FromNode, ToNode: c.ToNode, Lines: moves,
	}}}, nil
}

// ReceiveTransferCmd records the truck arriving at this node.
type ReceiveTransferCmd struct {
	TransferID string
	To         LocationCode
	Lines      []Line
}

// DoReceiveTransfer requires that this node has already learned of the dispatch —
// a TransferReceived with no matching TransferDispatched is rejected outright,
// because it is either a typo or a transfer this node was never sent. It does not
// compare quantities against the dispatch: the operator records what came off the
// truck, and central, which holds both halves authoritatively, compensates any
// excess with reason transfer_overreceipt.
func DoReceiveTransfer(s *State, c ReceiveTransferCmd) ([]Event, error) {
	if len(c.Lines) == 0 {
		return nil, Violation(RuleQtyPositive, "transfer receipt %s has no lines", c.TransferID)
	}
	transfer, ok := s.Transfers[c.TransferID]
	if !ok {
		return nil, Violation(RuleAggregateState, "transfer %s has no matching dispatch on this node", c.TransferID)
	}
	if transfer.Status != TransferInFlight {
		return nil, Violation(RuleAggregateState, "transfer %s is %s; cannot receive it again", c.TransferID, transfer.Status)
	}
	if err := s.RequireLocation(c.To); err != nil {
		return nil, err
	}
	moves := make([]Movement, 0, len(c.Lines))
	for _, l := range c.Lines {
		move, _, err := s.ResolveLine(l)
		if err != nil {
			return nil, err
		}
		move.From, move.To = External, c.To
		moves = append(moves, move)
	}
	return []Event{{Type: TypeTransferReceived, AggregateID: c.TransferID,
		Payload: TransferReceived{TransferID: c.TransferID, Lines: moves}}}, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/domain/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 5: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/internal/domain
git commit -m "feat(domain): add two-phase transfer commands

- DoDispatchTransfer checks aggregate per-key stock, lot expiry and self-targeting
- DoReceiveTransfer requires a known dispatch and records what came off the truck
- TransferState.InTransit exposes dispatched-minus-received per sku/lot"
```

---

### Task 8: Stock counts and variance adjustments

A **cycle count** is an operator physically recounting a location. Where the count differs
from the book quantity, the difference is a **variance**, and closing the count emits one
`StockAdjusted` per variance line with reason `count_variance`. Adjustments are modelled as
movements against `external`, so the same balanced-pair arithmetic applies: a shortfall
moves stock out (`location → external`), a surplus moves stock in
(`external → location`). Counting is also the sanctioned human fix for a location driven
negative by a compensation, so a count must be able to *raise* a negative balance and must
never itself be blocked by one.

**Files:**
- Create: `warehouse-node/internal/domain/count.go`
- Test: `warehouse-node/internal/domain/count_test.go`

**Interfaces:**
- Consumes: `domain.State`, `domain.CountState`, `domain.StockAdjusted`, `domain.ReasonCountVariance` (Tasks 2-3).
- Produces:
  - `domain.StartCountCmd{CountID string; Location LocationCode}` and `func DoStartCount(s *State, c StartCountCmd) ([]Event, error)`
  - `domain.CountLineCmd{CountID string; Line Line}` and `func DoCountLine(s *State, c CountLineCmd) ([]Event, error)`
  - `domain.CloseCountCmd{CountID string}` and `func DoCloseCount(s *State, c CloseCountCmd) ([]Event, error)`

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/domain/count_test.go`:

```go
package domain

import (
	"sort"
	"testing"
	"time"
)

func TestDoStartCount(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		existing []Event
		cmd      StartCountCmd
		wantRule string
	}{
		{name: "starts a count on a known location", cmd: StartCountCmd{CountID: "C1", Location: "PICK-01"}},
		{name: "rejects an unknown location", cmd: StartCountCmd{CountID: "C1", Location: "NOWHERE"}, wantRule: RuleLocationExists},
		{
			name:     "rejects a duplicate count id",
			existing: []Event{{Type: TypeCountStarted, AggregateID: "C1", Payload: CountStarted{CountID: "C1", Location: "PICK-01"}}},
			cmd:      StartCountCmd{CountID: "C1", Location: "PICK-01"},
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 1000, day, tt.existing...)
			events, err := DoStartCount(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := CountStarted{CountID: "C1", Location: "PICK-01"}
			if len(events) != 1 || events[0].Payload != want || events[0].AggregateID != "C1" {
				t.Fatalf("events = %+v, want %+v", events, want)
			}
		})
	}
}

func TestDoCountLine(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	open := []Event{{Type: TypeCountStarted, AggregateID: "C1", Payload: CountStarted{CountID: "C1", Location: "PICK-01"}}}
	closed := append(append([]Event{}, open...),
		Event{Type: TypeCountClosed, AggregateID: "C1", Payload: CountClosed{CountID: "C1"}})

	tests := []struct {
		name     string
		existing []Event
		cmd      CountLineCmd
		wantRule string
		wantQty  float64
	}{
		{
			name:     "records a counted quantity in base units",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 7, UoM: "EA"}},
			wantQty:  7,
		},
		{
			name:     "converts an alternate uom",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "CASE"}},
			wantQty:  12,
		},
		{
			name:     "rejects a line on an unknown count",
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}},
			wantRule: RuleAggregateState,
		},
		{
			name:     "rejects a line on a closed count",
			existing: closed,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}},
			wantRule: RuleAggregateState,
		},
		{
			name:     "rejects an unknown sku",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "GHOST", Qty: 1, UoM: "EA"}},
			wantRule: RuleSKUExists,
		},
		{
			name:     "rejects a bad uom",
			existing: open,
			cmd:      CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "TONNE"}},
			wantRule: RuleUoMValid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 1100, day, tt.existing...)
			events, err := DoCountLine(s, tt.cmd)
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := CountLineCounted{CountID: "C1",
				Key: StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}, CountedQty: tt.wantQty}
			if len(events) != 1 || events[0].Payload != want {
				t.Fatalf("events = %+v, want %+v", events, want)
			}
		})
	}
}

func TestDoCountLineCountingZeroIsAllowed(t *testing.T) {
	// Counting zero is how an operator says "the shelf is empty", so unlike every
	// other command a count line accepts a zero quantity.
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := stocked(t, day)
	applyEvents(t, s, 1200, day, Event{Type: TypeCountStarted, AggregateID: "C1",
		Payload: CountStarted{CountID: "C1", Location: "PICK-01"}})
	events, err := DoCountLine(s, CountLineCmd{CountID: "C1", Line: Line{SKU: "WIDGET", LotID: "L1", Qty: 0, UoM: "EA"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := events[0].Payload.(CountLineCounted).CountedQty; got != 0 {
		t.Fatalf("counted qty = %v, want 0", got)
	}
}

func TestDoCloseCountEmitsVarianceAdjustments(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	pickL1 := StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}
	tests := []struct {
		name     string
		counted  map[StockKey]float64
		seed     []Event
		wantAdj  []StockAdjusted
		wantRule string
	}{
		{
			name:    "shortfall moves stock out to external",
			counted: map[StockKey]float64{pickL1: 7}, // book is 10
			wantAdj: []StockAdjusted{{Move: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 3}, Reason: ReasonCountVariance}},
		},
		{
			name:    "surplus moves stock in from external",
			counted: map[StockKey]float64{pickL1: 12},
			wantAdj: []StockAdjusted{{Move: Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "PICK-01", Qty: 2}, Reason: ReasonCountVariance}},
		},
		{
			name:    "no variance emits no adjustment",
			counted: map[StockKey]float64{pickL1: 10},
			wantAdj: nil,
		},
		{
			name:    "counting zero writes the whole balance off",
			counted: map[StockKey]float64{pickL1: 0},
			wantAdj: []StockAdjusted{{Move: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 10}, Reason: ReasonCountVariance}},
		},
		{
			name: "a count repairs a location driven negative by a compensation",
			seed: []Event{{Type: TypeStockAdjusted, AggregateID: "WIDGET", Payload: StockAdjusted{
				Move: Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: External, Qty: 14}, Reason: ReasonDuplicateReceipt}}},
			counted: map[StockKey]float64{pickL1: 2}, // book is now -4
			wantAdj: []StockAdjusted{{Move: Movement{SKU: "WIDGET", LotID: "L1", From: External, To: "PICK-01", Qty: 6}, Reason: ReasonCountVariance}},
		},
		{
			name:     "closing an unknown count is rejected",
			wantRule: RuleAggregateState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stocked(t, day)
			applyEvents(t, s, 1300, day, tt.seed...)
			if tt.wantRule == "" {
				seedEvents := []Event{{Type: TypeCountStarted, AggregateID: "C1",
					Payload: CountStarted{CountID: "C1", Location: "PICK-01"}}}
				for k, qty := range tt.counted {
					seedEvents = append(seedEvents, Event{Type: TypeCountLineCounted, AggregateID: "C1",
						Payload: CountLineCounted{CountID: "C1", Key: k, CountedQty: qty}})
				}
				applyEvents(t, s, 1400, day, seedEvents...)
			}

			events, err := DoCloseCount(s, CloseCountCmd{CountID: "C1"})
			if tt.wantRule != "" {
				if !IsViolation(err, tt.wantRule) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantRule)
				}
				if len(events) != 0 {
					t.Fatalf("rejected command produced %d events, want 0", len(events))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// Adjustments come first so the balance is corrected before the count
			// closes; CountClosed is always last.
			if last := events[len(events)-1]; last.Type != TypeCountClosed {
				t.Fatalf("last event = %s, want %s", last.Type, TypeCountClosed)
			}
			var got []StockAdjusted
			for _, e := range events[:len(events)-1] {
				if e.Type != TypeStockAdjusted {
					t.Fatalf("unexpected event type %s", e.Type)
				}
				got = append(got, e.Payload.(StockAdjusted))
			}
			sortAdjustments(got)
			sortAdjustments(tt.wantAdj)
			if len(got) != len(tt.wantAdj) {
				t.Fatalf("got %d adjustments, want %d: %+v", len(got), len(tt.wantAdj), got)
			}
			for i := range got {
				if got[i] != tt.wantAdj[i] {
					t.Fatalf("adjustment %d = %+v, want %+v", i, got[i], tt.wantAdj[i])
				}
			}
			// Applying the adjustments must make the book agree with the count.
			applyEvents(t, s, 1500, day, events...)
			for k, qty := range tt.counted {
				if s.OnHand(k) != qty {
					t.Fatalf("after close OnHand(%+v) = %v, want counted %v", k, s.OnHand(k), qty)
				}
			}
		})
	}
}

func sortAdjustments(a []StockAdjusted) {
	sort.Slice(a, func(i, j int) bool {
		if a[i].Move.SKU != a[j].Move.SKU {
			return a[i].Move.SKU < a[j].Move.SKU
		}
		return a[i].Move.LotID < a[j].Move.LotID
	})
}

func TestDoCloseCountRejectsAlreadyClosed(t *testing.T) {
	day := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := stocked(t, day)
	applyEvents(t, s, 1600, day,
		Event{Type: TypeCountStarted, AggregateID: "C1", Payload: CountStarted{CountID: "C1", Location: "PICK-01"}},
		Event{Type: TypeCountClosed, AggregateID: "C1", Payload: CountClosed{CountID: "C1"}},
	)
	if _, err := DoCloseCount(s, CloseCountCmd{CountID: "C1"}); !IsViolation(err, RuleAggregateState) {
		t.Fatalf("err = %v, want violation of %s", err, RuleAggregateState)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/domain/ -run 'Count' -v`
Expected: FAIL — `undefined: StartCountCmd`, `undefined: DoStartCount`, etc.

- [ ] **Step 3: Write `count.go`**

```go
package domain

import "sort"

// StartCountCmd opens a physical recount of one location.
type StartCountCmd struct {
	CountID  string
	Location LocationCode
}

// DoStartCount validates and returns the CountStarted event.
func DoStartCount(s *State, c StartCountCmd) ([]Event, error) {
	if err := s.RequireLocation(c.Location); err != nil {
		return nil, err
	}
	if _, exists := s.Counts[c.CountID]; exists {
		return nil, Violation(RuleAggregateState, "count %s already exists", c.CountID)
	}
	return []Event{{Type: TypeCountStarted, AggregateID: c.CountID,
		Payload: CountStarted{CountID: c.CountID, Location: c.Location}}}, nil
}

// CountLineCmd records what the operator physically counted for one SKU/lot at the
// count's location.
type CountLineCmd struct {
	CountID string
	Line    Line
}

// DoCountLine validates and returns the CountLineCounted event. Unlike other
// commands a zero quantity is legitimate: it is how the operator reports an empty
// shelf, which is the most consequential count result there is.
func DoCountLine(s *State, c CountLineCmd) ([]Event, error) {
	count, err := s.requireOpenCount(c.CountID)
	if err != nil {
		return nil, err
	}
	item, err := s.Item(c.Line.SKU)
	if err != nil {
		return nil, err
	}
	if item.LotTracked && c.Line.LotID == "" {
		return nil, Violation(RuleLotRequired, "sku %s is lot-tracked; a lot id is required", c.Line.SKU)
	}
	qty := c.Line.Qty
	if qty != 0 {
		if qty, err = item.ToBase(qty, c.Line.UoM); err != nil {
			return nil, err
		}
	} else if _, err := item.ToBase(1, c.Line.UoM); err != nil {
		// Still validate the unit itself even when the quantity is zero.
		return nil, err
	}
	return []Event{{Type: TypeCountLineCounted, AggregateID: c.CountID, Payload: CountLineCounted{
		CountID:    c.CountID,
		Key:        StockKey{SKU: c.Line.SKU, Location: count.Location, LotID: c.Line.LotID},
		CountedQty: qty,
	}}}, nil
}

// CloseCountCmd ends a count and books its variances.
type CloseCountCmd struct {
	CountID string
}

// DoCloseCount emits one StockAdjusted per variance line, then CountClosed.
// Variances are modelled as movements against external so the stock projection
// stays a single balanced-pair code path: a shortfall leaves the location, a
// surplus arrives from outside. A count can raise a negative balance, which is how
// a human repairs a location that a compensation drove below zero.
func DoCloseCount(s *State, c CloseCountCmd) ([]Event, error) {
	count, err := s.requireOpenCount(c.CountID)
	if err != nil {
		return nil, err
	}
	// Sort keys so the emitted event order is deterministic and replays identically.
	keys := make([]StockKey, 0, len(count.Counted))
	for k := range count.Counted {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].SKU != keys[j].SKU {
			return keys[i].SKU < keys[j].SKU
		}
		return keys[i].LotID < keys[j].LotID
	})

	events := make([]Event, 0, len(keys)+1)
	for _, k := range keys {
		variance := count.Counted[k] - s.OnHand(k)
		if variance == 0 {
			continue
		}
		move := Movement{SKU: k.SKU, LotID: k.LotID, Qty: variance, From: External, To: k.Location}
		if variance < 0 {
			move.Qty, move.From, move.To = -variance, k.Location, External
		}
		events = append(events, Event{Type: TypeStockAdjusted, AggregateID: k.SKU,
			Payload: StockAdjusted{Move: move, Reason: ReasonCountVariance}})
	}
	return append(events, Event{Type: TypeCountClosed, AggregateID: c.CountID,
		Payload: CountClosed{CountID: c.CountID}}), nil
}

func (s *State) requireOpenCount(id string) (*CountState, error) {
	count, ok := s.Counts[id]
	if !ok {
		return nil, Violation(RuleAggregateState, "count %s does not exist", id)
	}
	if count.Status != CountOpen {
		return nil, Violation(RuleAggregateState, "count %s is already %s", id, count.Status)
	}
	return count, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/domain/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 5: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean. The domain package is now complete and has never imported a
database, a network package, or `time.Now`. Verify that:

Run: `cd warehouse-node && go list -deps ./internal/domain | grep -Ev '^(internal/|[a-z]+$|[a-z]+/[a-z]+)' ; go vet ./internal/domain`
Expected: no third-party or database dependencies listed.

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/internal/domain
git commit -m "feat(domain): add stock count lifecycle with variance adjustments

- DoStartCount/DoCountLine/DoCloseCount with open-count guards
- count lines accept zero as 'shelf empty' while still validating the uom
- closing emits deterministic StockAdjusted variances against external, and can
  raise a location driven negative by a compensation"
```

---

### Task 9: The node event log — SQLite append-only storage

The node's source of truth. One SQLite file, `modernc.org/sqlite` so there is no cgo. The
log is append-only and **idempotent**: re-ingesting an event already present by
`(node_id, seq)` is a no-op, which is what makes sync retries safe. Locally emitted events
get their sequence number and HLC assigned here — the domain never touches a clock. Reads
come back in a total order (`hlc_wall, hlc_counter, hlc_node, seq`) so a replay is
deterministic no matter what order events physically arrived in.

Cursors persist how far sync has got, in both directions, so a node offline for a week
resumes rather than restarting.

**Files:**
- Create: `warehouse-node/internal/eventlog/schema.go`
- Create: `warehouse-node/internal/eventlog/log.go`
- Test: `warehouse-node/internal/eventlog/log_test.go`
- Modify: `warehouse-node/go.mod` (adds `modernc.org/sqlite`)

**Interfaces:**
- Consumes: `domain.Envelope`, `domain.Event`, `domain.EventID`, `domain.HLC`, `domain.Tick`, `domain.Merge`, `domain.NewEnvelope`.
- Produces:
  - `func eventlog.Open(path string, nodeID domain.NodeID, now func() time.Time) (*Log, error)`
  - `func (*Log) Close() error`
  - `func (*Log) NodeID() domain.NodeID`
  - `func (*Log) Emit(events []domain.Event, causation *domain.EventID) ([]domain.Envelope, error)` — assigns seq + HLC, appends atomically
  - `func (*Log) Ingest(envs []domain.Envelope) (int, error)` — idempotent; returns how many were new; merges the local HLC with each remote reading
  - `func (*Log) ReadAll() ([]domain.Envelope, error)` — total HLC order
  - `func (*Log) ReadOwnAfter(seq uint64, limit int) ([]domain.Envelope, error)` — this node's own events for pushing upstream
  - `func (*Log) HighestSeq(node domain.NodeID) (uint64, error)`
  - `func (*Log) VersionVector() (map[domain.NodeID]uint64, error)`
  - `func (*Log) Cursor(name string) (uint64, error)` / `func (*Log) SetCursor(name string, value uint64) error`
  - `func (*Log) ProjectionVersion() (int, error)` / `func (*Log) SetProjectionVersion(v int) error`
  - `eventlog.CursorPushed = "pushed_to_central"`, `eventlog.CursorPulled = "pulled_from_central"`

- [ ] **Step 1: Add the dependency**

```bash
cd warehouse-node && go get modernc.org/sqlite@latest
```

- [ ] **Step 2: Write the failing test**

`warehouse-node/internal/eventlog/log_test.go`:

```go
package eventlog

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// fixedClock returns a clock that advances by one second per call, so HLC
// behaviour is deterministic in tests.
func fixedClock(start time.Time) func() time.Time {
	n := 0
	return func() time.Time {
		n++
		return start.Add(time.Duration(n) * time.Second)
	}
}

func openLog(t *testing.T, node domain.NodeID) *Log {
	t.Helper()
	l, err := Open(filepath.Join(t.TempDir(), "node.db"), node, fixedClock(time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return l
}

func putAway(qty float64) domain.Event {
	return domain.Event{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.PutAway{
		Move: domain.Movement{SKU: "WIDGET", From: "RECV-01", To: "PICK-01", Qty: qty}}}
}

func TestEmitAssignsSequentialIdentityAndMonotonicHLC(t *testing.T) {
	l := openLog(t, "wh-a")
	envs, err := l.Emit([]domain.Event{putAway(1), putAway(2)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(envs) != 2 {
		t.Fatalf("got %d envelopes, want 2", len(envs))
	}
	for i, e := range envs {
		if e.ID != (domain.EventID{NodeID: "wh-a", Seq: uint64(i + 1)}) {
			t.Fatalf("envelope %d id = %+v", i, e.ID)
		}
		if e.HLC.Node != "wh-a" {
			t.Fatalf("envelope %d hlc node = %q", i, e.HLC.Node)
		}
	}
	if envs[0].HLC.Compare(envs[1].HLC) >= 0 {
		t.Fatalf("hlc did not advance: %+v then %+v", envs[0].HLC, envs[1].HLC)
	}

	more, err := l.Emit([]domain.Event{putAway(3)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if more[0].ID.Seq != 3 {
		t.Fatalf("seq = %d, want 3", more[0].ID.Seq)
	}
}

func TestEmitCarriesCausationID(t *testing.T) {
	l := openLog(t, "central")
	cause := domain.EventID{NodeID: "wh-a", Seq: 7}
	envs, err := l.Emit([]domain.Event{{Type: domain.TypeStockAdjusted, AggregateID: "WIDGET",
		Payload: domain.StockAdjusted{Reason: domain.ReasonUnknownSKU}}}, &cause)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	round, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(round) != 1 || round[0].CausationID == nil || *round[0].CausationID != cause {
		t.Fatalf("causation did not survive storage: %+v", round)
	}
	if envs[0].CausationID == nil || *envs[0].CausationID != cause {
		t.Fatalf("returned envelope lost causation: %+v", envs[0])
	}
}

func TestEmitIsAtomicSoAPartialFailureWritesNothing(t *testing.T) {
	// The second event cannot be JSON-encoded. Nothing at all must be appended:
	// a crash or error mid-transaction leaves the log exactly as it was.
	l := openLog(t, "wh-a")
	_, err := l.Emit([]domain.Event{putAway(1), {Type: domain.TypePutAway, AggregateID: "X", Payload: math.Inf(1)}}, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	all, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("log holds %d events after a failed emit, want 0", len(all))
	}
	// The in-memory sequence must not have advanced either, or the next emit
	// would leave a hole.
	envs, err := l.Emit([]domain.Event{putAway(9)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if envs[0].ID.Seq != 1 {
		t.Fatalf("seq after failed emit = %d, want 1", envs[0].ID.Seq)
	}
}

func TestIngestIsIdempotent(t *testing.T) {
	l := openLog(t, "wh-a")
	remote := remoteEnvelopes(t, "wh-b", 3)

	n, err := l.Ingest(remote)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if n != 3 {
		t.Fatalf("first ingest reported %d new, want 3", n)
	}
	n, err = l.Ingest(remote)
	if err != nil {
		t.Fatalf("second Ingest: %v", err)
	}
	if n != 0 {
		t.Fatalf("second ingest reported %d new, want 0", n)
	}
	all, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("log holds %d events, want 3", len(all))
	}
	// Overlapping batches, the normal case on a resumed stream, must also be safe.
	n, err = l.Ingest(append(remote, remoteEnvelopes(t, "wh-b", 5)[3:]...))
	if err != nil {
		t.Fatalf("overlapping Ingest: %v", err)
	}
	if n != 2 {
		t.Fatalf("overlapping ingest reported %d new, want 2", n)
	}
}

func TestIngestAdvancesLocalHLCPastRemote(t *testing.T) {
	l := openLog(t, "wh-a")
	far := domain.Envelope{
		ID: domain.EventID{NodeID: "wh-b", Seq: 1}, AggregateID: "WIDGET", Type: domain.TypePutAway,
		HLC:        domain.HLC{Wall: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), Counter: 4, Node: "wh-b"},
		RecordedAt: time.Unix(0, 0).UTC(), Payload: []byte(`{"move":{"sku":"WIDGET","from":"RECV-01","to":"PICK-01","qty":1}}`),
	}
	if _, err := l.Ingest([]domain.Envelope{far}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	envs, err := l.Emit([]domain.Event{putAway(1)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if envs[0].HLC.Compare(far.HLC) <= 0 {
		t.Fatalf("local hlc %+v did not overtake remote %+v", envs[0].HLC, far.HLC)
	}
}

func TestIngestRejectsAnEnvelopeItCannotStore(t *testing.T) {
	l := openLog(t, "wh-a")
	// An empty node id would collide with nothing and corrupt the version vector,
	// so it must be refused loudly rather than skipped.
	_, err := l.Ingest([]domain.Envelope{{ID: domain.EventID{Seq: 1}, Type: domain.TypePutAway, Payload: []byte(`{}`)}})
	if err == nil {
		t.Fatal("expected an error for an envelope with no node id")
	}
}

func TestReadAllIsInTotalHLCOrderRegardlessOfArrivalOrder(t *testing.T) {
	l := openLog(t, "wh-a")
	late := remoteEnvelopes(t, "wh-b", 2)
	// Arrive out of order: highest HLC first.
	if _, err := l.Ingest([]domain.Envelope{late[1], late[0]}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	all, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].HLC.Compare(all[i].HLC) >= 0 {
			t.Fatalf("event %d (%+v) not after %d (%+v)", i, all[i].HLC, i-1, all[i-1].HLC)
		}
	}
}

func TestReadOwnAfterRespectsCursorAndLimit(t *testing.T) {
	l := openLog(t, "wh-a")
	if _, err := l.Emit([]domain.Event{putAway(1), putAway(2), putAway(3), putAway(4)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := l.Ingest(remoteEnvelopes(t, "wh-b", 2)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	tests := []struct {
		name     string
		after    uint64
		limit    int
		wantSeqs []uint64
	}{
		{name: "from the start, bounded chunk", after: 0, limit: 2, wantSeqs: []uint64{1, 2}},
		{name: "resumed after two", after: 2, limit: 2, wantSeqs: []uint64{3, 4}},
		{name: "caught up", after: 4, limit: 2, wantSeqs: nil},
		{name: "limit larger than the tail", after: 3, limit: 10, wantSeqs: []uint64{4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := l.ReadOwnAfter(tt.after, tt.limit)
			if err != nil {
				t.Fatalf("ReadOwnAfter: %v", err)
			}
			if len(got) != len(tt.wantSeqs) {
				t.Fatalf("got %d events, want %d", len(got), len(tt.wantSeqs))
			}
			for i, seq := range tt.wantSeqs {
				if got[i].ID != (domain.EventID{NodeID: "wh-a", Seq: seq}) {
					t.Fatalf("event %d id = %+v, want wh-a/%d", i, got[i].ID, seq)
				}
			}
		})
	}
}

func TestVersionVectorAndHighestSeq(t *testing.T) {
	l := openLog(t, "wh-a")
	if _, err := l.Emit([]domain.Event{putAway(1), putAway(2)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := l.Ingest(remoteEnvelopes(t, "wh-b", 5)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	vv, err := l.VersionVector()
	if err != nil {
		t.Fatalf("VersionVector: %v", err)
	}
	if vv["wh-a"] != 2 || vv["wh-b"] != 5 || len(vv) != 2 {
		t.Fatalf("version vector = %+v", vv)
	}
	for node, want := range map[domain.NodeID]uint64{"wh-a": 2, "wh-b": 5, "wh-z": 0} {
		got, err := l.HighestSeq(node)
		if err != nil {
			t.Fatalf("HighestSeq(%s): %v", node, err)
		}
		if got != want {
			t.Fatalf("HighestSeq(%s) = %d, want %d", node, got, want)
		}
	}
}

func TestCursors(t *testing.T) {
	l := openLog(t, "wh-a")
	got, err := l.Cursor(CursorPushed)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if got != 0 {
		t.Fatalf("fresh cursor = %d, want 0", got)
	}
	if err := l.SetCursor(CursorPushed, 12); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	if err := l.SetCursor(CursorPushed, 34); err != nil {
		t.Fatalf("SetCursor overwrite: %v", err)
	}
	if err := l.SetCursor(CursorPulled, 7); err != nil {
		t.Fatalf("SetCursor other: %v", err)
	}
	for name, want := range map[string]uint64{CursorPushed: 34, CursorPulled: 7} {
		got, err := l.Cursor(name)
		if err != nil {
			t.Fatalf("Cursor(%s): %v", name, err)
		}
		if got != want {
			t.Fatalf("Cursor(%s) = %d, want %d", name, got, want)
		}
	}
}

func TestProjectionVersionRoundTrip(t *testing.T) {
	l := openLog(t, "wh-a")
	v, err := l.ProjectionVersion()
	if err != nil {
		t.Fatalf("ProjectionVersion: %v", err)
	}
	if v != 0 {
		t.Fatalf("fresh projection version = %d, want 0", v)
	}
	if err := l.SetProjectionVersion(3); err != nil {
		t.Fatalf("SetProjectionVersion: %v", err)
	}
	if err := l.SetProjectionVersion(4); err != nil {
		t.Fatalf("SetProjectionVersion again: %v", err)
	}
	if v, err = l.ProjectionVersion(); err != nil || v != 4 {
		t.Fatalf("ProjectionVersion = %d, %v; want 4, nil", v, err)
	}
}

func TestReopenRecoversSequenceAndClock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.db")
	clock := fixedClock(time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC))

	first, err := Open(path, "wh-a", clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	before, err := first.Emit([]domain.Event{putAway(1), putAway(2)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with a clock stuck in the past: the recovered HLC must still move
	// forward, so a restart cannot produce duplicate or out-of-order readings.
	second, err := Open(path, "wh-a", func() time.Time { return time.Unix(0, 0).UTC() })
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := second.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	after, err := second.Emit([]domain.Event{putAway(3)}, nil)
	if err != nil {
		t.Fatalf("Emit after reopen: %v", err)
	}
	if after[0].ID.Seq != 3 {
		t.Fatalf("seq after reopen = %d, want 3", after[0].ID.Seq)
	}
	if after[0].HLC.Compare(before[1].HLC) <= 0 {
		t.Fatalf("hlc %+v did not advance past %+v after reopen", after[0].HLC, before[1].HLC)
	}
	if got := second.NodeID(); got != "wh-a" {
		t.Fatalf("NodeID = %q", got)
	}
}

func TestOpenFailsOnAnUnusablePath(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "no-such-dir", "node.db"), "wh-a", time.Now); err == nil {
		t.Fatal("expected an error opening a database in a missing directory")
	}
}

func TestReplayDeterminism(t *testing.T) {
	// Two logs receive the same events in opposite orders. Folding ReadAll into a
	// fresh domain state must produce identical stock maps.
	remote := remoteEnvelopes(t, "wh-b", 6)
	forward, backward := openLog(t, "wh-a"), openLog(t, "wh-a")
	if _, err := forward.Ingest(remote); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	reversed := make([]domain.Envelope, len(remote))
	for i, e := range remote {
		reversed[len(remote)-1-i] = e
	}
	if _, err := backward.Ingest(reversed); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	fold := func(l *Log) map[domain.StockKey]float64 {
		all, err := l.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		s := domain.NewState()
		for _, e := range all {
			if err := s.Apply(e); err != nil {
				t.Fatalf("Apply: %v", err)
			}
		}
		return s.Stock
	}
	a, b := fold(forward), fold(backward)
	if len(a) != len(b) {
		t.Fatalf("different key counts: %d vs %d", len(a), len(b))
	}
	for k, v := range a {
		if b[k] != v {
			t.Fatalf("key %+v: %v vs %v", k, v, b[k])
		}
	}
}

// remoteEnvelopes builds n well-formed envelopes as if node had emitted them,
// with strictly increasing HLC readings.
func remoteEnvelopes(t *testing.T, node domain.NodeID, n int) []domain.Envelope {
	t.Helper()
	out := make([]domain.Envelope, 0, n)
	for i := 1; i <= n; i++ {
		env, err := domain.NewEnvelope(
			domain.EventID{NodeID: node, Seq: uint64(i)},
			domain.HLC{Wall: int64(i) * 1000, Node: node},
			time.Unix(int64(i), 0).UTC(), nil,
			domain.Event{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.PutAway{
				Move: domain.Movement{SKU: "WIDGET", From: "RECV-01", To: "PICK-01", Qty: float64(i)}}},
		)
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		out = append(out, env)
	}
	return out
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/eventlog/ -v`
Expected: FAIL — `undefined: Open`, `undefined: Log`, `undefined: CursorPushed`.

- [ ] **Step 4: Write `schema.go`**

```go
// Package eventlog is the node's source of truth: an append-only SQLite event log
// with idempotent ingestion, persisted sync cursors and deterministic read order.
package eventlog

// schema is applied on every Open. Every statement is idempotent so opening an
// existing database is a no-op.
//
// events is keyed (node_id, seq): that pair is the event's global identity, so
// re-ingesting an event the log already holds conflicts on the primary key and is
// silently ignored. That is what makes sync retries and overlapping batches safe.
//
// events_order indexes the HLC triple, which is the total order reads use. Physical
// arrival order is irrelevant, so a replay is deterministic.
const schema = `
CREATE TABLE IF NOT EXISTS events (
    node_id        TEXT    NOT NULL,
    seq            INTEGER NOT NULL,
    aggregate_id   TEXT    NOT NULL,
    type           TEXT    NOT NULL,
    hlc_wall       INTEGER NOT NULL,
    hlc_counter    INTEGER NOT NULL,
    hlc_node       TEXT    NOT NULL,
    recorded_at    TEXT    NOT NULL,
    causation_node TEXT,
    causation_seq  INTEGER,
    payload        BLOB    NOT NULL,
    PRIMARY KEY (node_id, seq),
    CHECK (node_id <> ''),
    CHECK (seq > 0)
);

CREATE INDEX IF NOT EXISTS events_order ON events (hlc_wall, hlc_counter, hlc_node, node_id, seq);

CREATE TABLE IF NOT EXISTS cursors (
    name  TEXT    PRIMARY KEY,
    value INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);
`

// Cursor names. Pushed is the highest sequence of this node's own events that
// central has acknowledged; Pulled is the highest count of events accepted from
// central. Persisting both is what makes the sync stream resumable after a week
// offline rather than restarting from zero.
const (
	CursorPushed = "pushed_to_central"
	CursorPulled = "pulled_from_central"
)
```

- [ ] **Step 5: Write `log.go`**

```go
package eventlog

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"; no cgo

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// Log is one node's append-only event log.
type Log struct {
	db     *sql.DB
	nodeID domain.NodeID
	now    func() time.Time

	// mu guards seq and clock: Emit must assign a unique, gapless sequence and a
	// monotonic HLC even under concurrent gRPC handlers.
	mu    sync.Mutex
	seq   uint64
	clock domain.HLC
}

// Open opens or creates the log at path. now supplies wall time; it is injected so
// tests are deterministic and so no other package needs to reach for time.Now.
func Open(path string, nodeID domain.NodeID, now func() time.Time) (*Log, error) {
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite at %s: %w", path, err)
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, errors.Join(fmt.Errorf("apply schema: %w", err), db.Close())
	}
	l := &Log{db: db, nodeID: nodeID, now: now}
	if err := l.recover(); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return l, nil
}

// recover restores the sequence counter and clock from storage so a restart never
// reuses a sequence number nor emits an HLC below one already written.
func (l *Log) recover() error {
	seq, err := l.HighestSeq(l.nodeID)
	if err != nil {
		return err
	}
	l.seq = seq
	row := l.db.QueryRow(`SELECT hlc_wall, hlc_counter FROM events ORDER BY hlc_wall DESC, hlc_counter DESC LIMIT 1`)
	var wall int64
	var counter uint32
	switch err := row.Scan(&wall, &counter); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("recover clock: %w", err)
	}
	l.clock = domain.HLC{Wall: wall, Counter: counter, Node: l.nodeID}
	return nil
}

// Close releases the database handle.
func (l *Log) Close() error { return l.db.Close() }

// NodeID is the identity of the node owning this log.
func (l *Log) NodeID() domain.NodeID { return l.nodeID }

// Emit seals locally produced events into envelopes and appends them atomically.
// Sequence numbers and HLC readings are assigned here, which is why the domain
// needs no clock. causation is non-nil only when this log belongs to central and
// the events compensate a rejected event.
//
// The whole batch is one transaction, so a failure or crash part-way through leaves
// the log exactly as it was and no sequence numbers are burnt.
func (l *Log) Emit(events []domain.Event, causation *domain.EventID) ([]domain.Envelope, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	nowMillis := l.now().UnixMilli()
	seq, clock := l.seq, l.clock
	envs := make([]domain.Envelope, 0, len(events))
	for _, e := range events {
		seq++
		clock = domain.Tick(clock, nowMillis, l.nodeID)
		env, err := domain.NewEnvelope(domain.EventID{NodeID: l.nodeID, Seq: seq}, clock,
			time.UnixMilli(nowMillis).UTC(), causation, e)
		if err != nil {
			return nil, err
		}
		envs = append(envs, env)
	}
	if err := l.insert(envs); err != nil {
		return nil, err
	}
	l.seq, l.clock = seq, clock
	return envs, nil
}

// Ingest appends envelopes produced elsewhere — central's compensations, item-master
// updates, and transfer events destined for this node. It returns how many were new.
// Re-ingesting an event already present is a no-op, so a resumed stream may safely
// resend an overlapping batch.
func (l *Log) Ingest(envs []domain.Envelope) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	nowMillis := l.now().UnixMilli()
	clock := l.clock
	for _, e := range envs {
		// Merging every remote reading keeps causality: anything this node emits
		// after seeing these events sorts after them.
		clock = domain.Merge(clock, e.HLC, nowMillis, l.nodeID)
	}
	before, err := l.countEvents()
	if err != nil {
		return 0, err
	}
	if err := l.insert(envs); err != nil {
		return 0, err
	}
	after, err := l.countEvents()
	if err != nil {
		return 0, err
	}
	l.clock = clock
	// If any of the ingested events came from this node (a resend of our own
	// events echoed back), keep the sequence counter ahead of them.
	if seq, err := l.highestSeqLocked(l.nodeID); err == nil && seq > l.seq {
		l.seq = seq
	}
	return after - before, nil
}

// insert writes envelopes in a single transaction, ignoring any whose (node_id, seq)
// is already stored.
func (l *Log) insert(envs []domain.Envelope) error {
	tx, err := l.db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once committed

	stmt, err := tx.Prepare(`
		INSERT INTO events (node_id, seq, aggregate_id, type, hlc_wall, hlc_counter, hlc_node,
		                    recorded_at, causation_node, causation_seq, payload)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (node_id, seq) DO NOTHING`)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, e := range envs {
		var causeNode any
		var causeSeq any
		if e.CausationID != nil {
			causeNode, causeSeq = string(e.CausationID.NodeID), e.CausationID.Seq
		}
		if _, err := stmt.Exec(string(e.ID.NodeID), e.ID.Seq, e.AggregateID, e.Type,
			e.HLC.Wall, e.HLC.Counter, string(e.HLC.Node),
			e.RecordedAt.UTC().Format(time.RFC3339Nano), causeNode, causeSeq, []byte(e.Payload)); err != nil {
			return fmt.Errorf("append %s: %w", e.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (l *Log) countEvents() (int, error) {
	var n int
	if err := l.db.QueryRow(`SELECT count(*) FROM events`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count events: %w", err)
	}
	return n, nil
}

const selectColumns = `node_id, seq, aggregate_id, type, hlc_wall, hlc_counter, hlc_node,
	recorded_at, causation_node, causation_seq, payload`

// ReadAll returns every event in total HLC order, which is the order a projection
// must replay to be deterministic.
func (l *Log) ReadAll() ([]domain.Envelope, error) {
	return l.query(`SELECT ` + selectColumns + ` FROM events
		ORDER BY hlc_wall, hlc_counter, hlc_node, node_id, seq`)
}

// ReadOwnAfter returns up to limit of this node's own events with a sequence above
// after, in sequence order. This is what the sync client pushes upstream, and the
// limit is what keeps a week-long backlog to bounded chunks.
func (l *Log) ReadOwnAfter(seq uint64, limit int) ([]domain.Envelope, error) {
	return l.query(`SELECT `+selectColumns+` FROM events
		WHERE node_id = ? AND seq > ? ORDER BY seq LIMIT ?`, string(l.nodeID), seq, limit)
}

func (l *Log) query(q string, args ...any) ([]domain.Envelope, error) {
	rows, err := l.db.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []domain.Envelope
	for rows.Next() {
		var (
			e          domain.Envelope
			nodeID     string
			hlcNode    string
			recordedAt string
			causeNode  sql.NullString
			causeSeq   sql.NullInt64
			payload    []byte
		)
		if err := rows.Scan(&nodeID, &e.ID.Seq, &e.AggregateID, &e.Type, &e.HLC.Wall, &e.HLC.Counter,
			&hlcNode, &recordedAt, &causeNode, &causeSeq, &payload); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		e.ID.NodeID = domain.NodeID(nodeID)
		e.HLC.Node = domain.NodeID(hlcNode)
		if e.RecordedAt, err = time.Parse(time.RFC3339Nano, recordedAt); err != nil {
			return nil, fmt.Errorf("parse recorded_at of %s: %w", e.ID, err)
		}
		if causeNode.Valid {
			e.CausationID = &domain.EventID{NodeID: domain.NodeID(causeNode.String), Seq: uint64(causeSeq.Int64)}
		}
		e.Payload = payload
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return out, nil
}

// HighestSeq is the highest sequence number stored for node, or zero if none.
func (l *Log) HighestSeq(node domain.NodeID) (uint64, error) {
	return l.highestSeqLocked(node)
}

func (l *Log) highestSeqLocked(node domain.NodeID) (uint64, error) {
	var seq sql.NullInt64
	if err := l.db.QueryRow(`SELECT max(seq) FROM events WHERE node_id = ?`, string(node)).Scan(&seq); err != nil {
		return 0, fmt.Errorf("highest seq for %s: %w", node, err)
	}
	if !seq.Valid {
		return 0, nil
	}
	return uint64(seq.Int64), nil
}

// VersionVector reports the highest sequence held per originating node. It is what
// the Hello frame carries so central knows what this node already has.
func (l *Log) VersionVector() (map[domain.NodeID]uint64, error) {
	rows, err := l.db.Query(`SELECT node_id, max(seq) FROM events GROUP BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("version vector: %w", err)
	}
	defer func() { _ = rows.Close() }()

	vv := map[domain.NodeID]uint64{}
	for rows.Next() {
		var node string
		var seq uint64
		if err := rows.Scan(&node, &seq); err != nil {
			return nil, fmt.Errorf("scan version vector: %w", err)
		}
		vv[domain.NodeID(node)] = seq
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate version vector: %w", err)
	}
	return vv, nil
}

// Cursor reads a persisted sync cursor; an unset cursor reads as zero.
func (l *Log) Cursor(name string) (uint64, error) {
	var v sql.NullInt64
	err := l.db.QueryRow(`SELECT value FROM cursors WHERE name = ?`, name).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read cursor %s: %w", name, err)
	}
	return uint64(v.Int64), nil
}

// SetCursor persists a sync cursor.
func (l *Log) SetCursor(name string, value uint64) error {
	if _, err := l.db.Exec(`INSERT INTO cursors (name, value) VALUES (?, ?)
		ON CONFLICT (name) DO UPDATE SET value = excluded.value`, name, value); err != nil {
		return fmt.Errorf("write cursor %s: %w", name, err)
	}
	return nil
}

// ProjectionVersion is the projection schema version the stored projections were
// built with; zero means they have never been built.
func (l *Log) ProjectionVersion() (int, error) {
	var v sql.NullInt64
	err := l.db.QueryRow(`SELECT version FROM projection_version WHERE id = 1`).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read projection version: %w", err)
	}
	return int(v.Int64), nil
}

// SetProjectionVersion records the version the projections were last built with.
func (l *Log) SetProjectionVersion(v int) error {
	if _, err := l.db.Exec(`INSERT INTO projection_version (id, version) VALUES (1, ?)
		ON CONFLICT (id) DO UPDATE SET version = excluded.version`, v); err != nil {
		return fmt.Errorf("write projection version: %w", err)
	}
	return nil
}

// DB exposes the handle so the projection package can keep its tables in the same
// file and rebuild them in the same transaction as a cursor update.
func (l *Log) DB() *sql.DB { return l.db }
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/eventlog/ -cover -v`
Expected: PASS, coverage 100.0%. If any error path is uncovered, add a focused test —
for example close the log and call `ReadAll` to cover the query error branch:

```go
func TestOperationsFailAfterClose(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "node.db"), "wh-a", time.Now)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := l.ReadAll(); err == nil {
		t.Error("ReadAll after close: expected an error")
	}
	if _, err := l.VersionVector(); err == nil {
		t.Error("VersionVector after close: expected an error")
	}
	if _, err := l.HighestSeq("wh-a"); err == nil {
		t.Error("HighestSeq after close: expected an error")
	}
	if _, err := l.Cursor(CursorPushed); err == nil {
		t.Error("Cursor after close: expected an error")
	}
	if err := l.SetCursor(CursorPushed, 1); err == nil {
		t.Error("SetCursor after close: expected an error")
	}
	if _, err := l.ProjectionVersion(); err == nil {
		t.Error("ProjectionVersion after close: expected an error")
	}
	if err := l.SetProjectionVersion(1); err == nil {
		t.Error("SetProjectionVersion after close: expected an error")
	}
	if _, err := l.Emit([]domain.Event{putAway(1)}, nil); err == nil {
		t.Error("Emit after close: expected an error")
	}
	if _, err := l.Ingest(remoteEnvelopes(t, "wh-b", 1)); err == nil {
		t.Error("Ingest after close: expected an error")
	}
}
```

Also add a test that a stored `recorded_at` which is not RFC3339 fails loudly rather than
being silently skipped, since skipping an event forks state:

```go
func TestReadAllFailsOnCorruptRecordedAt(t *testing.T) {
	l := openLog(t, "wh-a")
	if _, err := l.Emit([]domain.Event{putAway(1)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := l.DB().Exec(`UPDATE events SET recorded_at = 'not-a-time'`); err != nil {
		t.Fatalf("corrupt row: %v", err)
	}
	if _, err := l.ReadAll(); err == nil {
		t.Fatal("expected an error parsing a corrupt recorded_at")
	}
}
```

- [ ] **Step 7: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 8: Commit**

```bash
git add warehouse-node
git commit -m "feat(eventlog): add append-only SQLite event log

- idempotent append keyed (node_id, seq) so sync retries and overlapping
  batches are safe
- Emit assigns sequence and HLC atomically; a failed batch writes nothing
- Ingest merges remote HLC readings so local events sort after what we saw
- total-order reads make replay deterministic; cursors make sync resumable
- projection_version bookkeeping for rebuild-on-code-change"
```

---

### Task 10: Projections — stock on hand, reservations, and the rebuild mechanism

Projections are read models derived from the log; they are never commanded. They live in
the same SQLite file as the log and are maintained incrementally as events are applied,
tracking per originating node which sequence numbers have already been folded in so
applying the same event twice is a no-op.

A `projection_version` constant forces a full rebuild when projection code changes: on
open, if the stored version differs, every projection table is emptied and the whole log
is replayed in total HLC order. That is the escape hatch for "I changed how a projection
is computed" and it is why replay determinism (Task 9) matters.

`stock_on_hand` keeps the `external` sentinel rows — they are what makes every balance sum
to zero — but the operator-facing query filters them out.

**Files:**
- Create: `warehouse-node/internal/projection/registry.go`
- Create: `warehouse-node/internal/projection/stock.go`
- Create: `warehouse-node/internal/projection/reservation.go`
- Test: `warehouse-node/internal/projection/projection_test.go`

**Interfaces:**
- Consumes: `eventlog.Log` (`ReadAll`, `ProjectionVersion`, `SetProjectionVersion`, `DB`), `domain.Envelope`, `domain.DecodePayload`, payload types.
- Produces:
  - `projection.Version` (int const, currently 1)
  - `func projection.Open(l *eventlog.Log) (*Set, error)` — applies DDL, rebuilds on version mismatch, then catches up
  - `func (*Set) Apply(env domain.Envelope) error` — idempotent per `EventID`
  - `func (*Set) CatchUp() error` — folds in every log event not yet applied, in total HLC order
  - `func (*Set) Rebuild() error` — empties every projection table and replays the whole log
  - `projection.StockRow{SKU string; Location domain.LocationCode; LotID string; Qty float64; Available float64}`
  - `func (*Set) StockOnHand(sku string, location domain.LocationCode) ([]StockRow, error)` — empty arguments mean "no filter"
  - `func (*Set) Balance(k domain.StockKey) (float64, error)`
  - `projection.ReservationRow{ID string; SKU string; Location domain.LocationCode; LotID string; Qty float64; Status domain.ReservationStatus}`
  - `func (*Set) Reservations() ([]ReservationRow, error)`
  - `func projection.MovementsOf(payload any) []domain.Movement` — the one place that knows which payloads move stock

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/projection/projection_test.go`:

```go
package projection

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/eventlog"
)

func openSet(t *testing.T) (*eventlog.Log, *Set) {
	t.Helper()
	base := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	n := 0
	l, err := eventlog.Open(filepath.Join(t.TempDir(), "node.db"), "wh-a", func() time.Time {
		n++
		return base.Add(time.Duration(n) * time.Second)
	})
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	set, err := Open(l)
	if err != nil {
		t.Fatalf("projection.Open: %v", err)
	}
	return l, set
}

// emit appends events to the log and folds them into the projection set, which is
// exactly what a command handler does.
func emit(t *testing.T, l *eventlog.Log, set *Set, events ...domain.Event) []domain.Envelope {
	t.Helper()
	envs, err := l.Emit(events, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	for _, e := range envs {
		if err := set.Apply(e); err != nil {
			t.Fatalf("Apply(%s): %v", e.Type, err)
		}
	}
	return envs
}

func received(sku, lot string, to domain.LocationCode, qty float64) domain.Event {
	return domain.Event{Type: domain.TypeGoodsReceived, AggregateID: "R1", Payload: domain.GoodsReceived{
		ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
		Move:      domain.Movement{SKU: sku, LotID: lot, From: domain.External, To: to, Qty: qty}}}
}

func TestStockOnHandProjection(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set,
		received("WIDGET", "L1", "RECV-01", 10),
		domain.Event{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.PutAway{
			Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: "PICK-01", Qty: 4}}},
		domain.Event{Type: domain.TypePicked, AggregateID: "WIDGET", Payload: domain.Picked{
			Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 1}}},
		received("BOLT", "", "RECV-01", 500),
	)

	tests := []struct {
		name     string
		sku      string
		location domain.LocationCode
		want     map[domain.StockKey]float64
	}{
		{
			name: "unfiltered excludes the external sentinel",
			want: map[domain.StockKey]float64{
				{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}: 6,
				{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}: 3,
				{SKU: "BOLT", Location: "RECV-01"}:                500,
			},
		},
		{
			name: "filtered by sku",
			sku:  "WIDGET",
			want: map[domain.StockKey]float64{
				{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}: 6,
				{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}: 3,
			},
		},
		{
			name:     "filtered by location",
			location: "PICK-01",
			want: map[domain.StockKey]float64{
				{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}: 3,
			},
		},
		{
			name:     "filtered by both",
			sku:      "BOLT",
			location: "RECV-01",
			want:     map[domain.StockKey]float64{{SKU: "BOLT", Location: "RECV-01"}: 500},
		},
		{
			name:     "no match",
			sku:      "GHOST",
			location: "PICK-01",
			want:     map[domain.StockKey]float64{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := set.StockOnHand(tt.sku, tt.location)
			if err != nil {
				t.Fatalf("StockOnHand: %v", err)
			}
			if len(rows) != len(tt.want) {
				t.Fatalf("got %d rows, want %d: %+v", len(rows), len(tt.want), rows)
			}
			for _, r := range rows {
				k := domain.StockKey{SKU: r.SKU, Location: r.Location, LotID: r.LotID}
				want, ok := tt.want[k]
				if !ok {
					t.Fatalf("unexpected row %+v", r)
				}
				if r.Qty != want {
					t.Fatalf("row %+v qty = %v, want %v", k, r.Qty, want)
				}
			}
		})
	}

	// The external sentinel is stored, so all balances still sum to zero.
	if got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: domain.External, LotID: "L1"}); err != nil || got != -9 {
		t.Fatalf("external balance = %v, %v; want -9, nil", got, err)
	}
}

func TestStockRowsAreDeletedWhenTheyReachZero(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set,
		received("WIDGET", "L1", "RECV-01", 5),
		domain.Event{Type: domain.TypePicked, AggregateID: "WIDGET", Payload: domain.Picked{
			Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 5}}},
	)
	rows, err := set.StockOnHand("", "")
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("got %d rows, want 0 once the balance is zero: %+v", len(rows), rows)
	}
}

func TestStockGoesNegativeWhenCompensationOutpacesPicking(t *testing.T) {
	// Received 5, picked all 5, then central compensates the receipt. The balance
	// must be recorded as -5 and remain visible.
	l, set := openSet(t)
	emit(t, l, set,
		received("WIDGET", "L1", "RECV-01", 5),
		domain.Event{Type: domain.TypePicked, AggregateID: "WIDGET", Payload: domain.Picked{
			Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 5}}},
	)
	cause := domain.EventID{NodeID: "wh-a", Seq: 1}
	comp, err := l.Emit([]domain.Event{{Type: domain.TypeStockAdjusted, AggregateID: "WIDGET",
		Payload: domain.StockAdjusted{Reason: domain.ReasonDuplicateReceipt,
			Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 5}}}}, &cause)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := set.Apply(comp[0]); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != -5 {
		t.Fatalf("balance = %v, want -5", got)
	}
	rows, err := set.StockOnHand("WIDGET", "RECV-01")
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(rows) != 1 || rows[0].Qty != -5 {
		t.Fatalf("negative balance is hidden from the operator view: %+v", rows)
	}
}

func TestReservationsProjection(t *testing.T) {
	l, set := openSet(t)
	key := domain.StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}
	emit(t, l, set,
		received("WIDGET", "L1", "PICK-01", 10),
		domain.Event{Type: domain.TypeStockReserved, AggregateID: "RS1", Payload: domain.StockReserved{
			ReservationID: "RS1", Key: key, Qty: 4}},
		domain.Event{Type: domain.TypeStockReserved, AggregateID: "RS2", Payload: domain.StockReserved{
			ReservationID: "RS2", Key: key, Qty: 3}},
		domain.Event{Type: domain.TypeReservationReleased, AggregateID: "RS2", Payload: domain.ReservationReleased{ReservationID: "RS2"}},
		domain.Event{Type: domain.TypeStockReserved, AggregateID: "RS3", Payload: domain.StockReserved{
			ReservationID: "RS3", Key: key, Qty: 2}},
		domain.Event{Type: domain.TypeReservationConsumed, AggregateID: "RS3", Payload: domain.ReservationConsumed{ReservationID: "RS3"}},
	)

	rows, err := set.Reservations()
	if err != nil {
		t.Fatalf("Reservations: %v", err)
	}
	wantStatus := map[string]domain.ReservationStatus{"RS1": domain.ResActive, "RS2": domain.ResReleased, "RS3": domain.ResConsumed}
	if len(rows) != 3 {
		t.Fatalf("got %d reservation rows, want 3", len(rows))
	}
	for _, r := range rows {
		if r.Status != wantStatus[r.ID] {
			t.Fatalf("reservation %s status = %q, want %q", r.ID, r.Status, wantStatus[r.ID])
		}
		if r.SKU != "WIDGET" || r.Location != "PICK-01" || r.LotID != "L1" {
			t.Fatalf("reservation %s key wrong: %+v", r.ID, r)
		}
	}

	// Only the active hold reduces available: 10 on hand minus RS1's 4.
	stock, err := set.StockOnHand("WIDGET", "PICK-01")
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(stock) != 1 || stock[0].Available != 6 || stock[0].Qty != 10 {
		t.Fatalf("stock row = %+v, want qty 10 available 6", stock)
	}
}

func TestApplyIsIdempotent(t *testing.T) {
	l, set := openSet(t)
	envs := emit(t, l, set, received("WIDGET", "L1", "RECV-01", 10))
	for i := 0; i < 3; i++ {
		if err := set.Apply(envs[0]); err != nil {
			t.Fatalf("re-Apply: %v", err)
		}
	}
	got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 10 {
		t.Fatalf("balance = %v, want 10 after re-applying the same event", got)
	}
}

func TestCatchUpFoldsEventsAppliedBehindOurBack(t *testing.T) {
	l, set := openSet(t)
	// Written straight to the log, as the sync client does when ingesting from
	// central, bypassing Apply.
	if _, err := l.Emit([]domain.Event{received("WIDGET", "L1", "RECV-01", 7)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}); err != nil || got != 0 {
		t.Fatalf("balance before catch up = %v, %v; want 0, nil", got, err)
	}
	if err := set.CatchUp(); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	if got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}); err != nil || got != 7 {
		t.Fatalf("balance after catch up = %v, %v; want 7, nil", got, err)
	}
	// A second catch up must change nothing.
	if err := set.CatchUp(); err != nil {
		t.Fatalf("second CatchUp: %v", err)
	}
	if got, _ := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}); got != 7 {
		t.Fatalf("balance after second catch up = %v, want 7", got)
	}
}

func TestVersionBumpForcesRebuild(t *testing.T) {
	base := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	n := 0
	path := filepath.Join(t.TempDir(), "node.db")
	l, err := eventlog.Open(path, "wh-a", func() time.Time { n++; return base.Add(time.Duration(n) * time.Second) })
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	set, err := Open(l)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 10))
	if v, err := l.ProjectionVersion(); err != nil || v != Version {
		t.Fatalf("stored version = %d, %v; want %d, nil", v, err, Version)
	}

	// Simulate projection code having changed since the tables were built, and
	// corrupt a row so a stale table is detectable.
	if err := l.SetProjectionVersion(Version - 1); err != nil {
		t.Fatalf("SetProjectionVersion: %v", err)
	}
	if _, err := l.DB().Exec(`UPDATE stock_on_hand SET qty = 999`); err != nil {
		t.Fatalf("corrupt projection: %v", err)
	}

	reopened, err := Open(l)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := reopened.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 10 {
		t.Fatalf("balance after rebuild = %v, want 10 (rebuilt from the log)", got)
	}
	if v, err := l.ProjectionVersion(); err != nil || v != Version {
		t.Fatalf("version after rebuild = %d, %v; want %d, nil", v, err, Version)
	}
}

func TestRebuildIsIdempotentAndClearsStaleRows(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 10))
	if _, err := l.DB().Exec(`INSERT INTO stock_on_hand (sku, location, lot_id, qty) VALUES ('GHOST','PICK-01','',42)`); err != nil {
		t.Fatalf("insert stale row: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := set.Rebuild(); err != nil {
			t.Fatalf("Rebuild: %v", err)
		}
		rows, err := set.StockOnHand("", "")
		if err != nil {
			t.Fatalf("StockOnHand: %v", err)
		}
		if len(rows) != 1 || rows[0].SKU != "WIDGET" || rows[0].Qty != 10 {
			t.Fatalf("after rebuild %d: rows = %+v", i, rows)
		}
	}
}

func TestApplyRejectsAnUndecodableEvent(t *testing.T) {
	_, set := openSet(t)
	if err := set.Apply(domain.Envelope{ID: domain.EventID{NodeID: "wh-a", Seq: 1},
		Type: "NopeHappened", Payload: []byte(`{}`)}); err == nil {
		t.Fatal("expected an error: an unknown event must never be silently skipped")
	}
}

func TestMovementsOf(t *testing.T) {
	mv := domain.Movement{SKU: "WIDGET", From: "A", To: "B", Qty: 1}
	tests := []struct {
		name    string
		payload any
		want    int
	}{
		{"goods received", domain.GoodsReceived{Move: mv}, 1},
		{"put away", domain.PutAway{Move: mv}, 1},
		{"picked", domain.Picked{Move: mv}, 1},
		{"stock adjusted", domain.StockAdjusted{Move: mv}, 1},
		{"transfer dispatched", domain.TransferDispatched{Lines: []domain.Movement{mv, mv}}, 2},
		{"transfer received", domain.TransferReceived{Lines: []domain.Movement{mv}}, 1},
		{"reservation moves nothing", domain.StockReserved{}, 0},
		{"receipt paperwork moves nothing", domain.ReceiptLineRecorded{}, 0},
		{"count line moves nothing", domain.CountLineCounted{}, 0},
		{"item master moves nothing", domain.ItemUpserted{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MovementsOf(tt.payload); len(got) != tt.want {
				t.Fatalf("MovementsOf() returned %d movements, want %d", len(got), tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/projection/ -v`
Expected: FAIL — `undefined: Open`, `undefined: Set`, `undefined: Version`.

- [ ] **Step 3: Write `registry.go`**

```go
// Package projection builds the node's read models from the event log. Projections
// are derived, never commanded: any of them can be thrown away and rebuilt by
// replaying the log.
package projection

import (
	"database/sql"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/eventlog"
)

// Version is the projection schema/logic version. Bump it whenever the way any
// projection is computed changes: Open then empties every projection table and
// replays the whole log, which is only safe because replay is deterministic.
const Version = 1

// projectionSchema creates the read-model tables. applied records, per originating
// node, the highest sequence already folded in, which makes Apply idempotent.
const projectionSchema = `
CREATE TABLE IF NOT EXISTS stock_on_hand (
    sku      TEXT NOT NULL,
    location TEXT NOT NULL,
    lot_id   TEXT NOT NULL,
    qty      REAL NOT NULL,
    PRIMARY KEY (sku, location, lot_id)
);

CREATE TABLE IF NOT EXISTS reservations (
    id       TEXT PRIMARY KEY,
    sku      TEXT NOT NULL,
    location TEXT NOT NULL,
    lot_id   TEXT NOT NULL,
    qty      REAL NOT NULL,
    status   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_applied (
    node_id TEXT    PRIMARY KEY,
    seq     INTEGER NOT NULL
);
`

// projectionTables is every table Rebuild empties. Adding a projection means adding
// its table here.
var projectionTables = []string{"stock_on_hand", "reservations", "projection_applied"}

// Set is the collection of read models for one node, stored alongside its log.
type Set struct {
	log *eventlog.Log
	db  *sql.DB
}

// Open creates the projection tables, rebuilds them if the stored version differs
// from Version, and then folds in any log events not yet applied.
func Open(l *eventlog.Log) (*Set, error) {
	s := &Set{log: l, db: l.DB()}
	if _, err := s.db.Exec(projectionSchema); err != nil {
		return nil, fmt.Errorf("apply projection schema: %w", err)
	}
	stored, err := l.ProjectionVersion()
	if err != nil {
		return nil, err
	}
	if stored != Version {
		return s, s.Rebuild()
	}
	return s, s.CatchUp()
}

// Rebuild empties every projection table and replays the entire log in total HLC
// order, then records the current Version.
func (s *Set) Rebuild() error {
	for _, table := range projectionTables {
		if _, err := s.db.Exec(`DELETE FROM ` + table); err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}
	if err := s.CatchUp(); err != nil {
		return err
	}
	return s.log.SetProjectionVersion(Version)
}

// CatchUp folds in every log event this set has not already applied.
func (s *Set) CatchUp() error {
	envs, err := s.log.ReadAll()
	if err != nil {
		return err
	}
	for _, env := range envs {
		if err := s.Apply(env); err != nil {
			return err
		}
	}
	return nil
}

// Apply folds one event into every projection. It is idempotent: an event whose
// sequence is at or below the highest already applied for its node is skipped, so
// re-delivery on a resumed sync stream cannot double-count stock.
//
// An event that cannot be decoded is an error, never a skip: silently ignoring an
// event forks this node's state from the rest of the system.
func (s *Set) Apply(env domain.Envelope) error {
	applied, err := s.appliedSeq(env.ID.NodeID)
	if err != nil {
		return err
	}
	if env.ID.Seq <= applied {
		return nil
	}
	payload, err := domain.DecodePayload(env)
	if err != nil {
		return fmt.Errorf("project %s: %w", env.ID, err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin projection tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := applyStock(tx, payload); err != nil {
		return err
	}
	if err := applyReservation(tx, payload); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO projection_applied (node_id, seq) VALUES (?, ?)
		ON CONFLICT (node_id) DO UPDATE SET seq = excluded.seq`, string(env.ID.NodeID), env.ID.Seq); err != nil {
		return fmt.Errorf("record applied %s: %w", env.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit projection tx: %w", err)
	}
	return nil
}

func (s *Set) appliedSeq(node domain.NodeID) (uint64, error) {
	var seq sql.NullInt64
	err := s.db.QueryRow(`SELECT seq FROM projection_applied WHERE node_id = ?`, string(node)).Scan(&seq)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read applied seq for %s: %w", node, err)
	}
	return uint64(seq.Int64), nil
}

// MovementsOf returns the stock movements a payload carries, and is the single place
// that knows which event types move stock. Everything else — receipt paperwork,
// reservations, count lines, item master — moves nothing.
func MovementsOf(payload any) []domain.Movement {
	switch p := payload.(type) {
	case domain.GoodsReceived:
		return []domain.Movement{p.Move}
	case domain.PutAway:
		return []domain.Movement{p.Move}
	case domain.Picked:
		return []domain.Movement{p.Move}
	case domain.StockAdjusted:
		return []domain.Movement{p.Move}
	case domain.TransferDispatched:
		return p.Lines
	case domain.TransferReceived:
		return p.Lines
	default:
		return nil
	}
}
```

- [ ] **Step 4: Write `stock.go`**

```go
package projection

import (
	"database/sql"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// StockRow is one line of the stock-on-hand read model. Available is Qty minus every
// active reservation on the same key.
type StockRow struct {
	SKU       string
	Location  domain.LocationCode
	LotID     string
	Qty       float64
	Available float64
}

// applyStock folds a payload's movements into stock_on_hand. Because every movement
// is balanced this is one code path: subtract at From, add at To. Rows reaching zero
// are deleted so the table is a canonical representation of the same balances no
// matter which order events arrived in.
func applyStock(tx *sql.Tx, payload any) error {
	for _, m := range MovementsOf(payload) {
		if err := addStock(tx, m.FromKey(), -m.Qty); err != nil {
			return err
		}
		if err := addStock(tx, m.ToKey(), m.Qty); err != nil {
			return err
		}
	}
	return nil
}

func addStock(tx *sql.Tx, k domain.StockKey, delta float64) error {
	if _, err := tx.Exec(`INSERT INTO stock_on_hand (sku, location, lot_id, qty) VALUES (?, ?, ?, ?)
		ON CONFLICT (sku, location, lot_id) DO UPDATE SET qty = qty + excluded.qty`,
		k.SKU, string(k.Location), k.LotID, delta); err != nil {
		return fmt.Errorf("adjust stock at %+v: %w", k, err)
	}
	if _, err := tx.Exec(`DELETE FROM stock_on_hand WHERE sku = ? AND location = ? AND lot_id = ? AND qty = 0`,
		k.SKU, string(k.Location), k.LotID); err != nil {
		return fmt.Errorf("prune zero stock at %+v: %w", k, err)
	}
	return nil
}

// StockOnHand returns operator-visible balances, optionally filtered by SKU and
// location. Empty arguments mean "no filter". The external sentinel is excluded: it
// is bookkeeping that makes balances sum to zero, not a place in the warehouse.
// Negative balances are included, because a location driven negative by a
// compensation is precisely what the operator must see.
func (s *Set) StockOnHand(sku string, location domain.LocationCode) ([]StockRow, error) {
	rows, err := s.db.Query(`
		SELECT h.sku, h.location, h.lot_id, h.qty,
		       h.qty - coalesce((SELECT sum(r.qty) FROM reservations r
		                          WHERE r.sku = h.sku AND r.location = h.location
		                            AND r.lot_id = h.lot_id AND r.status = ?), 0)
		FROM stock_on_hand h
		WHERE h.location <> ?
		  AND (? = '' OR h.sku = ?)
		  AND (? = '' OR h.location = ?)
		ORDER BY h.sku, h.location, h.lot_id`,
		string(domain.ResActive), string(domain.External), sku, sku, string(location), string(location))
	if err != nil {
		return nil, fmt.Errorf("query stock on hand: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []StockRow{}
	for rows.Next() {
		var r StockRow
		var loc string
		if err := rows.Scan(&r.SKU, &loc, &r.LotID, &r.Qty, &r.Available); err != nil {
			return nil, fmt.Errorf("scan stock row: %w", err)
		}
		r.Location = domain.LocationCode(loc)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stock rows: %w", err)
	}
	return out, nil
}

// Balance is the raw stored quantity at one key, including the external sentinel.
// Summing every Balance in the projection must come to zero.
func (s *Set) Balance(k domain.StockKey) (float64, error) {
	var qty sql.NullFloat64
	err := s.db.QueryRow(`SELECT qty FROM stock_on_hand WHERE sku = ? AND location = ? AND lot_id = ?`,
		k.SKU, string(k.Location), k.LotID).Scan(&qty)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read balance at %+v: %w", k, err)
	}
	return qty.Float64, nil
}
```

- [ ] **Step 5: Write `reservation.go`**

```go
package projection

import (
	"database/sql"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// ReservationRow is one line of the reservations read model.
type ReservationRow struct {
	ID       string
	SKU      string
	Location domain.LocationCode
	LotID    string
	Qty      float64
	Status   domain.ReservationStatus
}

// applyReservation folds reservation events into the reservations table. Reservations
// are node-local — no other node consults them — but they still replicate to central
// for visibility, and central's copy is built by this same code.
func applyReservation(tx *sql.Tx, payload any) error {
	switch p := payload.(type) {
	case domain.StockReserved:
		if _, err := tx.Exec(`INSERT INTO reservations (id, sku, location, lot_id, qty, status)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (id) DO UPDATE SET sku = excluded.sku, location = excluded.location,
				lot_id = excluded.lot_id, qty = excluded.qty, status = excluded.status`,
			p.ReservationID, p.Key.SKU, string(p.Key.Location), p.Key.LotID, p.Qty, string(domain.ResActive)); err != nil {
			return fmt.Errorf("insert reservation %s: %w", p.ReservationID, err)
		}
	case domain.ReservationReleased:
		return setReservationStatus(tx, p.ReservationID, domain.ResReleased)
	case domain.ReservationConsumed:
		return setReservationStatus(tx, p.ReservationID, domain.ResConsumed)
	}
	return nil
}

func setReservationStatus(tx *sql.Tx, id string, status domain.ReservationStatus) error {
	if _, err := tx.Exec(`UPDATE reservations SET status = ? WHERE id = ?`, string(status), id); err != nil {
		return fmt.Errorf("set reservation %s status: %w", id, err)
	}
	return nil
}

// Reservations returns every reservation the node knows about, in id order.
func (s *Set) Reservations() ([]ReservationRow, error) {
	rows, err := s.db.Query(`SELECT id, sku, location, lot_id, qty, status FROM reservations ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query reservations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ReservationRow{}
	for rows.Next() {
		var r ReservationRow
		var loc, status string
		if err := rows.Scan(&r.ID, &r.SKU, &loc, &r.LotID, &r.Qty, &status); err != nil {
			return nil, fmt.Errorf("scan reservation row: %w", err)
		}
		r.Location, r.Status = domain.LocationCode(loc), domain.ReservationStatus(status)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reservation rows: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/projection/ -cover -v`
Expected: PASS, coverage 100.0%. Cover the remaining error branches by closing the
underlying log and asserting each query and `Apply` returns an error, as in Task 9's
`TestOperationsFailAfterClose`:

```go
func TestProjectionOperationsFailAfterClose(t *testing.T) {
	l, set := openSet(t)
	envs := emit(t, l, set, received("WIDGET", "L1", "RECV-01", 1))
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := set.StockOnHand("", ""); err == nil {
		t.Error("StockOnHand: expected an error")
	}
	if _, err := set.Balance(domain.StockKey{SKU: "WIDGET"}); err == nil {
		t.Error("Balance: expected an error")
	}
	if _, err := set.Reservations(); err == nil {
		t.Error("Reservations: expected an error")
	}
	if err := set.Apply(envs[0]); err == nil {
		t.Error("Apply: expected an error")
	}
	if err := set.CatchUp(); err == nil {
		t.Error("CatchUp: expected an error")
	}
	if err := set.Rebuild(); err == nil {
		t.Error("Rebuild: expected an error")
	}
}
```

Note this test calls `l.Close()` itself, and `openSet`'s cleanup closes again; make
`openSet`'s cleanup tolerant by ignoring an "already closed" error:

```go
	t.Cleanup(func() { _ = l.Close() })
```

Apply that change to `openSet` and drop the error check there.

- [ ] **Step 7: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 8: Commit**

```bash
git add warehouse-node/internal/projection
git commit -m "feat(projection): add stock-on-hand and reservation read models

- idempotent Apply tracked per originating node sequence
- MovementsOf is the single place that knows which events move stock
- balances stored including the external sentinel so they sum to zero; the
  operator view hides the sentinel but shows negative balances
- projection_version mismatch on open empties the tables and replays the log"
```

---

### Task 11: Exceptions and transfers projections — what central reversed, and why

Two more node-side read models, both operator-facing.

The **exceptions** projection is the whole point of compensation being visible. Central can
decide, after the fact, that an event a node already appended was invalid; it cannot delete
it (the log is immutable history, mistakes included), so it emits a *compensating event* —
a normal event that undoes the effect and carries `CausationID` pointing at the event it
answers. The node applies compensations unconditionally: it has no veto, and that is
exactly what makes central authoritative. What the node *does* get is visibility, and this
projection is it: every compensation, its reason code, and the event it reversed.

It also carries the spec's accepted limitation. Compensation is not rollback: downstream
events that already consumed the bad stock are left alone. So compensating a receipt whose
goods were already picked and shipped can drive a location **negative**. The spec accepts
that, requires it be flagged rather than hidden, and requires a human to repair it with a
stock count. So this projection tracks negative balances as exceptions too, and clears them
when a later event (in practice a count variance) brings the balance back to zero or above.

The **transfers** projection is the node's view of a two-phase inter-node move: how much
was dispatched, how much has been received, and whether central marked it failed.

**Files:**
- Create: `warehouse-node/internal/projection/exceptions.go`
- Create: `warehouse-node/internal/projection/transfer.go`
- Modify: `warehouse-node/internal/projection/registry.go` (schema, `projectionTables`, `Version`, `Apply`)
- Test: `warehouse-node/internal/projection/exceptions_test.go`
- Test: `warehouse-node/internal/projection/transfer_test.go`

**Interfaces:**
- Consumes: `projection.Set`, `projection.MovementsOf`, `projection.Version`, `projection.projectionSchema`, `projection.projectionTables` (Task 10); `domain.Envelope`, `domain.Movement`, `domain.StockAdjusted`, `domain.TransferDispatched`, `domain.TransferReceived`, `domain.ReasonTransferRejected`, `domain.TransferStatus` with `TransferInFlight`/`TransferComplete`/`TransferFailed`, `domain.External` (Tasks 1-3, 7).
- Produces:
  - `projection.ExceptionKind` (string) with `ExceptionCompensation = "compensation"` and `ExceptionNegativeBalance = "negative_balance"`
  - `projection.ReasonReceiptReversal = "receipt_line_reversal"` — the reason recorded for a compensating `ReceiptLineRecorded`, which carries no reason field of its own
  - `projection.ExceptionRow{ID string; Kind ExceptionKind; Reason string; CausedBy string; Key domain.StockKey; Qty float64; RecordedAt time.Time; Resolved bool}`
  - `func (*Set) Exceptions() ([]ExceptionRow, error)` — every exception, unresolved first, then by ID
  - `projection.TransferRow{ID string; FromNode, ToNode domain.NodeID; Dispatched, Received float64; Status domain.TransferStatus; DispatchedAt time.Time}`
  - `func (*Set) Transfers() ([]TransferRow, error)`
  - `projection.Version` is bumped to `2` (two new tables changes how projections are computed, so existing databases must rebuild)

- [ ] **Step 1: Write the failing exceptions test**

`warehouse-node/internal/projection/exceptions_test.go`:

```go
package projection

import (
	"testing"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/eventlog"
)

// adjust builds a StockAdjusted event moving qty between two locations with a reason.
func adjust(sku, lot string, from, to domain.LocationCode, qty float64, reason string) domain.Event {
	return domain.Event{Type: domain.TypeStockAdjusted, AggregateID: sku, Payload: domain.StockAdjusted{
		Move:   domain.Movement{SKU: sku, LotID: lot, From: from, To: to, Qty: qty},
		Reason: reason,
	}}
}

// emitCaused appends events marked as compensating the given event, which is what
// central's compensations look like once they arrive on the sync stream.
func emitCaused(t *testing.T, l *eventlog.Log, set *Set, cause domain.EventID, events ...domain.Event) []domain.Envelope {
	t.Helper()
	envs, err := l.Emit(events, &cause)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	for _, e := range envs {
		if err := set.Apply(e); err != nil {
			t.Fatalf("Apply(%s): %v", e.Type, err)
		}
	}
	return envs
}

func TestExceptionsRecordCompensations(t *testing.T) {
	tests := []struct {
		name       string
		event      domain.Event
		wantKind   ExceptionKind
		wantReason string
		wantQty    float64
	}{
		{
			name:       "po over-receipt compensation",
			event:      adjust("WIDGET", "L1", "RECV-01", domain.External, 4, domain.ReasonPOOverReceipt),
			wantKind:   ExceptionCompensation,
			wantReason: domain.ReasonPOOverReceipt,
			wantQty:    4,
		},
		{
			name:       "duplicate receipt compensation",
			event:      adjust("WIDGET", "L1", "RECV-01", domain.External, 2, domain.ReasonDuplicateReceipt),
			wantKind:   ExceptionCompensation,
			wantReason: domain.ReasonDuplicateReceipt,
			wantQty:    2,
		},
		{
			name: "receipt line reversal has no reason field of its own",
			event: domain.Event{Type: domain.TypeReceiptLineRecorded, AggregateID: "R1",
				Payload: domain.ReceiptLineRecorded{ReceiptID: "R1", LineNo: 1, SKU: "WIDGET", LotID: "L1", QtyBase: -4}},
			wantKind:   ExceptionCompensation,
			wantReason: ReasonReceiptReversal,
			wantQty:    0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, set := openSet(t)
			origin := emit(t, l, set, received("WIDGET", "L1", "RECV-01", 10))[0]
			comp := emitCaused(t, l, set, origin.ID, tt.event)[0]

			rows, err := set.Exceptions()
			if err != nil {
				t.Fatalf("Exceptions: %v", err)
			}
			var got *ExceptionRow
			for i := range rows {
				if rows[i].ID == comp.ID.String() {
					got = &rows[i]
				}
			}
			if got == nil {
				t.Fatalf("no exception row for %s; rows = %+v", comp.ID, rows)
			}
			if got.Kind != tt.wantKind {
				t.Errorf("Kind = %q, want %q", got.Kind, tt.wantKind)
			}
			if got.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tt.wantReason)
			}
			if got.CausedBy != origin.ID.String() {
				t.Errorf("CausedBy = %q, want %q", got.CausedBy, origin.ID.String())
			}
			if got.Qty != tt.wantQty {
				t.Errorf("Qty = %v, want %v", got.Qty, tt.wantQty)
			}
			if got.RecordedAt.IsZero() {
				t.Error("RecordedAt is zero")
			}
		})
	}
}

func TestExceptionsFlagAndClearNegativeBalances(t *testing.T) {
	l, set := openSet(t)
	// 10 in, 10 picked and shipped: the location is legitimately empty.
	origin := emit(t, l, set,
		received("WIDGET", "L1", "RECV-01", 10),
		domain.Event{Type: domain.TypePicked, AggregateID: "WIDGET", Payload: domain.Picked{
			Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 10}}},
	)[0]

	// Central now compensates the receipt. The goods are gone, so this drives the
	// location to -10. That is accepted, and must be flagged.
	emitCaused(t, l, set, origin.ID,
		adjust("WIDGET", "L1", "RECV-01", domain.External, 10, domain.ReasonDuplicateReceipt))

	key := domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}
	if bal, err := set.Balance(key); err != nil || bal != -10 {
		t.Fatalf("Balance = %v, %v; want -10, nil", bal, err)
	}
	neg := findException(t, set, negativeID(key))
	if neg.Kind != ExceptionNegativeBalance {
		t.Errorf("Kind = %q, want %q", neg.Kind, ExceptionNegativeBalance)
	}
	if neg.Qty != -10 {
		t.Errorf("Qty = %v, want -10", neg.Qty)
	}
	if neg.Resolved {
		t.Error("Resolved = true, want false: a negative balance needs a human")
	}

	// A stock count is the sanctioned human repair: counting 0 raises the balance.
	emit(t, l, set, adjust("WIDGET", "L1", domain.External, "RECV-01", 10, domain.ReasonCountVariance))
	if bal, err := set.Balance(key); err != nil || bal != 0 {
		t.Fatalf("Balance after count = %v, %v; want 0, nil", bal, err)
	}
	if got := findException(t, set, negativeID(key)); !got.Resolved {
		t.Error("Resolved = false after the count repaired the balance, want true")
	}
}

func TestExceptionsOrderUnresolvedFirst(t *testing.T) {
	l, set := openSet(t)
	origin := emit(t, l, set,
		received("WIDGET", "L1", "RECV-01", 5),
		domain.Event{Type: domain.TypePicked, AggregateID: "WIDGET", Payload: domain.Picked{
			Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 5}}},
	)[0]
	emitCaused(t, l, set, origin.ID,
		adjust("WIDGET", "L1", "RECV-01", domain.External, 5, domain.ReasonUnknownSKU))

	rows, err := set.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2: one compensation and one negative balance", len(rows))
	}
	if rows[0].Resolved {
		t.Errorf("rows[0] = %+v, want an unresolved row first", rows[0])
	}
}

func TestExceptionsSurviveRebuild(t *testing.T) {
	l, set := openSet(t)
	origin := emit(t, l, set, received("WIDGET", "L1", "RECV-01", 3))[0]
	emitCaused(t, l, set, origin.ID,
		adjust("WIDGET", "L1", "RECV-01", domain.External, 3, domain.ReasonPOOverReceipt))

	before, err := set.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if err := set.Rebuild(); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	after, err := set.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions after rebuild: %v", err)
	}
	if len(before) != len(after) {
		t.Fatalf("rebuild changed the exception count: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Errorf("row %d changed: %+v -> %+v", i, before[i], after[i])
		}
	}
}

// findException fetches one exception row by ID or fails the test.
func findException(t *testing.T, set *Set, id string) ExceptionRow {
	t.Helper()
	rows, err := set.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	for _, r := range rows {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("no exception with id %q; rows = %+v", id, rows)
	return ExceptionRow{}
}
```

Note: `negativeID` is defined in `exceptions.go` in Step 3; the test calls it directly
because a negative-balance row's identity is derived from the stock key, not from an event.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd warehouse-node && go test ./internal/projection/ -run Exception -v`
Expected: FAIL to compile — `undefined: ExceptionKind`, `undefined: ReasonReceiptReversal`,
`set.Exceptions undefined`, `undefined: negativeID`.

- [ ] **Step 3: Write `exceptions.go`**

```go
package projection

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// ExceptionKind distinguishes the two things an operator must be told about.
type ExceptionKind string

const (
	// ExceptionCompensation is an event central emitted to undo the effect of an
	// event this node had already appended. The node cannot refuse it; it can only
	// show it.
	ExceptionCompensation ExceptionKind = "compensation"
	// ExceptionNegativeBalance is a location driven below zero, which happens when a
	// compensation reverses stock that had already been picked and shipped.
	// Compensation is deliberately not cascaded through downstream events, so this
	// is an accepted outcome that a human resolves with a stock count.
	ExceptionNegativeBalance ExceptionKind = "negative_balance"
)

// ReasonReceiptReversal is the reason recorded for a compensating
// ReceiptLineRecorded. Unlike StockAdjusted that payload carries no reason field,
// because the reason lives on the StockAdjusted emitted alongside it.
const ReasonReceiptReversal = "receipt_line_reversal"

// ExceptionRow is one operator-visible exception. ID is the compensating event's ID
// for a compensation, and a key-derived identity for a negative balance, because a
// negative balance is a standing condition rather than a single event.
type ExceptionRow struct {
	ID         string
	Kind       ExceptionKind
	Reason     string
	CausedBy   string
	Key        domain.StockKey
	Qty        float64
	RecordedAt time.Time
	Resolved   bool
}

// negativeID is the stable identity of the negative-balance exception for one stock
// key, so repeated dips below zero update one row instead of accumulating rows.
func negativeID(k domain.StockKey) string {
	return "negative:" + k.SKU + "|" + string(k.Location) + "|" + k.LotID
}

// applyException records the exceptions one event creates: the compensation itself
// if the event is one, and any balance the event left negative. It must run after
// applyStock, because it reads the balances applyStock has just written.
func applyException(tx *sql.Tx, env domain.Envelope, payload any) error {
	if env.CausationID != nil {
		if err := recordCompensation(tx, env, payload); err != nil {
			return err
		}
	}
	return refreshNegatives(tx, env, payload)
}

func recordCompensation(tx *sql.Tx, env domain.Envelope, payload any) error {
	reason := ReasonReceiptReversal
	if a, ok := payload.(domain.StockAdjusted); ok {
		reason = a.Reason
	}
	var key domain.StockKey
	var qty float64
	if moves := MovementsOf(payload); len(moves) > 0 {
		key, qty = moves[0].FromKey(), moves[0].Qty
		if key.Location == domain.External {
			key = moves[0].ToKey()
		}
	}
	_, err := tx.Exec(`INSERT INTO exceptions
		(id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
		ON CONFLICT (id) DO NOTHING`,
		env.ID.String(), string(ExceptionCompensation), reason, env.CausationID.String(),
		key.SKU, string(key.Location), key.LotID, qty, env.RecordedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("record compensation %s: %w", env.ID, err)
	}
	return nil
}

// refreshNegatives re-checks every key the event touched. A key below zero is
// flagged unresolved; a key back at or above zero resolves an existing flag. The
// external sentinel is skipped: it is bookkeeping and is negative by design.
func refreshNegatives(tx *sql.Tx, env domain.Envelope, payload any) error {
	for _, m := range MovementsOf(payload) {
		for _, k := range []domain.StockKey{m.FromKey(), m.ToKey()} {
			if k.Location == domain.External {
				continue
			}
			if err := refreshNegative(tx, env, k); err != nil {
				return err
			}
		}
	}
	return nil
}

func refreshNegative(tx *sql.Tx, env domain.Envelope, k domain.StockKey) error {
	var qty sql.NullFloat64
	err := tx.QueryRow(`SELECT qty FROM stock_on_hand WHERE sku = ? AND location = ? AND lot_id = ?`,
		k.SKU, string(k.Location), k.LotID).Scan(&qty)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("read balance for negative check at %+v: %w", k, err)
	}
	if qty.Float64 < 0 {
		if _, err := tx.Exec(`INSERT INTO exceptions
			(id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved)
			VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, 0)
			ON CONFLICT (id) DO UPDATE SET qty = excluded.qty, recorded_at = excluded.recorded_at, resolved = 0`,
			negativeID(k), string(ExceptionNegativeBalance), "negative_after_compensation",
			k.SKU, string(k.Location), k.LotID, qty.Float64,
			env.RecordedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("flag negative balance at %+v: %w", k, err)
		}
		return nil
	}
	if _, err := tx.Exec(`UPDATE exceptions SET resolved = 1, qty = ? WHERE id = ?`,
		qty.Float64, negativeID(k)); err != nil {
		return fmt.Errorf("resolve negative balance at %+v: %w", k, err)
	}
	return nil
}

// Exceptions returns every exception, unresolved first so the operator sees what
// still needs a human, then by ID for a stable order.
func (s *Set) Exceptions() ([]ExceptionRow, error) {
	rows, err := s.db.Query(`SELECT id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved
		FROM exceptions ORDER BY resolved, id`)
	if err != nil {
		return nil, fmt.Errorf("query exceptions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ExceptionRow{}
	for rows.Next() {
		var r ExceptionRow
		var kind, loc, at string
		var resolved int
		if err := rows.Scan(&r.ID, &kind, &r.Reason, &r.CausedBy,
			&r.Key.SKU, &loc, &r.Key.LotID, &r.Qty, &at, &resolved); err != nil {
			return nil, fmt.Errorf("scan exception row: %w", err)
		}
		r.Kind, r.Key.Location, r.Resolved = ExceptionKind(kind), domain.LocationCode(loc), resolved == 1
		if r.RecordedAt, err = time.Parse(time.RFC3339Nano, at); err != nil {
			return nil, fmt.Errorf("parse exception timestamp %q: %w", at, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate exception rows: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Write `transfer.go`**

```go
package projection

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// TransferRow is the node's view of one inter-node transfer: how much left, how much
// has arrived, and what central made of it. Dispatched and Received are totals in
// base units across every line.
type TransferRow struct {
	ID           string
	FromNode     domain.NodeID
	ToNode       domain.NodeID
	Dispatched   float64
	Received     float64
	Status       domain.TransferStatus
	DispatchedAt time.Time
}

// applyTransfer folds the two halves of a transfer, plus central's rejection, into
// the transfers table. A destination node sees the dispatch first because central
// forwards it, but the upsert tolerates either order.
func applyTransfer(tx *sql.Tx, env domain.Envelope, payload any) error {
	switch p := payload.(type) {
	case domain.TransferDispatched:
		if _, err := tx.Exec(`INSERT INTO transfers
			(id, from_node, to_node, dispatched, received, status, dispatched_at)
			VALUES (?, ?, ?, ?, 0, ?, ?)
			ON CONFLICT (id) DO UPDATE SET from_node = excluded.from_node,
				to_node = excluded.to_node, dispatched = excluded.dispatched,
				dispatched_at = excluded.dispatched_at,
				status = CASE WHEN transfers.received >= excluded.dispatched
				              THEN ? ELSE transfers.status END`,
			p.TransferID, string(p.FromNode), string(p.ToNode), totalQty(p.Lines),
			string(domain.TransferInFlight), env.RecordedAt.UTC().Format(time.RFC3339Nano),
			string(domain.TransferComplete)); err != nil {
			return fmt.Errorf("project transfer dispatch %s: %w", p.TransferID, err)
		}
	case domain.TransferReceived:
		if _, err := tx.Exec(`INSERT INTO transfers
			(id, from_node, to_node, dispatched, received, status, dispatched_at)
			VALUES (?, '', '', 0, ?, ?, '')
			ON CONFLICT (id) DO UPDATE SET received = transfers.received + excluded.received,
				status = CASE WHEN transfers.received + excluded.received >= transfers.dispatched
				                   AND transfers.dispatched > 0
				              THEN ? ELSE transfers.status END`,
			p.TransferID, totalQty(p.Lines), string(domain.TransferInFlight),
			string(domain.TransferComplete)); err != nil {
			return fmt.Errorf("project transfer receipt %s: %w", p.TransferID, err)
		}
	case domain.StockAdjusted:
		if p.Reason != domain.ReasonTransferRejected {
			return nil
		}
		// Central emits the transfer_rejected compensation against the transfer as
		// its aggregate, which is how the node learns the transfer failed.
		if _, err := tx.Exec(`UPDATE transfers SET status = ? WHERE id = ?`,
			string(domain.TransferFailed), env.AggregateID); err != nil {
			return fmt.Errorf("mark transfer %s failed: %w", env.AggregateID, err)
		}
	}
	return nil
}

func totalQty(lines []domain.Movement) float64 {
	var total float64
	for _, m := range lines {
		total += m.Qty
	}
	return total
}

// Transfers returns every transfer this node participates in, in ID order.
func (s *Set) Transfers() ([]TransferRow, error) {
	rows, err := s.db.Query(`SELECT id, from_node, to_node, dispatched, received, status, dispatched_at
		FROM transfers ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query transfers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []TransferRow{}
	for rows.Next() {
		var r TransferRow
		var from, to, status, at string
		if err := rows.Scan(&r.ID, &from, &to, &r.Dispatched, &r.Received, &status, &at); err != nil {
			return nil, fmt.Errorf("scan transfer row: %w", err)
		}
		r.FromNode, r.ToNode = domain.NodeID(from), domain.NodeID(to)
		r.Status = domain.TransferStatus(status)
		if at != "" {
			if r.DispatchedAt, err = time.Parse(time.RFC3339Nano, at); err != nil {
				return nil, fmt.Errorf("parse dispatch timestamp %q: %w", at, err)
			}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transfer rows: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 5: Wire the two projections into the registry**

In `warehouse-node/internal/projection/registry.go`, bump the version, extend the
schema, extend the table list, and call the two new appliers. Replace the `Version`
constant:

```go
// Version is the projection schema/logic version. Bump it whenever the way any
// projection is computed changes: Open then empties every projection table and
// replays the whole log, which is only safe because replay is deterministic.
// 2 added the exceptions and transfers read models.
const Version = 2
```

Append to the `projectionSchema` string, immediately before its closing backtick:

```sql
CREATE TABLE IF NOT EXISTS exceptions (
    id          TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    reason      TEXT NOT NULL,
    caused_by   TEXT NOT NULL,
    sku         TEXT NOT NULL,
    location    TEXT NOT NULL,
    lot_id      TEXT NOT NULL,
    qty         REAL NOT NULL,
    recorded_at TEXT NOT NULL,
    resolved    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS transfers (
    id            TEXT PRIMARY KEY,
    from_node     TEXT NOT NULL,
    to_node       TEXT NOT NULL,
    dispatched    REAL NOT NULL,
    received      REAL NOT NULL,
    status        TEXT NOT NULL,
    dispatched_at TEXT NOT NULL
);
```

Replace `projectionTables`:

```go
// projectionTables is every table Rebuild empties. Adding a projection means adding
// its table here.
var projectionTables = []string{
	"stock_on_hand", "reservations", "exceptions", "transfers", "projection_applied",
}
```

And in `(*Set).Apply`, after the `applyReservation` call and before the
`projection_applied` upsert, add — order matters, `applyException` reads the balances
`applyStock` wrote:

```go
	if err := applyTransfer(tx, env, payload); err != nil {
		return err
	}
	if err := applyException(tx, env, payload); err != nil {
		return err
	}
```

- [ ] **Step 6: Write the failing transfers test**

`warehouse-node/internal/projection/transfer_test.go`:

```go
package projection

import (
	"testing"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

func dispatched(id string, from, to domain.NodeID, qty float64) domain.Event {
	return domain.Event{Type: domain.TypeTransferDispatched, AggregateID: id,
		Payload: domain.TransferDispatched{TransferID: id, FromNode: from, ToNode: to,
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: qty}}}}
}

func arrived(id string, qty float64) domain.Event {
	return domain.Event{Type: domain.TypeTransferReceived, AggregateID: id,
		Payload: domain.TransferReceived{TransferID: id,
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-01", Qty: qty}}}}
}

func TestTransfersProjection(t *testing.T) {
	tests := []struct {
		name           string
		events         []domain.Event
		wantDispatched float64
		wantReceived   float64
		wantStatus     domain.TransferStatus
	}{
		{
			name:           "dispatch only is in flight",
			events:         []domain.Event{dispatched("T1", "wh-a", "wh-b", 6)},
			wantDispatched: 6,
			wantStatus:     domain.TransferInFlight,
		},
		{
			name:           "partial receipt stays in flight",
			events:         []domain.Event{dispatched("T1", "wh-a", "wh-b", 6), arrived("T1", 4)},
			wantDispatched: 6,
			wantReceived:   4,
			wantStatus:     domain.TransferInFlight,
		},
		{
			name:           "full receipt completes",
			events:         []domain.Event{dispatched("T1", "wh-a", "wh-b", 6), arrived("T1", 6)},
			wantDispatched: 6,
			wantReceived:   6,
			wantStatus:     domain.TransferComplete,
		},
		{
			name:           "receipt arriving before the forwarded dispatch still completes",
			events:         []domain.Event{arrived("T1", 6), dispatched("T1", "wh-a", "wh-b", 6)},
			wantDispatched: 6,
			wantReceived:   6,
			wantStatus:     domain.TransferComplete,
		},
		{
			name: "central rejection marks the transfer failed",
			events: []domain.Event{
				dispatched("T1", "wh-a", "wh-b", 6),
				{Type: domain.TypeStockAdjusted, AggregateID: "T1", Payload: domain.StockAdjusted{
					Move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "PICK-01", Qty: 6},
					Reason: domain.ReasonTransferRejected}},
			},
			wantDispatched: 6,
			wantStatus:     domain.TransferFailed,
		},
		{
			name: "an unrelated adjustment leaves the status alone",
			events: []domain.Event{
				dispatched("T1", "wh-a", "wh-b", 6),
				adjust("WIDGET", "L1", domain.External, "PICK-01", 1, domain.ReasonCountVariance),
			},
			wantDispatched: 6,
			wantStatus:     domain.TransferInFlight,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, set := openSet(t)
			emit(t, l, set, tt.events...)

			rows, err := set.Transfers()
			if err != nil {
				t.Fatalf("Transfers: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("len(rows) = %d, want 1: %+v", len(rows), rows)
			}
			got := rows[0]
			if got.ID != "T1" {
				t.Errorf("ID = %q, want T1", got.ID)
			}
			if got.Dispatched != tt.wantDispatched {
				t.Errorf("Dispatched = %v, want %v", got.Dispatched, tt.wantDispatched)
			}
			if got.Received != tt.wantReceived {
				t.Errorf("Received = %v, want %v", got.Received, tt.wantReceived)
			}
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", got.Status, tt.wantStatus)
			}
		})
	}
}

func TestTransferQueriesFailAfterClose(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, dispatched("T1", "wh-a", "wh-b", 6))
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := set.Transfers(); err == nil {
		t.Error("Transfers: expected an error")
	}
	if _, err := set.Exceptions(); err == nil {
		t.Error("Exceptions: expected an error")
	}
}

func TestExceptionsRejectAnUnparseableTimestamp(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 1))
	if _, err := l.DB().Exec(`INSERT INTO exceptions
		(id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved)
		VALUES ('x', 'compensation', 'r', 'c', 'WIDGET', 'RECV-01', 'L1', 1, 'not-a-time', 0)`); err != nil {
		t.Fatalf("seed bad row: %v", err)
	}
	if _, err := set.Exceptions(); err == nil {
		t.Error("Exceptions: expected a parse error, got nil")
	}
	if _, err := l.DB().Exec(`INSERT INTO transfers
		(id, from_node, to_node, dispatched, received, status, dispatched_at)
		VALUES ('y', 'a', 'b', 1, 0, 'in_flight', 'not-a-time')`); err != nil {
		t.Fatalf("seed bad row: %v", err)
	}
	if _, err := set.Transfers(); err == nil {
		t.Error("Transfers: expected a parse error, got nil")
	}
}
```

- [ ] **Step 7: Run the projection tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/projection/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 8: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 9: Commit**

```bash
git add warehouse-node/internal/projection
git commit -m "feat(projection): add exceptions and transfers read models

- exceptions records every compensation with its reason and the event it reversed
- negative balances arising from compensation are flagged unresolved and cleared
  when a later count variance brings the balance back to zero or above
- transfers tracks dispatched/received totals and central's rejection verdict
- projection Version bumped to 2 so existing databases rebuild on open"
```

---

### Task 12: The node service — commands over the log and projections

The seam between the pure domain and every transport. One type owns a node's log, its
in-memory `domain.State`, and its projections, and offers exactly two mutating
operations:

- `Execute` runs a domain command. The command validates against `State`; if it returns a
  `RuleError`, **nothing is appended** and the error goes straight back to the operator.
  Otherwise the events are sealed into the log (which assigns sequence numbers and HLC
  readings), then folded into `State` and the projections.
- `Ingest` accepts envelopes produced elsewhere — central's compensations, item-master
  updates, and transfer events forwarded from the other node — and folds the ones that were
  new into `State` and the projections. There is no validation path here on purpose: a node
  cannot refuse a compensation.

Everything is serialised under one mutex. A single warehouse node serves one operator, so
throughput is not the constraint; a consistent read-modify-write of `State` is.

**Files:**
- Create: `warehouse-node/internal/node/service.go`
- Test: `warehouse-node/internal/node/service_test.go`

**Interfaces:**
- Consumes: `eventlog.Open`, `(*eventlog.Log).Emit/Ingest/ReadAll/Close/NodeID/DB/ReadOwnAfter/Cursor/SetCursor/VersionVector` (Task 9); `projection.Open`, `(*projection.Set).Apply/CatchUp/StockOnHand/Balance/Reservations/Exceptions/Transfers` (Tasks 10-11); every `domain.Do*` command and `domain.NewState` (Tasks 3-8).
- Produces:
  - `node.Command` = `func(*domain.State) ([]domain.Event, error)` — the adapter that lets any `domain.Do*` function be executed without a wrapper per command
  - `func node.Open(path string, id domain.NodeID, now func() time.Time) (*Service, error)`
  - `func (*Service) Close() error`
  - `func (*Service) NodeID() domain.NodeID`
  - `func (*Service) Log() *eventlog.Log` — for the sync client
  - `func (*Service) Execute(cmd Command) ([]domain.Envelope, error)`
  - `func (*Service) Ingest(envs []domain.Envelope) (int, error)`
  - `func (*Service) RegisterLocation(code domain.LocationCode, typ domain.LocationType) error`
  - `func (*Service) StockOnHand(sku string, location domain.LocationCode) ([]projection.StockRow, error)`
  - `func (*Service) Reservations() ([]projection.ReservationRow, error)`
  - `func (*Service) Exceptions() ([]projection.ExceptionRow, error)`
  - `func (*Service) Transfers() ([]projection.TransferRow, error)`

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/node/service_test.go`:

```go
package node

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

func widget() domain.Item {
	return domain.Item{SKU: "WIDGET", Description: "Blue widget", BaseUoM: "EA",
		AltUoM: map[domain.UoM]float64{"CASE": 12}, LotTracked: true, ShelfLifeDays: 30}
}

// openService starts a node on a temp file with a deterministic clock, registers a
// receiving and a pick location, and replicates the widget item master down as if
// central had sent it.
func openService(t *testing.T, id domain.NodeID) *Service {
	t.Helper()
	base := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	n := 0
	svc, err := Open(filepath.Join(t.TempDir(), "node.db"), id, func() time.Time {
		n++
		return base.Add(time.Duration(n) * time.Second)
	})
	if err != nil {
		t.Fatalf("node.Open: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	for _, loc := range []struct {
		code domain.LocationCode
		typ  domain.LocationType
	}{{"RECV-01", domain.LocReceiving}, {"PICK-01", domain.LocPick}} {
		if err := svc.RegisterLocation(loc.code, loc.typ); err != nil {
			t.Fatalf("RegisterLocation(%s): %v", loc.code, err)
		}
	}
	if _, err := svc.Execute(func(*domain.State) ([]domain.Event, error) {
		return []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "WIDGET",
			Payload: domain.ItemUpserted{Item: widget()}}}, nil
	}); err != nil {
		t.Fatalf("seed item master: %v", err)
	}
	return svc
}

func TestExecuteAppendsAndProjects(t *testing.T) {
	svc := openService(t, "wh-a")

	envs, err := svc.Execute(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReceive(s, domain.ReceiveCmd{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
			Line: domain.Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "CASE"}, To: "RECV-01"})
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(envs) != 3 {
		t.Fatalf("len(envs) = %d, want 3 (ReceiptOpened, ReceiptLineRecorded, GoodsReceived)", len(envs))
	}
	rows, err := svc.StockOnHand("", "")
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(rows) != 1 || rows[0].Qty != 12 {
		t.Fatalf("rows = %+v, want one row of 12 base units", rows)
	}
}

func TestExecuteAppendsNothingWhenAnInvariantFails(t *testing.T) {
	svc := openService(t, "wh-a")
	before, err := svc.Log().ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	_, err = svc.Execute(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoPick(s, domain.PickCmd{From: "PICK-01", At: time.Now(),
			Line: domain.Line{SKU: "WIDGET", LotID: "L1", Qty: 5, UoM: "EA"}})
	})
	if !domain.IsViolation(err, domain.RuleStockNonNegative) {
		t.Fatalf("err = %v, want a %s violation", err, domain.RuleStockNonNegative)
	}
	after, err := svc.Log().ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("log grew from %d to %d events on a rejected command", len(before), len(after))
	}
}

func TestIngestAppliesCompensationsAndIsIdempotent(t *testing.T) {
	svc := openService(t, "wh-a")
	if _, err := svc.Execute(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReceive(s, domain.ReceiveCmd{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
			Line: domain.Line{SKU: "WIDGET", LotID: "L1", Qty: 10, UoM: "EA"}, To: "RECV-01"})
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	origin := domain.EventID{NodeID: "wh-a", Seq: 4}
	comp, err := domain.NewEnvelope(
		domain.EventID{NodeID: "central", Seq: 1},
		domain.HLC{Wall: 99, Counter: 0, Node: "central"},
		time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC),
		&origin,
		domain.Event{Type: domain.TypeStockAdjusted, AggregateID: "WIDGET", Payload: domain.StockAdjusted{
			Move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 4},
			Reason: domain.ReasonPOOverReceipt}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}

	tests := []struct {
		name    string
		wantNew int
	}{
		{"first ingest is new", 1},
		{"second ingest is a no-op", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := svc.Ingest([]domain.Envelope{comp})
			if err != nil {
				t.Fatalf("Ingest: %v", err)
			}
			if n != tt.wantNew {
				t.Errorf("Ingest returned %d new, want %d", n, tt.wantNew)
			}
		})
	}

	bal, err := svc.StockOnHand("WIDGET", "RECV-01")
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(bal) != 1 || bal[0].Qty != 6 {
		t.Fatalf("balance = %+v, want a single row of 6", bal)
	}
	exc, err := svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exc) != 1 || exc[0].Reason != domain.ReasonPOOverReceipt {
		t.Fatalf("exceptions = %+v, want one po_overreceipt row", exc)
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.db")
	clock := func() time.Time { return time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC) }

	svc, err := Open(path, "wh-a", clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := svc.RegisterLocation("RECV-01", domain.LocReceiving); err != nil {
		t.Fatalf("RegisterLocation: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path, "wh-a", clock)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if err := reopened.RegisterLocation("PICK-01", domain.LocPick); err != nil {
		t.Fatalf("RegisterLocation after reopen: %v", err)
	}
	envs, err := reopened.Log().ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(envs) != 2 {
		t.Fatalf("len(envs) = %d, want 2: state must be rebuilt from the log on open", len(envs))
	}
}

func TestQueriesAndOpenErrors(t *testing.T) {
	svc := openService(t, "wh-a")
	if got := svc.NodeID(); got != "wh-a" {
		t.Errorf("NodeID() = %q, want wh-a", got)
	}
	if _, err := svc.Reservations(); err != nil {
		t.Errorf("Reservations: %v", err)
	}
	if _, err := svc.Transfers(); err != nil {
		t.Errorf("Transfers: %v", err)
	}
	if _, err := Open(filepath.Join(t.TempDir(), "nested", "missing", "node.db"), "wh-a", time.Now); err == nil {
		t.Error("Open into a nonexistent directory: expected an error")
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := svc.Execute(func(*domain.State) ([]domain.Event, error) {
		return []domain.Event{{Type: domain.TypePutAway, AggregateID: "WIDGET",
			Payload: domain.PutAway{Move: domain.Movement{SKU: "WIDGET", From: "RECV-01", To: "PICK-01", Qty: 1}}}}, nil
	}); err == nil {
		t.Error("Execute after Close: expected an error")
	}
	if _, err := svc.Ingest(nil); err == nil {
		t.Error("Ingest after Close: expected an error")
	}
	if _, err := svc.StockOnHand("", ""); err == nil {
		t.Error("StockOnHand after Close: expected an error")
	}
	if _, err := svc.Exceptions(); err == nil {
		t.Error("Exceptions after Close: expected an error")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd warehouse-node && go test ./internal/node/ -v`
Expected: FAIL to compile — `undefined: Open`, `undefined: Service`.

- [ ] **Step 3: Write `service.go`**

```go
// Package node ties a warehouse node's event log, in-memory domain state and
// projections together. It is the only place that knows all three, and it is what
// every transport — the operator gRPC API, the sync client — talks to.
package node

import (
	"fmt"
	"sync"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/eventlog"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/projection"
)

// Command is a domain command bound to its arguments. Every domain.Do* function has
// this shape once its command struct is supplied, so no per-command wrapper is
// needed:
//
//	svc.Execute(func(s *domain.State) ([]domain.Event, error) {
//	    return domain.DoPutAway(s, cmd)
//	})
type Command func(*domain.State) ([]domain.Event, error)

// Service is one warehouse node.
type Service struct {
	mu    sync.Mutex
	log   *eventlog.Log
	set   *projection.Set
	state *domain.State
}

// Open opens (or creates) the node at path, brings its projections up to date, and
// rebuilds the in-memory domain state by replaying the log. now supplies wall time
// and is injected so tests are deterministic.
func Open(path string, id domain.NodeID, now func() time.Time) (*Service, error) {
	log, err := eventlog.Open(path, id, now)
	if err != nil {
		return nil, err
	}
	set, err := projection.Open(log)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	s := &Service{log: log, set: set, state: domain.NewState()}
	envs, err := log.ReadAll()
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	for _, env := range envs {
		if err := s.state.Apply(env); err != nil {
			_ = log.Close()
			return nil, fmt.Errorf("rebuild state: %w", err)
		}
	}
	return s, nil
}

// Close releases the underlying database.
func (s *Service) Close() error { return s.log.Close() }

// NodeID is this node's identity.
func (s *Service) NodeID() domain.NodeID { return s.log.NodeID() }

// Log exposes the event log for the sync client, which needs cursors and raw reads.
func (s *Service) Log() *eventlog.Log { return s.log }

// Execute validates a command against current state and, only if it passes, appends
// its events and folds them into state and the projections. A rejected command
// writes nothing at all: the operator sees the violated rule immediately and the log
// is untouched.
func (s *Service) Execute(cmd Command) ([]domain.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events, err := cmd(s.state)
	if err != nil {
		return nil, err
	}
	envs, err := s.log.Emit(events, nil)
	if err != nil {
		return nil, err
	}
	return envs, s.apply(envs)
}

// Ingest folds in envelopes produced elsewhere: compensations from central,
// item-master updates, and transfer events forwarded from another node. It returns
// how many were new. There is no validation: a node cannot refuse a compensation,
// which is what makes central authoritative.
func (s *Service) Ingest(envs []domain.Envelope) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	before, err := s.log.VersionVector()
	if err != nil {
		return 0, err
	}
	n, err := s.log.Ingest(envs)
	if err != nil {
		return 0, err
	}
	fresh := make([]domain.Envelope, 0, n)
	for _, env := range envs {
		if env.ID.Seq > before[env.ID.NodeID] {
			fresh = append(fresh, env)
			before[env.ID.NodeID] = env.ID.Seq
		}
	}
	return n, s.apply(fresh)
}

// RegisterLocation declares a node-owned stock location. Locations are node-owned —
// no other node has authority over them — so this is a local command, not something
// replicated down from central.
func (s *Service) RegisterLocation(code domain.LocationCode, typ domain.LocationType) error {
	_, err := s.Execute(func(*domain.State) ([]domain.Event, error) {
		if !typ.Valid() {
			return nil, domain.Violation(domain.RuleLocationExists, "unknown location type %q", typ)
		}
		return []domain.Event{{Type: domain.TypeLocationRegistered, AggregateID: string(code),
			Payload: domain.LocationRegistered{Code: code, Type: typ}}}, nil
	})
	return err
}

// apply folds envelopes into the in-memory state and the projections. State is
// applied first because it is in memory and cannot fail partway in a way the
// projections could observe.
func (s *Service) apply(envs []domain.Envelope) error {
	for _, env := range envs {
		if err := s.state.Apply(env); err != nil {
			return err
		}
		if err := s.set.Apply(env); err != nil {
			return err
		}
	}
	return nil
}

// StockOnHand returns operator-visible balances. Empty arguments mean "no filter".
func (s *Service) StockOnHand(sku string, location domain.LocationCode) ([]projection.StockRow, error) {
	return s.set.StockOnHand(sku, location)
}

// Reservations returns every reservation this node knows about.
func (s *Service) Reservations() ([]projection.ReservationRow, error) { return s.set.Reservations() }

// Exceptions returns what central reversed and why, plus any balance a compensation
// drove negative.
func (s *Service) Exceptions() ([]projection.ExceptionRow, error) { return s.set.Exceptions() }

// Transfers returns this node's view of every inter-node transfer.
func (s *Service) Transfers() ([]projection.TransferRow, error) { return s.set.Transfers() }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/node/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 5: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/internal/node
git commit -m "feat(node): add the service seam over log, state and projections

- Command is a bound domain command, so no wrapper per command is needed
- Execute appends nothing when a node-enforced invariant fails
- Ingest applies central's events unconditionally and is idempotent
- state is rebuilt by replaying the log on open"
```

---

### Task 13: The operator gRPC API — ten commands and three queries

The interface the operator's client (a handheld terminal, in a real deployment) talks to.
Every command returns immediately from local state; **nothing blocks on central**. That is
the whole point of the architecture: the warehouse keeps working when the network does not.

Errors map deliberately. A node-enforced invariant violation is a `RuleError` and becomes
gRPC `FailedPrecondition` with the rule name in the status message, because the operator can
act on it (count the shelf, pick a different lot). Anything else is `Internal`.

`ConsumeReservation` exists in the domain but is not exposed here: the spec's API list does
not include it, and consuming a hold is a consequence of picking against it rather than
something an operator asks for directly.

**Files:**
- Create: `warehouse-node/proto/node_api.proto`
- Create: `warehouse-node/internal/transport/grpc/node_api.go`
- Test: `warehouse-node/internal/transport/grpc/node_api_test.go`
- Modify: `warehouse-node/go.mod` (adds `google.golang.org/grpc`, `google.golang.org/protobuf`)

**Interfaces:**
- Consumes: `node.Service` with `Execute`, `StockOnHand`, `Exceptions`, `Transfers` (Task 12); every `domain.Do*` command and command struct (Tasks 4-8); `projection.ExceptionRow`, `projection.TransferRow`, `projection.StockRow` (Tasks 10-11).
- Produces:
  - Generated package `nodeapi` at `warehouse-node/proto/nodeapi`, with `nodeapi.NodeAPIServer`, `nodeapi.RegisterNodeAPIServer`, `nodeapi.NewNodeAPIClient`, `nodeapi.UnimplementedNodeAPIServer` and the request/response messages listed in the proto below.
  - `func grpctransport.NewNodeAPI(svc *node.Service, now func() time.Time) *NodeAPI`
  - `grpctransport.NodeAPI` implementing `nodeapi.NodeAPIServer`
  - `func grpctransport.StatusError(err error) error` — `RuleError` becomes `FailedPrecondition`, everything else `Internal`

The Go package in `internal/transport/grpc` is named `grpctransport` so it does not collide
with `google.golang.org/grpc` at its call sites. Go permits a package name that differs from
its directory; import it as `grpctransport "…/internal/transport/grpc"`.

- [ ] **Step 1: Add the dependencies**

```bash
cd warehouse-node
go get google.golang.org/grpc@latest google.golang.org/protobuf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

- [ ] **Step 2: Write `proto/node_api.proto`**

```proto
syntax = "proto3";

package warehouse.node.v1;

option go_package = "github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi;nodeapi";

// Line is an operator-entered quantity before unit conversion: the uom may be the
// item's base unit or any of its declared alternates.
message Line {
  string sku = 1;
  string lot_id = 2;
  double qty = 3;
  string uom = 4;
}

// CommandResponse returns the identities of the events the command appended, in
// order. An empty list is impossible: a command either appends or fails.
message CommandResponse {
  repeated string event_ids = 1;
}

message ReceiveRequest {
  string receipt_id = 1;
  string delivery_note = 2;
  string po_ref = 3;
  Line line = 4;
  string to = 5;
}

message PutAwayRequest {
  Line line = 1;
  string from = 2;
  string to = 3;
}

message PickRequest {
  Line line = 1;
  string from = 2;
  string order_ref = 3;
}

message ReserveRequest {
  string reservation_id = 1;
  Line line = 2;
  string location = 3;
}

message ReleaseReservationRequest {
  string reservation_id = 1;
}

message StartCountRequest {
  string count_id = 1;
  string location = 2;
}

message CountLineRequest {
  string count_id = 1;
  Line line = 2;
}

message CloseCountRequest {
  string count_id = 1;
}

message DispatchTransferRequest {
  string transfer_id = 1;
  string to_node = 2;
  string from = 3;
  repeated Line lines = 4;
}

message ReceiveTransferRequest {
  string transfer_id = 1;
  string to = 2;
  repeated Line lines = 3;
}

message StockOnHandRequest {
  string sku = 1;
  string location = 2;
}

message StockRow {
  string sku = 1;
  string location = 2;
  string lot_id = 3;
  double qty = 4;
  double available = 5;
}

message StockOnHandResponse {
  repeated StockRow rows = 1;
}

message ExceptionsRequest {}

// ExceptionRow is one thing central reversed, or one balance a compensation drove
// negative. resolved is false while it still needs a human.
message ExceptionRow {
  string id = 1;
  string kind = 2;
  string reason = 3;
  string caused_by = 4;
  string sku = 5;
  string location = 6;
  string lot_id = 7;
  double qty = 8;
  string recorded_at = 9;
  bool resolved = 10;
}

message ExceptionsResponse {
  repeated ExceptionRow rows = 1;
}

message TransfersRequest {}

message TransferRow {
  string id = 1;
  string from_node = 2;
  string to_node = 3;
  double dispatched = 4;
  double received = 5;
  string status = 6;
  string dispatched_at = 7;
}

message TransfersResponse {
  repeated TransferRow rows = 1;
}

// NodeAPI is the operator-facing service. Every command is answered from local
// state; none of them waits on central.
service NodeAPI {
  rpc Receive(ReceiveRequest) returns (CommandResponse);
  rpc PutAway(PutAwayRequest) returns (CommandResponse);
  rpc Pick(PickRequest) returns (CommandResponse);
  rpc Reserve(ReserveRequest) returns (CommandResponse);
  rpc ReleaseReservation(ReleaseReservationRequest) returns (CommandResponse);
  rpc StartCount(StartCountRequest) returns (CommandResponse);
  rpc CountLine(CountLineRequest) returns (CommandResponse);
  rpc CloseCount(CloseCountRequest) returns (CommandResponse);
  rpc DispatchTransfer(DispatchTransferRequest) returns (CommandResponse);
  rpc ReceiveTransfer(ReceiveTransferRequest) returns (CommandResponse);
  rpc StockOnHand(StockOnHandRequest) returns (StockOnHandResponse);
  rpc Exceptions(ExceptionsRequest) returns (ExceptionsResponse);
  rpc Transfers(TransfersRequest) returns (TransfersResponse);
}
```

Generate the Go code:

```bash
cd warehouse-node && make proto
```

- [ ] **Step 3: Write the failing test**

`warehouse-node/internal/transport/grpc/node_api_test.go`:

```go
package grpctransport

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

func widget() domain.Item {
	return domain.Item{SKU: "WIDGET", Description: "Blue widget", BaseUoM: "EA",
		AltUoM: map[domain.UoM]float64{"CASE": 12}, LotTracked: true, ShelfLifeDays: 3650}
}

// newAPI starts a node with two locations and the widget item master, and wraps it
// in the gRPC service with a fixed clock.
func newAPI(t *testing.T) *NodeAPI {
	t.Helper()
	at := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	n := 0
	svc, err := node.Open(filepath.Join(t.TempDir(), "node.db"), "wh-a", func() time.Time {
		n++
		return at.Add(time.Duration(n) * time.Second)
	})
	if err != nil {
		t.Fatalf("node.Open: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	for code, typ := range map[domain.LocationCode]domain.LocationType{
		"RECV-01": domain.LocReceiving, "PICK-01": domain.LocPick, "STAGE-01": domain.LocStaging,
	} {
		if err := svc.RegisterLocation(code, typ); err != nil {
			t.Fatalf("RegisterLocation(%s): %v", code, err)
		}
	}
	if _, err := svc.Execute(func(*domain.State) ([]domain.Event, error) {
		return []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "WIDGET",
			Payload: domain.ItemUpserted{Item: widget()}}}, nil
	}); err != nil {
		t.Fatalf("seed item master: %v", err)
	}
	return NewNodeAPI(svc, func() time.Time { return at })
}

func line(qty float64, uom string) *nodeapi.Line {
	return &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: qty, Uom: uom}
}

// TestCommandsSucceedFromLocalStateOnly walks the whole command surface in the order
// an operator would: receive, put away, reserve, release, pick, count, transfer.
// Nothing in this test is connected to central, which is the property being asserted.
func TestCommandsSucceedFromLocalStateOnly(t *testing.T) {
	api := newAPI(t)
	ctx := context.Background()

	steps := []struct {
		name      string
		call      func() (*nodeapi.CommandResponse, error)
		wantCount int
	}{
		{
			name: "receive one case into the dock",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R1", DeliveryNote: "DN-1",
					PoRef: "PO-1", Line: line(1, "CASE"), To: "RECV-01"})
			},
			wantCount: 3,
		},
		{
			name: "put six away to the pick face",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.PutAway(ctx, &nodeapi.PutAwayRequest{Line: line(6, "EA"), From: "RECV-01", To: "PICK-01"})
			},
			wantCount: 1,
		},
		{
			name: "reserve two",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.Reserve(ctx, &nodeapi.ReserveRequest{ReservationId: "RS1", Line: line(2, "EA"), Location: "PICK-01"})
			},
			wantCount: 1,
		},
		{
			name: "release the reservation",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.ReleaseReservation(ctx, &nodeapi.ReleaseReservationRequest{ReservationId: "RS1"})
			},
			wantCount: 1,
		},
		{
			name: "pick one for a customer",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.Pick(ctx, &nodeapi.PickRequest{Line: line(1, "EA"), From: "PICK-01", OrderRef: "SO-9"})
			},
			wantCount: 1,
		},
		{
			name: "start a count of the pick face",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.StartCount(ctx, &nodeapi.StartCountRequest{CountId: "C1", Location: "PICK-01"})
			},
			wantCount: 1,
		},
		{
			name: "count four where the book says five",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.CountLine(ctx, &nodeapi.CountLineRequest{CountId: "C1", Line: line(4, "EA")})
			},
			wantCount: 1,
		},
		{
			name: "close the count, booking the variance",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.CloseCount(ctx, &nodeapi.CloseCountRequest{CountId: "C1"})
			},
			wantCount: 2,
		},
		{
			name: "dispatch two to the other warehouse",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
					ToNode: "wh-b", From: "PICK-01", Lines: []*nodeapi.Line{line(2, "EA")}})
			},
			wantCount: 1,
		},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			resp, err := s.call()
			if err != nil {
				t.Fatalf("%s: %v", s.name, err)
			}
			if len(resp.GetEventIds()) != s.wantCount {
				t.Fatalf("event ids = %v, want %d", resp.GetEventIds(), s.wantCount)
			}
		})
	}

	stock, err := api.StockOnHand(ctx, &nodeapi.StockOnHandRequest{Sku: "WIDGET", Location: "PICK-01"})
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(stock.GetRows()) != 1 || stock.GetRows()[0].GetQty() != 2 {
		t.Fatalf("rows = %+v, want a single row of 2", stock.GetRows())
	}

	transfers, err := api.Transfers(ctx, &nodeapi.TransfersRequest{})
	if err != nil {
		t.Fatalf("Transfers: %v", err)
	}
	if len(transfers.GetRows()) != 1 || transfers.GetRows()[0].GetStatus() != string(domain.TransferInFlight) {
		t.Fatalf("transfers = %+v, want one in-flight row", transfers.GetRows())
	}

	exceptions, err := api.Exceptions(ctx, &nodeapi.ExceptionsRequest{})
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions.GetRows()) != 0 {
		t.Fatalf("exceptions = %+v, want none: nothing has been compensated", exceptions.GetRows())
	}
}

// TestCommandsRejectViolationsAsFailedPrecondition asserts every command surfaces a
// node-enforced invariant as FailedPrecondition naming the rule, and appends nothing.
func TestCommandsRejectViolationsAsFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		call     func(*NodeAPI) error
		wantRule string
	}{
		{
			name: "receive an unknown sku",
			call: func(a *NodeAPI) error {
				_, err := a.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R1", DeliveryNote: "DN-1", PoRef: "PO-1",
					Line: &nodeapi.Line{Sku: "GHOST", Qty: 1, Uom: "EA"}, To: "RECV-01"})
				return err
			},
			wantRule: domain.RuleSKUExists,
		},
		{
			name: "put away more than is on the dock",
			call: func(a *NodeAPI) error {
				_, err := a.PutAway(ctx, &nodeapi.PutAwayRequest{Line: line(1, "EA"), From: "RECV-01", To: "PICK-01"})
				return err
			},
			wantRule: domain.RuleStockNonNegative,
		},
		{
			name: "pick from an empty shelf",
			call: func(a *NodeAPI) error {
				_, err := a.Pick(ctx, &nodeapi.PickRequest{Line: line(1, "EA"), From: "PICK-01"})
				return err
			},
			wantRule: domain.RuleStockNonNegative,
		},
		{
			name: "reserve more than is available",
			call: func(a *NodeAPI) error {
				_, err := a.Reserve(ctx, &nodeapi.ReserveRequest{ReservationId: "RS1", Line: line(1, "EA"), Location: "PICK-01"})
				return err
			},
			wantRule: domain.RuleReservationAvailable,
		},
		{
			name: "release a reservation that does not exist",
			call: func(a *NodeAPI) error {
				_, err := a.ReleaseReservation(ctx, &nodeapi.ReleaseReservationRequest{ReservationId: "nope"})
				return err
			},
			wantRule: domain.RuleAggregateState,
		},
		{
			name: "start a count at an unknown location",
			call: func(a *NodeAPI) error {
				_, err := a.StartCount(ctx, &nodeapi.StartCountRequest{CountId: "C1", Location: "MARS-01"})
				return err
			},
			wantRule: domain.RuleLocationExists,
		},
		{
			name: "count a line on a count that was never started",
			call: func(a *NodeAPI) error {
				_, err := a.CountLine(ctx, &nodeapi.CountLineRequest{CountId: "C9", Line: line(1, "EA")})
				return err
			},
			wantRule: domain.RuleAggregateState,
		},
		{
			name: "close a count that was never started",
			call: func(a *NodeAPI) error {
				_, err := a.CloseCount(ctx, &nodeapi.CloseCountRequest{CountId: "C9"})
				return err
			},
			wantRule: domain.RuleAggregateState,
		},
		{
			name: "dispatch stock the node does not have",
			call: func(a *NodeAPI) error {
				_, err := a.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1", ToNode: "wh-b",
					From: "PICK-01", Lines: []*nodeapi.Line{line(1, "EA")}})
				return err
			},
			wantRule: domain.RuleStockNonNegative,
		},
		{
			name: "receive a transfer this node was never told about",
			call: func(a *NodeAPI) error {
				_, err := a.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{TransferId: "T9", To: "RECV-01",
					Lines: []*nodeapi.Line{line(1, "EA")}})
				return err
			},
			wantRule: domain.RuleAggregateState,
		},
		{
			name: "bad uom for the item",
			call: func(a *NodeAPI) error {
				_, err := a.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R1", DeliveryNote: "DN-1", PoRef: "PO-1",
					Line: &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: 1, Uom: "PALLET"}, To: "RECV-01"})
				return err
			},
			wantRule: domain.RuleUoMValid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newAPI(t)
			err := tt.call(api)
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.FailedPrecondition {
				t.Fatalf("err = %v, want a FailedPrecondition status", err)
			}
			if !strings.Contains(st.Message(), tt.wantRule) {
				t.Fatalf("message = %q, want it to name rule %q", st.Message(), tt.wantRule)
			}
		})
	}
}

func TestReceiveTransferSucceedsAfterTheDispatchArrives(t *testing.T) {
	api := newAPI(t)
	ctx := context.Background()

	// Central forwards the source node's dispatch to this node.
	dispatch, err := domain.NewEnvelope(
		domain.EventID{NodeID: "wh-b", Seq: 1},
		domain.HLC{Wall: 1, Node: "wh-b"},
		time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC), nil,
		domain.Event{Type: domain.TypeTransferDispatched, AggregateID: "T1",
			Payload: domain.TransferDispatched{TransferID: "T1", FromNode: "wh-b", ToNode: "wh-a",
				Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-09", To: domain.External, Qty: 5}}}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if _, err := api.svc.Ingest([]domain.Envelope{dispatch}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if _, err := api.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{TransferId: "T1", To: "RECV-01",
		Lines: []*nodeapi.Line{line(5, "EA")}}); err != nil {
		t.Fatalf("ReceiveTransfer: %v", err)
	}
	rows, err := api.StockOnHand(ctx, &nodeapi.StockOnHandRequest{})
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(rows.GetRows()) != 1 || rows.GetRows()[0].GetQty() != 5 {
		t.Fatalf("rows = %+v, want a single row of 5", rows.GetRows())
	}
}

func TestNonRuleErrorsBecomeInternal(t *testing.T) {
	api := newAPI(t)
	ctx := context.Background()
	if err := api.svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	calls := map[string]func() error{
		"Receive": func() error {
			_, err := api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R2", DeliveryNote: "DN-2", PoRef: "PO-1",
				Line: line(1, "EA"), To: "RECV-01"})
			return err
		},
		"StockOnHand": func() error {
			_, err := api.StockOnHand(ctx, &nodeapi.StockOnHandRequest{})
			return err
		},
		"Exceptions": func() error {
			_, err := api.Exceptions(ctx, &nodeapi.ExceptionsRequest{})
			return err
		},
		"Transfers": func() error {
			_, err := api.Transfers(ctx, &nodeapi.TransfersRequest{})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			st, ok := status.FromError(call())
			if !ok || st.Code() != codes.Internal {
				t.Fatalf("%s: status = %v, want Internal", name, st)
			}
		})
	}
	if err := StatusError(nil); err != nil {
		t.Errorf("StatusError(nil) = %v, want nil", err)
	}
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/transport/grpc/ -v`
Expected: FAIL to compile — `undefined: NewNodeAPI`, `undefined: NodeAPI`, `undefined: StatusError`.

- [ ] **Step 5: Write `node_api.go`**

```go
// Package grpctransport exposes a warehouse node over gRPC to the operator's client.
// It is named grpctransport rather than grpc so it does not shadow
// google.golang.org/grpc at its call sites.
package grpctransport

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

// NodeAPI serves the operator-facing service from one node's local state. No method
// here waits on central: that is what keeps the warehouse working offline.
type NodeAPI struct {
	nodeapi.UnimplementedNodeAPIServer
	svc *node.Service
	now func() time.Time
}

// NewNodeAPI wraps a node service. now supplies the instant commands that care about
// dates (pick, dispatch — both check lot expiry) validate against; it is injected so
// the domain never reaches for a clock.
func NewNodeAPI(svc *node.Service, now func() time.Time) *NodeAPI {
	return &NodeAPI{svc: svc, now: now}
}

// StatusError maps a domain error onto a gRPC status. A violated node-enforced
// invariant is FailedPrecondition and names its rule, because the operator can act
// on it. Anything else is Internal.
func StatusError(err error) error {
	if err == nil {
		return nil
	}
	var rule domain.RuleError
	if errors.As(err, &rule) {
		return status.Error(codes.FailedPrecondition, rule.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// run executes a command and turns the resulting envelopes into their identities.
func (a *NodeAPI) run(cmd node.Command) (*nodeapi.CommandResponse, error) {
	envs, err := a.svc.Execute(cmd)
	if err != nil {
		return nil, StatusError(err)
	}
	ids := make([]string, 0, len(envs))
	for _, env := range envs {
		ids = append(ids, env.ID.String())
	}
	return &nodeapi.CommandResponse{EventIds: ids}, nil
}

// toLine converts one wire line into a domain line.
func toLine(l *nodeapi.Line) domain.Line {
	return domain.Line{SKU: l.GetSku(), LotID: l.GetLotId(), Qty: l.GetQty(), UoM: domain.UoM(l.GetUom())}
}

func toLines(ls []*nodeapi.Line) []domain.Line {
	out := make([]domain.Line, 0, len(ls))
	for _, l := range ls {
		out = append(out, toLine(l))
	}
	return out
}

// Receive records a supplier delivery line arriving at the dock.
func (a *NodeAPI) Receive(_ context.Context, r *nodeapi.ReceiveRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReceive(s, domain.ReceiveCmd{
			ReceiptID: r.GetReceiptId(), DeliveryNote: r.GetDeliveryNote(), PORef: r.GetPoRef(),
			Line: toLine(r.GetLine()), To: domain.LocationCode(r.GetTo())})
	})
}

// PutAway moves received goods into storage. Nothing enters or leaves the warehouse.
func (a *NodeAPI) PutAway(_ context.Context, r *nodeapi.PutAwayRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoPutAway(s, domain.PutAwayCmd{Line: toLine(r.GetLine()),
			From: domain.LocationCode(r.GetFrom()), To: domain.LocationCode(r.GetTo())})
	})
}

// Pick takes goods off a shelf for a customer order.
func (a *NodeAPI) Pick(_ context.Context, r *nodeapi.PickRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoPick(s, domain.PickCmd{Line: toLine(r.GetLine()),
			From: domain.LocationCode(r.GetFrom()), OrderRef: r.GetOrderRef(), At: a.now()})
	})
}

// Reserve places a soft hold so two orders cannot promise the same units.
func (a *NodeAPI) Reserve(_ context.Context, r *nodeapi.ReserveRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReserve(s, domain.ReserveCmd{ReservationID: r.GetReservationId(),
			Line: toLine(r.GetLine()), Location: domain.LocationCode(r.GetLocation())})
	})
}

// ReleaseReservation cancels a hold, returning its quantity to available.
func (a *NodeAPI) ReleaseReservation(_ context.Context, r *nodeapi.ReleaseReservationRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReleaseReservation(s, domain.ReleaseReservationCmd{ReservationID: r.GetReservationId()})
	})
}

// StartCount opens a physical recount of one location.
func (a *NodeAPI) StartCount(_ context.Context, r *nodeapi.StartCountRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoStartCount(s, domain.StartCountCmd{CountID: r.GetCountId(),
			Location: domain.LocationCode(r.GetLocation())})
	})
}

// CountLine records what the operator physically counted.
func (a *NodeAPI) CountLine(_ context.Context, r *nodeapi.CountLineRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoCountLine(s, domain.CountLineCmd{CountID: r.GetCountId(), Line: toLine(r.GetLine())})
	})
}

// CloseCount ends a count and books one adjustment per variance line.
func (a *NodeAPI) CloseCount(_ context.Context, r *nodeapi.CloseCountRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoCloseCount(s, domain.CloseCountCmd{CountID: r.GetCountId()})
	})
}

// DispatchTransfer is the first half of an inter-node transfer: this node's stock
// leaves now. Whether the destination exists and accepts the item is central's call,
// not this node's, so it is not checked here.
func (a *NodeAPI) DispatchTransfer(_ context.Context, r *nodeapi.DispatchTransferRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoDispatchTransfer(s, domain.DispatchTransferCmd{
			TransferID: r.GetTransferId(), FromNode: a.svc.NodeID(), ToNode: domain.NodeID(r.GetToNode()),
			From: domain.LocationCode(r.GetFrom()), Lines: toLines(r.GetLines()), At: a.now()})
	})
}

// ReceiveTransfer is the second half: the truck arrived. It requires that central has
// already forwarded the matching dispatch to this node.
func (a *NodeAPI) ReceiveTransfer(_ context.Context, r *nodeapi.ReceiveTransferRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReceiveTransfer(s, domain.ReceiveTransferCmd{TransferID: r.GetTransferId(),
			To: domain.LocationCode(r.GetTo()), Lines: toLines(r.GetLines())})
	})
}

// StockOnHand answers the balance query. Empty fields mean "no filter".
func (a *NodeAPI) StockOnHand(_ context.Context, r *nodeapi.StockOnHandRequest) (*nodeapi.StockOnHandResponse, error) {
	rows, err := a.svc.StockOnHand(r.GetSku(), domain.LocationCode(r.GetLocation()))
	if err != nil {
		return nil, StatusError(err)
	}
	out := make([]*nodeapi.StockRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, &nodeapi.StockRow{Sku: r.SKU, Location: string(r.Location), LotId: r.LotID,
			Qty: r.Qty, Available: r.Available})
	}
	return &nodeapi.StockOnHandResponse{Rows: out}, nil
}

// Exceptions answers "what did central reverse, and why". It is the operator's only
// window onto compensation, including balances a compensation drove negative.
func (a *NodeAPI) Exceptions(_ context.Context, _ *nodeapi.ExceptionsRequest) (*nodeapi.ExceptionsResponse, error) {
	rows, err := a.svc.Exceptions()
	if err != nil {
		return nil, StatusError(err)
	}
	out := make([]*nodeapi.ExceptionRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, &nodeapi.ExceptionRow{ID: r.ID, Kind: string(r.Kind), Reason: r.Reason,
			CausedBy: r.CausedBy, Sku: r.Key.SKU, Location: string(r.Key.Location), LotId: r.Key.LotID,
			Qty: r.Qty, RecordedAt: r.RecordedAt.UTC().Format(time.RFC3339Nano), Resolved: r.Resolved})
	}
	return &nodeapi.ExceptionsResponse{Rows: out}, nil
}

// Transfers answers this node's view of every inter-node transfer.
func (a *NodeAPI) Transfers(_ context.Context, _ *nodeapi.TransfersRequest) (*nodeapi.TransfersResponse, error) {
	rows, err := a.svc.Transfers()
	if err != nil {
		return nil, StatusError(err)
	}
	out := make([]*nodeapi.TransferRow, 0, len(rows))
	for _, r := range rows {
		at := ""
		if !r.DispatchedAt.IsZero() {
			at = r.DispatchedAt.UTC().Format(time.RFC3339Nano)
		}
		out = append(out, &nodeapi.TransferRow{ID: r.ID, FromNode: string(r.FromNode), ToNode: string(r.ToNode),
			Dispatched: r.Dispatched, Received: r.Received, Status: string(r.Status), DispatchedAt: at})
	}
	return &nodeapi.TransfersResponse{Rows: out}, nil
}
```

The generated field for a proto field named `id` is `ID` under the default
`protoc-gen-go` initialisms; if your generator version emits `Id`, adjust the two
struct literals above accordingly. Verify once with
`grep -n 'ID\b' proto/nodeapi/node_api.pb.go | head`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/transport/grpc/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 7: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean. Generated `.pb.go` files are excluded from linting; if
`golangci-lint` reports on them, add to `.golangci.yml`:

```yaml
issues:
  exclude-files:
    - ".*\\.pb\\.go$"
```

- [ ] **Step 8: Commit**

```bash
git add warehouse-node/proto warehouse-node/internal/transport warehouse-node/go.mod warehouse-node/go.sum warehouse-node/.golangci.yml
git commit -m "feat(transport): add the operator gRPC node API

- ten commands and three queries, all answered from local state
- node-enforced invariant violations become FailedPrecondition naming the rule
- transfer dispatch does not check the destination: that is central's call"
```

---

### Task 14: The sync wire format — `sync.proto` and the envelope codec

The replication protocol. One bidirectional gRPC stream, opened by the node, carrying
frames in both directions:

- the node sends `Hello` (who it is, what it already holds, how far it has pulled), then
  `EventBatch`es of its own events, and `Ack`s for what it has applied;
- central replies `Welcome`, then `EventBatch`es of item-master updates, compensations and
  transfer events destined for this node, plus `Ack`s for what it has persisted.

Everything is resumable because both sides persist cursors: the node's `pushed_to_central`
and `pulled_from_central` (Task 9), and central's per-node pair (Task 15). A node offline
for a week does not restart from zero, and it does not send a week of events in one
message either — batches are capped by both event count and encoded size, so catch-up is a
sequence of bounded chunks.

This task is only the wire format and the pure conversion between it and
`domain.Envelope`. No streaming yet, so it is fully testable with round-trip tables.

**Files:**
- Create: `warehouse-node/proto/sync.proto`
- Create: `warehouse-node/internal/sync/codec.go`
- Test: `warehouse-node/internal/sync/codec_test.go`

**Interfaces:**
- Consumes: `domain.Envelope`, `domain.EventID`, `domain.HLC`, `domain.NodeID` (Tasks 1-2).
- Produces:
  - Generated package `syncpb` at `warehouse-node/proto/syncpb`, with `syncpb.SyncServer`, `syncpb.RegisterSyncServer`, `syncpb.NewSyncClient`, `syncpb.NodeFrame`, `syncpb.CentralFrame`, `syncpb.EventBatch`, `syncpb.Hello`, `syncpb.Welcome`, `syncpb.Ack`, `syncpb.Event`, `syncpb.EventID`, `syncpb.HLC`
  - `syncrepl.MaxBatchEvents = 200` and `syncrepl.MaxBatchBytes = 1 << 20` — the size cap that bounds catch-up chunks
  - `func syncrepl.EncodeEnvelope(env domain.Envelope) *syncpb.Event`
  - `func syncrepl.DecodeEnvelope(e *syncpb.Event) (domain.Envelope, error)`
  - `func syncrepl.EncodeBatch(envs []domain.Envelope) *syncpb.EventBatch`
  - `func syncrepl.DecodeBatch(b *syncpb.EventBatch) ([]domain.Envelope, error)`
  - `func syncrepl.Chunk(envs []domain.Envelope) [][]domain.Envelope`

The Go package in `internal/sync` is named `syncrepl` so it does not shadow the standard
library's `sync`; import it as `syncrepl "…/internal/sync"`.

- [ ] **Step 1: Write `proto/sync.proto`**

```proto
syntax = "proto3";

package warehouse.sync.v1;

option go_package = "github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb;syncpb";

// EventID is an event's global identity: the node that produced it and that node's
// own monotonic sequence number.
message EventID {
  string node_id = 1;
  uint64 seq = 2;
}

// HLC is a hybrid logical clock reading. It orders events across nodes without
// requiring their wall clocks to agree.
message HLC {
  int64 wall = 1;
  uint32 counter = 2;
  string node = 3;
}

// Event is one envelope on the wire. causation_id is set only on compensating events
// emitted by central, and names the event being compensated.
message Event {
  EventID id = 1;
  string aggregate_id = 2;
  string type = 3;
  HLC hlc = 4;
  int64 recorded_at_unix_nano = 5;
  EventID causation_id = 6;
  bytes payload = 7;
}

// EventBatch is one bounded chunk. more is true when the sender has further events
// ready, which lets the receiver keep asking without guessing. last_ord is set only
// on batches central sends down, and is the outbound position of the final event in
// the batch — what the node acknowledges and persists as its pull cursor.
message EventBatch {
  repeated Event events = 1;
  bool more = 2;
  uint64 last_ord = 3;
}

// Hello opens a session. version_vector is the highest sequence the node holds per
// originating node, so central can tell what it still needs to send.
message Hello {
  string node_id = 1;
  map<string, uint64> version_vector = 2;
  uint64 pulled_cursor = 3;
}

// Welcome answers Hello with the highest sequence central holds for this node, so
// the node knows where to resume pushing from.
message Welcome {
  uint64 known_seq = 1;
}

// Ack confirms durability. seq is the highest sequence of the node's own events that
// central has persisted; ord is the highest downstream position the node has applied.
message Ack {
  uint64 seq = 1;
  uint64 ord = 2;
}

message NodeFrame {
  oneof body {
    Hello hello = 1;
    EventBatch events = 2;
    Ack ack = 3;
  }
}

message CentralFrame {
  oneof body {
    Welcome welcome = 1;
    EventBatch events = 2;
    Ack ack = 3;
  }
}

// Sync replicates events in both directions over one stream. The node always opens
// it, because the node may be behind a connection central cannot dial.
service Sync {
  rpc Replicate(stream NodeFrame) returns (stream CentralFrame);
}
```

Add the file to the `proto` target (it is already listed in the Makefile from Task 1) and
generate:

```bash
cd warehouse-node && make proto
```

- [ ] **Step 2: Write the failing test**

`warehouse-node/internal/sync/codec_test.go`:

```go
package syncrepl

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	cause := domain.EventID{NodeID: "wh-a", Seq: 7}
	tests := []struct {
		name string
		env  domain.Envelope
	}{
		{
			name: "plain event",
			env: domain.Envelope{
				ID:          domain.EventID{NodeID: "wh-a", Seq: 1},
				AggregateID: "WIDGET",
				Type:        domain.TypePutAway,
				HLC:         domain.HLC{Wall: 1700000000000, Counter: 3, Node: "wh-a"},
				RecordedAt:  time.Date(2026, 7, 30, 8, 0, 0, 123, time.UTC),
				Payload:     json.RawMessage(`{"move":{"sku":"WIDGET","from":"RECV-01","to":"PICK-01","qty":4}}`),
			},
		},
		{
			name: "compensating event carries its causation",
			env: domain.Envelope{
				ID:          domain.EventID{NodeID: "central", Seq: 2},
				AggregateID: "WIDGET",
				Type:        domain.TypeStockAdjusted,
				HLC:         domain.HLC{Wall: 1700000000001, Counter: 0, Node: "central"},
				RecordedAt:  time.Date(2026, 7, 31, 9, 0, 0, 0, time.UTC),
				CausationID: &cause,
				Payload:     json.RawMessage(`{"move":{"sku":"WIDGET","from":"RECV-01","to":"external","qty":4},"reason":"po_overreceipt"}`),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeEnvelope(EncodeEnvelope(tt.env))
			if err != nil {
				t.Fatalf("DecodeEnvelope: %v", err)
			}
			if got.ID != tt.env.ID || got.Type != tt.env.Type || got.AggregateID != tt.env.AggregateID {
				t.Errorf("identity round-tripped as %+v, want %+v", got, tt.env)
			}
			if got.HLC != tt.env.HLC {
				t.Errorf("HLC = %+v, want %+v", got.HLC, tt.env.HLC)
			}
			if !got.RecordedAt.Equal(tt.env.RecordedAt) {
				t.Errorf("RecordedAt = %v, want %v", got.RecordedAt, tt.env.RecordedAt)
			}
			if string(got.Payload) != string(tt.env.Payload) {
				t.Errorf("Payload = %s, want %s", got.Payload, tt.env.Payload)
			}
			switch {
			case tt.env.CausationID == nil && got.CausationID != nil:
				t.Errorf("CausationID = %v, want nil", got.CausationID)
			case tt.env.CausationID != nil && got.CausationID == nil:
				t.Error("CausationID = nil, want it preserved")
			case tt.env.CausationID != nil && *got.CausationID != *tt.env.CausationID:
				t.Errorf("CausationID = %v, want %v", *got.CausationID, *tt.env.CausationID)
			}
		})
	}
}

func TestDecodeEnvelopeRejectsMalformedFrames(t *testing.T) {
	tests := []struct {
		name string
		in   *syncpb.Event
	}{
		{"nil event", nil},
		{"missing id", &syncpb.Event{Type: domain.TypePutAway}},
		{"empty node id", &syncpb.Event{Id: &syncpb.EventID{Seq: 1}, Type: domain.TypePutAway}},
		{"zero sequence", &syncpb.Event{Id: &syncpb.EventID{NodeId: "wh-a"}, Type: domain.TypePutAway}},
		{"missing type", &syncpb.Event{Id: &syncpb.EventID{NodeId: "wh-a", Seq: 1}}},
		{"missing hlc", &syncpb.Event{Id: &syncpb.EventID{NodeId: "wh-a", Seq: 1}, Type: domain.TypePutAway}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeEnvelope(tt.in); err == nil {
				t.Fatalf("DecodeEnvelope(%+v) = nil error, want a rejection", tt.in)
			}
		})
	}
}

func TestBatchRoundTripAndRejection(t *testing.T) {
	envs := []domain.Envelope{
		{ID: domain.EventID{NodeID: "wh-a", Seq: 1}, Type: domain.TypePutAway, AggregateID: "W",
			HLC: domain.HLC{Wall: 1, Node: "wh-a"}, RecordedAt: time.Unix(0, 0).UTC(), Payload: json.RawMessage(`{}`)},
		{ID: domain.EventID{NodeID: "wh-a", Seq: 2}, Type: domain.TypePicked, AggregateID: "W",
			HLC: domain.HLC{Wall: 2, Node: "wh-a"}, RecordedAt: time.Unix(1, 0).UTC(), Payload: json.RawMessage(`{}`)},
	}
	got, err := DecodeBatch(EncodeBatch(envs))
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(got) != 2 || got[0].ID != envs[0].ID || got[1].ID != envs[1].ID {
		t.Fatalf("batch = %+v, want the two originals in order", got)
	}
	if _, err := DecodeBatch(&syncpb.EventBatch{Events: []*syncpb.Event{{}}}); err == nil {
		t.Error("DecodeBatch with a malformed event: expected an error")
	}
	if _, err := DecodeBatch(nil); err != nil {
		t.Errorf("DecodeBatch(nil) = %v, want no error and an empty result", err)
	}
}

func TestChunkBoundsCatchUp(t *testing.T) {
	tests := []struct {
		name       string
		count      int
		payload    int
		wantChunks int
	}{
		{"empty input yields no chunks", 0, 10, 0},
		{"under both caps is one chunk", 5, 10, 1},
		{"count cap splits", MaxBatchEvents + 1, 10, 2},
		{"exactly the count cap is one chunk", MaxBatchEvents, 10, 1},
		// Each envelope costs payload + type + aggregate + 64 bytes of framing, so
		// three of these fit in a 1 MiB chunk and eight of them need three chunks.
		{"byte cap splits before the count cap", 8, MaxBatchBytes / 4, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envs := make([]domain.Envelope, tt.count)
			for i := range envs {
				envs[i] = domain.Envelope{
					ID:      domain.EventID{NodeID: "wh-a", Seq: uint64(i + 1)},
					Type:    domain.TypePutAway,
					HLC:     domain.HLC{Wall: int64(i), Node: "wh-a"},
					Payload: json.RawMessage(make([]byte, tt.payload)),
				}
			}
			chunks := Chunk(envs)
			if len(chunks) != tt.wantChunks {
				t.Fatalf("len(chunks) = %d, want %d", len(chunks), tt.wantChunks)
			}
			var total int
			for _, c := range chunks {
				if len(c) == 0 {
					t.Fatal("a chunk is empty, which would stall the stream")
				}
				if len(c) > MaxBatchEvents {
					t.Errorf("chunk of %d events exceeds the cap of %d", len(c), MaxBatchEvents)
				}
				total += len(c)
			}
			if total != tt.count {
				t.Errorf("chunks contain %d events, want all %d", total, tt.count)
			}
		})
	}
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/sync/ -v`
Expected: FAIL to compile — `undefined: EncodeEnvelope`, `undefined: MaxBatchEvents`.

- [ ] **Step 4: Write `codec.go`**

```go
// Package syncrepl implements bidirectional replication between a warehouse node and
// central. It is named syncrepl rather than sync so it does not shadow the standard
// library's sync package at its call sites.
package syncrepl

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// Batch caps. A node offline for a week has a large backlog, and sending it in one
// message would blow both the gRPC message limit and the receiver's memory. Both
// caps are enforced, whichever bites first.
const (
	MaxBatchEvents = 200
	MaxBatchBytes  = 1 << 20
)

// EncodeEnvelope converts a stored envelope to its wire form. The payload travels as
// opaque bytes: only the node and central's projections interpret it, and the wire
// format deliberately does not need to know the payload types.
func EncodeEnvelope(env domain.Envelope) *syncpb.Event {
	e := &syncpb.Event{
		Id:                 &syncpb.EventID{NodeId: string(env.ID.NodeID), Seq: env.ID.Seq},
		AggregateId:        env.AggregateID,
		Type:               env.Type,
		Hlc:                &syncpb.HLC{Wall: env.HLC.Wall, Counter: env.HLC.Counter, Node: string(env.HLC.Node)},
		RecordedAtUnixNano: env.RecordedAt.UnixNano(),
		Payload:            env.Payload,
	}
	if env.CausationID != nil {
		e.CausationId = &syncpb.EventID{NodeId: string(env.CausationID.NodeID), Seq: env.CausationID.Seq}
	}
	return e
}

// DecodeEnvelope converts a wire event back to an envelope. Every structural field is
// required: an event that cannot be decoded must fail the session loudly rather than
// be skipped, because skipping an event silently forks state.
func DecodeEnvelope(e *syncpb.Event) (domain.Envelope, error) {
	switch {
	case e == nil:
		return domain.Envelope{}, fmt.Errorf("decode event: frame is nil")
	case e.GetId() == nil || e.GetId().GetNodeId() == "" || e.GetId().GetSeq() == 0:
		return domain.Envelope{}, fmt.Errorf("decode event: identity is missing or incomplete")
	case e.GetType() == "":
		return domain.Envelope{}, fmt.Errorf("decode event %s/%d: type is empty",
			e.GetId().GetNodeId(), e.GetId().GetSeq())
	case e.GetHlc() == nil:
		return domain.Envelope{}, fmt.Errorf("decode event %s/%d: hlc is missing",
			e.GetId().GetNodeId(), e.GetId().GetSeq())
	}
	env := domain.Envelope{
		ID:          domain.EventID{NodeID: domain.NodeID(e.GetId().GetNodeId()), Seq: e.GetId().GetSeq()},
		AggregateID: e.GetAggregateId(),
		Type:        e.GetType(),
		HLC: domain.HLC{Wall: e.GetHlc().GetWall(), Counter: e.GetHlc().GetCounter(),
			Node: domain.NodeID(e.GetHlc().GetNode())},
		RecordedAt: time.Unix(0, e.GetRecordedAtUnixNano()).UTC(),
		Payload:    json.RawMessage(e.GetPayload()),
	}
	if c := e.GetCausationId(); c != nil {
		env.CausationID = &domain.EventID{NodeID: domain.NodeID(c.GetNodeId()), Seq: c.GetSeq()}
	}
	return env, nil
}

// EncodeBatch wraps envelopes in a batch frame. Callers pass a chunk from Chunk, so
// this does not itself enforce the caps.
func EncodeBatch(envs []domain.Envelope) *syncpb.EventBatch {
	out := make([]*syncpb.Event, 0, len(envs))
	for _, env := range envs {
		out = append(out, EncodeEnvelope(env))
	}
	return &syncpb.EventBatch{Events: out}
}

// DecodeBatch converts a batch frame. One undecodable event fails the whole batch.
func DecodeBatch(b *syncpb.EventBatch) ([]domain.Envelope, error) {
	out := make([]domain.Envelope, 0, len(b.GetEvents()))
	for _, e := range b.GetEvents() {
		env, err := DecodeEnvelope(e)
		if err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, nil
}

// Chunk splits envelopes into batches within both caps, preserving order. A single
// envelope larger than MaxBatchBytes still gets its own chunk rather than being
// dropped: a chunk is never empty, because an empty batch would stall the stream.
func Chunk(envs []domain.Envelope) [][]domain.Envelope {
	var chunks [][]domain.Envelope
	var current []domain.Envelope
	var bytes int
	for _, env := range envs {
		size := len(env.Payload) + len(env.Type) + len(env.AggregateID) + 64
		if len(current) > 0 && (len(current) >= MaxBatchEvents || bytes+size > MaxBatchBytes) {
			chunks = append(chunks, current)
			current, bytes = nil, 0
		}
		current = append(current, env)
		bytes += size
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd warehouse-node && go test ./internal/sync/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 6: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 7: Commit**

```bash
git add warehouse-node/proto warehouse-node/internal/sync
git commit -m "feat(sync): add the replication wire format and envelope codec

- NodeFrame/CentralFrame with Hello/EventBatch/Ack and Welcome oneofs
- payloads travel as opaque bytes; the wire format knows no payload types
- an undecodable event fails the batch, never skips: skipping forks state
- Chunk bounds catch-up by both event count and encoded size"
```

---

### Task 15: The central store contract and its in-memory implementation

Central needs to hold rather more than a log. To arbitrate it must see things no node can:
every node's receipts against a shared purchase order, every delivery note ever keyed,
which nodes exist and what they refuse to stock, and the **in-transit** balance of each
transfer — the quantity that has left the source but not yet arrived at the destination,
belonging to neither node.

All of that goes behind one `Store` interface, for one reason: the arbitration rules are the
interesting, heavily tested part of this system, and they must be testable without a
database. So there are two implementations — an in-memory one (this task) used by the
arbiter tests and the integration suite, and a Postgres one (Task 16) used in deployment —
and **one** contract test table that both must pass. Writing the behaviour tests once is the
only reason a second implementation is affordable.

The interface is deliberately wide rather than split into many narrow ones: it describes a
single cohesive thing (everything central persists), and splitting it would only mean every
caller assembling the same pieces back together.

**Files:**
- Create: `warehouse-node/internal/central/store.go`
- Create: `warehouse-node/internal/central/memory.go`
- Test: `warehouse-node/internal/central/contract_test.go`
- Test: `warehouse-node/internal/central/memory_test.go`

**Interfaces:**
- Consumes: `domain.Envelope`, `domain.Event`, `domain.EventID`, `domain.HLC`, `domain.NodeID`, `domain.StockKey`, `domain.Item`, `domain.Tick`, `domain.NewEnvelope` (Tasks 1-2).
- Produces:
  - `central.CentralNode domain.NodeID = "central"` — the node identity central's own events carry
  - `central.Verdict` (string) with `VerdictAccepted = "accepted"` and `VerdictRejected = "rejected"`
  - `central.Decision{EventID domain.EventID; Verdict Verdict; Reason string; CompensatingID *domain.EventID}`
  - `central.ReceiptFact{EventID domain.EventID; Node domain.NodeID; PORef, DeliveryNote, SKU string; QtyBase float64}`
  - `central.InTransitRow{TransferID string; Key domain.StockKey; FromNode, ToNode domain.NodeID; Dispatched, Received float64; DispatchedAt time.Time; Failed bool}`
  - `central.Outbound{Ord uint64; Env domain.Envelope}`
  - `central.Store` interface, exactly as written in Step 2
  - `func central.NewMemory() *Memory` implementing `central.Store`

- [ ] **Step 1: Write the contract test**

`warehouse-node/internal/central/contract_test.go`:

```go
package central

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// runStoreContract is the behaviour every Store implementation must satisfy. It is
// run once against Memory and once against Postgres, which is what makes having two
// implementations affordable.
func runStoreContract(t *testing.T, open func(t *testing.T) Store) {
	t.Helper()

	at := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)

	nodeEnv := func(node domain.NodeID, seq uint64, typ string) domain.Envelope {
		env, err := domain.NewEnvelope(
			domain.EventID{NodeID: node, Seq: seq},
			domain.HLC{Wall: at.UnixMilli() + int64(seq), Node: node},
			at.Add(time.Duration(seq)*time.Second), nil,
			domain.Event{Type: typ, AggregateID: "WIDGET", Payload: domain.PutAway{
				Move: domain.Movement{SKU: "WIDGET", From: "RECV-01", To: "PICK-01", Qty: float64(seq)}}})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		return env
	}

	t.Run("append is idempotent and returns only what was new", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		first := []domain.Envelope{nodeEnv("wh-a", 1, domain.TypePutAway), nodeEnv("wh-a", 2, domain.TypePutAway)}

		got, err := s.Append(ctx, first)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("first Append returned %d new, want 2", len(got))
		}
		got, err = s.Append(ctx, append(first, nodeEnv("wh-a", 3, domain.TypePicked)))
		if err != nil {
			t.Fatalf("re-Append: %v", err)
		}
		if len(got) != 1 || got[0].ID.Seq != 3 {
			t.Fatalf("overlapping Append returned %+v, want only seq 3", got)
		}
		all, err := s.Events(ctx)
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if len(all) != 3 {
			t.Fatalf("Events returned %d, want 3", len(all))
		}
		for i := range all {
			if all[i].ID.Seq != uint64(i+1) {
				t.Errorf("Events[%d].Seq = %d, want %d: reads must come back in HLC order",
					i, all[i].ID.Seq, i+1)
			}
		}
	})

	t.Run("events are partitioned by node", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, err := s.Append(ctx, []domain.Envelope{
			nodeEnv("wh-a", 1, domain.TypePutAway), nodeEnv("wh-b", 1, domain.TypePutAway),
		}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		// The same sequence number from two nodes is two distinct events, because
		// identity is the (node, seq) pair.
		all, err := s.Events(ctx)
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("Events returned %d, want 2", len(all))
		}
	})

	t.Run("central emits its own events with sequence and hlc assigned", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		envs, err := s.EmitCentral(ctx, []domain.Event{
			{Type: domain.TypeStockAdjusted, AggregateID: "WIDGET", Payload: domain.StockAdjusted{
				Move:   domain.Movement{SKU: "WIDGET", From: "RECV-01", To: domain.External, Qty: 4},
				Reason: domain.ReasonPOOverReceipt}},
		}, &domain.EventID{NodeID: "wh-a", Seq: 7}, at)
		if err != nil {
			t.Fatalf("EmitCentral: %v", err)
		}
		if len(envs) != 1 {
			t.Fatalf("EmitCentral returned %d envelopes, want 1", len(envs))
		}
		e := envs[0]
		if e.ID.NodeID != CentralNode || e.ID.Seq != 1 {
			t.Errorf("ID = %v, want central/1", e.ID)
		}
		if e.HLC.Node != CentralNode || e.HLC.Wall == 0 {
			t.Errorf("HLC = %+v, want central's clock reading", e.HLC)
		}
		if e.CausationID == nil || e.CausationID.Seq != 7 {
			t.Errorf("CausationID = %v, want wh-a/7", e.CausationID)
		}

		next, err := s.EmitCentral(ctx, []domain.Event{
			{Type: domain.TypeItemUpserted, AggregateID: "WIDGET", Payload: domain.ItemUpserted{
				Item: domain.Item{SKU: "WIDGET", BaseUoM: "EA"}}},
		}, nil, at.Add(time.Second))
		if err != nil {
			t.Fatalf("second EmitCentral: %v", err)
		}
		if next[0].ID.Seq != 2 {
			t.Errorf("second Seq = %d, want 2: central's sequence must be monotonic", next[0].ID.Seq)
		}
		if next[0].CausationID != nil {
			t.Errorf("CausationID = %v, want nil for a non-compensating event", next[0].CausationID)
		}
	})

	t.Run("cursors are per node and default to zero", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		for _, node := range []domain.NodeID{"wh-a", "wh-b"} {
			if got, err := s.PushedSeq(ctx, node); err != nil || got != 0 {
				t.Fatalf("PushedSeq(%s) = %d, %v; want 0, nil", node, got, err)
			}
			if got, err := s.DeliveredOrd(ctx, node); err != nil || got != 0 {
				t.Fatalf("DeliveredOrd(%s) = %d, %v; want 0, nil", node, got, err)
			}
		}
		if err := s.SetPushedSeq(ctx, "wh-a", 42); err != nil {
			t.Fatalf("SetPushedSeq: %v", err)
		}
		if err := s.SetDeliveredOrd(ctx, "wh-a", 7); err != nil {
			t.Fatalf("SetDeliveredOrd: %v", err)
		}
		if err := s.SetPushedSeq(ctx, "wh-a", 43); err != nil {
			t.Fatalf("SetPushedSeq again: %v", err)
		}
		if got, err := s.PushedSeq(ctx, "wh-a"); err != nil || got != 43 {
			t.Errorf("PushedSeq = %d, %v; want 43, nil", got, err)
		}
		if got, err := s.DeliveredOrd(ctx, "wh-a"); err != nil || got != 7 {
			t.Errorf("DeliveredOrd = %d, %v; want 7, nil", got, err)
		}
		if got, err := s.PushedSeq(ctx, "wh-b"); err != nil || got != 0 {
			t.Errorf("PushedSeq(wh-b) = %d, %v; want 0, nil: cursors are per node", got, err)
		}
	})

	t.Run("outbound queue is ordered, per target and resumable", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		envs := []domain.Envelope{nodeEnv("wh-a", 1, domain.TypePutAway), nodeEnv("wh-a", 2, domain.TypePicked)}
		if _, err := s.Append(ctx, envs); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := s.Enqueue(ctx, "wh-b", envs); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		out, err := s.Outbound(ctx, "wh-b", 0, 10)
		if err != nil {
			t.Fatalf("Outbound: %v", err)
		}
		if len(out) != 2 || out[0].Ord >= out[1].Ord {
			t.Fatalf("Outbound = %+v, want two rows in increasing ord order", out)
		}
		if out[0].Env.ID != envs[0].ID {
			t.Errorf("Outbound[0] = %v, want %v", out[0].Env.ID, envs[0].ID)
		}
		resumed, err := s.Outbound(ctx, "wh-b", out[0].Ord, 10)
		if err != nil {
			t.Fatalf("resumed Outbound: %v", err)
		}
		if len(resumed) != 1 || resumed[0].Env.ID != envs[1].ID {
			t.Fatalf("resumed = %+v, want only the second event", resumed)
		}
		limited, err := s.Outbound(ctx, "wh-b", 0, 1)
		if err != nil {
			t.Fatalf("limited Outbound: %v", err)
		}
		if len(limited) != 1 {
			t.Fatalf("limited = %+v, want the limit respected", limited)
		}
		other, err := s.Outbound(ctx, "wh-a", 0, 10)
		if err != nil {
			t.Fatalf("Outbound(wh-a): %v", err)
		}
		if len(other) != 0 {
			t.Errorf("Outbound(wh-a) = %+v, want empty: the queue is per target", other)
		}
	})

	t.Run("item master round-trips including deletion", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, ok, err := s.Item(ctx, "GHOST"); err != nil || ok {
			t.Fatalf("Item(GHOST) = ok %v, err %v; want false, nil", ok, err)
		}
		item := domain.Item{SKU: "WIDGET", Description: "Blue widget", BaseUoM: "EA",
			AltUoM: map[domain.UoM]float64{"CASE": 12}, LotTracked: true, ShelfLifeDays: 30}
		if err := s.UpsertItem(ctx, item); err != nil {
			t.Fatalf("UpsertItem: %v", err)
		}
		got, ok, err := s.Item(ctx, "WIDGET")
		if err != nil || !ok {
			t.Fatalf("Item = ok %v, err %v; want true, nil", ok, err)
		}
		if got.BaseUoM != "EA" || got.AltUoM["CASE"] != 12 || !got.LotTracked || got.ShelfLifeDays != 30 {
			t.Errorf("Item = %+v, want it to round-trip %+v", got, item)
		}
		item.Deleted = true
		if err := s.UpsertItem(ctx, item); err != nil {
			t.Fatalf("UpsertItem deleted: %v", err)
		}
		got, ok, err = s.Item(ctx, "WIDGET")
		if err != nil || !ok || !got.Deleted {
			t.Errorf("deleted Item = %+v, ok %v, err %v; want the row present and flagged deleted", got, ok, err)
		}
	})

	t.Run("purchase orders and receipts against them accumulate across nodes", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, ok, err := s.PurchaseOrder(ctx, "PO-1", "WIDGET"); err != nil || ok {
			t.Fatalf("PurchaseOrder before upsert = ok %v, err %v; want false, nil", ok, err)
		}
		if err := s.UpsertPurchaseOrder(ctx, "PO-1", "WIDGET", 100); err != nil {
			t.Fatalf("UpsertPurchaseOrder: %v", err)
		}
		qty, ok, err := s.PurchaseOrder(ctx, "PO-1", "WIDGET")
		if err != nil || !ok || qty != 100 {
			t.Fatalf("PurchaseOrder = %v, ok %v, err %v; want 100, true, nil", qty, ok, err)
		}
		facts := []ReceiptFact{
			{EventID: domain.EventID{NodeID: "wh-a", Seq: 1}, Node: "wh-a", PORef: "PO-1",
				DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 60},
			{EventID: domain.EventID{NodeID: "wh-b", Seq: 1}, Node: "wh-b", PORef: "PO-1",
				DeliveryNote: "DN-2", SKU: "WIDGET", QtyBase: 30},
		}
		for _, f := range facts {
			if err := s.RecordReceipt(ctx, f); err != nil {
				t.Fatalf("RecordReceipt: %v", err)
			}
		}
		total, err := s.ReceivedAgainstPO(ctx, "PO-1", "WIDGET")
		if err != nil || total != 90 {
			t.Fatalf("ReceivedAgainstPO = %v, %v; want 90, nil: receipts from both nodes count", total, err)
		}
		// Recording the same event twice must not double-count: retried sync
		// batches deliver the same event again.
		if err := s.RecordReceipt(ctx, facts[0]); err != nil {
			t.Fatalf("re-RecordReceipt: %v", err)
		}
		if total, err = s.ReceivedAgainstPO(ctx, "PO-1", "WIDGET"); err != nil || total != 90 {
			t.Fatalf("ReceivedAgainstPO after replay = %v, %v; want 90, nil", total, err)
		}
		if total, err = s.ReceivedAgainstPO(ctx, "PO-9", "WIDGET"); err != nil || total != 0 {
			t.Fatalf("ReceivedAgainstPO for an unknown po = %v, %v; want 0, nil", total, err)
		}
	})

	t.Run("delivery notes remember who keyed them first", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, ok, err := s.DeliveryNoteFirstSeen(ctx, "DN-1", "WIDGET"); err != nil || ok {
			t.Fatalf("first seen before any receipt = ok %v, err %v; want false, nil", ok, err)
		}
		first := ReceiptFact{EventID: domain.EventID{NodeID: "wh-a", Seq: 1}, Node: "wh-a",
			PORef: "PO-1", DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 10}
		if err := s.RecordReceipt(ctx, first); err != nil {
			t.Fatalf("RecordReceipt: %v", err)
		}
		if err := s.RecordReceipt(ctx, ReceiptFact{EventID: domain.EventID{NodeID: "wh-b", Seq: 4},
			Node: "wh-b", PORef: "PO-1", DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 10}); err != nil {
			t.Fatalf("duplicate RecordReceipt: %v", err)
		}
		got, ok, err := s.DeliveryNoteFirstSeen(ctx, "DN-1", "WIDGET")
		if err != nil || !ok {
			t.Fatalf("first seen = ok %v, err %v; want true, nil", ok, err)
		}
		if got != first.EventID {
			t.Errorf("first seen = %v, want %v: the earliest receipt owns the note", got, first.EventID)
		}
	})

	t.Run("node configuration records existence and refusals", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, known, err := s.NodeConfig(ctx, "wh-z"); err != nil || known {
			t.Fatalf("NodeConfig(wh-z) = known %v, err %v; want false, nil", known, err)
		}
		if err := s.RegisterNode(ctx, "wh-b", []string{"HAZMAT"}); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
		rejects, known, err := s.NodeConfig(ctx, "wh-b")
		if err != nil || !known {
			t.Fatalf("NodeConfig = known %v, err %v; want true, nil", known, err)
		}
		if !rejects["HAZMAT"] || rejects["WIDGET"] {
			t.Errorf("rejects = %+v, want HAZMAT only", rejects)
		}
		if err := s.RegisterNode(ctx, "wh-b", nil); err != nil {
			t.Fatalf("re-RegisterNode: %v", err)
		}
		if rejects, _, err = s.NodeConfig(ctx, "wh-b"); err != nil || len(rejects) != 0 {
			t.Errorf("rejects after reconfiguration = %+v, %v; want empty", rejects, err)
		}
	})

	t.Run("in-transit is non-zero only between dispatch and receipt", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
		if _, ok, err := s.InTransit(ctx, "T1", key); err != nil || ok {
			t.Fatalf("InTransit before dispatch = ok %v, err %v; want false, nil", ok, err)
		}
		row := InTransitRow{TransferID: "T1", Key: key, FromNode: "wh-a", ToNode: "wh-b",
			Dispatched: 6, DispatchedAt: at}
		if err := s.RecordDispatch(ctx, row); err != nil {
			t.Fatalf("RecordDispatch: %v", err)
		}
		got, ok, err := s.InTransit(ctx, "T1", key)
		if err != nil || !ok {
			t.Fatalf("InTransit = ok %v, err %v; want true, nil", ok, err)
		}
		if got.Dispatched-got.Received != 6 {
			t.Errorf("in transit = %v, want 6 after dispatch", got.Dispatched-got.Received)
		}
		if err := s.AddReceived(ctx, "T1", key, 4); err != nil {
			t.Fatalf("AddReceived: %v", err)
		}
		if got, _, err = s.InTransit(ctx, "T1", key); err != nil || got.Dispatched-got.Received != 2 {
			t.Fatalf("in transit after partial receipt = %+v, %v; want 2", got, err)
		}
		if err := s.AddReceived(ctx, "T1", key, 2); err != nil {
			t.Fatalf("AddReceived: %v", err)
		}
		if got, _, err = s.InTransit(ctx, "T1", key); err != nil || got.Dispatched-got.Received != 0 {
			t.Fatalf("in transit after full receipt = %+v, %v; want 0", got, err)
		}
		if err := s.AddReceived(ctx, "T9", key, 1); err == nil {
			t.Error("AddReceived on an unknown transfer: expected an error")
		}
	})

	t.Run("open transfers past a window are reportable and failure is recorded", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
		if err := s.RecordDispatch(ctx, InTransitRow{TransferID: "T1", Key: key, FromNode: "wh-a",
			ToNode: "wh-b", Dispatched: 6, DispatchedAt: at}); err != nil {
			t.Fatalf("RecordDispatch: %v", err)
		}
		if err := s.RecordDispatch(ctx, InTransitRow{TransferID: "T2", Key: key, FromNode: "wh-a",
			ToNode: "wh-b", Dispatched: 1, DispatchedAt: at.Add(48 * time.Hour)}); err != nil {
			t.Fatalf("RecordDispatch: %v", err)
		}
		open1, err := s.OpenTransfers(ctx, at.Add(24*time.Hour))
		if err != nil {
			t.Fatalf("OpenTransfers: %v", err)
		}
		if len(open1) != 1 || open1[0].TransferID != "T1" {
			t.Fatalf("OpenTransfers = %+v, want only T1: T2 is inside the window", open1)
		}
		if err := s.AddReceived(ctx, "T1", key, 6); err != nil {
			t.Fatalf("AddReceived: %v", err)
		}
		if open1, err = s.OpenTransfers(ctx, at.Add(24*time.Hour)); err != nil || len(open1) != 0 {
			t.Fatalf("OpenTransfers after receipt = %+v, %v; want empty", open1, err)
		}
		if err := s.FailTransfer(ctx, "T2"); err != nil {
			t.Fatalf("FailTransfer: %v", err)
		}
		got, _, err := s.InTransit(ctx, "T2", key)
		if err != nil || !got.Failed {
			t.Errorf("InTransit(T2) = %+v, %v; want Failed", got, err)
		}
		if open2, err := s.OpenTransfers(ctx, at.Add(1000*time.Hour)); err != nil || len(open2) != 0 {
			t.Errorf("OpenTransfers = %+v, %v; want empty: a failed transfer is not a lost truck", open2, err)
		}
	})

	t.Run("decisions are recorded per event and readable back", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		id := domain.EventID{NodeID: "wh-a", Seq: 3}
		if _, ok, err := s.Decision(ctx, id); err != nil || ok {
			t.Fatalf("Decision before recording = ok %v, err %v; want false, nil", ok, err)
		}
		comp := domain.EventID{NodeID: CentralNode, Seq: 1}
		want := Decision{EventID: id, Verdict: VerdictRejected,
			Reason: domain.ReasonPOOverReceipt, CompensatingID: &comp}
		if err := s.RecordDecision(ctx, want); err != nil {
			t.Fatalf("RecordDecision: %v", err)
		}
		got, ok, err := s.Decision(ctx, id)
		if err != nil || !ok {
			t.Fatalf("Decision = ok %v, err %v; want true, nil", ok, err)
		}
		if got.Verdict != want.Verdict || got.Reason != want.Reason ||
			got.CompensatingID == nil || *got.CompensatingID != comp {
			t.Errorf("Decision = %+v, want %+v", got, want)
		}
		accepted := Decision{EventID: domain.EventID{NodeID: "wh-a", Seq: 4}, Verdict: VerdictAccepted}
		if err := s.RecordDecision(ctx, accepted); err != nil {
			t.Fatalf("RecordDecision accepted: %v", err)
		}
		if got, _, err = s.Decision(ctx, accepted.EventID); err != nil || got.CompensatingID != nil {
			t.Errorf("accepted Decision = %+v, %v; want no compensating event", got, err)
		}
	})

	t.Run("payloads survive as raw json", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		env := nodeEnv("wh-a", 1, domain.TypePutAway)
		if _, err := s.Append(ctx, []domain.Envelope{env}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		all, err := s.Events(ctx)
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		var want, got map[string]any
		if err := json.Unmarshal(env.Payload, &want); err != nil {
			t.Fatalf("unmarshal want: %v", err)
		}
		if err := json.Unmarshal(all[0].Payload, &got); err != nil {
			t.Fatalf("unmarshal got: %v", err)
		}
		if len(got) != len(want) {
			t.Errorf("payload = %v, want %v", got, want)
		}
	})
}
```

- [ ] **Step 2: Write `store.go`**

```go
// Package central holds everything the central server persists: the replicated event
// log partitioned by node, the reference data only central owns, in-transit transfer
// balances, per-node sync cursors and arbitration decisions.
//
// Store is one wide interface rather than several narrow ones on purpose: it
// describes a single cohesive thing, and there are exactly two implementations —
// Memory for tests and the integration suite, Postgres for deployment — validated by
// one shared contract test.
package central

import (
	"context"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// CentralNode is the node identity central's own events carry, so a compensation is
// distinguishable from anything a warehouse produced.
const CentralNode domain.NodeID = "central"

// Verdict is arbitration's outcome for one event.
type Verdict string

const (
	// VerdictAccepted means the event passed every central-enforced invariant.
	VerdictAccepted Verdict = "accepted"
	// VerdictRejected means it did not, and a compensating event was emitted. The
	// original event is still persisted: the log is immutable history, mistakes
	// included.
	VerdictRejected Verdict = "rejected"
)

// Decision is the audit record of arbitration. CompensatingID is nil for an
// acceptance.
type Decision struct {
	EventID        domain.EventID
	Verdict        Verdict
	Reason         string
	CompensatingID *domain.EventID
}

// ReceiptFact is what central remembers about one received line, so it can later see
// that two nodes received against the same purchase order or keyed the same supplier
// delivery note. No node can see either.
type ReceiptFact struct {
	EventID      domain.EventID
	Node         domain.NodeID
	PORef        string
	DeliveryNote string
	SKU          string
	QtyBase      float64
}

// InTransitRow is the balance of goods that have left the source warehouse but not
// yet arrived at the destination, for one transfer and one SKU/lot. It belongs to
// neither node. Key.Location is empty because in-transit stock is at no location.
// Dispatched minus Received is the live in-transit quantity, and it must be zero once
// the transfer closes.
type InTransitRow struct {
	TransferID   string
	Key          domain.StockKey
	FromNode     domain.NodeID
	ToNode       domain.NodeID
	Dispatched   float64
	Received     float64
	DispatchedAt time.Time
	Failed       bool
}

// Outbound is one entry of a node's downstream queue. Ord is a per-store monotonic
// position, and it is what a node's cursor stores so a resumed session picks up
// exactly where it left off.
type Outbound struct {
	Ord uint64
	Env domain.Envelope
}

// Store is everything central persists.
type Store interface {
	Close() error

	// Append stores node events idempotently by (node, seq) and returns only those
	// that were new, which is what arbitration then runs over.
	Append(ctx context.Context, envs []domain.Envelope) ([]domain.Envelope, error)
	// Events returns the whole log in total HLC order, for replay and determinism
	// checks.
	Events(ctx context.Context) ([]domain.Envelope, error)
	// EmitCentral seals central's own events — compensations and item-master
	// updates — assigning central's sequence numbers and HLC readings, and appends
	// them. causation is non-nil exactly when the events compensate a rejection.
	EmitCentral(ctx context.Context, events []domain.Event, causation *domain.EventID,
		now time.Time) ([]domain.Envelope, error)

	// PushedSeq is the highest sequence of a node's own events central has stored.
	PushedSeq(ctx context.Context, node domain.NodeID) (uint64, error)
	SetPushedSeq(ctx context.Context, node domain.NodeID, seq uint64) error
	// DeliveredOrd is the highest outbound position a node has acknowledged.
	DeliveredOrd(ctx context.Context, node domain.NodeID) (uint64, error)
	SetDeliveredOrd(ctx context.Context, node domain.NodeID, ord uint64) error

	// Enqueue adds events to a node's downstream queue.
	Enqueue(ctx context.Context, target domain.NodeID, envs []domain.Envelope) error
	// Outbound reads up to limit queued events for a node above afterOrd.
	Outbound(ctx context.Context, target domain.NodeID, afterOrd uint64, limit int) ([]Outbound, error)

	UpsertItem(ctx context.Context, item domain.Item) error
	Item(ctx context.Context, sku string) (domain.Item, bool, error)
	UpsertPurchaseOrder(ctx context.Context, poRef, sku string, ordered float64) error
	PurchaseOrder(ctx context.Context, poRef, sku string) (float64, bool, error)
	// RegisterNode records that a node exists and which SKUs it refuses to stock.
	RegisterNode(ctx context.Context, node domain.NodeID, rejects []string) error
	NodeConfig(ctx context.Context, node domain.NodeID) (rejects map[string]bool, known bool, err error)

	// RecordReceipt is idempotent per event id, so a resent batch cannot
	// double-count a receipt against its purchase order.
	RecordReceipt(ctx context.Context, f ReceiptFact) error
	ReceivedAgainstPO(ctx context.Context, poRef, sku string) (float64, error)
	// DeliveryNoteFirstSeen returns the event that first keyed this note for this
	// SKU, which is how a duplicate is identified.
	DeliveryNoteFirstSeen(ctx context.Context, note, sku string) (domain.EventID, bool, error)

	RecordDispatch(ctx context.Context, row InTransitRow) error
	AddReceived(ctx context.Context, transferID string, k domain.StockKey, qty float64) error
	InTransit(ctx context.Context, transferID string, k domain.StockKey) (InTransitRow, bool, error)
	// OpenTransfers returns transfers dispatched before the given instant that are
	// still carrying stock. They are reported as discrepancies, never
	// auto-compensated: a lost truck is a human problem.
	OpenTransfers(ctx context.Context, dispatchedBefore time.Time) ([]InTransitRow, error)
	FailTransfer(ctx context.Context, transferID string) error

	RecordDecision(ctx context.Context, d Decision) error
	Decision(ctx context.Context, id domain.EventID) (Decision, bool, error)
}
```

- [ ] **Step 3: Run the contract test to verify it fails**

Run: `cd warehouse-node && go test ./internal/central/ -v`
Expected: FAIL to compile — `runStoreContract` is declared and not used, and there is
no implementation to run it against yet.

- [ ] **Step 4: Write `memory.go`**

```go
package central

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// Memory is an in-memory Store. It is not a test double: the arbiter and the
// integration suite run against it because arbitration logic must be testable
// without a database, and it passes the same contract test as Postgres.
type Memory struct {
	mu        sync.Mutex
	events    map[domain.EventID]domain.Envelope
	centralHL domain.HLC
	centralN  uint64
	pushed    map[domain.NodeID]uint64
	delivered map[domain.NodeID]uint64
	queue     map[domain.NodeID][]Outbound
	nextOrd   uint64
	items     map[string]domain.Item
	orders    map[string]float64
	receipts  map[domain.EventID]ReceiptFact
	nodes     map[domain.NodeID]map[string]bool
	transit   map[string]InTransitRow
	decisions map[domain.EventID]Decision
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{
		events:    map[domain.EventID]domain.Envelope{},
		pushed:    map[domain.NodeID]uint64{},
		delivered: map[domain.NodeID]uint64{},
		queue:     map[domain.NodeID][]Outbound{},
		items:     map[string]domain.Item{},
		orders:    map[string]float64{},
		receipts:  map[domain.EventID]ReceiptFact{},
		nodes:     map[domain.NodeID]map[string]bool{},
		transit:   map[string]InTransitRow{},
		decisions: map[domain.EventID]Decision{},
	}
}

// Close is a no-op; nothing outlives the process.
func (m *Memory) Close() error { return nil }

// poKey and transitKey compose the composite map keys, in one place so the two
// implementations of Store cannot drift on what "the same purchase order line" means.
func poKey(poRef, sku string) string { return poRef + "\x00" + sku }

func transitKey(transferID string, k domain.StockKey) string {
	return transferID + "\x00" + k.SKU + "\x00" + k.LotID
}

func (m *Memory) Append(_ context.Context, envs []domain.Envelope) ([]domain.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fresh := make([]domain.Envelope, 0, len(envs))
	for _, env := range envs {
		if _, ok := m.events[env.ID]; ok {
			continue
		}
		m.events[env.ID] = env
		fresh = append(fresh, env)
	}
	return fresh, nil
}

func (m *Memory) Events(_ context.Context) ([]domain.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sortedEvents(), nil
}

func (m *Memory) sortedEvents() []domain.Envelope {
	out := make([]domain.Envelope, 0, len(m.events))
	for _, env := range m.events {
		out = append(out, env)
	}
	sort.Slice(out, func(i, j int) bool {
		if c := out[i].HLC.Compare(out[j].HLC); c != 0 {
			return c < 0
		}
		return out[i].ID.Seq < out[j].ID.Seq
	})
	return out
}

func (m *Memory) EmitCentral(_ context.Context, events []domain.Event, causation *domain.EventID,
	now time.Time) ([]domain.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]domain.Envelope, 0, len(events))
	for _, e := range events {
		m.centralN++
		m.centralHL = domain.Tick(m.centralHL, now.UnixMilli(), CentralNode)
		env, err := domain.NewEnvelope(
			domain.EventID{NodeID: CentralNode, Seq: m.centralN}, m.centralHL, now, causation, e)
		if err != nil {
			return nil, err
		}
		m.events[env.ID] = env
		out = append(out, env)
	}
	return out, nil
}

func (m *Memory) PushedSeq(_ context.Context, node domain.NodeID) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pushed[node], nil
}

func (m *Memory) SetPushedSeq(_ context.Context, node domain.NodeID, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pushed[node] = seq
	return nil
}

func (m *Memory) DeliveredOrd(_ context.Context, node domain.NodeID) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delivered[node], nil
}

func (m *Memory) SetDeliveredOrd(_ context.Context, node domain.NodeID, ord uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered[node] = ord
	return nil
}

func (m *Memory) Enqueue(_ context.Context, target domain.NodeID, envs []domain.Envelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, env := range envs {
		m.nextOrd++
		m.queue[target] = append(m.queue[target], Outbound{Ord: m.nextOrd, Env: env})
	}
	return nil
}

func (m *Memory) Outbound(_ context.Context, target domain.NodeID, afterOrd uint64, limit int) ([]Outbound, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := []Outbound{}
	for _, row := range m.queue[target] {
		if row.Ord <= afterOrd {
			continue
		}
		out = append(out, row)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (m *Memory) UpsertItem(_ context.Context, item domain.Item) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[item.SKU] = item
	return nil
}

func (m *Memory) Item(_ context.Context, sku string) (domain.Item, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item, ok := m.items[sku]
	return item, ok, nil
}

func (m *Memory) UpsertPurchaseOrder(_ context.Context, poRef, sku string, ordered float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.orders[poKey(poRef, sku)] = ordered
	return nil
}

func (m *Memory) PurchaseOrder(_ context.Context, poRef, sku string) (float64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	qty, ok := m.orders[poKey(poRef, sku)]
	return qty, ok, nil
}

func (m *Memory) RegisterNode(_ context.Context, node domain.NodeID, rejects []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := map[string]bool{}
	for _, sku := range rejects {
		set[sku] = true
	}
	m.nodes[node] = set
	return nil
}

func (m *Memory) NodeConfig(_ context.Context, node domain.NodeID) (map[string]bool, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set, ok := m.nodes[node]
	if !ok {
		return nil, false, nil
	}
	out := make(map[string]bool, len(set))
	for sku := range set {
		out[sku] = true
	}
	return out, true, nil
}

func (m *Memory) RecordReceipt(_ context.Context, f ReceiptFact) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.receipts[f.EventID]; ok {
		return nil
	}
	m.receipts[f.EventID] = f
	return nil
}

func (m *Memory) ReceivedAgainstPO(_ context.Context, poRef, sku string) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total float64
	for _, f := range m.receipts {
		if f.PORef == poRef && f.SKU == sku {
			total += f.QtyBase
		}
	}
	return total, nil
}

func (m *Memory) DeliveryNoteFirstSeen(_ context.Context, note, sku string) (domain.EventID, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var first domain.EventID
	found := false
	for _, f := range m.receipts {
		if f.DeliveryNote != note || f.SKU != sku {
			continue
		}
		if !found || less(f.EventID, first) {
			first, found = f.EventID, true
		}
	}
	return first, found, nil
}

// less orders event ids so "first seen" is deterministic regardless of arrival order.
func less(a, b domain.EventID) bool {
	if a.NodeID != b.NodeID {
		return a.NodeID < b.NodeID
	}
	return a.Seq < b.Seq
}

func (m *Memory) RecordDispatch(_ context.Context, row InTransitRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := transitKey(row.TransferID, row.Key)
	if existing, ok := m.transit[key]; ok {
		existing.Dispatched = row.Dispatched
		m.transit[key] = existing
		return nil
	}
	m.transit[key] = row
	return nil
}

func (m *Memory) AddReceived(_ context.Context, transferID string, k domain.StockKey, qty float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := transitKey(transferID, k)
	row, ok := m.transit[key]
	if !ok {
		return fmt.Errorf("transfer %s has no dispatched line for %s/%s", transferID, k.SKU, k.LotID)
	}
	row.Received += qty
	m.transit[key] = row
	return nil
}

func (m *Memory) InTransit(_ context.Context, transferID string, k domain.StockKey) (InTransitRow, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.transit[transitKey(transferID, k)]
	return row, ok, nil
}

func (m *Memory) OpenTransfers(_ context.Context, dispatchedBefore time.Time) ([]InTransitRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := []InTransitRow{}
	for _, row := range m.transit {
		if row.Failed || row.Dispatched-row.Received <= 0 || !row.DispatchedAt.Before(dispatchedBefore) {
			continue
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TransferID < out[j].TransferID })
	return out, nil
}

func (m *Memory) FailTransfer(_ context.Context, transferID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, row := range m.transit {
		if row.TransferID == transferID {
			row.Failed = true
			m.transit[key] = row
		}
	}
	return nil
}

func (m *Memory) RecordDecision(_ context.Context, d Decision) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.decisions[d.EventID] = d
	return nil
}

func (m *Memory) Decision(_ context.Context, id domain.EventID) (Decision, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.decisions[id]
	return d, ok, nil
}
```

- [ ] **Step 5: Write `memory_test.go`**

```go
package central

import "testing"

func TestMemorySatisfiesTheStoreContract(t *testing.T) {
	runStoreContract(t, func(*testing.T) Store { return NewMemory() })
}

func TestMemoryCloseIsANoOp(t *testing.T) {
	if err := NewMemory().Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/central/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 7: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 8: Commit**

```bash
git add warehouse-node/internal/central
git commit -m "feat(central): add the store contract and its in-memory implementation

- one wide Store covering the node-partitioned log, reference data central alone
  owns, in-transit transfer balances, per-node cursors and arbitration decisions
- in-transit is dispatched minus received, and belongs to neither node
- receipts are recorded idempotently per event so a resent batch cannot
  double-count against a purchase order
- one contract test table, run against every implementation"
```

---

### Task 16: The Postgres central store

The deployment implementation of the same `Store`. It must pass the contract test from Task
15 unchanged — that is the whole point of having written the contract once.

Two things are Postgres-specific and required by the spec. The `events` table is
**partitioned by list on `node_id`**, so each warehouse's events live in their own physical
partition; a `DEFAULT` partition catches nodes nobody has provisioned a partition for yet,
so onboarding a warehouse needs no migration. And `outbound` uses a `bigserial` position,
which is the monotonic `Ord` a node's downstream cursor stores.

Tests run against a real Postgres. `github.com/fergusstrange/embedded-postgres` downloads
and runs a real server in a temp directory, so no Docker and no developer setup is needed,
and nothing is ever skipped.

**Files:**
- Create: `warehouse-node/internal/central/schema.sql`
- Create: `warehouse-node/internal/central/postgres.go`
- Test: `warehouse-node/internal/central/postgres_test.go`
- Modify: `warehouse-node/go.mod` (adds `github.com/jackc/pgx/v5`, `github.com/fergusstrange/embedded-postgres`)

**Interfaces:**
- Consumes: `central.Store`, `central.Decision`, `central.ReceiptFact`, `central.InTransitRow`, `central.Outbound`, `central.CentralNode`, `central.poKey`, `central.transitKey` (Task 15); `domain.NewEnvelope`, `domain.Tick`, `domain.Envelope` (Tasks 1-2).
- Produces:
  - `func central.OpenPostgres(ctx context.Context, dsn string) (*Postgres, error)` — applies the schema, recovers central's sequence and clock, returns a `Store`
  - `central.Postgres` implementing `central.Store`

- [ ] **Step 1: Add the dependencies**

```bash
cd warehouse-node
go get github.com/jackc/pgx/v5@latest
go get github.com/fergusstrange/embedded-postgres@latest
```

- [ ] **Step 2: Write `schema.sql`**

```sql
-- The replicated event log. Partitioned by list on node_id so each warehouse's
-- events live in their own partition; the DEFAULT partition means onboarding a new
-- warehouse needs no migration.
CREATE TABLE IF NOT EXISTS events (
    node_id       TEXT        NOT NULL,
    seq           BIGINT      NOT NULL,
    aggregate_id  TEXT        NOT NULL,
    type          TEXT        NOT NULL,
    hlc_wall      BIGINT      NOT NULL,
    hlc_counter   BIGINT      NOT NULL,
    hlc_node      TEXT        NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL,
    causation_node TEXT       NOT NULL DEFAULT '',
    causation_seq BIGINT      NOT NULL DEFAULT 0,
    payload       JSONB       NOT NULL,
    PRIMARY KEY (node_id, seq)
) PARTITION BY LIST (node_id);

CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

CREATE INDEX IF NOT EXISTS events_order ON events (hlc_wall, hlc_counter, hlc_node, seq);

-- Per-node sync cursors: how far central has received from the node, and how far the
-- node has acknowledged what central sent down. Both are why a week offline resumes.
CREATE TABLE IF NOT EXISTS cursors (
    node_id       TEXT   PRIMARY KEY,
    pushed_seq    BIGINT NOT NULL DEFAULT 0,
    delivered_ord BIGINT NOT NULL DEFAULT 0
);

-- The downstream queue, one row per (target node, event). ord is the monotonic
-- position a node's cursor stores.
CREATE TABLE IF NOT EXISTS outbound (
    ord         BIGSERIAL PRIMARY KEY,
    target_node TEXT   NOT NULL,
    node_id     TEXT   NOT NULL,
    seq         BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS outbound_target ON outbound (target_node, ord);

CREATE TABLE IF NOT EXISTS items (
    sku             TEXT PRIMARY KEY,
    description     TEXT    NOT NULL,
    base_uom        TEXT    NOT NULL,
    alt_uom         JSONB   NOT NULL,
    lot_tracked     BOOLEAN NOT NULL,
    shelf_life_days INTEGER NOT NULL,
    deleted         BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS purchase_orders (
    po_ref      TEXT             NOT NULL,
    sku         TEXT             NOT NULL,
    qty_ordered DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (po_ref, sku)
);

-- One row per received line, keyed by the event that produced it so a resent sync
-- batch cannot double-count a receipt against its purchase order.
CREATE TABLE IF NOT EXISTS receipts (
    event_node    TEXT             NOT NULL,
    event_seq     BIGINT           NOT NULL,
    node_id       TEXT             NOT NULL,
    po_ref        TEXT             NOT NULL,
    delivery_note TEXT             NOT NULL,
    sku           TEXT             NOT NULL,
    qty_base      DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (event_node, event_seq)
);

CREATE INDEX IF NOT EXISTS receipts_po ON receipts (po_ref, sku);
CREATE INDEX IF NOT EXISTS receipts_note ON receipts (delivery_note, sku);

CREATE TABLE IF NOT EXISTS nodes (node_id TEXT PRIMARY KEY);

CREATE TABLE IF NOT EXISTS node_rejects (
    node_id TEXT NOT NULL,
    sku     TEXT NOT NULL,
    PRIMARY KEY (node_id, sku)
);

-- In-transit balances: goods that have left the source and not yet arrived at the
-- destination. They belong to neither node. lot_id is part of the key; there is no
-- location, because in-transit stock is at no location.
CREATE TABLE IF NOT EXISTS in_transit (
    transfer_id   TEXT             NOT NULL,
    sku           TEXT             NOT NULL,
    lot_id        TEXT             NOT NULL,
    from_node     TEXT             NOT NULL,
    to_node       TEXT             NOT NULL,
    dispatched    DOUBLE PRECISION NOT NULL,
    received      DOUBLE PRECISION NOT NULL DEFAULT 0,
    dispatched_at TIMESTAMPTZ      NOT NULL,
    failed        BOOLEAN          NOT NULL DEFAULT FALSE,
    PRIMARY KEY (transfer_id, sku, lot_id)
);

-- The arbitration audit trail: what central decided about each event, why, and which
-- compensating event it emitted.
CREATE TABLE IF NOT EXISTS decisions (
    event_node TEXT   NOT NULL,
    event_seq  BIGINT NOT NULL,
    verdict    TEXT   NOT NULL,
    reason     TEXT   NOT NULL,
    comp_node  TEXT   NOT NULL DEFAULT '',
    comp_seq   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (event_node, event_seq)
);
```

- [ ] **Step 3: Write the failing test**

`warehouse-node/internal/central/postgres_test.go`:

```go
package central

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// pgDSN is the connection string of the embedded server started for this package's
// tests, and dbSeq names a fresh database per subtest so they cannot interfere.
// testInstant is a fixed wall time so HLC readings are comparable across subtests.
var (
	pgDSN       string
	dbSeq       int
	testInstant = time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
)

// TestMain starts one real Postgres for the whole package. embedded-postgres
// downloads and runs an actual server in a temp directory, so this needs no Docker
// and no developer setup, and no test is ever skipped for want of a database.
func TestMain(m *testing.M) {
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Port(54329).
		Database("warehouse").
		Username("warehouse").
		Password("warehouse").
		RuntimePath(os.TempDir() + "/warehouse-embedded-postgres"))
	if err := pg.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start embedded postgres: %v\n", err)
		os.Exit(1)
	}
	pgDSN = "postgres://warehouse:warehouse@localhost:54329/warehouse?sslmode=disable"
	code := m.Run()
	if err := pg.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "stop embedded postgres: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

// freshPostgres creates an empty database and opens a store on it.
func freshPostgres(t *testing.T) Store {
	t.Helper()
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	dbSeq++
	name := fmt.Sprintf("t%d", dbSeq)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin: %v", err)
	}

	store, err := OpenPostgres(ctx, fmt.Sprintf(
		"postgres://warehouse:warehouse@localhost:54329/%s?sslmode=disable", name))
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
}

func TestPostgresSatisfiesTheStoreContract(t *testing.T) {
	runStoreContract(t, freshPostgres)
}

func TestPostgresRecoversCentralSequenceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	dbSeq++
	name := fmt.Sprintf("t%d", dbSeq)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin: %v", err)
	}
	dsn := fmt.Sprintf("postgres://warehouse:warehouse@localhost:54329/%s?sslmode=disable", name)

	first, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	envs, err := first.EmitCentral(ctx, []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "WIDGET",
		Payload: domain.ItemUpserted{Item: domain.Item{SKU: "WIDGET", BaseUoM: "EA"}}}}, nil, testInstant)
	if err != nil {
		t.Fatalf("EmitCentral: %v", err)
	}
	if envs[0].ID.Seq != 1 {
		t.Fatalf("first seq = %d, want 1", envs[0].ID.Seq)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close() }()
	again, err := second.EmitCentral(ctx, []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "BOLT",
		Payload: domain.ItemUpserted{Item: domain.Item{SKU: "BOLT", BaseUoM: "EA"}}}}, nil, testInstant)
	if err != nil {
		t.Fatalf("EmitCentral after reopen: %v", err)
	}
	if again[0].ID.Seq != 2 {
		t.Errorf("seq after restart = %d, want 2: a restart must never reuse a sequence", again[0].ID.Seq)
	}
	if again[0].HLC.Compare(envs[0].HLC) <= 0 {
		t.Errorf("HLC after restart = %+v, want it above %+v", again[0].HLC, envs[0].HLC)
	}
}

func TestPostgresRejectsABadDSN(t *testing.T) {
	if _, err := OpenPostgres(context.Background(), "postgres://nobody@localhost:1/none"); err == nil {
		t.Fatal("OpenPostgres with an unreachable server: expected an error")
	}
}

func TestPostgresOperationsFailAfterClose(t *testing.T) {
	store := freshPostgres(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()
	if _, err := store.Events(ctx); err == nil {
		t.Error("Events after Close: expected an error")
	}
	if _, err := store.Append(ctx, []domain.Envelope{{ID: domain.EventID{NodeID: "wh-a", Seq: 1},
		Type: domain.TypePutAway, Payload: []byte(`{}`)}}); err == nil {
		t.Error("Append after Close: expected an error")
	}
	if _, err := store.Item(ctx, "WIDGET"); err == nil {
		t.Error("Item after Close: expected an error")
	}
	if _, err := store.Outbound(ctx, "wh-a", 0, 10); err == nil {
		t.Error("Outbound after Close: expected an error")
	}
	if _, err := store.OpenTransfers(ctx, testInstant); err == nil {
		t.Error("OpenTransfers after Close: expected an error")
	}
	if _, err := store.NodeConfig(ctx, "wh-a"); err == nil {
		t.Error("NodeConfig after Close: expected an error")
	}
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/central/ -run Postgres -v`
Expected: FAIL to compile — `undefined: OpenPostgres`.

- [ ] **Step 5: Write `postgres.go`**

```go
package central

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

//go:embed schema.sql
var schemaFS embed.FS

// Postgres is the deployment Store. It passes the same contract test as Memory.
type Postgres struct {
	pool *pgxpool.Pool

	// mu guards central's own sequence counter and clock, which are read-modify-write
	// and must never hand out the same sequence twice.
	mu        sync.Mutex
	centralN  uint64
	centralHL domain.HLC
}

// OpenPostgres connects, applies the schema, and recovers central's sequence number
// and clock reading so a restart never reuses a sequence nor goes backwards in time.
func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	schema, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("read schema: %w", err)
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	p := &Postgres{pool: pool}
	if err := p.recoverCentral(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

func (p *Postgres) recoverCentral(ctx context.Context) error {
	var seq, wall, counter int64
	err := p.pool.QueryRow(ctx, `SELECT coalesce(max(seq), 0),
		coalesce(max(hlc_wall), 0),
		coalesce(max(hlc_counter) FILTER (WHERE hlc_wall = (SELECT max(hlc_wall) FROM events WHERE node_id = $1)), 0)
		FROM events WHERE node_id = $1`, string(CentralNode)).Scan(&seq, &wall, &counter)
	if err != nil {
		return fmt.Errorf("recover central sequence: %w", err)
	}
	p.centralN = uint64(seq)
	p.centralHL = domain.HLC{Wall: wall, Counter: uint32(counter), Node: CentralNode}
	return nil
}

// Close releases the connection pool.
func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

func (p *Postgres) Append(ctx context.Context, envs []domain.Envelope) ([]domain.Envelope, error) {
	fresh := make([]domain.Envelope, 0, len(envs))
	for _, env := range envs {
		inserted, err := p.insert(ctx, env)
		if err != nil {
			return nil, err
		}
		if inserted {
			fresh = append(fresh, env)
		}
	}
	return fresh, nil
}

// insert appends one envelope, reporting whether it was new. Identity is the
// (node_id, seq) pair, so a conflict means the event is already stored and
// re-ingesting it is a no-op — which is what makes sync retries safe.
func (p *Postgres) insert(ctx context.Context, env domain.Envelope) (bool, error) {
	causeNode, causeSeq := "", uint64(0)
	if env.CausationID != nil {
		causeNode, causeSeq = string(env.CausationID.NodeID), env.CausationID.Seq
	}
	tag, err := p.pool.Exec(ctx, `INSERT INTO events
		(node_id, seq, aggregate_id, type, hlc_wall, hlc_counter, hlc_node,
		 recorded_at, causation_node, causation_seq, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (node_id, seq) DO NOTHING`,
		string(env.ID.NodeID), env.ID.Seq, env.AggregateID, env.Type,
		env.HLC.Wall, int64(env.HLC.Counter), string(env.HLC.Node),
		env.RecordedAt, causeNode, causeSeq, []byte(env.Payload))
	if err != nil {
		return false, fmt.Errorf("append event %s: %w", env.ID, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (p *Postgres) Events(ctx context.Context) ([]domain.Envelope, error) {
	rows, err := p.pool.Query(ctx, `SELECT node_id, seq, aggregate_id, type, hlc_wall, hlc_counter,
		hlc_node, recorded_at, causation_node, causation_seq, payload
		FROM events ORDER BY hlc_wall, hlc_counter, hlc_node, seq`)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	return scanEnvelopes(rows)
}

func scanEnvelopes(rows pgx.Rows) ([]domain.Envelope, error) {
	defer rows.Close()

	out := []domain.Envelope{}
	for rows.Next() {
		var env domain.Envelope
		var node, hlcNode, causeNode string
		var counter, causeSeq int64
		var payload []byte
		if err := rows.Scan(&node, &env.ID.Seq, &env.AggregateID, &env.Type, &env.HLC.Wall,
			&counter, &hlcNode, &env.RecordedAt, &causeNode, &causeSeq, &payload); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		env.ID.NodeID = domain.NodeID(node)
		env.HLC.Counter, env.HLC.Node = uint32(counter), domain.NodeID(hlcNode)
		env.RecordedAt = env.RecordedAt.UTC()
		env.Payload = payload
		if causeNode != "" {
			env.CausationID = &domain.EventID{NodeID: domain.NodeID(causeNode), Seq: uint64(causeSeq)}
		}
		out = append(out, env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return out, nil
}

func (p *Postgres) EmitCentral(ctx context.Context, events []domain.Event, causation *domain.EventID,
	now time.Time) ([]domain.Envelope, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]domain.Envelope, 0, len(events))
	for _, e := range events {
		p.centralN++
		p.centralHL = domain.Tick(p.centralHL, now.UnixMilli(), CentralNode)
		env, err := domain.NewEnvelope(
			domain.EventID{NodeID: CentralNode, Seq: p.centralN}, p.centralHL, now, causation, e)
		if err != nil {
			return nil, err
		}
		if _, err := p.insert(ctx, env); err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, nil
}

func (p *Postgres) PushedSeq(ctx context.Context, node domain.NodeID) (uint64, error) {
	return p.cursor(ctx, node, "pushed_seq")
}

func (p *Postgres) DeliveredOrd(ctx context.Context, node domain.NodeID) (uint64, error) {
	return p.cursor(ctx, node, "delivered_ord")
}

func (p *Postgres) cursor(ctx context.Context, node domain.NodeID, column string) (uint64, error) {
	var value int64
	err := p.pool.QueryRow(ctx,
		`SELECT `+column+` FROM cursors WHERE node_id = $1`, string(node)).Scan(&value)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read %s for %s: %w", column, node, err)
	}
	return uint64(value), nil
}

func (p *Postgres) SetPushedSeq(ctx context.Context, node domain.NodeID, seq uint64) error {
	return p.setCursor(ctx, node, "pushed_seq", seq)
}

func (p *Postgres) SetDeliveredOrd(ctx context.Context, node domain.NodeID, ord uint64) error {
	return p.setCursor(ctx, node, "delivered_ord", ord)
}

func (p *Postgres) setCursor(ctx context.Context, node domain.NodeID, column string, value uint64) error {
	if _, err := p.pool.Exec(ctx, `INSERT INTO cursors (node_id, `+column+`) VALUES ($1, $2)
		ON CONFLICT (node_id) DO UPDATE SET `+column+` = excluded.`+column,
		string(node), int64(value)); err != nil {
		return fmt.Errorf("set %s for %s: %w", column, node, err)
	}
	return nil
}

func (p *Postgres) Enqueue(ctx context.Context, target domain.NodeID, envs []domain.Envelope) error {
	for _, env := range envs {
		if _, err := p.pool.Exec(ctx,
			`INSERT INTO outbound (target_node, node_id, seq) VALUES ($1, $2, $3)`,
			string(target), string(env.ID.NodeID), env.ID.Seq); err != nil {
			return fmt.Errorf("enqueue %s for %s: %w", env.ID, target, err)
		}
	}
	return nil
}

func (p *Postgres) Outbound(ctx context.Context, target domain.NodeID, afterOrd uint64, limit int) ([]Outbound, error) {
	rows, err := p.pool.Query(ctx, `SELECT o.ord, e.node_id, e.seq, e.aggregate_id, e.type,
		e.hlc_wall, e.hlc_counter, e.hlc_node, e.recorded_at, e.causation_node, e.causation_seq, e.payload
		FROM outbound o JOIN events e ON e.node_id = o.node_id AND e.seq = o.seq
		WHERE o.target_node = $1 AND o.ord > $2
		ORDER BY o.ord LIMIT $3`, string(target), int64(afterOrd), limit)
	if err != nil {
		return nil, fmt.Errorf("query outbound for %s: %w", target, err)
	}
	defer rows.Close()

	out := []Outbound{}
	for rows.Next() {
		var row Outbound
		var ord int64
		var env domain.Envelope
		var node, hlcNode, causeNode string
		var counter, causeSeq int64
		var payload []byte
		if err := rows.Scan(&ord, &node, &env.ID.Seq, &env.AggregateID, &env.Type, &env.HLC.Wall,
			&counter, &hlcNode, &env.RecordedAt, &causeNode, &causeSeq, &payload); err != nil {
			return nil, fmt.Errorf("scan outbound row: %w", err)
		}
		env.ID.NodeID = domain.NodeID(node)
		env.HLC.Counter, env.HLC.Node = uint32(counter), domain.NodeID(hlcNode)
		env.RecordedAt, env.Payload = env.RecordedAt.UTC(), payload
		if causeNode != "" {
			env.CausationID = &domain.EventID{NodeID: domain.NodeID(causeNode), Seq: uint64(causeSeq)}
		}
		row.Ord, row.Env = uint64(ord), env
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbound rows: %w", err)
	}
	return out, nil
}

func (p *Postgres) UpsertItem(ctx context.Context, item domain.Item) error {
	alt, err := json.Marshal(item.AltUoM)
	if err != nil {
		return fmt.Errorf("encode alternate units for %s: %w", item.SKU, err)
	}
	if _, err := p.pool.Exec(ctx, `INSERT INTO items
		(sku, description, base_uom, alt_uom, lot_tracked, shelf_life_days, deleted)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (sku) DO UPDATE SET description = excluded.description,
			base_uom = excluded.base_uom, alt_uom = excluded.alt_uom,
			lot_tracked = excluded.lot_tracked, shelf_life_days = excluded.shelf_life_days,
			deleted = excluded.deleted`,
		item.SKU, item.Description, string(item.BaseUoM), alt,
		item.LotTracked, item.ShelfLifeDays, item.Deleted); err != nil {
		return fmt.Errorf("upsert item %s: %w", item.SKU, err)
	}
	return nil
}

func (p *Postgres) Item(ctx context.Context, sku string) (domain.Item, bool, error) {
	var item domain.Item
	var base string
	var alt []byte
	err := p.pool.QueryRow(ctx, `SELECT sku, description, base_uom, alt_uom, lot_tracked,
		shelf_life_days, deleted FROM items WHERE sku = $1`, sku).
		Scan(&item.SKU, &item.Description, &base, &alt, &item.LotTracked, &item.ShelfLifeDays, &item.Deleted)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.Item{}, false, nil
	case err != nil:
		return domain.Item{}, false, fmt.Errorf("read item %s: %w", sku, err)
	}
	item.BaseUoM = domain.UoM(base)
	if err := json.Unmarshal(alt, &item.AltUoM); err != nil {
		return domain.Item{}, false, fmt.Errorf("decode alternate units for %s: %w", sku, err)
	}
	return item, true, nil
}

func (p *Postgres) UpsertPurchaseOrder(ctx context.Context, poRef, sku string, ordered float64) error {
	if _, err := p.pool.Exec(ctx, `INSERT INTO purchase_orders (po_ref, sku, qty_ordered)
		VALUES ($1,$2,$3) ON CONFLICT (po_ref, sku) DO UPDATE SET qty_ordered = excluded.qty_ordered`,
		poRef, sku, ordered); err != nil {
		return fmt.Errorf("upsert purchase order %s/%s: %w", poRef, sku, err)
	}
	return nil
}

func (p *Postgres) PurchaseOrder(ctx context.Context, poRef, sku string) (float64, bool, error) {
	var qty float64
	err := p.pool.QueryRow(ctx,
		`SELECT qty_ordered FROM purchase_orders WHERE po_ref = $1 AND sku = $2`, poRef, sku).Scan(&qty)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("read purchase order %s/%s: %w", poRef, sku, err)
	}
	return qty, true, nil
}

func (p *Postgres) RegisterNode(ctx context.Context, node domain.NodeID, rejects []string) error {
	if _, err := p.pool.Exec(ctx,
		`INSERT INTO nodes (node_id) VALUES ($1) ON CONFLICT (node_id) DO NOTHING`, string(node)); err != nil {
		return fmt.Errorf("register node %s: %w", node, err)
	}
	if _, err := p.pool.Exec(ctx, `DELETE FROM node_rejects WHERE node_id = $1`, string(node)); err != nil {
		return fmt.Errorf("clear rejects for %s: %w", node, err)
	}
	for _, sku := range rejects {
		if _, err := p.pool.Exec(ctx, `INSERT INTO node_rejects (node_id, sku) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, string(node), sku); err != nil {
			return fmt.Errorf("record reject %s for %s: %w", sku, node, err)
		}
	}
	return nil
}

func (p *Postgres) NodeConfig(ctx context.Context, node domain.NodeID) (map[string]bool, bool, error) {
	var known bool
	err := p.pool.QueryRow(ctx,
		`SELECT true FROM nodes WHERE node_id = $1`, string(node)).Scan(&known)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("read node %s: %w", node, err)
	}
	rows, err := p.pool.Query(ctx, `SELECT sku FROM node_rejects WHERE node_id = $1`, string(node))
	if err != nil {
		return nil, false, fmt.Errorf("read rejects for %s: %w", node, err)
	}
	defer rows.Close()

	rejects := map[string]bool{}
	for rows.Next() {
		var sku string
		if err := rows.Scan(&sku); err != nil {
			return nil, false, fmt.Errorf("scan reject: %w", err)
		}
		rejects[sku] = true
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate rejects: %w", err)
	}
	return rejects, true, nil
}

func (p *Postgres) RecordReceipt(ctx context.Context, f ReceiptFact) error {
	if _, err := p.pool.Exec(ctx, `INSERT INTO receipts
		(event_node, event_seq, node_id, po_ref, delivery_note, sku, qty_base)
		VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (event_node, event_seq) DO NOTHING`,
		string(f.EventID.NodeID), f.EventID.Seq, string(f.Node),
		f.PORef, f.DeliveryNote, f.SKU, f.QtyBase); err != nil {
		return fmt.Errorf("record receipt %s: %w", f.EventID, err)
	}
	return nil
}

func (p *Postgres) ReceivedAgainstPO(ctx context.Context, poRef, sku string) (float64, error) {
	var total float64
	if err := p.pool.QueryRow(ctx,
		`SELECT coalesce(sum(qty_base), 0) FROM receipts WHERE po_ref = $1 AND sku = $2`,
		poRef, sku).Scan(&total); err != nil {
		return 0, fmt.Errorf("sum receipts against %s/%s: %w", poRef, sku, err)
	}
	return total, nil
}

func (p *Postgres) DeliveryNoteFirstSeen(ctx context.Context, note, sku string) (domain.EventID, bool, error) {
	var node string
	var seq int64
	err := p.pool.QueryRow(ctx, `SELECT event_node, event_seq FROM receipts
		WHERE delivery_note = $1 AND sku = $2 ORDER BY event_node, event_seq LIMIT 1`, note, sku).
		Scan(&node, &seq)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.EventID{}, false, nil
	case err != nil:
		return domain.EventID{}, false, fmt.Errorf("read first receipt for note %s: %w", note, err)
	}
	return domain.EventID{NodeID: domain.NodeID(node), Seq: uint64(seq)}, true, nil
}

func (p *Postgres) RecordDispatch(ctx context.Context, row InTransitRow) error {
	if _, err := p.pool.Exec(ctx, `INSERT INTO in_transit
		(transfer_id, sku, lot_id, from_node, to_node, dispatched, received, dispatched_at, failed)
		VALUES ($1,$2,$3,$4,$5,$6,0,$7,false)
		ON CONFLICT (transfer_id, sku, lot_id) DO UPDATE SET dispatched = excluded.dispatched`,
		row.TransferID, row.Key.SKU, row.Key.LotID, string(row.FromNode), string(row.ToNode),
		row.Dispatched, row.DispatchedAt); err != nil {
		return fmt.Errorf("record dispatch %s: %w", row.TransferID, err)
	}
	return nil
}

func (p *Postgres) AddReceived(ctx context.Context, transferID string, k domain.StockKey, qty float64) error {
	tag, err := p.pool.Exec(ctx, `UPDATE in_transit SET received = received + $4
		WHERE transfer_id = $1 AND sku = $2 AND lot_id = $3`, transferID, k.SKU, k.LotID, qty)
	if err != nil {
		return fmt.Errorf("add received to %s: %w", transferID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("transfer %s has no dispatched line for %s/%s", transferID, k.SKU, k.LotID)
	}
	return nil
}

func (p *Postgres) InTransit(ctx context.Context, transferID string, k domain.StockKey) (InTransitRow, bool, error) {
	row := InTransitRow{TransferID: transferID, Key: k}
	var from, to string
	err := p.pool.QueryRow(ctx, `SELECT from_node, to_node, dispatched, received, dispatched_at, failed
		FROM in_transit WHERE transfer_id = $1 AND sku = $2 AND lot_id = $3`,
		transferID, k.SKU, k.LotID).
		Scan(&from, &to, &row.Dispatched, &row.Received, &row.DispatchedAt, &row.Failed)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return InTransitRow{}, false, nil
	case err != nil:
		return InTransitRow{}, false, fmt.Errorf("read in-transit for %s: %w", transferID, err)
	}
	row.FromNode, row.ToNode = domain.NodeID(from), domain.NodeID(to)
	row.DispatchedAt = row.DispatchedAt.UTC()
	return row, true, nil
}

func (p *Postgres) OpenTransfers(ctx context.Context, dispatchedBefore time.Time) ([]InTransitRow, error) {
	rows, err := p.pool.Query(ctx, `SELECT transfer_id, sku, lot_id, from_node, to_node,
		dispatched, received, dispatched_at, failed FROM in_transit
		WHERE failed = false AND dispatched - received > 0 AND dispatched_at < $1
		ORDER BY transfer_id, sku, lot_id`, dispatchedBefore)
	if err != nil {
		return nil, fmt.Errorf("query open transfers: %w", err)
	}
	defer rows.Close()

	out := []InTransitRow{}
	for rows.Next() {
		var row InTransitRow
		var from, to string
		if err := rows.Scan(&row.TransferID, &row.Key.SKU, &row.Key.LotID, &from, &to,
			&row.Dispatched, &row.Received, &row.DispatchedAt, &row.Failed); err != nil {
			return nil, fmt.Errorf("scan open transfer: %w", err)
		}
		row.FromNode, row.ToNode = domain.NodeID(from), domain.NodeID(to)
		row.DispatchedAt = row.DispatchedAt.UTC()
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate open transfers: %w", err)
	}
	return out, nil
}

func (p *Postgres) FailTransfer(ctx context.Context, transferID string) error {
	if _, err := p.pool.Exec(ctx,
		`UPDATE in_transit SET failed = true WHERE transfer_id = $1`, transferID); err != nil {
		return fmt.Errorf("fail transfer %s: %w", transferID, err)
	}
	return nil
}

func (p *Postgres) RecordDecision(ctx context.Context, d Decision) error {
	compNode, compSeq := "", uint64(0)
	if d.CompensatingID != nil {
		compNode, compSeq = string(d.CompensatingID.NodeID), d.CompensatingID.Seq
	}
	if _, err := p.pool.Exec(ctx, `INSERT INTO decisions
		(event_node, event_seq, verdict, reason, comp_node, comp_seq)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (event_node, event_seq) DO UPDATE SET verdict = excluded.verdict,
			reason = excluded.reason, comp_node = excluded.comp_node, comp_seq = excluded.comp_seq`,
		string(d.EventID.NodeID), d.EventID.Seq, string(d.Verdict), d.Reason,
		compNode, compSeq); err != nil {
		return fmt.Errorf("record decision for %s: %w", d.EventID, err)
	}
	return nil
}

func (p *Postgres) Decision(ctx context.Context, id domain.EventID) (Decision, bool, error) {
	d := Decision{EventID: id}
	var verdict, compNode string
	var compSeq int64
	err := p.pool.QueryRow(ctx, `SELECT verdict, reason, comp_node, comp_seq FROM decisions
		WHERE event_node = $1 AND event_seq = $2`, string(id.NodeID), id.Seq).
		Scan(&verdict, &d.Reason, &compNode, &compSeq)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Decision{}, false, nil
	case err != nil:
		return Decision{}, false, fmt.Errorf("read decision for %s: %w", id, err)
	}
	d.Verdict = Verdict(verdict)
	if compNode != "" {
		d.CompensatingID = &domain.EventID{NodeID: domain.NodeID(compNode), Seq: uint64(compSeq)}
	}
	return d, true, nil
}
```

`poKey` and `transitKey` from Task 15 are used only by `Memory`; leave them there.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/central/ -cover -v`
Expected: PASS for both `TestMemorySatisfiesTheStoreContract` and
`TestPostgresSatisfiesTheStoreContract`, coverage 100.0%. The first run downloads a
Postgres binary, so allow a minute.

- [ ] **Step 7: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 8: Commit**

```bash
git add warehouse-node/internal/central warehouse-node/go.mod warehouse-node/go.sum
git commit -m "feat(central): add the Postgres store

- events partitioned by list on node_id with a DEFAULT partition, so onboarding a
  warehouse needs no migration
- outbound uses a bigserial position, which is the node's downstream cursor
- in-transit balances, per-node cursors and the arbitration decisions table
- central's sequence and hlc are recovered on open so a restart never reuses one
- tested against a real embedded Postgres, so nothing is skipped"
```

---

### Task 17: The arbiter — five validators and their compensating events

The heart of the design. Every event a node pushes up runs through a chain of validators,
each owning exactly one invariant from the spec's ownership table that **no single node can
check**. The outcome is one of two things:

- **accept** — the event is persisted and its consequences recorded (a receipt counted
  against its purchase order, a dispatch added to the in-transit balance);
- **reject** — the event is *still* persisted, because the log is immutable history including
  mistakes, and central emits a **compensating event**: a normal event that undoes the
  effect, carrying `CausationID` pointing at the rejected event and a reason code. It flows
  down the same sync stream and the node applies it like anything else.

The five rules, in the order they run:

| Rule | Compensation | Reason |
|---|---|---|
| Unknown or deleted SKU | `StockAdjusted` zeroing that SKU at the receiving location | `unknown_sku` |
| Duplicate supplier delivery note | `StockAdjusted` removing the whole duplicated line | `duplicate_receipt` |
| Receipt exceeds the open purchase-order quantity | `StockAdjusted` removing the excess, plus a `ReceiptLineRecorded` reversal with a negative quantity | `po_overreceipt` |
| Transfer to an unknown or item-refusing node | `StockAdjusted` restoring the source stock, transfer marked failed | `transfer_rejected` |
| `TransferReceived` exceeds what was dispatched | `StockAdjusted` removing the excess at the destination | `transfer_overreceipt` |

Order matters: an event that both names a dead SKU and duplicates a delivery note produces
exactly one compensation, the first rule that fires. That is what makes "exactly one
over-receipt compensation" assertable.

Purchase orders are a stub reference in this project, so a receipt against a `PORef` central
has never heard of is not an over-receipt — there is no ordered quantity to exceed. It is
accepted.

**Files:**
- Create: `warehouse-node/internal/arbiter/arbiter.go`
- Create: `warehouse-node/internal/arbiter/validators.go`
- Test: `warehouse-node/internal/arbiter/arbiter_test.go`

**Interfaces:**
- Consumes: `central.Store` and all its methods, `central.Decision`, `central.Verdict`, `central.VerdictAccepted`, `central.VerdictRejected`, `central.ReceiptFact`, `central.InTransitRow`, `central.NewMemory`, `central.CentralNode` (Tasks 15-16); `domain.DecodePayload`, payload structs, `domain.Movement`, `domain.StockKey`, `domain.External`, the `Reason*` constants (Tasks 1-2).
- Produces:
  - `arbiter.Rejection{Reason string; Events []domain.Event; Excess float64}` — `Excess` is the quantity the compensation removes, and is what lets the accepted portion still be recorded
  - `arbiter.Validator` interface: `Name() string` and `Validate(ctx context.Context, s central.Store, env domain.Envelope, payload any) (*Rejection, error)`
  - `func arbiter.New(store central.Store, now func() time.Time) *Arbiter`
  - `func (*Arbiter) Arbitrate(ctx context.Context, env domain.Envelope) (central.Decision, []domain.Envelope, error)` — returns the decision and the compensating envelopes it emitted and enqueued
  - `func (*Arbiter) Validators() []Validator` — the chain, in order, for tests and diagnostics
  - `func arbiter.Discrepancies(ctx context.Context, s central.Store, olderThan time.Time) ([]central.InTransitRow, error)`

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/arbiter/arbiter_test.go`:

```go
package arbiter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

var at = time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)

// env seals a payload into an envelope with the given identity, the way a node's log
// would before pushing it upstream.
func env(t *testing.T, node domain.NodeID, seq uint64, typ, agg string, payload any) domain.Envelope {
	t.Helper()
	e, err := domain.NewEnvelope(
		domain.EventID{NodeID: node, Seq: seq},
		domain.HLC{Wall: at.UnixMilli() + int64(seq), Node: node},
		at.Add(time.Duration(seq)*time.Second), nil,
		domain.Event{Type: typ, AggregateID: agg, Payload: payload})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	return e
}

func goodsReceived(receipt, note, po, sku, lot string, qty float64, to domain.LocationCode) domain.GoodsReceived {
	return domain.GoodsReceived{ReceiptID: receipt, DeliveryNote: note, PORef: po,
		Move: domain.Movement{SKU: sku, LotID: lot, From: domain.External, To: to, Qty: qty}}
}

// newArbiter returns an arbiter over a memory store seeded with the widget item
// master, a 100-unit purchase order and two known nodes.
func newArbiter(t *testing.T) (*Arbiter, central.Store) {
	t.Helper()
	ctx := context.Background()
	store := central.NewMemory()
	if err := store.UpsertItem(ctx, domain.Item{SKU: "WIDGET", Description: "Blue widget",
		BaseUoM: "EA", LotTracked: true, ShelfLifeDays: 30}); err != nil {
		t.Fatalf("UpsertItem: %v", err)
	}
	if err := store.UpsertPurchaseOrder(ctx, "PO-1", "WIDGET", 100); err != nil {
		t.Fatalf("UpsertPurchaseOrder: %v", err)
	}
	for _, node := range []domain.NodeID{"wh-a", "wh-b"} {
		if err := store.RegisterNode(ctx, node, nil); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
	}
	return New(store, func() time.Time { return at }), store
}

// decodeAdjusted pulls the StockAdjusted payload out of a compensating envelope.
func decodeAdjusted(t *testing.T, e domain.Envelope) domain.StockAdjusted {
	t.Helper()
	payload, err := domain.DecodePayload(e)
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	a, ok := payload.(domain.StockAdjusted)
	if !ok {
		t.Fatalf("payload is %T, want domain.StockAdjusted", payload)
	}
	return a
}

func TestArbitrateAcceptsAValidReceipt(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	e := env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
		goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 60, "RECV-01"))

	decision, comps, err := a.Arbitrate(ctx, e)
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictAccepted || decision.Reason != "" {
		t.Fatalf("decision = %+v, want an unqualified acceptance", decision)
	}
	if len(comps) != 0 {
		t.Fatalf("comps = %+v, want none", comps)
	}
	total, err := store.ReceivedAgainstPO(ctx, "PO-1", "WIDGET")
	if err != nil || total != 60 {
		t.Fatalf("ReceivedAgainstPO = %v, %v; want 60, nil", total, err)
	}
	recorded, ok, err := store.Decision(ctx, e.ID)
	if err != nil || !ok || recorded.Verdict != central.VerdictAccepted {
		t.Errorf("stored decision = %+v, ok %v, err %v; want an accepted verdict", recorded, ok, err)
	}
}

// TestRejectionRules is one case per central-enforced invariant, asserting the exact
// compensating event emitted: its type, reason, movement and CausationID.
func TestRejectionRules(t *testing.T) {
	type wantEvent struct {
		typ    string
		reason string
		move   domain.Movement
	}
	tests := []struct {
		name       string
		seed       func(t *testing.T, a *Arbiter, s central.Store)
		event      func(t *testing.T) domain.Envelope
		wantReason string
		wantEvents []wantEvent
		wantFailed string
	}{
		{
			name: "unknown sku",
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
					goodsReceived("R1", "DN-1", "PO-1", "GHOST", "", 5, "RECV-01"))
			},
			wantReason: domain.ReasonUnknownSKU,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonUnknownSKU,
				move:   domain.Movement{SKU: "GHOST", From: "RECV-01", To: domain.External, Qty: 5},
			}},
		},
		{
			name: "deleted sku is treated as unknown",
			seed: func(t *testing.T, _ *Arbiter, s central.Store) {
				if err := s.UpsertItem(context.Background(), domain.Item{SKU: "WIDGET",
					BaseUoM: "EA", LotTracked: true, Deleted: true}); err != nil {
					t.Fatalf("UpsertItem: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
					goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 5, "RECV-01"))
			},
			wantReason: domain.ReasonUnknownSKU,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonUnknownSKU,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 5},
			}},
		},
		{
			name: "duplicate delivery note",
			seed: func(t *testing.T, a *Arbiter, _ central.Store) {
				first := env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
					goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 10, "RECV-01"))
				if _, _, err := a.Arbitrate(context.Background(), first); err != nil {
					t.Fatalf("seed Arbitrate: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-b", 1, domain.TypeGoodsReceived, "R2",
					goodsReceived("R2", "DN-1", "PO-1", "WIDGET", "L1", 10, "RECV-09"))
			},
			wantReason: domain.ReasonDuplicateReceipt,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonDuplicateReceipt,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-09", To: domain.External, Qty: 10},
			}},
		},
		{
			name: "receipt exceeds the open purchase order",
			seed: func(t *testing.T, a *Arbiter, _ central.Store) {
				first := env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
					goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 90, "RECV-01"))
				if _, _, err := a.Arbitrate(context.Background(), first); err != nil {
					t.Fatalf("seed Arbitrate: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-b", 1, domain.TypeGoodsReceived, "R2",
					goodsReceived("R2", "DN-2", "PO-1", "WIDGET", "L1", 30, "RECV-09"))
			},
			wantReason: domain.ReasonPOOverReceipt,
			wantEvents: []wantEvent{
				{
					typ:    domain.TypeStockAdjusted,
					reason: domain.ReasonPOOverReceipt,
					move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-09", To: domain.External, Qty: 20},
				},
				{typ: domain.TypeReceiptLineRecorded},
			},
		},
		{
			name: "transfer to an unknown node",
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1", domain.TransferDispatched{
					TransferID: "T1", FromNode: "wh-a", ToNode: "wh-z",
					Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})
			},
			wantReason: domain.ReasonTransferRejected,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonTransferRejected,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "PICK-01", Qty: 6},
			}},
			wantFailed: "T1",
		},
		{
			name: "transfer to a node that refuses the item",
			seed: func(t *testing.T, _ *Arbiter, s central.Store) {
				if err := s.RegisterNode(context.Background(), "wh-b", []string{"WIDGET"}); err != nil {
					t.Fatalf("RegisterNode: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1", domain.TransferDispatched{
					TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
					Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})
			},
			wantReason: domain.ReasonTransferRejected,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonTransferRejected,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "PICK-01", Qty: 6},
			}},
			wantFailed: "T1",
		},
		{
			name: "transfer received exceeds what was dispatched",
			seed: func(t *testing.T, a *Arbiter, _ central.Store) {
				dispatch := env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1", domain.TransferDispatched{
					TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
					Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})
				if _, _, err := a.Arbitrate(context.Background(), dispatch); err != nil {
					t.Fatalf("seed Arbitrate: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-b", 1, domain.TypeTransferReceived, "T1", domain.TransferReceived{
					TransferID: "T1",
					Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-09", Qty: 10}}})
			},
			wantReason: domain.ReasonTransferOverReceipt,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonTransferOverReceipt,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-09", To: domain.External, Qty: 4},
			}},
		},
		{
			name: "transfer received with no dispatch at all",
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-b", 1, domain.TypeTransferReceived, "T9", domain.TransferReceived{
					TransferID: "T9",
					Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-09", Qty: 3}}})
			},
			wantReason: domain.ReasonTransferOverReceipt,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonTransferOverReceipt,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-09", To: domain.External, Qty: 3},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, store := newArbiter(t)
			ctx := context.Background()
			if tt.seed != nil {
				tt.seed(t, a, store)
			}
			e := tt.event(t)

			decision, comps, err := a.Arbitrate(ctx, e)
			if err != nil {
				t.Fatalf("Arbitrate: %v", err)
			}
			if decision.Verdict != central.VerdictRejected {
				t.Fatalf("verdict = %q, want %q", decision.Verdict, central.VerdictRejected)
			}
			if decision.Reason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", decision.Reason, tt.wantReason)
			}
			if len(comps) != len(tt.wantEvents) {
				t.Fatalf("emitted %d compensating events, want %d: %+v", len(comps), len(tt.wantEvents), comps)
			}
			if decision.CompensatingID == nil || *decision.CompensatingID != comps[0].ID {
				t.Errorf("decision.CompensatingID = %v, want %v", decision.CompensatingID, comps[0].ID)
			}
			for i, want := range tt.wantEvents {
				got := comps[i]
				if got.Type != want.typ {
					t.Errorf("comps[%d].Type = %q, want %q", i, got.Type, want.typ)
				}
				if got.ID.NodeID != central.CentralNode {
					t.Errorf("comps[%d] came from %q, want it attributed to central", i, got.ID.NodeID)
				}
				if got.CausationID == nil || *got.CausationID != e.ID {
					t.Errorf("comps[%d].CausationID = %v, want %v", i, got.CausationID, e.ID)
				}
				if want.typ != domain.TypeStockAdjusted {
					continue
				}
				adjusted := decodeAdjusted(t, got)
				if adjusted.Reason != want.reason {
					t.Errorf("comps[%d].Reason = %q, want %q", i, adjusted.Reason, want.reason)
				}
				if adjusted.Move != want.move {
					t.Errorf("comps[%d].Move = %+v, want %+v", i, adjusted.Move, want.move)
				}
			}
			// The original event is still stored: history is immutable, mistakes
			// included.
			all, err := store.Events(ctx)
			if err != nil {
				t.Fatalf("Events: %v", err)
			}
			var found bool
			for _, stored := range all {
				found = found || stored.ID == e.ID
			}
			if !found {
				t.Error("the rejected event is not in the log; rejection must never delete history")
			}
			if tt.wantFailed != "" {
				row, ok, err := store.InTransit(ctx, tt.wantFailed, domain.StockKey{SKU: "WIDGET", LotID: "L1"})
				if err != nil || !ok || !row.Failed {
					t.Errorf("InTransit(%s) = %+v, ok %v, err %v; want it marked failed",
						tt.wantFailed, row, ok, err)
				}
			}
			// The compensation is queued for the node that produced the event, so it
			// arrives on the next sync session. A node cannot refuse it.
			queued, err := store.Outbound(ctx, e.ID.NodeID, 0, 10)
			if err != nil {
				t.Fatalf("Outbound: %v", err)
			}
			if len(queued) < len(comps) {
				t.Errorf("queued %d events for %s, want at least the %d compensations",
					len(queued), e.ID.NodeID, len(comps))
			}
		})
	}
}

func TestPOOverReceiptRecordsOnlyTheAcceptedPortion(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	if _, _, err := a.Arbitrate(ctx, env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
		goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 90, "RECV-01"))); err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if _, _, err := a.Arbitrate(ctx, env(t, "wh-b", 1, domain.TypeGoodsReceived, "R2",
		goodsReceived("R2", "DN-2", "PO-1", "WIDGET", "L1", 30, "RECV-09"))); err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	total, err := store.ReceivedAgainstPO(ctx, "PO-1", "WIDGET")
	if err != nil {
		t.Fatalf("ReceivedAgainstPO: %v", err)
	}
	if total != 100 {
		t.Fatalf("ReceivedAgainstPO = %v, want 100: the excess is compensated, not counted", total)
	}
	// A third receipt against a now-full purchase order over-receives by its whole
	// quantity, and produces exactly one compensation — not one per earlier receipt.
	_, comps, err := a.Arbitrate(ctx, env(t, "wh-a", 2, domain.TypeGoodsReceived, "R3",
		goodsReceived("R3", "DN-3", "PO-1", "WIDGET", "L1", 5, "RECV-01")))
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if len(comps) != 2 {
		t.Fatalf("emitted %d events, want 2 (the adjustment and the line reversal)", len(comps))
	}
	if got := decodeAdjusted(t, comps[0]).Move.Qty; got != 5 {
		t.Errorf("compensated quantity = %v, want the whole 5", got)
	}
}

func TestUnknownPurchaseOrderIsAccepted(t *testing.T) {
	a, _ := newArbiter(t)
	decision, comps, err := a.Arbitrate(context.Background(),
		env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
			goodsReceived("R1", "DN-1", "PO-UNKNOWN", "WIDGET", "L1", 5, "RECV-01")))
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictAccepted || len(comps) != 0 {
		t.Fatalf("decision = %+v, comps = %+v; want acceptance: a stub po has no ordered quantity to exceed",
			decision, comps)
	}
}

func TestAcceptedTransferHalvesMoveInTransitToZero(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}

	if _, _, err := a.Arbitrate(ctx, env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1",
		domain.TransferDispatched{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})); err != nil {
		t.Fatalf("Arbitrate dispatch: %v", err)
	}
	row, ok, err := store.InTransit(ctx, "T1", key)
	if err != nil || !ok || row.Dispatched-row.Received != 6 {
		t.Fatalf("in transit after dispatch = %+v, ok %v, err %v; want 6", row, ok, err)
	}
	if _, _, err := a.Arbitrate(ctx, env(t, "wh-b", 1, domain.TypeTransferReceived, "T1",
		domain.TransferReceived{TransferID: "T1",
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-09", Qty: 6}}})); err != nil {
		t.Fatalf("Arbitrate receipt: %v", err)
	}
	if row, _, err = store.InTransit(ctx, "T1", key); err != nil || row.Dispatched-row.Received != 0 {
		t.Fatalf("in transit after receipt = %+v, %v; want 0", row, err)
	}
	// The dispatch was forwarded to the destination, and the receipt back to the
	// source, so both nodes can close their view of the transfer.
	toDest, err := store.Outbound(ctx, "wh-b", 0, 10)
	if err != nil {
		t.Fatalf("Outbound(wh-b): %v", err)
	}
	if len(toDest) != 1 || toDest[0].Env.Type != domain.TypeTransferDispatched {
		t.Errorf("queued for wh-b = %+v, want the dispatch", toDest)
	}
	toSource, err := store.Outbound(ctx, "wh-a", 0, 10)
	if err != nil {
		t.Fatalf("Outbound(wh-a): %v", err)
	}
	if len(toSource) != 1 || toSource[0].Env.Type != domain.TypeTransferReceived {
		t.Errorf("queued for wh-a = %+v, want the receipt", toSource)
	}
}

func TestDiscrepanciesReportUnmatchedDispatchesWithoutCompensating(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	if _, _, err := a.Arbitrate(ctx, env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1",
		domain.TransferDispatched{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})); err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	before, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	if rows, err := Discrepancies(ctx, store, at); err != nil || len(rows) != 0 {
		t.Fatalf("Discrepancies inside the window = %+v, %v; want none", rows, err)
	}
	rows, err := Discrepancies(ctx, store, at.Add(72*time.Hour))
	if err != nil {
		t.Fatalf("Discrepancies: %v", err)
	}
	if len(rows) != 1 || rows[0].TransferID != "T1" {
		t.Fatalf("Discrepancies = %+v, want T1 reported", rows)
	}

	after, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(after) != len(before) {
		t.Error("reporting a discrepancy emitted events; a lost truck is a human problem, never auto-compensated")
	}
}

func TestArbitrateRejectsAnUndecodableEvent(t *testing.T) {
	a, _ := newArbiter(t)
	bad := domain.Envelope{
		ID:      domain.EventID{NodeID: "wh-a", Seq: 1},
		Type:    "NotAnEventType",
		HLC:     domain.HLC{Wall: 1, Node: "wh-a"},
		Payload: json.RawMessage(`{}`),
	}
	if _, _, err := a.Arbitrate(context.Background(), bad); err == nil {
		t.Fatal("Arbitrate on an unknown event type: expected an error, never a silent skip")
	}
}

func TestValidatorsAreNamedAndOrdered(t *testing.T) {
	a, _ := newArbiter(t)
	want := []string{"unknown_sku", "duplicate_delivery_note", "po_over_receipt",
		"transfer_destination", "transfer_over_receipt"}
	got := a.Validators()
	if len(got) != len(want) {
		t.Fatalf("%d validators, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Name() != want[i] {
			t.Errorf("validator %d is %q, want %q", i, got[i].Name(), want[i])
		}
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/arbiter/ -v`
Expected: FAIL to compile — `undefined: New`, `undefined: Arbiter`, `undefined: Discrepancies`.

- [ ] **Step 3: Write `arbiter.go`**

```go
// Package arbiter is central's authority. It runs each event a node pushed up through
// the validators for the invariants no single node can check, and when one rejects, it
// emits a compensating event: a normal event that undoes the effect, attributed to
// central, carrying CausationID pointing at the event it answers.
//
// Rejection never deletes anything. The original event stays in the log because the
// log is immutable history including mistakes, and compensation is deliberately not
// cascaded through downstream events — that way lies distributed rollback, which real
// ERPs do not do either.
package arbiter

import (
	"context"
	"fmt"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// Rejection is what a validator returns when it refuses an event: the reason code that
// goes on the compensation and into the decisions table, the compensating events to
// emit in order, and the quantity being taken back, which is what lets the accepted
// portion of a partly valid receipt still count.
type Rejection struct {
	Reason string
	Events []domain.Event
	Excess float64
}

// Validator checks one central-enforced invariant. It returns nil to accept.
type Validator interface {
	Name() string
	Validate(ctx context.Context, s central.Store, env domain.Envelope, payload any) (*Rejection, error)
}

// Arbiter applies the validator chain to incoming node events.
type Arbiter struct {
	store      central.Store
	now        func() time.Time
	validators []Validator
}

// New builds an arbiter over a store. now supplies the instant compensating events
// are stamped with; it is injected so tests are deterministic.
func New(store central.Store, now func() time.Time) *Arbiter {
	return &Arbiter{store: store, now: now, validators: []Validator{
		unknownSKU{}, duplicateDeliveryNote{}, poOverReceipt{}, transferDestination{}, transferOverReceipt{},
	}}
}

// Validators returns the chain in the order it runs. The order is load-bearing: an
// event breaking two rules yields exactly one compensation, from the first to fire.
func (a *Arbiter) Validators() []Validator { return a.validators }

// Arbitrate persists an event, decides on it, records the consequences, and — on
// rejection — emits and enqueues the compensating events for the node that produced
// it. It returns the decision and the compensations.
func (a *Arbiter) Arbitrate(ctx context.Context, env domain.Envelope) (central.Decision, []domain.Envelope, error) {
	if _, err := a.store.Append(ctx, []domain.Envelope{env}); err != nil {
		return central.Decision{}, nil, err
	}
	// An event that cannot be decoded fails loudly. Skipping it would leave central
	// and the node with different histories, which is the one failure this system
	// must not have.
	payload, err := domain.DecodePayload(env)
	if err != nil {
		return central.Decision{}, nil, fmt.Errorf("arbitrate %s: %w", env.ID, err)
	}

	var rejection *Rejection
	for _, v := range a.validators {
		rejection, err = v.Validate(ctx, a.store, env, payload)
		if err != nil {
			return central.Decision{}, nil, fmt.Errorf("validator %s on %s: %w", v.Name(), env.ID, err)
		}
		if rejection != nil {
			break
		}
	}

	if err := a.record(ctx, env, payload, rejection); err != nil {
		return central.Decision{}, nil, err
	}
	if err := a.fanOut(ctx, env, payload, rejection); err != nil {
		return central.Decision{}, nil, err
	}

	decision := central.Decision{EventID: env.ID, Verdict: central.VerdictAccepted}
	if rejection == nil {
		return decision, nil, a.store.RecordDecision(ctx, decision)
	}

	comps, err := a.store.EmitCentral(ctx, rejection.Events, &env.ID, a.now())
	if err != nil {
		return central.Decision{}, nil, err
	}
	if err := a.store.Enqueue(ctx, env.ID.NodeID, comps); err != nil {
		return central.Decision{}, nil, err
	}
	decision.Verdict, decision.Reason = central.VerdictRejected, rejection.Reason
	if len(comps) > 0 {
		decision.CompensatingID = &comps[0].ID
	}
	return decision, comps, a.store.RecordDecision(ctx, decision)
}

// record folds the event's consequences into central's cross-node bookkeeping. For a
// rejected receipt only the accepted portion is counted, so the purchase order is not
// left permanently over-received by an event that was compensated away.
func (a *Arbiter) record(ctx context.Context, env domain.Envelope, payload any, rej *Rejection) error {
	switch p := payload.(type) {
	case domain.GoodsReceived:
		if rej != nil && rej.Reason != domain.ReasonPOOverReceipt {
			// A duplicate or an unknown SKU contributes nothing at all; the note's
			// first sighting is already recorded against the original receipt.
			return nil
		}
		qty := p.Move.Qty
		if rej != nil {
			qty -= rej.Excess
		}
		return a.store.RecordReceipt(ctx, central.ReceiptFact{EventID: env.ID, Node: env.ID.NodeID,
			PORef: p.PORef, DeliveryNote: p.DeliveryNote, SKU: p.Move.SKU, QtyBase: qty})
	case domain.TransferDispatched:
		for _, m := range p.Lines {
			if err := a.store.RecordDispatch(ctx, central.InTransitRow{TransferID: p.TransferID,
				Key: domain.StockKey{SKU: m.SKU, LotID: m.LotID}, FromNode: p.FromNode, ToNode: p.ToNode,
				Dispatched: m.Qty, DispatchedAt: env.RecordedAt}); err != nil {
				return err
			}
		}
		if rej == nil {
			return nil
		}
		// The transfer is dead: the stock is going back to the source, so its
		// in-transit rows must stop counting as goods on a truck.
		return a.store.FailTransfer(ctx, p.TransferID)
	case domain.TransferReceived:
		for _, m := range p.Lines {
			key := domain.StockKey{SKU: m.SKU, LotID: m.LotID}
			row, ok, err := a.store.InTransit(ctx, p.TransferID, key)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			accepted := m.Qty
			if remaining := row.Dispatched - row.Received; accepted > remaining {
				accepted = remaining
			}
			if accepted <= 0 {
				continue
			}
			if err := a.store.AddReceived(ctx, p.TransferID, key, accepted); err != nil {
				return err
			}
		}
	}
	return nil
}

// fanOut queues events for the other node that needs to see them. Transfers are the
// only cross-node flow: the destination must learn of a dispatch before it can record
// the truck arriving, and the source must learn the goods landed.
func (a *Arbiter) fanOut(ctx context.Context, env domain.Envelope, payload any, rej *Rejection) error {
	if rej != nil {
		return nil
	}
	switch p := payload.(type) {
	case domain.TransferDispatched:
		return a.store.Enqueue(ctx, p.ToNode, []domain.Envelope{env})
	case domain.TransferReceived:
		for _, m := range p.Lines {
			row, ok, err := a.store.InTransit(ctx, p.TransferID, domain.StockKey{SKU: m.SKU, LotID: m.LotID})
			if err != nil {
				return err
			}
			if ok {
				return a.store.Enqueue(ctx, row.FromNode, []domain.Envelope{env})
			}
		}
	}
	return nil
}

// Discrepancies reports transfers dispatched before olderThan that are still carrying
// stock. It is a report and nothing else: an unmatched dispatch past the window is a
// lost truck, which is a human problem, and auto-compensating it would silently
// invent stock at the source that may well be sitting in a lay-by.
func Discrepancies(ctx context.Context, s central.Store, olderThan time.Time) ([]central.InTransitRow, error) {
	return s.OpenTransfers(ctx, olderThan)
}
```

- [ ] **Step 4: Write `validators.go`**

```go
package arbiter

import (
	"context"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// movementsOf returns the stock movements a payload carries. Only these payloads move
// stock, and only they can need a stock compensation.
func movementsOf(payload any) []domain.Movement {
	switch p := payload.(type) {
	case domain.GoodsReceived:
		return []domain.Movement{p.Move}
	case domain.TransferDispatched:
		return p.Lines
	case domain.TransferReceived:
		return p.Lines
	default:
		return nil
	}
}

// reverse turns a movement into the movement that undoes it.
func reverse(m domain.Movement, qty float64) domain.Movement {
	return domain.Movement{SKU: m.SKU, LotID: m.LotID, From: m.To, To: m.From, Qty: qty}
}

// adjustment builds the compensating StockAdjusted for one movement.
func adjustment(aggregateID string, move domain.Movement, reason string) domain.Event {
	return domain.Event{Type: domain.TypeStockAdjusted, AggregateID: aggregateID,
		Payload: domain.StockAdjusted{Move: move, Reason: reason}}
}

// unknownSKU rejects an event naming a SKU central's item master does not have, or has
// but has deleted. A node validates SKUs against its replicated copy of the master,
// which may be stale; central re-checks against the live one. The compensation zeroes
// that SKU at the location it landed in, and the exceptions projection on the node
// flags it for manual cleanup.
type unknownSKU struct{}

func (unknownSKU) Name() string { return "unknown_sku" }

func (unknownSKU) Validate(ctx context.Context, s central.Store, env domain.Envelope, payload any) (*Rejection, error) {
	moves := movementsOf(payload)
	events := make([]domain.Event, 0, len(moves))
	var excess float64
	for _, m := range moves {
		item, ok, err := s.Item(ctx, m.SKU)
		if err != nil {
			return nil, err
		}
		if ok && !item.Deleted {
			continue
		}
		events = append(events, adjustment(m.SKU, reverse(m, m.Qty), domain.ReasonUnknownSKU))
		excess += m.Qty
	}
	if len(events) == 0 {
		return nil, nil
	}
	return &Rejection{Reason: domain.ReasonUnknownSKU, Events: events, Excess: excess}, nil
}

// duplicateDeliveryNote rejects a receipt keying a supplier delivery note some other
// receipt already keyed. Only central can see this: the two receipts may be at
// different warehouses, and neither node can see the other's log.
type duplicateDeliveryNote struct{}

func (duplicateDeliveryNote) Name() string { return "duplicate_delivery_note" }

func (duplicateDeliveryNote) Validate(ctx context.Context, s central.Store, env domain.Envelope, payload any) (*Rejection, error) {
	p, ok := payload.(domain.GoodsReceived)
	if !ok {
		return nil, nil
	}
	first, found, err := s.DeliveryNoteFirstSeen(ctx, p.DeliveryNote, p.Move.SKU)
	if err != nil {
		return nil, err
	}
	if !found || first == env.ID {
		return nil, nil
	}
	return &Rejection{
		Reason: domain.ReasonDuplicateReceipt,
		Events: []domain.Event{adjustment(p.Move.SKU, reverse(p.Move, p.Move.Qty), domain.ReasonDuplicateReceipt)},
		Excess: p.Move.Qty,
	}, nil
}

// poOverReceipt rejects the portion of a receipt that takes a purchase order past its
// ordered quantity. Two warehouses can receive against the same order and neither sees
// the other's receipts, so this is structurally central's call.
//
// A PORef central has no order for is accepted: purchase-order lifecycle is out of
// scope in this project, so there is no ordered quantity to exceed.
type poOverReceipt struct{}

func (poOverReceipt) Name() string { return "po_over_receipt" }

func (poOverReceipt) Validate(ctx context.Context, s central.Store, env domain.Envelope, payload any) (*Rejection, error) {
	p, ok := payload.(domain.GoodsReceived)
	if !ok {
		return nil, nil
	}
	ordered, known, err := s.PurchaseOrder(ctx, p.PORef, p.Move.SKU)
	if err != nil || !known {
		return nil, err
	}
	already, err := s.ReceivedAgainstPO(ctx, p.PORef, p.Move.SKU)
	if err != nil {
		return nil, err
	}
	excess := already + p.Move.Qty - ordered
	if excess <= 0 {
		return nil, nil
	}
	if excess > p.Move.Qty {
		excess = p.Move.Qty
	}
	return &Rejection{
		Reason: domain.ReasonPOOverReceipt,
		Events: []domain.Event{
			adjustment(p.Move.SKU, reverse(p.Move, excess), domain.ReasonPOOverReceipt),
			// The paperwork is reversed too, by a negative line: the receipt as
			// recorded claimed more than the order allowed.
			{Type: domain.TypeReceiptLineRecorded, AggregateID: p.ReceiptID,
				Payload: domain.ReceiptLineRecorded{ReceiptID: p.ReceiptID, LineNo: 0,
					SKU: p.Move.SKU, LotID: p.Move.LotID, QtyBase: -excess}},
		},
		Excess: excess,
	}, nil
}

// transferDestination rejects a dispatch to a node central does not know, or to a node
// configured to refuse that item. No node has authority over another node's
// configuration, so the source cannot check this before dispatching.
type transferDestination struct{}

func (transferDestination) Name() string { return "transfer_destination" }

func (transferDestination) Validate(ctx context.Context, s central.Store, env domain.Envelope, payload any) (*Rejection, error) {
	p, ok := payload.(domain.TransferDispatched)
	if !ok {
		return nil, nil
	}
	rejects, known, err := s.NodeConfig(ctx, p.ToNode)
	if err != nil {
		return nil, err
	}
	refused := !known
	for _, m := range p.Lines {
		refused = refused || rejects[m.SKU]
	}
	if !refused {
		return nil, nil
	}
	events := make([]domain.Event, 0, len(p.Lines))
	var excess float64
	for _, m := range p.Lines {
		// Restore the source stock: the goods never left, as far as the books go.
		// The aggregate is the transfer, so the node's transfers projection learns
		// the transfer failed.
		events = append(events, adjustment(p.TransferID, reverse(m, m.Qty), domain.ReasonTransferRejected))
		excess += m.Qty
	}
	return &Rejection{Reason: domain.ReasonTransferRejected, Events: events, Excess: excess}, nil
}

// transferOverReceipt rejects the portion of a transfer receipt beyond what was
// dispatched, including the whole of a receipt for a transfer that was never
// dispatched at all. The two halves live on different nodes, so only central holds
// both.
type transferOverReceipt struct{}

func (transferOverReceipt) Name() string { return "transfer_over_receipt" }

func (transferOverReceipt) Validate(ctx context.Context, s central.Store, env domain.Envelope, payload any) (*Rejection, error) {
	p, ok := payload.(domain.TransferReceived)
	if !ok {
		return nil, nil
	}
	events := make([]domain.Event, 0, len(p.Lines))
	var excess float64
	for _, m := range p.Lines {
		var remaining float64
		row, found, err := s.InTransit(ctx, p.TransferID, domain.StockKey{SKU: m.SKU, LotID: m.LotID})
		if err != nil {
			return nil, err
		}
		if found {
			remaining = row.Dispatched - row.Received
		}
		over := m.Qty - remaining
		if over <= 0 {
			continue
		}
		events = append(events, adjustment(m.SKU, reverse(m, over), domain.ReasonTransferOverReceipt))
		excess += over
	}
	if len(events) == 0 {
		return nil, nil
	}
	return &Rejection{Reason: domain.ReasonTransferOverReceipt, Events: events, Excess: excess}, nil
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/arbiter/ -cover -v`
Expected: PASS, coverage 100.0%. If any error branch inside a validator is uncovered,
cover it by closing a Postgres-backed store and arbitrating against it — a closed pool
makes every store call fail, which exercises the `return nil, err` paths.

- [ ] **Step 6: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 7: Commit**

```bash
git add warehouse-node/internal/arbiter
git commit -m "feat(arbiter): add central's validator chain and compensating events

- one validator per central-enforced invariant, in a load-bearing order so an
  event breaking two rules yields exactly one compensation
- every compensation is attributed to central, carries CausationID and a reason
- a rejected receipt still counts its accepted portion against the purchase order
- unmatched dispatches past a window are reported, never auto-compensated"
```

---

### Task 18: Replication — the bidirectional sync client and server

The stream that carries events up and compensations down. The node always opens it, because
a warehouse may sit behind a connection central cannot dial.

One session is one deterministic round, which is what makes this testable and keeps the
implementation small:

1. node sends `Hello` — its identity, its version vector (the highest sequence it holds per
   originating node) and its pull cursor;
2. central answers `Welcome` with the highest sequence of that node's own events it already
   holds, so the node knows where to resume pushing from;
3. node pushes its own events as `EventBatch` chunks, the last one with `more = false`;
   central arbitrates each event and `Ack`s each batch with the sequence it has persisted;
4. central pushes down everything queued for this node — compensations, item-master
   updates, forwarded transfer events — as chunks, the last with `more = false`;
5. node sends one `Ack` with the highest outbound position it applied, and closes.

Both sides persist cursors at every step, so a session that dies halfway loses nothing: the
next one resumes from the last acknowledged position. A node offline for a week pushes its
backlog over many chunks and many sessions rather than one enormous message.

Two failure rules from the spec are load-bearing here. An unparseable event from central
fails the session loudly and does **not** advance the cursor, so it is retried — never
skipped, because skipping an event from the authority silently forks state. And central
being unreachable is not an error the operator ever sees: the node keeps working and the
client retries with backoff.

**Files:**
- Create: `warehouse-node/internal/sync/server.go`
- Create: `warehouse-node/internal/sync/client.go`
- Test: `warehouse-node/internal/sync/replicate_test.go`

**Interfaces:**
- Consumes: `syncrepl.EncodeBatch`, `syncrepl.DecodeBatch`, `syncrepl.Chunk`, `syncrepl.MaxBatchEvents` (Task 14); `central.Store` (Task 15); `arbiter.Arbiter.Arbitrate` (Task 17); `node.Service.Log/Ingest/NodeID` and `eventlog.CursorPushed`/`CursorPulled` (Tasks 9, 12); `syncpb` (Task 14).
- Produces:
  - `func syncrepl.NewServer(store central.Store, arb *arbiter.Arbiter) *Server` implementing `syncpb.SyncServer`
  - `func syncrepl.NewClient(svc *node.Service, client syncpb.SyncClient) *Client`
  - `func (*Client) Session(ctx context.Context) error` — one full round
  - `func (*Client) Run(ctx context.Context, every, backoff time.Duration) error` — sessions on a ticker, retrying after failures, returning only when ctx is done

- [ ] **Step 1: Write the failing test**

`warehouse-node/internal/sync/replicate_test.go`:

```go
package syncrepl

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

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/arbiter"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

var at = time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)

func widget() domain.Item {
	return domain.Item{SKU: "WIDGET", Description: "Blue widget", BaseUoM: "EA",
		AltUoM: map[domain.UoM]float64{"CASE": 12}, LotTracked: true, ShelfLifeDays: 3650}
}

// startCentral runs the sync server over an in-process bufconn listener and returns a
// client stub for it. The transport is real gRPC; only the network is in-process.
func startCentral(t *testing.T, store central.Store) syncpb.SyncClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, NewServer(store, arbiter.New(store, func() time.Time { return at })))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close conn: %v", err)
		}
		srv.Stop()
		if err := lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close listener: %v", err)
		}
	})
	return syncpb.NewSyncClient(conn)
}

// startNode opens a node with two locations and the widget item master.
func startNode(t *testing.T, id domain.NodeID) *node.Service {
	t.Helper()
	n := 0
	svc, err := node.Open(filepath.Join(t.TempDir(), "node.db"), id, func() time.Time {
		n++
		return at.Add(time.Duration(n) * time.Second)
	})
	if err != nil {
		t.Fatalf("node.Open: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	for code, typ := range map[domain.LocationCode]domain.LocationType{
		"RECV-01": domain.LocReceiving, "PICK-01": domain.LocPick,
	} {
		if err := svc.RegisterLocation(code, typ); err != nil {
			t.Fatalf("RegisterLocation: %v", err)
		}
	}
	if _, err := svc.Execute(func(*domain.State) ([]domain.Event, error) {
		return []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "WIDGET",
			Payload: domain.ItemUpserted{Item: widget()}}}, nil
	}); err != nil {
		t.Fatalf("seed item master: %v", err)
	}
	return svc
}

func seedCentral(t *testing.T, store central.Store, ordered float64) {
	t.Helper()
	ctx := context.Background()
	if err := store.UpsertItem(ctx, widget()); err != nil {
		t.Fatalf("UpsertItem: %v", err)
	}
	if err := store.UpsertPurchaseOrder(ctx, "PO-1", "WIDGET", ordered); err != nil {
		t.Fatalf("UpsertPurchaseOrder: %v", err)
	}
	for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
		if err := store.RegisterNode(ctx, id, nil); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
	}
}

func receive(receipt, note string, qty float64) node.Command {
	return func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReceive(s, domain.ReceiveCmd{ReceiptID: receipt, DeliveryNote: note, PORef: "PO-1",
			Line: domain.Line{SKU: "WIDGET", LotID: "L1", Qty: qty, UoM: "EA"}, To: "RECV-01"})
	}
}

func TestSessionPushesEventsUpAndCursorsAdvance(t *testing.T) {
	store := central.NewMemory()
	seedCentral(t, store, 1000)
	svc := startNode(t, "wh-a")
	client := NewClient(svc, startCentral(t, store))
	ctx := context.Background()

	if _, err := svc.Execute(receive("R1", "DN-1", 10)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err := client.Session(ctx); err != nil {
		t.Fatalf("Session: %v", err)
	}

	events, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	local, err := svc.Log().ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(events) != len(local) {
		t.Fatalf("central holds %d events, node holds %d; want them equal", len(events), len(local))
	}
	pushed, err := svc.Log().Cursor("pushed_to_central")
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if pushed != uint64(len(local)) {
		t.Errorf("pushed cursor = %d, want %d", pushed, len(local))
	}
	if seq, err := store.PushedSeq(ctx, "wh-a"); err != nil || seq != pushed {
		t.Errorf("central PushedSeq = %d, %v; want %d", seq, err, pushed)
	}
}

func TestSessionIsResumableAndIdempotent(t *testing.T) {
	store := central.NewMemory()
	seedCentral(t, store, 1000)
	svc := startNode(t, "wh-a")
	client := NewClient(svc, startCentral(t, store))
	ctx := context.Background()

	if _, err := svc.Execute(receive("R1", "DN-1", 10)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := client.Session(ctx); err != nil {
			t.Fatalf("Session %d: %v", i, err)
		}
	}
	before, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	if _, err := svc.Execute(receive("R2", "DN-2", 5)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err := client.Session(ctx); err != nil {
		t.Fatalf("resumed Session: %v", err)
	}
	after, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(after) != len(before)+3 {
		t.Errorf("central holds %d events, want %d: only the new receipt's three events",
			len(after), len(before)+3)
	}
}

func TestCompensationFlowsDownAndCannotBeRefused(t *testing.T) {
	store := central.NewMemory()
	seedCentral(t, store, 6) // the purchase order allows only six
	svc := startNode(t, "wh-a")
	client := NewClient(svc, startCentral(t, store))
	ctx := context.Background()

	if _, err := svc.Execute(receive("R1", "DN-1", 10)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err := client.Session(ctx); err != nil {
		t.Fatalf("Session: %v", err)
	}
	// Central rejected four units while receiving, and the same session pushes the
	// compensation straight back down, so one round is enough.

	rows, err := svc.StockOnHand("WIDGET", "RECV-01")
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(rows) != 1 || rows[0].Qty != 6 {
		t.Fatalf("balance = %+v, want 6: the node applied the compensation", rows)
	}
	exceptions, err := svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 || exceptions[0].Reason != domain.ReasonPOOverReceipt {
		t.Fatalf("exceptions = %+v, want one po_overreceipt row", exceptions)
	}
	pulled, err := svc.Log().Cursor("pulled_from_central")
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if pulled == 0 {
		t.Error("pull cursor = 0, want it advanced past the compensation")
	}
	// A second session must be a no-op: the cursor stops the same events arriving
	// twice and double-compensating.
	if err := client.Session(ctx); err != nil {
		t.Fatalf("second Session: %v", err)
	}
	if rows, err = svc.StockOnHand("WIDGET", "RECV-01"); err != nil || rows[0].Qty != 6 {
		t.Fatalf("balance after a repeat session = %+v, %v; want it unchanged at 6", rows, err)
	}
}

func TestWeekOfflineCatchesUpInBoundedChunks(t *testing.T) {
	store := central.NewMemory()
	seedCentral(t, store, 100000)
	svc := startNode(t, "wh-a")
	client := NewClient(svc, startCentral(t, store))
	ctx := context.Background()

	// 200 operations offline. Each receipt emits three events, so the backlog is well
	// past MaxBatchEvents and must arrive in more than one chunk.
	for i := 0; i < 200; i++ {
		if _, err := svc.Execute(receive(
			"R"+string(rune('a'+i%26))+string(rune('a'+i/26)),
			"DN"+string(rune('a'+i%26))+string(rune('a'+i/26)), 1)); err != nil {
			t.Fatalf("Execute %d: %v", i, err)
		}
	}
	if err := client.Session(ctx); err != nil {
		t.Fatalf("Session: %v", err)
	}
	local, err := svc.Log().ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(local) <= MaxBatchEvents {
		t.Fatalf("backlog is %d events, want more than the %d batch cap for this test to mean anything",
			len(local), MaxBatchEvents)
	}
	events, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != len(local) {
		t.Errorf("central holds %d of %d events; the whole backlog must arrive", len(events), len(local))
	}
}

func TestSessionSurvivesCentralBeingUnreachable(t *testing.T) {
	store := central.NewMemory()
	seedCentral(t, store, 1000)
	svc := startNode(t, "wh-a")
	stub := startCentral(t, store)
	client := NewClient(svc, stub)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Session(cancelled); err == nil {
		t.Fatal("Session on a cancelled context: expected an error")
	}
	// The node is untouched by the failure: commands still work offline.
	if _, err := svc.Execute(receive("R1", "DN-1", 1)); err != nil {
		t.Fatalf("Execute after a failed session: %v", err)
	}

	// Run returns when its context is done, having swallowed session failures.
	ctx, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	if err := client.Run(ctx, time.Millisecond, time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestServerRejectsAStreamThatDoesNotStartWithHello(t *testing.T) {
	store := central.NewMemory()
	seedCentral(t, store, 1000)
	stub := startCentral(t, store)

	stream, err := stub.Replicate(context.Background())
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Ack{Ack: &syncpb.Ack{Seq: 1}}}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected the server to reject a session that does not open with Hello")
	}
}

func TestServerRejectsAnUndecodableEvent(t *testing.T) {
	store := central.NewMemory()
	seedCentral(t, store, 1000)
	stub := startCentral(t, store)

	stream, err := stub.Replicate(context.Background())
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Hello{
		Hello: &syncpb.Hello{NodeId: "wh-a"}}}); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv welcome: %v", err)
	}
	if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Events{
		Events: &syncpb.EventBatch{Events: []*syncpb.Event{{Type: "Nonsense"}}}}}); err != nil {
		t.Fatalf("Send batch: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected the server to fail the session on an undecodable event, never skip it")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./internal/sync/ -run Session -v`
Expected: FAIL to compile — `undefined: NewServer`, `undefined: NewClient`.

- [ ] **Step 3: Add the test-only dependency**

```bash
cd warehouse-node && go get google.golang.org/grpc/test/bufconn@latest
```

(`bufconn` ships inside the `grpc` module, so this resolves to the version already
required; the command only makes the import explicit if `go mod tidy` has trimmed it.)

- [ ] **Step 4: Write `server.go`**

```go
package syncrepl

import (
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/arbiter"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// Server is central's side of the replication stream.
type Server struct {
	syncpb.UnimplementedSyncServer
	store central.Store
	arb   *arbiter.Arbiter
}

// NewServer builds the sync server over a store and an arbiter.
func NewServer(store central.Store, arb *arbiter.Arbiter) *Server {
	return &Server{store: store, arb: arb}
}

// Replicate runs one session: Hello, Welcome, the node's events upstream, then
// everything queued downstream, then the node's acknowledgement. Cursors are
// persisted as it goes, so a session that dies halfway costs nothing but a retry.
func (s *Server) Replicate(stream syncpb.Sync_ReplicateServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil || hello.GetNodeId() == "" {
		return status.Error(codes.InvalidArgument, "a session must open with Hello naming the node")
	}
	node := domain.NodeID(hello.GetNodeId())

	known, err := s.store.PushedSeq(ctx, node)
	if err != nil {
		return err
	}
	if err := stream.Send(&syncpb.CentralFrame{Body: &syncpb.CentralFrame_Welcome{
		Welcome: &syncpb.Welcome{KnownSeq: known}}}); err != nil {
		return err
	}

	if err := s.receive(stream, node); err != nil {
		return err
	}
	if err := s.push(stream, node); err != nil {
		return err
	}

	// One final Ack tells us how far the node applied what we sent. EOF instead means
	// the node dropped out: nothing is lost, the cursor simply does not advance.
	last, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	if ack := last.GetAck(); ack != nil && ack.GetOrd() > 0 {
		return s.store.SetDeliveredOrd(ctx, node, ack.GetOrd())
	}
	return nil
}

// receive consumes the node's upstream batches, arbitrating every event, until a batch
// arrives with more = false.
func (s *Server) receive(stream syncpb.Sync_ReplicateServer, node domain.NodeID) error {
	ctx := stream.Context()
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		batch := frame.GetEvents()
		if batch == nil {
			return status.Error(codes.InvalidArgument, "expected an EventBatch while receiving")
		}
		envs, err := DecodeBatch(batch)
		if err != nil {
			// Never skip. A malformed event means the session fails and is retried;
			// accepting the rest would fork central's history from the node's.
			return status.Error(codes.InvalidArgument, err.Error())
		}
		highest, err := s.store.PushedSeq(ctx, node)
		if err != nil {
			return err
		}
		for _, env := range envs {
			if _, _, err := s.arb.Arbitrate(ctx, env); err != nil {
				return err
			}
			if env.ID.NodeID == node && env.ID.Seq > highest {
				highest = env.ID.Seq
			}
		}
		if err := s.store.SetPushedSeq(ctx, node, highest); err != nil {
			return err
		}
		if err := stream.Send(&syncpb.CentralFrame{Body: &syncpb.CentralFrame_Ack{
			Ack: &syncpb.Ack{Seq: highest}}}); err != nil {
			return err
		}
		if !batch.GetMore() {
			return nil
		}
	}
}

// push sends everything queued for this node — compensations, item-master updates and
// forwarded transfer events — in chunks bounded by MaxBatchEvents.
func (s *Server) push(stream syncpb.Sync_ReplicateServer, node domain.NodeID) error {
	ctx := stream.Context()
	cursor, err := s.store.DeliveredOrd(ctx, node)
	if err != nil {
		return err
	}
	for {
		rows, err := s.store.Outbound(ctx, node, cursor, MaxBatchEvents)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return stream.Send(&syncpb.CentralFrame{Body: &syncpb.CentralFrame_Events{
				Events: &syncpb.EventBatch{More: false, LastOrd: cursor}}})
		}
		envs := make([]domain.Envelope, 0, len(rows))
		for _, row := range rows {
			envs = append(envs, row.Env)
			cursor = row.Ord
		}
		remaining, err := s.store.Outbound(ctx, node, cursor, 1)
		if err != nil {
			return err
		}
		batch := EncodeBatch(envs)
		batch.LastOrd, batch.More = cursor, len(remaining) > 0
		if err := stream.Send(&syncpb.CentralFrame{Body: &syncpb.CentralFrame_Events{Events: batch}}); err != nil {
			return err
		}
		if !batch.More {
			return nil
		}
	}
}
```

- [ ] **Step 5: Write `client.go`**

```go
package syncrepl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/eventlog"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// Client is the node's side of the replication stream. The transport is injected as a
// syncpb.SyncClient, so tests drive a real gRPC stream over an in-process listener.
type Client struct {
	svc    *node.Service
	remote syncpb.SyncClient
}

// NewClient builds a sync client for one node.
func NewClient(svc *node.Service, remote syncpb.SyncClient) *Client {
	return &Client{svc: svc, remote: remote}
}

// Run keeps syncing until ctx is done. A failed session is not an error the operator
// sees: central being unreachable is a normal state for a warehouse, so the failure is
// logged by the caller at most and retried after backoff.
func (c *Client) Run(ctx context.Context, every, backoff time.Duration) error {
	for {
		wait := every
		if err := c.Session(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			wait = backoff
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// Session runs one full round: push this node's backlog, then apply everything central
// has for it. Both cursors are persisted as they advance, so the next session resumes
// rather than restarting.
func (c *Client) Session(ctx context.Context) error {
	stream, err := c.remote.Replicate(ctx)
	if err != nil {
		return fmt.Errorf("open replication stream: %w", err)
	}

	vector, err := c.svc.Log().VersionVector()
	if err != nil {
		return err
	}
	pulled, err := c.svc.Log().Cursor(eventlog.CursorPulled)
	if err != nil {
		return err
	}
	hello := &syncpb.Hello{NodeId: string(c.svc.NodeID()), PulledCursor: pulled,
		VersionVector: map[string]uint64{}}
	for id, seq := range vector {
		hello.VersionVector[string(id)] = seq
	}
	if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Hello{Hello: hello}}); err != nil {
		return err
	}
	welcome, err := stream.Recv()
	if err != nil {
		return err
	}
	if welcome.GetWelcome() == nil {
		return errors.New("central did not answer Hello with Welcome")
	}

	// Resume from whichever is lower: what we think we pushed, or what central admits
	// to holding. Central's answer wins, because re-sending is free and idempotent
	// while a gap is not.
	from, err := c.svc.Log().Cursor(eventlog.CursorPushed)
	if err != nil {
		return err
	}
	if known := welcome.GetWelcome().GetKnownSeq(); known < from {
		from = known
	}
	if err := c.pushBacklog(ctx, stream, from); err != nil {
		return err
	}
	return c.applyDownstream(stream)
}

// pushBacklog sends this node's own events above from, in bounded chunks, updating the
// push cursor from each acknowledgement. A week of backlog is many chunks, never one
// enormous message.
func (c *Client) pushBacklog(ctx context.Context, stream syncpb.Sync_ReplicateClient, from uint64) error {
	for {
		envs, err := c.svc.Log().ReadOwnAfter(from, MaxBatchEvents)
		if err != nil {
			return err
		}
		batch := EncodeBatch(envs)
		if len(envs) > 0 {
			from = envs[len(envs)-1].ID.Seq
			next, err := c.svc.Log().ReadOwnAfter(from, 1)
			if err != nil {
				return err
			}
			batch.More = len(next) > 0
		}
		if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Events{Events: batch}}); err != nil {
			return err
		}
		ack, err := stream.Recv()
		if err != nil {
			return err
		}
		if ack.GetAck() == nil {
			return errors.New("central did not acknowledge the batch")
		}
		if seq := ack.GetAck().GetSeq(); seq > 0 {
			if err := c.svc.Log().SetCursor(eventlog.CursorPushed, seq); err != nil {
				return err
			}
		}
		if !batch.GetMore() {
			return ctx.Err()
		}
	}
}

// applyDownstream ingests central's batches. An undecodable event fails the session
// and leaves the cursor where it was, so it is retried: skipping an event from the
// authority silently forks this node's state, which is the one outcome that must never
// happen.
func (c *Client) applyDownstream(stream syncpb.Sync_ReplicateClient) error {
	var highest uint64
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		batch := frame.GetEvents()
		if batch == nil {
			return errors.New("central sent a frame that was not an EventBatch")
		}
		envs, err := DecodeBatch(batch)
		if err != nil {
			return err
		}
		if len(envs) > 0 {
			if _, err := c.svc.Ingest(envs); err != nil {
				return err
			}
		}
		if batch.GetLastOrd() > highest {
			highest = batch.GetLastOrd()
		}
		if batch.GetMore() {
			continue
		}
		if highest > 0 {
			if err := c.svc.Log().SetCursor(eventlog.CursorPulled, highest); err != nil {
				return err
			}
		}
		if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Ack{
			Ack: &syncpb.Ack{Ord: highest}}}); err != nil {
			return err
		}
		return stream.CloseSend()
	}
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/sync/ -cover -v`
Expected: PASS, coverage 100.0%.

- [ ] **Step 7: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 8: Commit**

```bash
git add warehouse-node/internal/sync
git commit -m "feat(sync): add the bidirectional replication client and server

- one session is one deterministic round: Hello, Welcome, upstream, downstream, Ack
- cursors persist on both sides, so a dead session costs a retry and nothing else
- an undecodable event fails the session and never advances the cursor
- backlog is pushed and pulled in chunks bounded by MaxBatchEvents"
```

---

### Task 19: `cmd/node` — the node server, fully offline-capable

The binary an operator's warehouse runs. It serves the operator gRPC API and, *if* a central
address is configured, runs the sync client in the background. With no central address it is
a complete, working warehouse: that is the point, and the test asserts it.

The sync client runs in its own goroutine and its failures never reach a command. A command
answered from local state cannot be made to wait on the network.

**Files:**
- Create: `warehouse-node/cmd/node/main.go`
- Test: `warehouse-node/cmd/node/main_test.go`

**Interfaces:**
- Consumes: `node.Open` (Task 12); `grpctransport.NewNodeAPI` (Task 13); `syncrepl.NewClient` (Task 18); `nodeapi.RegisterNodeAPIServer`, `syncpb.NewSyncClient` (Tasks 13-14).
- Produces:
  - `main.Config{DB string; ID domain.NodeID; Central string; SyncEvery, Backoff time.Duration; Locations map[domain.LocationCode]domain.LocationType}`
  - `func run(ctx context.Context, cfg Config, lis net.Listener) error` — serves until ctx is done or the listener closes
  - `func main()` — parses flags, builds the listener, calls `run`

- [ ] **Step 1: Write the failing test**

`warehouse-node/cmd/node/main_test.go`:

```go
package main

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

// startNode runs the binary's run function over an in-process listener and returns an
// operator client for it.
func startNode(t *testing.T, cfg Config) nodeapi.NodeAPIClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close conn: %v", err)
		}
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
	return nodeapi.NewNodeAPIClient(conn)
}

// TestNodeServesWithNoCentralConfigured is the offline-capability assertion: with no
// central address at all, every command still works.
func TestNodeServesWithNoCentralConfigured(t *testing.T) {
	client := startNode(t, Config{
		DB: filepath.Join(t.TempDir(), "node.db"),
		ID: "wh-a",
		Locations: map[domain.LocationCode]domain.LocationType{
			"RECV-01": domain.LocReceiving,
			"PICK-01": domain.LocPick,
		},
	})
	ctx := context.Background()

	// The item master normally arrives from central. With no central, an unknown SKU
	// is correctly refused — locally, immediately, naming the rule.
	_, err := client.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R1", DeliveryNote: "DN-1",
		PoRef: "PO-1", Line: &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: 1, Uom: "EA"}, To: "RECV-01"})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("Receive = %v, want FailedPrecondition: no item master has replicated yet", err)
	}

	// Queries work regardless, which is what an operator needs when the line is down.
	if _, err := client.StockOnHand(ctx, &nodeapi.StockOnHandRequest{}); err != nil {
		t.Errorf("StockOnHand: %v", err)
	}
	if _, err := client.Exceptions(ctx, &nodeapi.ExceptionsRequest{}); err != nil {
		t.Errorf("Exceptions: %v", err)
	}
	if _, err := client.Transfers(ctx, &nodeapi.TransfersRequest{}); err != nil {
		t.Errorf("Transfers: %v", err)
	}
}

func TestNodeKeepsServingWhenCentralIsUnreachable(t *testing.T) {
	client := startNode(t, Config{
		DB:        filepath.Join(t.TempDir(), "node.db"),
		ID:        "wh-a",
		Central:   "127.0.0.1:1", // nothing listens there, ever
		SyncEvery: 5 * time.Millisecond,
		Backoff:   5 * time.Millisecond,
		Locations: map[domain.LocationCode]domain.LocationType{"RECV-01": domain.LocReceiving},
	})
	// Give the sync loop several failed attempts, then assert the API is unaffected.
	time.Sleep(30 * time.Millisecond)
	if _, err := client.StockOnHand(context.Background(), &nodeapi.StockOnHandRequest{}); err != nil {
		t.Fatalf("StockOnHand while central is unreachable: %v", err)
	}
}

func TestRunRejectsBadConfiguration(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "unopenable database",
			cfg:  Config{DB: filepath.Join(t.TempDir(), "missing", "dir", "node.db"), ID: "wh-a"},
		},
		{
			name: "invalid location type",
			cfg: Config{DB: filepath.Join(t.TempDir(), "node.db"), ID: "wh-a",
				Locations: map[domain.LocationCode]domain.LocationType{"X-01": "mezzanine"}},
		},
		{
			name: "unparseable central address",
			cfg: Config{DB: filepath.Join(t.TempDir(), "node.db"), ID: "wh-a",
				Central: "\x00 not a target"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lis := bufconn.Listen(1 << 10)
			defer func() { _ = lis.Close() }()
			if err := run(context.Background(), tt.cfg, lis); err == nil {
				t.Fatal("run: expected an error")
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./cmd/node/ -v`
Expected: FAIL to compile — `undefined: Config`, `undefined: run`.

- [ ] **Step 3: Write `main.go`**

```go
// Command node runs one warehouse. It serves the operator gRPC API from a local
// SQLite event log and, when a central address is configured, replicates to central in
// the background. With no central address it is a complete working warehouse: no
// command ever waits on the network.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	syncrepl "github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/sync"
	grpctransport "github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/transport/grpc"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// Config is everything this binary needs. Central may be empty, which means fully
// offline operation.
type Config struct {
	DB        string
	ID        domain.NodeID
	Central   string
	SyncEvery time.Duration
	Backoff   time.Duration
	Locations map[domain.LocationCode]domain.LocationType
}

func main() {
	var cfg Config
	var id, locations string
	flag.StringVar(&cfg.DB, "db", "node.db", "path to this node's SQLite event log")
	flag.StringVar(&id, "id", "", "this node's identity, e.g. wh-a")
	flag.StringVar(&cfg.Central, "central", "", "central's address; empty means run offline")
	flag.DurationVar(&cfg.SyncEvery, "sync-every", 5*time.Second, "interval between sync sessions")
	flag.DurationVar(&cfg.Backoff, "backoff", 30*time.Second, "wait after a failed sync session")
	flag.StringVar(&locations, "locations", "",
		"comma-separated code:type pairs to register on startup, e.g. RECV-01:receiving,PICK-01:pick")
	listen := flag.String("listen", ":8080", "address to serve the operator API on")
	flag.Parse()

	cfg.ID = domain.NodeID(id)
	parsed, err := parseLocations(locations)
	if err != nil {
		log.Fatalf("node: %v", err)
	}
	cfg.Locations = parsed

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("node: listen on %s: %v", *listen, err)
	}
	if err := run(context.Background(), cfg, lis); err != nil {
		log.Fatalf("node: %v", err)
	}
}

// parseLocations turns the -locations flag into the map run expects.
func parseLocations(spec string) (map[domain.LocationCode]domain.LocationType, error) {
	out := map[domain.LocationCode]domain.LocationType{}
	if spec == "" {
		return out, nil
	}
	for _, pair := range strings.Split(spec, ",") {
		code, typ, ok := strings.Cut(pair, ":")
		if !ok {
			return nil, fmt.Errorf("location %q is not code:type", pair)
		}
		out[domain.LocationCode(code)] = domain.LocationType(typ)
	}
	return out, nil
}

// run opens the node, registers its locations, starts the sync client if central is
// configured, and serves the operator API until ctx is done.
func run(ctx context.Context, cfg Config, lis net.Listener) error {
	svc, err := node.Open(cfg.DB, cfg.ID, time.Now)
	if err != nil {
		return err
	}
	defer func() { _ = svc.Close() }()

	for code, typ := range cfg.Locations {
		if err := svc.RegisterLocation(code, typ); err != nil {
			return err
		}
	}

	if cfg.Central != "" {
		conn, err := grpc.NewClient(cfg.Central, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial central at %s: %w", cfg.Central, err)
		}
		defer func() { _ = conn.Close() }()
		client := syncrepl.NewClient(svc, syncpb.NewSyncClient(conn))
		// The sync loop is deliberately fire-and-forget. Central being unreachable is
		// a normal state for a warehouse, not an error an operator should ever see.
		go func() { _ = client.Run(ctx, cfg.SyncEvery, cfg.Backoff) }()
	}

	srv := grpc.NewServer()
	nodeapi.RegisterNodeAPIServer(srv, grpctransport.NewNodeAPI(svc, time.Now))
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve operator api: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./cmd/node/ -cover -v`
Expected: PASS, coverage 100.0%. `main` itself is covered by a table test over
`parseLocations`; add it if coverage reports the function short:

```go
func TestParseLocations(t *testing.T) {
	tests := []struct {
		name    string
		spec    string
		want    map[domain.LocationCode]domain.LocationType
		wantErr bool
	}{
		{name: "empty", spec: "", want: map[domain.LocationCode]domain.LocationType{}},
		{name: "one pair", spec: "RECV-01:receiving",
			want: map[domain.LocationCode]domain.LocationType{"RECV-01": domain.LocReceiving}},
		{name: "two pairs", spec: "RECV-01:receiving,PICK-01:pick",
			want: map[domain.LocationCode]domain.LocationType{
				"RECV-01": domain.LocReceiving, "PICK-01": domain.LocPick}},
		{name: "missing colon", spec: "RECV-01", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLocations(tt.spec)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
			for code, typ := range tt.want {
				if got[code] != typ {
					t.Errorf("got[%s] = %q, want %q", code, got[code], typ)
				}
			}
		})
	}
}
```

- [ ] **Step 5: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/cmd/node
git commit -m "feat(cmd/node): add the node server binary

- serves the operator API from local state; no command waits on central
- with no -central address it is a complete offline warehouse
- the sync loop is fire-and-forget: an unreachable central is a normal state"
```

---

### Task 20: `cmd/central` — the central server on Postgres

The other binary. It serves the sync stream over the Postgres store, bootstraps the
reference data only central owns (the item master, purchase orders, which nodes exist and
what they refuse to stock), replicates the item master down to every node, and periodically
reports transfers that have been in transit too long.

That last loop is a **report**, never a correction. An unmatched dispatch past the window
means a truck is late or lost, and inventing stock back at the source would be a lie. The
spec is explicit: reported as a discrepancy, resolved by a human.

**Files:**
- Create: `warehouse-node/cmd/central/main.go`
- Test: `warehouse-node/cmd/central/main_test.go`

**Interfaces:**
- Consumes: `central.OpenPostgres`, `central.Store`, `central.CentralNode` (Tasks 15-16); `arbiter.New`, `arbiter.Discrepancies` (Task 17); `syncrepl.NewServer` (Task 18); `syncpb.RegisterSyncServer` (Task 14).
- Produces:
  - `main.Bootstrap{Items []domain.Item; Orders []Order; Nodes []NodeSpec}` and `main.Order{PORef, SKU string; Qty float64}`, `main.NodeSpec{ID domain.NodeID; Rejects []string}`
  - `func loadBootstrap(path string) (Bootstrap, error)` — reads the JSON configuration
  - `func applyBootstrap(ctx context.Context, s central.Store, b Bootstrap, now time.Time) error` — upserts reference data and queues one `ItemUpserted` per item to every listed node
  - `func reportDiscrepancies(ctx context.Context, s central.Store, window time.Duration, now func() time.Time) ([]central.InTransitRow, error)`
  - `func run(ctx context.Context, s central.Store, b Bootstrap, window, every time.Duration, now func() time.Time, lis net.Listener) error`
  - `func main()`

- [ ] **Step 1: Write the failing test**

`warehouse-node/cmd/central/main_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

var at = time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)

func testBootstrap() Bootstrap {
	return Bootstrap{
		Items: []domain.Item{{SKU: "WIDGET", Description: "Blue widget", BaseUoM: "EA",
			AltUoM: map[domain.UoM]float64{"CASE": 12}, LotTracked: true, ShelfLifeDays: 30}},
		Orders: []Order{{PORef: "PO-1", SKU: "WIDGET", Qty: 100}},
		Nodes:  []NodeSpec{{ID: "wh-a"}, {ID: "wh-b", Rejects: []string{"HAZMAT"}}},
	}
}

func TestLoadBootstrap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap.json")
	encoded, err := json.Marshal(testBootstrap())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := loadBootstrap(path)
	if err != nil {
		t.Fatalf("loadBootstrap: %v", err)
	}
	if len(got.Items) != 1 || got.Items[0].SKU != "WIDGET" || len(got.Orders) != 1 || len(got.Nodes) != 2 {
		t.Fatalf("loadBootstrap = %+v, want the file's contents", got)
	}

	if _, err := loadBootstrap(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Error("loadBootstrap on a missing file: expected an error")
	}
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := loadBootstrap(bad); err == nil {
		t.Error("loadBootstrap on malformed json: expected an error")
	}
	if got, err := loadBootstrap(""); err != nil || len(got.Items) != 0 {
		t.Errorf("loadBootstrap(\"\") = %+v, %v; want an empty bootstrap and no error", got, err)
	}
}

func TestApplyBootstrapSeedsReferenceDataAndQueuesTheItemMaster(t *testing.T) {
	store := central.NewMemory()
	ctx := context.Background()
	if err := applyBootstrap(ctx, store, testBootstrap(), at); err != nil {
		t.Fatalf("applyBootstrap: %v", err)
	}

	item, ok, err := store.Item(ctx, "WIDGET")
	if err != nil || !ok || item.AltUoM["CASE"] != 12 {
		t.Fatalf("Item = %+v, ok %v, err %v; want the seeded widget", item, ok, err)
	}
	if qty, ok, err := store.PurchaseOrder(ctx, "PO-1", "WIDGET"); err != nil || !ok || qty != 100 {
		t.Fatalf("PurchaseOrder = %v, ok %v, err %v; want 100", qty, ok, err)
	}
	rejects, known, err := store.NodeConfig(ctx, "wh-b")
	if err != nil || !known || !rejects["HAZMAT"] {
		t.Fatalf("NodeConfig(wh-b) = %+v, known %v, err %v; want HAZMAT refused", rejects, known, err)
	}
	for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
		queued, err := store.Outbound(ctx, id, 0, 10)
		if err != nil {
			t.Fatalf("Outbound(%s): %v", id, err)
		}
		if len(queued) != 1 || queued[0].Env.Type != domain.TypeItemUpserted {
			t.Errorf("queued for %s = %+v, want one ItemUpserted", id, queued)
		}
		if queued[0].Env.ID.NodeID != central.CentralNode {
			t.Errorf("item master event came from %q, want central", queued[0].Env.ID.NodeID)
		}
	}
}

func TestReportDiscrepanciesReportsAndDoesNotCompensate(t *testing.T) {
	store := central.NewMemory()
	ctx := context.Background()
	key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
	if err := store.RecordDispatch(ctx, central.InTransitRow{TransferID: "T1", Key: key,
		FromNode: "wh-a", ToNode: "wh-b", Dispatched: 6, DispatchedAt: at}); err != nil {
		t.Fatalf("RecordDispatch: %v", err)
	}

	tests := []struct {
		name   string
		window time.Duration
		now    time.Time
		want   int
	}{
		{name: "inside the window nothing is reported", window: 48 * time.Hour, now: at.Add(time.Hour)},
		{name: "past the window it is reported", window: 24 * time.Hour, now: at.Add(72 * time.Hour), want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := reportDiscrepancies(ctx, store, tt.window, func() time.Time { return tt.now })
			if err != nil {
				t.Fatalf("reportDiscrepancies: %v", err)
			}
			if len(rows) != tt.want {
				t.Fatalf("reported %d rows, want %d: %+v", len(rows), tt.want, rows)
			}
		})
	}

	events, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("reporting emitted %d events; a lost truck is never auto-compensated", len(events))
	}
}

func TestRunServesTheSyncStream(t *testing.T) {
	store := central.NewMemory()
	lis := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, store, testBootstrap(), 24*time.Hour, 5*time.Millisecond,
			func() time.Time { return at }, lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	stream, err := syncpb.NewSyncClient(conn).Replicate(context.Background())
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Hello{
		Hello: &syncpb.Hello{NodeId: "wh-a"}}}); err != nil {
		t.Fatalf("Send hello: %v", err)
	}
	frame, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if frame.GetWelcome() == nil {
		t.Fatalf("frame = %+v, want a Welcome", frame)
	}

	if err := conn.Close(); err != nil {
		t.Errorf("close conn: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd warehouse-node && go test ./cmd/central/ -v`
Expected: FAIL to compile — `undefined: Bootstrap`, `undefined: loadBootstrap`.

- [ ] **Step 3: Write `main.go`**

```go
// Command central runs the arbitration server. It serves the replication stream over
// Postgres, seeds the reference data only central owns, replicates the item master
// down to every node, and reports transfers stuck in transit.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/arbiter"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	syncrepl "github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/sync"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// Order is one purchase-order line: how much of a SKU was ordered. Purchase-order
// lifecycle is out of scope, so an order is only ever this stub.
type Order struct {
	PORef string  `json:"po_ref"`
	SKU   string  `json:"sku"`
	Qty   float64 `json:"qty"`
}

// NodeSpec declares a warehouse and the SKUs it refuses to stock. Both are things no
// node has authority over, which is why a transfer's destination is central's call.
type NodeSpec struct {
	ID      domain.NodeID `json:"id"`
	Rejects []string      `json:"rejects,omitempty"`
}

// Bootstrap is the reference data central is configured with.
type Bootstrap struct {
	Items  []domain.Item `json:"items"`
	Orders []Order       `json:"orders"`
	Nodes  []NodeSpec    `json:"nodes"`
}

func main() {
	dsn := flag.String("dsn", "", "Postgres connection string")
	listen := flag.String("listen", ":9090", "address to serve the sync stream on")
	bootstrap := flag.String("bootstrap", "", "path to a JSON file of items, orders and nodes")
	window := flag.Duration("transit-window", 48*time.Hour,
		"how long a transfer may be in transit before it is reported as a discrepancy")
	every := flag.Duration("report-every", time.Hour, "interval between discrepancy reports")
	flag.Parse()

	ctx := context.Background()
	b, err := loadBootstrap(*bootstrap)
	if err != nil {
		log.Fatalf("central: %v", err)
	}
	store, err := central.OpenPostgres(ctx, *dsn)
	if err != nil {
		log.Fatalf("central: %v", err)
	}
	defer func() { _ = store.Close() }()

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("central: listen on %s: %v", *listen, err)
	}
	if err := run(ctx, store, b, *window, *every, time.Now, lis); err != nil {
		log.Fatalf("central: %v", err)
	}
}

// loadBootstrap reads the configuration file. An empty path means no reference data,
// which is valid: a fresh central can be seeded later.
func loadBootstrap(path string) (Bootstrap, error) {
	if path == "" {
		return Bootstrap{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Bootstrap{}, fmt.Errorf("read bootstrap %s: %w", path, err)
	}
	var b Bootstrap
	if err := json.Unmarshal(raw, &b); err != nil {
		return Bootstrap{}, fmt.Errorf("parse bootstrap %s: %w", path, err)
	}
	return b, nil
}

// applyBootstrap upserts the reference data and queues one ItemUpserted per item to
// every declared node. That queue is how the item master gets replicated read-only
// down to the warehouses: nodes never emit Item events, they only apply them.
func applyBootstrap(ctx context.Context, s central.Store, b Bootstrap, now time.Time) error {
	for _, spec := range b.Nodes {
		if err := s.RegisterNode(ctx, spec.ID, spec.Rejects); err != nil {
			return err
		}
	}
	for _, order := range b.Orders {
		if err := s.UpsertPurchaseOrder(ctx, order.PORef, order.SKU, order.Qty); err != nil {
			return err
		}
	}
	for _, item := range b.Items {
		if err := s.UpsertItem(ctx, item); err != nil {
			return err
		}
		envs, err := s.EmitCentral(ctx, []domain.Event{{Type: domain.TypeItemUpserted,
			AggregateID: item.SKU, Payload: domain.ItemUpserted{Item: item}}}, nil, now)
		if err != nil {
			return err
		}
		for _, spec := range b.Nodes {
			if err := s.Enqueue(ctx, spec.ID, envs); err != nil {
				return err
			}
		}
	}
	return nil
}

// reportDiscrepancies returns transfers dispatched longer than window ago that are
// still carrying stock. It only ever reports: compensating a late truck would invent
// stock at the source that may be sitting in a lay-by. A human resolves it.
func reportDiscrepancies(ctx context.Context, s central.Store, window time.Duration,
	now func() time.Time) ([]central.InTransitRow, error) {
	return arbiter.Discrepancies(ctx, s, now().Add(-window))
}

// run seeds the store, starts the discrepancy report loop, and serves the sync stream
// until ctx is done.
func run(ctx context.Context, s central.Store, b Bootstrap, window, every time.Duration,
	now func() time.Time, lis net.Listener) error {
	if err := applyBootstrap(ctx, s, b, now()); err != nil {
		return err
	}

	go reportLoop(ctx, s, window, every, now)

	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, syncrepl.NewServer(s, arbiter.New(s, now)))
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve sync: %w", err)
	}
	return nil
}

// reportLoop logs stuck transfers on an interval. It is deliberately toothless.
func reportLoop(ctx context.Context, s central.Store, window, every time.Duration, now func() time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		rows, err := reportDiscrepancies(ctx, s, window, now)
		if err != nil {
			log.Printf("central: discrepancy report failed: %v", err)
			continue
		}
		for _, row := range rows {
			log.Printf("central: transfer %s from %s to %s has %v of %s in transit since %s",
				row.TransferID, row.FromNode, row.ToNode,
				row.Dispatched-row.Received, row.Key.SKU, row.DispatchedAt)
		}
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd warehouse-node && go test ./cmd/central/ -cover -v`
Expected: PASS, coverage 100.0%. `reportLoop`'s body is covered by
`TestRunServesTheSyncStream`, whose `every` of 5ms fires it at least once before the
context is cancelled; if coverage reports it short, add a direct test:

```go
func TestReportLoopStopsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	store := central.NewMemory()
	if err := store.RecordDispatch(ctx, central.InTransitRow{TransferID: "T1",
		Key: domain.StockKey{SKU: "WIDGET"}, FromNode: "wh-a", ToNode: "wh-b",
		Dispatched: 1, DispatchedAt: at}); err != nil {
		t.Fatalf("RecordDispatch: %v", err)
	}
	done := make(chan struct{})
	go func() {
		reportLoop(ctx, store, time.Hour, time.Millisecond,
			func() time.Time { return at.Add(72 * time.Hour) })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reportLoop did not stop with its context")
	}
}
```

- [ ] **Step 5: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/cmd/central
git commit -m "feat(cmd/central): add the central arbitration server

- serves the sync stream over the Postgres store
- bootstraps items, purchase orders and node configuration from JSON, and queues
  the item master down to every declared node
- reports transfers stuck in transit past a configurable window, and only reports:
  a lost truck is never auto-compensated"
```

---

### Task 21: The integration harness and transfers end to end

Two nodes plus central, all in one process, over a real gRPC transport on an in-process
listener. Everything from here on is assembled from the pieces already built; nothing new is
implemented except the harness itself.

This task proves the two-phase transfer, which is the most interesting flow in the system
because the goods spend real time belonging to neither warehouse. The **in-transit** balance
at central is that ownership gap made explicit, and the assertion that matters is a
sandwich: in-transit is zero before the dispatch, non-zero between dispatch and receipt, and
zero again after. If it is ever non-zero outside that window, stock has been invented or
lost.

It also proves the two failure modes: a destination that refuses the item, where the source's
stock is restored and the transfer marked failed; and a truck that simply never arrives,
which is **reported** and never compensated.

**Files:**
- Create: `warehouse-node/internal/integration/harness_test.go`
- Create: `warehouse-node/internal/integration/transfer_test.go`

**Interfaces:**
- Consumes: `node.Open`, `node.Command` (Task 12); `grpctransport.NewNodeAPI` (Task 13); `central.NewMemory`, `central.Store`, `central.InTransitRow` (Task 15); `arbiter.New`, `arbiter.Discrepancies` (Task 17); `syncrepl.NewServer`, `syncrepl.NewClient`, `(*syncrepl.Client).Session` (Task 18); every `nodeapi` message (Task 13).
- Produces (test-only, used by Task 22):
  - `integration.cluster` with fields `t`, `store`, `arb`, `now`, and methods
    `node(id domain.NodeID) *warehouse`, `syncNode(id domain.NodeID)`, `syncAll()`,
    `inTransit(transferID string, k domain.StockKey) float64`
  - `integration.warehouse` with fields `svc *node.Service`, `api *grpctransport.NodeAPI`,
    `client *syncrepl.Client`, and method `balance(sku string, loc domain.LocationCode) float64`
  - `func newCluster(t *testing.T, ordered float64, rejects map[domain.NodeID][]string) *cluster`
  - `func replayFresh(t *testing.T, w *warehouse) *projection.Set`

- [ ] **Step 1: Write the harness**

`warehouse-node/internal/integration/harness_test.go`:

```go
// Package integration exercises two warehouse nodes and central together in one
// process. The transport is real gRPC over an in-process listener, so the wire
// format, the streaming protocol and the arbitration chain are all under test; only
// the network is simulated.
package integration

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/arbiter"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/eventlog"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/projection"
	syncrepl "github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/sync"
	grpctransport "github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/transport/grpc"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// at is the fixed instant every clock in the cluster reads from, so HLC readings and
// recorded timestamps are deterministic and replays are comparable.
var at = time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)

func widget() domain.Item {
	return domain.Item{SKU: "WIDGET", Description: "Blue widget", BaseUoM: "EA",
		AltUoM: map[domain.UoM]float64{"CASE": 12}, LotTracked: true, ShelfLifeDays: 3650}
}

// warehouse is one node: its service, its operator API and its sync client.
type warehouse struct {
	t      *testing.T
	svc    *node.Service
	api    *grpctransport.NodeAPI
	client *syncrepl.Client
}

// balance is the on-hand quantity at one SKU and location, summed across lots.
func (w *warehouse) balance(sku string, loc domain.LocationCode) float64 {
	w.t.Helper()
	rows, err := w.svc.StockOnHand(sku, loc)
	if err != nil {
		w.t.Fatalf("StockOnHand: %v", err)
	}
	var total float64
	for _, r := range rows {
		total += r.Qty
	}
	return total
}

// cluster is two warehouses plus central.
type cluster struct {
	t     *testing.T
	store central.Store
	arb   *arbiter.Arbiter
	now   func() time.Time
	nodes map[domain.NodeID]*warehouse
}

// newCluster builds central with a purchase order for `ordered` widgets, the two
// warehouses wh-a and wh-b, and whatever per-node item refusals the scenario needs.
func newCluster(t *testing.T, ordered float64, rejects map[domain.NodeID][]string) *cluster {
	t.Helper()
	ctx := context.Background()
	now := func() time.Time { return at }
	store := central.NewMemory()
	c := &cluster{t: t, store: store, arb: arbiter.New(store, now), now: now,
		nodes: map[domain.NodeID]*warehouse{}}

	if err := store.UpsertItem(ctx, widget()); err != nil {
		t.Fatalf("UpsertItem: %v", err)
	}
	if err := store.UpsertPurchaseOrder(ctx, "PO-1", "WIDGET", ordered); err != nil {
		t.Fatalf("UpsertPurchaseOrder: %v", err)
	}

	stub := c.serve()
	for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
		if err := store.RegisterNode(ctx, id, rejects[id]); err != nil {
			t.Fatalf("RegisterNode(%s): %v", id, err)
		}
		c.nodes[id] = c.startWarehouse(id, stub)
	}
	return c
}

// serve starts central's sync server on a bufconn listener and returns a client stub.
func (c *cluster) serve() syncpb.SyncClient {
	c.t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, syncrepl.NewServer(c.store, c.arb))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///central",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.t.Fatalf("grpc.NewClient: %v", err)
	}
	c.t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			c.t.Errorf("close conn: %v", err)
		}
		srv.Stop()
	})
	return syncpb.NewSyncClient(conn)
}

// startWarehouse opens one node with the standard locations and no item master; the
// master arrives from central on the first sync, exactly as in deployment.
func (c *cluster) startWarehouse(id domain.NodeID, stub syncpb.SyncClient) *warehouse {
	c.t.Helper()
	n := 0
	svc, err := node.Open(filepath.Join(c.t.TempDir(), string(id)+".db"), id, func() time.Time {
		n++
		return at.Add(time.Duration(n) * time.Second)
	})
	if err != nil {
		c.t.Fatalf("node.Open(%s): %v", id, err)
	}
	c.t.Cleanup(func() { _ = svc.Close() })

	for code, typ := range map[domain.LocationCode]domain.LocationType{
		"RECV-" + string(id): domain.LocReceiving,
		"PICK-" + string(id): domain.LocPick,
	} {
		if err := svc.RegisterLocation(code, typ); err != nil {
			c.t.Fatalf("RegisterLocation(%s): %v", code, err)
		}
	}
	// Replicate the item master down. In deployment central queues this from its
	// bootstrap; here the same events are pushed straight into the node's log.
	envs, err := c.store.EmitCentral(context.Background(), []domain.Event{
		{Type: domain.TypeItemUpserted, AggregateID: "WIDGET", Payload: domain.ItemUpserted{Item: widget()}},
	}, nil, at)
	if err != nil {
		c.t.Fatalf("EmitCentral: %v", err)
	}
	if _, err := svc.Ingest(envs); err != nil {
		c.t.Fatalf("Ingest item master: %v", err)
	}

	return &warehouse{t: c.t, svc: svc, api: grpctransport.NewNodeAPI(svc, c.now),
		client: syncrepl.NewClient(svc, stub)}
}

// node returns one warehouse by identity.
func (c *cluster) node(id domain.NodeID) *warehouse {
	c.t.Helper()
	w, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("no node %q in the cluster", id)
	}
	return w
}

// syncNode runs one full sync session for one node.
func (c *cluster) syncNode(id domain.NodeID) {
	c.t.Helper()
	if err := c.node(id).client.Session(context.Background()); err != nil {
		c.t.Fatalf("sync %s: %v", id, err)
	}
}

// syncAll runs a session for every node, twice, so an event one node pushed up and
// central forwarded to the other has actually landed there.
func (c *cluster) syncAll() {
	c.t.Helper()
	for i := 0; i < 2; i++ {
		for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
			c.syncNode(id)
		}
	}
}

// inTransit is the live in-transit quantity central holds for one transfer line:
// dispatched minus received. Zero when the goods are at rest in a warehouse.
func (c *cluster) inTransit(transferID string, k domain.StockKey) float64 {
	c.t.Helper()
	row, ok, err := c.store.InTransit(context.Background(), transferID, k)
	if err != nil {
		c.t.Fatalf("InTransit: %v", err)
	}
	if !ok {
		return 0
	}
	return row.Dispatched - row.Received
}

// replayFresh rebuilds a node's projections from scratch by replaying its whole log
// into an empty database. This is the determinism check: the same log must always
// produce the same read models, which is what licenses the rebuild-on-version-bump
// mechanism.
func replayFresh(t *testing.T, w *warehouse) *projection.Set {
	t.Helper()
	envs, err := w.svc.Log().ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	fresh, err := eventlog.Open(filepath.Join(t.TempDir(), "replay.db"), w.svc.NodeID(),
		func() time.Time { return at })
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	t.Cleanup(func() { _ = fresh.Close() })
	if _, err := fresh.Ingest(envs); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	set, err := projection.Open(fresh)
	if err != nil {
		t.Fatalf("projection.Open: %v", err)
	}
	return set
}

// assertDeterministic replays a node's log onto empty projections and asserts every
// read model comes out identical.
func assertDeterministic(t *testing.T, w *warehouse) {
	t.Helper()
	replayed := replayFresh(t, w)

	liveStock, err := w.svc.StockOnHand("", "")
	if err != nil {
		t.Fatalf("live StockOnHand: %v", err)
	}
	replayedStock, err := replayed.StockOnHand("", "")
	if err != nil {
		t.Fatalf("replayed StockOnHand: %v", err)
	}
	if len(liveStock) != len(replayedStock) {
		t.Fatalf("replay produced %d stock rows, want %d", len(replayedStock), len(liveStock))
	}
	for i := range liveStock {
		if liveStock[i] != replayedStock[i] {
			t.Errorf("stock row %d: replay gave %+v, want %+v", i, replayedStock[i], liveStock[i])
		}
	}

	liveRes, err := w.svc.Reservations()
	if err != nil {
		t.Fatalf("live Reservations: %v", err)
	}
	replayedRes, err := replayed.Reservations()
	if err != nil {
		t.Fatalf("replayed Reservations: %v", err)
	}
	if len(liveRes) != len(replayedRes) {
		t.Fatalf("replay produced %d reservations, want %d", len(replayedRes), len(liveRes))
	}
	for i := range liveRes {
		if liveRes[i] != replayedRes[i] {
			t.Errorf("reservation %d: replay gave %+v, want %+v", i, replayedRes[i], liveRes[i])
		}
	}

	liveExc, err := w.svc.Exceptions()
	if err != nil {
		t.Fatalf("live Exceptions: %v", err)
	}
	replayedExc, err := replayed.Exceptions()
	if err != nil {
		t.Fatalf("replayed Exceptions: %v", err)
	}
	if len(liveExc) != len(replayedExc) {
		t.Fatalf("replay produced %d exceptions, want %d", len(replayedExc), len(liveExc))
	}
	for i := range liveExc {
		if liveExc[i] != replayedExc[i] {
			t.Errorf("exception %d: replay gave %+v, want %+v", i, replayedExc[i], liveExc[i])
		}
	}

	liveTr, err := w.svc.Transfers()
	if err != nil {
		t.Fatalf("live Transfers: %v", err)
	}
	replayedTr, err := replayed.Transfers()
	if err != nil {
		t.Fatalf("replayed Transfers: %v", err)
	}
	if len(liveTr) != len(replayedTr) {
		t.Fatalf("replay produced %d transfers, want %d", len(replayedTr), len(liveTr))
	}
	for i := range liveTr {
		if liveTr[i] != replayedTr[i] {
			t.Errorf("transfer %d: replay gave %+v, want %+v", i, replayedTr[i], liveTr[i])
		}
	}
}
```

- [ ] **Step 2: Write the failing transfer test**

`warehouse-node/internal/integration/transfer_test.go`:

```go
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/arbiter"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

func line(qty float64) *nodeapi.Line {
	return &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: qty, Uom: "EA"}
}

// stockAt receives qty widgets at a node's dock and puts them on its pick face, so a
// transfer has something to dispatch.
func stockAt(t *testing.T, c *cluster, id domain.NodeID, receipt, note string, qty float64) {
	t.Helper()
	ctx := context.Background()
	api := c.node(id).api
	if _, err := api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: receipt, DeliveryNote: note,
		PoRef: "PO-1", Line: line(qty), To: "RECV-" + string(id)}); err != nil {
		t.Fatalf("Receive at %s: %v", id, err)
	}
	if _, err := api.PutAway(ctx, &nodeapi.PutAwayRequest{Line: line(qty),
		From: "RECV-" + string(id), To: "PICK-" + string(id)}); err != nil {
		t.Fatalf("PutAway at %s: %v", id, err)
	}
}

// TestTransferHappyPath is the in-transit sandwich: zero, then six, then zero.
func TestTransferHappyPath(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()
	key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
	source, dest := c.node("wh-a"), c.node("wh-b")

	stockAt(t, c, "wh-a", "R1", "DN-1", 10)
	c.syncAll()
	if got := c.inTransit("T1", key); got != 0 {
		t.Fatalf("in transit before the dispatch = %v, want 0", got)
	}

	if _, err := source.api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
		ToNode: "wh-b", From: "PICK-wh-a", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("DispatchTransfer: %v", err)
	}
	// The source's stock left the moment the truck did, before central knows anything.
	if got := source.balance("WIDGET", "PICK-wh-a"); got != 4 {
		t.Fatalf("source pick face = %v, want 4 immediately after dispatch", got)
	}
	c.syncAll()

	if got := c.inTransit("T1", key); got != 6 {
		t.Fatalf("in transit between dispatch and receipt = %v, want 6", got)
	}
	if got := dest.balance("WIDGET", "RECV-wh-b"); got != 0 {
		t.Fatalf("destination dock = %v, want 0: the truck has not arrived", got)
	}

	if _, err := dest.api.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{TransferId: "T1",
		To: "RECV-wh-b", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("ReceiveTransfer: %v", err)
	}
	c.syncAll()

	if got := c.inTransit("T1", key); got != 0 {
		t.Fatalf("in transit after the receipt = %v, want 0", got)
	}
	if got := dest.balance("WIDGET", "RECV-wh-b"); got != 6 {
		t.Fatalf("destination dock = %v, want 6", got)
	}
	if got := source.balance("WIDGET", "PICK-wh-a"); got != 4 {
		t.Fatalf("source pick face = %v, want 4", got)
	}

	// Both nodes' transfer views close, and nobody was compensated.
	for _, w := range []*warehouse{source, dest} {
		rows, err := w.svc.Transfers()
		if err != nil {
			t.Fatalf("Transfers: %v", err)
		}
		if len(rows) != 1 || rows[0].Status != domain.TransferComplete {
			t.Errorf("%s transfers = %+v, want one complete row", w.svc.NodeID(), rows)
		}
		exceptions, err := w.svc.Exceptions()
		if err != nil {
			t.Fatalf("Exceptions: %v", err)
		}
		if len(exceptions) != 0 {
			t.Errorf("%s exceptions = %+v, want none", w.svc.NodeID(), exceptions)
		}
		assertDeterministic(t, w)
	}
}

// TestTransferToARefusingDestinationRestoresSourceStock covers the invariant no node
// can check: the destination's configuration is not the source's business.
func TestTransferToARefusingDestinationRestoresSourceStock(t *testing.T) {
	c := newCluster(t, 1000, map[domain.NodeID][]string{"wh-b": {"WIDGET"}})
	ctx := context.Background()
	source := c.node("wh-a")

	stockAt(t, c, "wh-a", "R1", "DN-1", 10)
	c.syncAll()

	if _, err := source.api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
		ToNode: "wh-b", From: "PICK-wh-a", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("DispatchTransfer: %v", err)
	}
	if got := source.balance("WIDGET", "PICK-wh-a"); got != 4 {
		t.Fatalf("source pick face = %v, want 4 before central weighs in", got)
	}
	c.syncAll()

	if got := source.balance("WIDGET", "PICK-wh-a"); got != 10 {
		t.Fatalf("source pick face = %v, want 10: the compensation restores the stock", got)
	}
	rows, err := source.svc.Transfers()
	if err != nil {
		t.Fatalf("Transfers: %v", err)
	}
	if len(rows) != 1 || rows[0].Status != domain.TransferFailed {
		t.Fatalf("transfers = %+v, want one failed row", rows)
	}
	exceptions, err := source.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 || exceptions[0].Reason != domain.ReasonTransferRejected {
		t.Fatalf("exceptions = %+v, want one transfer_rejected row", exceptions)
	}
	// The destination never learns of a transfer that was refused.
	destRows, err := c.node("wh-b").svc.Transfers()
	if err != nil {
		t.Fatalf("destination Transfers: %v", err)
	}
	if len(destRows) != 0 {
		t.Errorf("destination transfers = %+v, want none", destRows)
	}
	assertDeterministic(t, source)
}

func TestTransferOverReceiptIsCompensatedAtTheDestination(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()
	source, dest := c.node("wh-a"), c.node("wh-b")

	stockAt(t, c, "wh-a", "R1", "DN-1", 10)
	c.syncAll()
	if _, err := source.api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
		ToNode: "wh-b", From: "PICK-wh-a", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("DispatchTransfer: %v", err)
	}
	c.syncAll()

	// The operator books ten off a truck that carried six. Only central can see that.
	if _, err := dest.api.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{TransferId: "T1",
		To: "RECV-wh-b", Lines: []*nodeapi.Line{line(10)}}); err != nil {
		t.Fatalf("ReceiveTransfer: %v", err)
	}
	if got := dest.balance("WIDGET", "RECV-wh-b"); got != 10 {
		t.Fatalf("destination dock = %v, want the operator's 10 before arbitration", got)
	}
	c.syncAll()

	if got := dest.balance("WIDGET", "RECV-wh-b"); got != 6 {
		t.Fatalf("destination dock = %v, want 6 after the over-receipt is compensated", got)
	}
	if got := c.inTransit("T1", domain.StockKey{SKU: "WIDGET", LotID: "L1"}); got != 0 {
		t.Errorf("in transit = %v, want 0", got)
	}
	exceptions, err := dest.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 || exceptions[0].Reason != domain.ReasonTransferOverReceipt {
		t.Fatalf("exceptions = %+v, want one transfer_overreceipt row", exceptions)
	}
	assertDeterministic(t, dest)
}

// TestUnmatchedDispatchIsReportedNotCompensated is the lost-truck case. The window is
// configurable; past it, central reports and does nothing else.
func TestUnmatchedDispatchIsReportedNotCompensated(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()
	source := c.node("wh-a")

	stockAt(t, c, "wh-a", "R1", "DN-1", 10)
	c.syncAll()
	if _, err := source.api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
		ToNode: "wh-b", From: "PICK-wh-a", Lines: []*nodeapi.Line{line(6)}}); err != nil {
		t.Fatalf("DispatchTransfer: %v", err)
	}
	c.syncAll()

	tests := []struct {
		name   string
		window time.Duration
		want   int
	}{
		{name: "inside a 48h window nothing is reported", window: 48 * time.Hour},
		{name: "past a 1h window the transfer is reported", window: time.Hour, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := arbiter.Discrepancies(ctx, c.store, at.Add(24*time.Hour).Add(-tt.window))
			if err != nil {
				t.Fatalf("Discrepancies: %v", err)
			}
			if len(rows) != tt.want {
				t.Fatalf("reported %d rows, want %d: %+v", len(rows), tt.want, rows)
			}
		})
	}

	c.syncAll()
	if got := source.balance("WIDGET", "PICK-wh-a"); got != 4 {
		t.Errorf("source pick face = %v, want 4: a late truck must not restore stock", got)
	}
	exceptions, err := source.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 0 {
		t.Errorf("exceptions = %+v, want none: a lost truck is reported, never compensated", exceptions)
	}
	if got := c.inTransit("T1", domain.StockKey{SKU: "WIDGET", LotID: "L1"}); got != 6 {
		t.Errorf("in transit = %v, want it still 6: the goods are somewhere", got)
	}
	assertDeterministic(t, source)
}
```

- [ ] **Step 3: Run the tests to verify they fail, then pass**

Run: `cd warehouse-node && go test ./internal/integration/ -v`
Expected: first FAIL (the harness and the tests are new; any mismatch here is a real
defect in an earlier task, so fix it there, not by weakening the assertion), then PASS
once the harness compiles and the flows behave.

- [ ] **Step 4: Run the full suite and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean.

- [ ] **Step 5: Commit**

```bash
git add warehouse-node/internal/integration
git commit -m "test(integration): add the two-node harness and transfers end to end

- real gRPC over an in-process listener, two nodes plus central
- in-transit is asserted zero before dispatch, non-zero in the gap, zero after
- a refusing destination restores the source's stock and fails the transfer
- an over-receipt is compensated at the destination
- an unmatched dispatch past the window is reported and nothing else"
```

---

### Task 22: The remaining integration scenarios and the determinism check

The rest of the spec's scenario list, on the harness from Task 21, plus the check that ties
the whole design together: replay every scenario's final log onto empty projections and
assert the state comes out identical. If that ever fails, the rebuild-on-version-bump
mechanism is unsafe and so is every sync retry.

The last scenario is the spec's accepted limitation, and it is the most important test in the
file. A node receives goods, picks and ships them while offline, and only then learns central
rejected the receipt. The compensation removes stock that is physically gone, so the location
goes **negative**. Compensation is deliberately not cascaded into the pick — that way lies
distributed rollback, which real ERPs do not attempt — so the negative balance is real. It
must be visible in the exceptions projection, unresolved, until a human repairs it with a
stock count. The test asserts exactly that, and asserts the balance is not quietly clamped to
zero.

**Files:**
- Create: `warehouse-node/internal/integration/scenarios_test.go`
- Create: `warehouse-node/internal/integration/determinism_test.go`

**Interfaces:**
- Consumes: `cluster`, `warehouse`, `newCluster`, `syncNode`, `syncAll`, `stockAt`, `line`, `assertDeterministic`, `replayFresh`, `at`, `widget` (Task 21); `projection.ExceptionCompensation`, `projection.ExceptionNegativeBalance` (Task 11); `nodeapi` messages (Task 13).
- Produces: no new production code. This task adds tests only.

- [ ] **Step 1: Write the scenario tests**

`warehouse-node/internal/integration/scenarios_test.go`:

```go
package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/projection"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

// TestBothNodesReceiveAgainstOnePurchaseOrder is the invariant no node can enforce:
// neither warehouse can see the other's receipts, so only central knows the order has
// been over-received. Exactly one compensation must result — one per rejected event,
// not one per earlier receipt.
func TestBothNodesReceiveAgainstOnePurchaseOrder(t *testing.T) {
	c := newCluster(t, 100, nil)
	ctx := context.Background()

	if _, err := c.node("wh-a").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA",
		DeliveryNote: "DN-A", PoRef: "PO-1", Line: line(80), To: "RECV-wh-a"}); err != nil {
		t.Fatalf("Receive at wh-a: %v", err)
	}
	if _, err := c.node("wh-b").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RB",
		DeliveryNote: "DN-B", PoRef: "PO-1", Line: line(40), To: "RECV-wh-b"}); err != nil {
		t.Fatalf("Receive at wh-b: %v", err)
	}
	// wh-a reaches central first, so its 80 units are accepted in full.
	c.syncNode("wh-a")
	c.syncNode("wh-b")

	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 80 {
		t.Errorf("wh-a dock = %v, want 80 untouched", got)
	}
	if got := c.node("wh-b").balance("WIDGET", "RECV-wh-b"); got != 20 {
		t.Errorf("wh-b dock = %v, want 20: the 20 over the order are compensated away", got)
	}
	if exceptions, err := c.node("wh-a").svc.Exceptions(); err != nil || len(exceptions) != 0 {
		t.Errorf("wh-a exceptions = %+v, %v; want none", exceptions, err)
	}
	exceptions, err := c.node("wh-b").svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 {
		t.Fatalf("wh-b exceptions = %+v, want exactly one over-receipt compensation", exceptions)
	}
	if exceptions[0].Reason != domain.ReasonPOOverReceipt || exceptions[0].Qty != 20 {
		t.Errorf("exception = %+v, want po_overreceipt for 20", exceptions[0])
	}
	for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
		assertDeterministic(t, c.node(id))
	}
}

// TestSameDeliveryNoteAtTwoNodes is cross-node duplicate detection: the same supplier
// paperwork keyed twice, once at each warehouse.
func TestSameDeliveryNoteAtTwoNodes(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()

	for _, spec := range []struct {
		id      domain.NodeID
		receipt string
	}{{"wh-a", "RA"}, {"wh-b", "RB"}} {
		if _, err := c.node(spec.id).api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: spec.receipt,
			DeliveryNote: "DN-DUPE", PoRef: "PO-1", Line: line(12),
			To: "RECV-" + string(spec.id)}); err != nil {
			t.Fatalf("Receive at %s: %v", spec.id, err)
		}
	}
	c.syncNode("wh-a")
	c.syncNode("wh-b")

	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 12 {
		t.Errorf("wh-a dock = %v, want 12: the first sighting of a note is the real one", got)
	}
	if got := c.node("wh-b").balance("WIDGET", "RECV-wh-b"); got != 0 {
		t.Errorf("wh-b dock = %v, want 0: the whole duplicated line is removed", got)
	}
	exceptions, err := c.node("wh-b").svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 || exceptions[0].Reason != domain.ReasonDuplicateReceipt {
		t.Fatalf("wh-b exceptions = %+v, want one duplicate_receipt row", exceptions)
	}
	for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
		assertDeterministic(t, c.node(id))
	}
}

// TestReceiptAgainstASKUCentralHasDeleted covers the stale-master case: a node
// validates SKUs against its replicated copy of the item master, which may be out of
// date. Central re-checks against the live one and rejects.
func TestReceiptAgainstASKUCentralHasDeleted(t *testing.T) {
	c := newCluster(t, 1000, nil)
	ctx := context.Background()

	deleted := widget()
	deleted.Deleted = true
	if err := c.store.UpsertItem(ctx, deleted); err != nil {
		t.Fatalf("UpsertItem: %v", err)
	}

	// The node's copy still says the SKU is live, so the command succeeds locally.
	if _, err := c.node("wh-a").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA",
		DeliveryNote: "DN-A", PoRef: "PO-1", Line: line(5), To: "RECV-wh-a"}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	c.syncAll()

	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 0 {
		t.Errorf("dock = %v, want 0: the sku is zeroed", got)
	}
	exceptions, err := c.node("wh-a").svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 || exceptions[0].Reason != domain.ReasonUnknownSKU {
		t.Fatalf("exceptions = %+v, want one unknown_sku row for manual cleanup", exceptions)
	}
	assertDeterministic(t, c.node("wh-a"))
}

// TestTwoHundredOperationsOfflineThenOneCompensation is the week-offline case: a long
// backlog syncs in bounded chunks, exactly one event in it is rejected, and the
// operator can see which one and why.
func TestTwoHundredOperationsOfflineThenOneCompensation(t *testing.T) {
	c := newCluster(t, 100000, nil)
	ctx := context.Background()

	// wh-b keys delivery note DN-003 first and syncs, so it owns that note.
	if _, err := c.node("wh-b").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RB",
		DeliveryNote: "DN-003", PoRef: "PO-1", Line: line(1), To: "RECV-wh-b"}); err != nil {
		t.Fatalf("Receive at wh-b: %v", err)
	}
	c.syncNode("wh-b")

	// wh-a now works offline for 200 operations. Operation 3 reuses DN-003.
	var rejected string
	for i := 1; i <= 200; i++ {
		note := fmt.Sprintf("DN-%03d", i)
		resp, err := c.node("wh-a").api.Receive(ctx, &nodeapi.ReceiveRequest{
			ReceiptId: fmt.Sprintf("RA-%03d", i), DeliveryNote: note, PoRef: "PO-1",
			Line: line(1), To: "RECV-wh-a"})
		if err != nil {
			t.Fatalf("Receive %d: %v", i, err)
		}
		if i == 3 {
			// The GoodsReceived event is the last of the three a new receipt emits,
			// and it is the one central rejects.
			rejected = resp.GetEventIds()[len(resp.GetEventIds())-1]
		}
	}
	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 200 {
		t.Fatalf("dock = %v, want 200: every offline operation stood", got)
	}

	c.syncNode("wh-a")

	if got := c.node("wh-a").balance("WIDGET", "RECV-wh-a"); got != 199 {
		t.Errorf("dock = %v, want 199: exactly one unit was compensated", got)
	}
	exceptions, err := c.node("wh-a").svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions) != 1 {
		t.Fatalf("exceptions = %+v, want exactly one", exceptions)
	}
	if exceptions[0].Kind != projection.ExceptionCompensation {
		t.Errorf("Kind = %q, want %q", exceptions[0].Kind, projection.ExceptionCompensation)
	}
	if exceptions[0].Reason != domain.ReasonDuplicateReceipt {
		t.Errorf("Reason = %q, want %q", exceptions[0].Reason, domain.ReasonDuplicateReceipt)
	}
	if exceptions[0].CausedBy != rejected {
		t.Errorf("CausedBy = %q, want the operation-3 event %q", exceptions[0].CausedBy, rejected)
	}
	assertDeterministic(t, c.node("wh-a"))
}

// TestCompensationDrivesALocationNegative is the spec's accepted limitation. The stock
// was picked and shipped before central's rejection arrived, so undoing the receipt
// cannot undo the pick — cascading compensation through downstream events is
// explicitly out of scope. The resulting negative balance is real, is flagged, and
// waits for a human with a clipboard.
func TestCompensationDrivesALocationNegative(t *testing.T) {
	c := newCluster(t, 6, nil) // the order allows six; the operator receives ten
	ctx := context.Background()
	w := c.node("wh-a")
	key := domain.StockKey{SKU: "WIDGET", Location: "RECV-wh-a", LotID: "L1"}

	if _, err := w.api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA", DeliveryNote: "DN-A",
		PoRef: "PO-1", Line: line(10), To: "RECV-wh-a"}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	// All ten are picked and shipped while the line is still down.
	if _, err := w.api.Pick(ctx, &nodeapi.PickRequest{Line: line(10), From: "RECV-wh-a",
		OrderRef: "SO-1"}); err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got := w.balance("WIDGET", "RECV-wh-a"); got != 0 {
		t.Fatalf("dock = %v, want 0 before the compensation lands", got)
	}

	c.syncAll()

	balance, err := w.svc.StockOnHand("WIDGET", "RECV-wh-a")
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(balance) != 1 || balance[0].Qty != -4 {
		t.Fatalf("balance = %+v, want a single row of -4: a negative balance must be shown, not hidden",
			balance)
	}

	exceptions, err := w.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	var negative, compensation *projection.ExceptionRow
	for i := range exceptions {
		switch exceptions[i].Kind {
		case projection.ExceptionNegativeBalance:
			negative = &exceptions[i]
		case projection.ExceptionCompensation:
			compensation = &exceptions[i]
		}
	}
	if compensation == nil || compensation.Reason != domain.ReasonPOOverReceipt {
		t.Fatalf("exceptions = %+v, want a po_overreceipt compensation row", exceptions)
	}
	if negative == nil {
		t.Fatalf("exceptions = %+v, want a negative_balance row", exceptions)
	}
	if negative.Resolved {
		t.Error("the negative balance is marked resolved; it needs a human, not a shrug")
	}
	if negative.Qty != -4 || negative.Key != key {
		t.Errorf("negative row = %+v, want -4 at %+v", negative, key)
	}

	// The sanctioned human repair: count the shelf. Counting zero books a +4 variance,
	// the balance returns to zero and the exception resolves.
	if _, err := w.api.StartCount(ctx, &nodeapi.StartCountRequest{CountId: "C1",
		Location: "RECV-wh-a"}); err != nil {
		t.Fatalf("StartCount: %v", err)
	}
	if _, err := w.api.CountLine(ctx, &nodeapi.CountLineRequest{CountId: "C1",
		Line: &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: 0, Uom: "EA"}}); err != nil {
		t.Fatalf("CountLine: %v", err)
	}
	if _, err := w.api.CloseCount(ctx, &nodeapi.CloseCountRequest{CountId: "C1"}); err != nil {
		t.Fatalf("CloseCount: %v", err)
	}

	if got, err := w.svc.StockOnHand("WIDGET", "RECV-wh-a"); err != nil || len(got) != 0 {
		t.Fatalf("balance after the count = %+v, %v; want the row gone at zero", got, err)
	}
	exceptions, err = w.svc.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	for _, row := range exceptions {
		if row.Kind == projection.ExceptionNegativeBalance && !row.Resolved {
			t.Errorf("negative row = %+v, want it resolved after the stock count", row)
		}
	}
	// The compensation itself is never resolved away: it is a permanent audit record.
	if compensation.Resolved {
		t.Error("the compensation row was resolved; it is history, not a task")
	}
	assertDeterministic(t, w)
}
```

- [ ] **Step 2: Write the determinism test**

`warehouse-node/internal/integration/determinism_test.go`:

```go
package integration

import (
	"context"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

// TestEveryScenarioReplaysIdentically runs each scenario's command sequence, then
// replays the resulting log onto empty projections and asserts the read models come
// out identical, for both nodes and for central's log too.
//
// This is what licenses two mechanisms the rest of the design leans on: the
// projection_version rebuild, which empties the tables and replays everything, and
// idempotent sync, which can deliver the same event twice in a different order.
func TestEveryScenarioReplaysIdentically(t *testing.T) {
	scenarios := []struct {
		name    string
		ordered float64
		rejects map[domain.NodeID][]string
		run     func(t *testing.T, c *cluster)
	}{
		{
			name:    "shared purchase order over-received",
			ordered: 100,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				if _, err := c.node("wh-a").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA",
					DeliveryNote: "DN-A", PoRef: "PO-1", Line: line(80), To: "RECV-wh-a"}); err != nil {
					t.Fatalf("Receive: %v", err)
				}
				if _, err := c.node("wh-b").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RB",
					DeliveryNote: "DN-B", PoRef: "PO-1", Line: line(40), To: "RECV-wh-b"}); err != nil {
					t.Fatalf("Receive: %v", err)
				}
				c.syncNode("wh-a")
				c.syncNode("wh-b")
			},
		},
		{
			name:    "duplicate delivery note",
			ordered: 1000,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				for _, spec := range []struct {
					id      domain.NodeID
					receipt string
				}{{"wh-a", "RA"}, {"wh-b", "RB"}} {
					if _, err := c.node(spec.id).api.Receive(ctx, &nodeapi.ReceiveRequest{
						ReceiptId: spec.receipt, DeliveryNote: "DN-DUPE", PoRef: "PO-1",
						Line: line(12), To: "RECV-" + string(spec.id)}); err != nil {
						t.Fatalf("Receive: %v", err)
					}
				}
				c.syncNode("wh-a")
				c.syncNode("wh-b")
			},
		},
		{
			name:    "transfer happy path",
			ordered: 1000,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				stockAt(t, c, "wh-a", "R1", "DN-1", 10)
				c.syncAll()
				if _, err := c.node("wh-a").api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{
					TransferId: "T1", ToNode: "wh-b", From: "PICK-wh-a",
					Lines: []*nodeapi.Line{line(6)}}); err != nil {
					t.Fatalf("DispatchTransfer: %v", err)
				}
				c.syncAll()
				if _, err := c.node("wh-b").api.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{
					TransferId: "T1", To: "RECV-wh-b", Lines: []*nodeapi.Line{line(6)}}); err != nil {
					t.Fatalf("ReceiveTransfer: %v", err)
				}
				c.syncAll()
			},
		},
		{
			name:    "transfer to a refusing destination",
			ordered: 1000,
			rejects: map[domain.NodeID][]string{"wh-b": {"WIDGET"}},
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				stockAt(t, c, "wh-a", "R1", "DN-1", 10)
				c.syncAll()
				if _, err := c.node("wh-a").api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{
					TransferId: "T1", ToNode: "wh-b", From: "PICK-wh-a",
					Lines: []*nodeapi.Line{line(6)}}); err != nil {
					t.Fatalf("DispatchTransfer: %v", err)
				}
				c.syncAll()
			},
		},
		{
			name:    "reservations taken and released",
			ordered: 1000,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				stockAt(t, c, "wh-a", "R1", "DN-1", 10)
				api := c.node("wh-a").api
				if _, err := api.Reserve(ctx, &nodeapi.ReserveRequest{ReservationId: "RS1",
					Line: line(4), Location: "PICK-wh-a"}); err != nil {
					t.Fatalf("Reserve: %v", err)
				}
				if _, err := api.Reserve(ctx, &nodeapi.ReserveRequest{ReservationId: "RS2",
					Line: line(2), Location: "PICK-wh-a"}); err != nil {
					t.Fatalf("Reserve: %v", err)
				}
				if _, err := api.ReleaseReservation(ctx,
					&nodeapi.ReleaseReservationRequest{ReservationId: "RS1"}); err != nil {
					t.Fatalf("ReleaseReservation: %v", err)
				}
				c.syncAll()
			},
		},
		{
			name:    "count variance booked",
			ordered: 1000,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				stockAt(t, c, "wh-a", "R1", "DN-1", 10)
				api := c.node("wh-a").api
				if _, err := api.StartCount(ctx, &nodeapi.StartCountRequest{CountId: "C1",
					Location: "PICK-wh-a"}); err != nil {
					t.Fatalf("StartCount: %v", err)
				}
				if _, err := api.CountLine(ctx, &nodeapi.CountLineRequest{CountId: "C1",
					Line: line(7)}); err != nil {
					t.Fatalf("CountLine: %v", err)
				}
				if _, err := api.CloseCount(ctx, &nodeapi.CloseCountRequest{CountId: "C1"}); err != nil {
					t.Fatalf("CloseCount: %v", err)
				}
				c.syncAll()
			},
		},
		{
			name:    "compensation drives a location negative",
			ordered: 6,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				api := c.node("wh-a").api
				if _, err := api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA",
					DeliveryNote: "DN-A", PoRef: "PO-1", Line: line(10), To: "RECV-wh-a"}); err != nil {
					t.Fatalf("Receive: %v", err)
				}
				if _, err := api.Pick(ctx, &nodeapi.PickRequest{Line: line(10), From: "RECV-wh-a",
					OrderRef: "SO-1"}); err != nil {
					t.Fatalf("Pick: %v", err)
				}
				c.syncAll()
			},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			c := newCluster(t, sc.ordered, sc.rejects)
			sc.run(t, c)
			for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
				assertDeterministic(t, c.node(id))
			}
			assertCentralBalancesSumToZero(t, c)
		})
	}
}

// assertCentralBalancesSumToZero folds central's whole log through the same movement
// arithmetic the nodes use. Because every movement carries an explicit from and to,
// with external as the sentinel for the outside world, the sum of every balance must
// be exactly zero. A non-zero sum means stock was invented or destroyed somewhere.
func assertCentralBalancesSumToZero(t *testing.T, c *cluster) {
	t.Helper()
	envs, err := c.store.Events(context.Background())
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	balances := map[domain.StockKey]float64{}
	for _, env := range envs {
		payload, err := domain.DecodePayload(env)
		if err != nil {
			t.Fatalf("DecodePayload(%s): %v", env.ID, err)
		}
		for _, m := range movementsOfEnvelope(payload) {
			balances[m.FromKey()] -= m.Qty
			balances[m.ToKey()] += m.Qty
		}
	}
	var total float64
	for _, qty := range balances {
		total += qty
	}
	if total != 0 {
		t.Errorf("central balances sum to %v, want 0: every movement is a balanced pair", total)
	}
}

// movementsOfEnvelope mirrors projection.MovementsOf for the payload types central
// sees. It is duplicated here rather than exported because the projection package's
// version is an internal detail of the read models.
func movementsOfEnvelope(payload any) []domain.Movement {
	switch p := payload.(type) {
	case domain.GoodsReceived:
		return []domain.Movement{p.Move}
	case domain.PutAway:
		return []domain.Movement{p.Move}
	case domain.Picked:
		return []domain.Movement{p.Move}
	case domain.StockAdjusted:
		return []domain.Movement{p.Move}
	case domain.TransferDispatched:
		return p.Lines
	case domain.TransferReceived:
		return p.Lines
	default:
		return nil
	}
}
```

- [ ] **Step 3: Run the integration tests to verify they pass**

Run: `cd warehouse-node && go test ./internal/integration/ -v`
Expected: PASS. A failure here is a defect in an earlier task: fix it there rather than
adjusting an assertion. In particular, if `assertDeterministic` fails, the projection
`Apply` path depends on something outside the log — find it, because the whole rebuild
mechanism rests on it not doing that.

- [ ] **Step 4: Run the full suite with coverage and the linter**

Run: `cd warehouse-node && go test ./... -cover && golangci-lint run`
Expected: both clean, every package at 100.0%.

Confirm no package is short:

Run: `cd warehouse-node && go test ./... -coverprofile=cover.out && go tool cover -func=cover.out | grep -v '100.0%'`
Expected: only the `total:` line, at 100.0%.

- [ ] **Step 5: Update the README**

Create or extend `warehouse-node/README.md` with what a newcomer needs: what the project
is, the two binaries and their flags, how to run the tests, and the one design decision
that explains everything else.

```markdown
# warehouse-node

An offline-first warehouse service. Each warehouse node's source of truth is its own
append-only event log in SQLite, so operators keep receiving, putting away, picking,
counting and transferring stock with no network at all. A central server on Postgres
reconciles asynchronously, enforces the invariants no single node can check, and pushes
compensating events back down for what it rejects.

## The design decision everything follows from

Invariants split into two kinds:

- **Node-enforced**, checked synchronously before anything is appended, because the node
  exclusively owns the thing being checked: stock never negative, reservations within
  available, lot not expired at pick time, unit-of-measure valid for the item.
- **Central-enforced**, checked after the fact, because they need state that lives on
  another node or only at central: a receipt not exceeding an open purchase order, no
  duplicate supplier delivery note, a transfer destination that exists and accepts the
  item, a transfer receipt not exceeding what was dispatched.

A node rejection is immediate and the operator sees the rule name. A central rejection
arrives later as a compensating event, which the node cannot refuse, and surfaces in an
exceptions projection so the operator can see what was reversed and why.

Compensation is not rollback. Downstream events that already consumed the bad stock are
left alone, so a compensation can drive a location negative. That is accepted, flagged
in exceptions, and resolved by a human doing a stock count.

## Running

```sh
# central, on Postgres
go run ./cmd/central -dsn "postgres://user:pass@localhost:5432/warehouse?sslmode=disable" \
  -listen :9090 -bootstrap bootstrap.json -transit-window 48h

# a node, offline-capable; drop -central to run with no network at all
go run ./cmd/node -db wh-a.db -id wh-a -listen :8080 -central localhost:9090 \
  -locations RECV-01:receiving,PICK-01:pick,STAGE-01:staging
```

`bootstrap.json` seeds the reference data only central owns:

```json
{
  "items": [{"sku": "WIDGET", "description": "Blue widget", "base_uom": "EA",
             "alt_uom": {"CASE": 12}, "lot_tracked": true, "shelf_life_days": 30}],
  "orders": [{"po_ref": "PO-1", "sku": "WIDGET", "qty": 100}],
  "nodes": [{"id": "wh-a"}, {"id": "wh-b", "rejects": ["HAZMAT"]}]
}
```

## Testing

```sh
make test   # go test ./... -cover
make lint   # golangci-lint run
make proto  # regenerate proto/nodeapi and proto/syncpb
```

The central store's tests run against a real Postgres started by
`embedded-postgres`, so no Docker and no local database setup is needed. The
integration suite runs two nodes and central in one process over a real gRPC transport
on an in-process listener. 100% coverage is a repository standard.
```

- [ ] **Step 6: Commit**

```bash
git add warehouse-node/internal/integration warehouse-node/README.md
git commit -m "test(integration): add the remaining scenarios and the determinism check

- shared purchase order over-received across two nodes: exactly one compensation
- same delivery note at two nodes: the second is compensated in full
- a receipt against a sku central has deleted is zeroed for manual cleanup
- 200 operations offline, then one compensation the operator can trace to op 3
- compensation drives a location negative: flagged unresolved, then repaired by a
  stock count, never hidden and never cascaded into the pick
- every scenario's final log replays onto empty projections identically, and
  central's balances sum to zero

docs: add the README explaining the invariant split and how to run both binaries"
```

---

## Self-review notes

Recorded for the implementer, from checking the finished plan against the spec:

- **Spec coverage.** Every section is claimed by a task: domain model and node invariants
  (Tasks 1-8), event log (Task 9), projections including exceptions and transfers
  (Tasks 10-11), node API and the node binary (Tasks 12-13, 19), the sync wire format and
  streaming (Tasks 14, 18), the Postgres central store with `node_id` partitioning,
  in-transit balances, per-node cursors and the decisions table (Tasks 15-16, 20), the
  arbiter's five validators and their compensations (Task 17), transfers end to end with the
  in-transit sandwich and the unmatched-dispatch report (Task 21), and every scenario in the
  spec's Testing section plus the determinism check (Task 22).
- **Deliberate scope calls.** `ConsumeReservation` exists in the domain but is not exposed on
  the operator API, because the spec's API list does not include it. A receipt against a
  `PORef` central has no order for is accepted, because purchase-order lifecycle is
  explicitly out of scope and there is no ordered quantity to exceed. Cascading compensation
  through downstream events is not implemented, per the spec.
- **Type consistency.** Compensating events use `AggregateID = SKU`, except the
  `transfer_rejected` compensation which uses `AggregateID = TransferID` — that is how the
  node's transfers projection learns a transfer failed, and both the arbiter and the
  projection agree on it (Tasks 11 and 17).
- **Version bump.** `projection.Version` is `1` in Task 10 and `2` from Task 11 onward, which
  is itself the first real exercise of the rebuild-on-version-change mechanism.








