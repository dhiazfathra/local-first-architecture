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
  internal/sync/
    codec.go                domain.Envelope <-> proto conversion
    client.go               node side of the bidi stream, cursor resume, backoff
    server.go               central side of the bidi stream, batching, fan-out
  internal/central/
    schema.sql              Postgres DDL
    store.go                pgx-backed store: events, in-transit, decisions, cursors
  internal/arbiter/
    arbiter.go              Validator chain, Decision, compensation emission
    validators.go           the five rejection rules
  cmd/node/main.go          node server: gRPC node API + sync client
  cmd/central/main.go       central server: gRPC sync server + arbitration
  proto/sync.proto
  proto/node_api.proto
  internal/integration/     two nodes + central, in-process, injectable transport
    harness_test.go
    scenarios_test.go
    determinism_test.go
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








