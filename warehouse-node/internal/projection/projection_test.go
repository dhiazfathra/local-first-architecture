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
	t.Cleanup(func() { _ = l.Close() })
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
		Move: domain.Movement{SKU: sku, LotID: lot, From: domain.External, To: to, Qty: qty}}}
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

// TestApplyRecoversWithoutDoubleCountingAfterATransientFailure proves the
// property Service.apply's projections-first ordering depends on: a failed
// Apply must not record the event as applied, and a retry after the fault
// clears must fold the event in exactly once, never twice.
func TestApplyRecoversWithoutDoubleCountingAfterATransientFailure(t *testing.T) {
	l, set := openSet(t)
	envs, err := l.Emit([]domain.Event{received("WIDGET", "L1", "RECV-01", 10)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}

	// Simulate a transient projection failure — e.g. a brief connection blip or
	// concurrent migration — by removing the table Apply writes to.
	if _, err := l.DB().Exec(`DROP TABLE stock_on_hand`); err != nil {
		t.Fatalf("drop stock_on_hand: %v", err)
	}
	if err := set.Apply(envs[0]); err == nil {
		t.Fatal("Apply succeeded despite the missing stock table")
	}
	applied, err := set.IsApplied(envs[0].ID)
	if err != nil {
		t.Fatalf("IsApplied: %v", err)
	}
	if applied {
		t.Fatal("IsApplied is true after a failed Apply — the rolled-back transaction must not have recorded it")
	}

	// The fault clears, exactly as a real transient failure resolves on its own.
	if _, err := l.DB().Exec(`CREATE TABLE stock_on_hand (
		sku TEXT NOT NULL, location TEXT NOT NULL, lot_id TEXT NOT NULL, qty REAL NOT NULL,
		PRIMARY KEY (sku, location, lot_id))`); err != nil {
		t.Fatalf("recreate stock_on_hand: %v", err)
	}
	if err := set.Apply(envs[0]); err != nil {
		t.Fatalf("Apply after the fault cleared: %v", err)
	}

	got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 10 {
		t.Fatalf("balance = %v, want exactly 10 — a retry after a failed Apply must not double-apply", got)
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

func TestOpenSkipsRebuildWhenVersionAlreadyMatches(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 10))
	// The projection version was already set to Version by the first Open (inside
	// openSet). Reopening on the same log must take the CatchUp path, not Rebuild.
	reopened, err := Open(l)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := reopened.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 10 {
		t.Fatalf("balance after reopen = %v, want 10", got)
	}
}

func TestOpenFailsWhenSchemaCannotBeApplied(t *testing.T) {
	l, _ := openSet(t)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := Open(l); err == nil {
		t.Fatal("Open: expected an error applying the schema against a closed database")
	}
}

func TestOpenFailsWhenProjectionVersionUnreadable(t *testing.T) {
	l, _ := openSet(t)
	if _, err := l.DB().Exec(`DROP TABLE projection_version`); err != nil {
		t.Fatalf("drop projection_version: %v", err)
	}
	if _, err := Open(l); err == nil {
		t.Fatal("Open: expected an error reading a missing projection_version table")
	}
}

func TestRebuildPropagatesACatchUpFailure(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 10))
	if _, err := l.DB().Exec(`DROP TABLE events`); err != nil {
		t.Fatalf("drop events: %v", err)
	}
	if err := set.Rebuild(); err == nil {
		t.Fatal("Rebuild: expected an error once the log itself cannot be read")
	}
}

func TestCatchUpPropagatesAnApplyFailure(t *testing.T) {
	l, set := openSet(t)
	// Written straight to the log, bypassing Apply, so CatchUp has something to fold in.
	if _, err := l.Emit([]domain.Event{received("WIDGET", "L1", "RECV-01", 7)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := l.DB().Exec(`DROP TABLE stock_on_hand`); err != nil {
		t.Fatalf("drop stock_on_hand: %v", err)
	}
	if err := set.CatchUp(); err == nil {
		t.Fatal("CatchUp: expected an error when a fold-in fails partway through")
	}
}

func TestApplyPropagatesAReservationProjectionFailure(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "PICK-01", 10))
	if _, err := l.DB().Exec(`DROP TABLE reservations`); err != nil {
		t.Fatalf("drop reservations: %v", err)
	}
	envs, err := l.Emit([]domain.Event{{Type: domain.TypeStockReserved, AggregateID: "RS1", Payload: domain.StockReserved{
		ReservationID: "RS1", Key: domain.StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}, Qty: 4}}}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := set.Apply(envs[0]); err == nil {
		t.Fatal("Apply: expected an error with the reservations table missing")
	}
}

func TestApplyFailsWhenAppliedTrackingTableIsMissing(t *testing.T) {
	l, set := openSet(t)
	if _, err := l.DB().Exec(`DROP TABLE projection_applied`); err != nil {
		t.Fatalf("drop projection_applied: %v", err)
	}
	envs, err := l.Emit([]domain.Event{received("WIDGET", "L1", "RECV-01", 10)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := set.Apply(envs[0]); err == nil {
		t.Fatal("Apply: expected an error recording the applied event")
	}
}

// TestApplyStockSkipsTransferDispatchedRelayAtOtherNodes and its sibling prove
// applyStock's home-node filtering: a TransferDispatched only moves stock at the
// node that is actually dispatching it, never at a node merely receiving central's
// relay of the event for visibility.
func TestApplyStockSkipsTransferDispatchedRelayAtOtherNodes(t *testing.T) {
	l, set := openSet(t) // home is "wh-a"
	mv := domain.Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 3}
	emit(t, l, set, domain.Event{Type: domain.TypeTransferDispatched, AggregateID: "T1", Payload: domain.TransferDispatched{
		TransferID: "T1", FromNode: "wh-b", ToNode: "wh-c", Lines: []domain.Movement{mv}}})
	got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 0 {
		t.Fatalf("balance = %v, want 0: a relay at a node that isn't dispatching must not move stock", got)
	}
}

func TestApplyStockAppliesTransferDispatchedAtTheOriginatingNode(t *testing.T) {
	l, set := openSet(t) // home is "wh-a"
	mv := domain.Movement{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 3}
	emit(t, l, set, received("WIDGET", "L1", "PICK-01", 3))
	emit(t, l, set, domain.Event{Type: domain.TypeTransferDispatched, AggregateID: "T1", Payload: domain.TransferDispatched{
		TransferID: "T1", FromNode: "wh-a", ToNode: "wh-c", Lines: []domain.Movement{mv}}})
	got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 0 {
		t.Fatalf("balance = %v, want 0: dispatching from this node must move the stock out", got)
	}
}

// TestApplyStockAppliesTransferReceivedWithNoTransfersTableYet proves the
// pre-Task-16 fallback: with no transfers table at all, a TransferReceived is
// always applied rather than silently dropped.
func TestApplyStockAppliesTransferReceivedWithNoTransfersTableYet(t *testing.T) {
	l, set := openSet(t)
	mv := domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-01", Qty: 5}
	emit(t, l, set, domain.Event{Type: domain.TypeTransferReceived, AggregateID: "T1",
		Payload: domain.TransferReceived{TransferID: "T1", Lines: []domain.Movement{mv}}})
	got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 5 {
		t.Fatalf("balance = %v, want 5: a TransferReceived must apply when there is no transfers table to consult", got)
	}
}

// TestApplyStockAppliesTransferReceivedWhenTransferRowIsMissing proves the
// transfers-table-exists-but-no-matching-row fallback also applies the move.
func TestApplyStockAppliesTransferReceivedWhenTransferRowIsMissing(t *testing.T) {
	l, set := openSet(t)
	if _, err := l.DB().Exec(`CREATE TABLE transfers (id TEXT PRIMARY KEY, to_node TEXT NOT NULL)`); err != nil {
		t.Fatalf("create transfers: %v", err)
	}
	mv := domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-01", Qty: 5}
	emit(t, l, set, domain.Event{Type: domain.TypeTransferReceived, AggregateID: "T9",
		Payload: domain.TransferReceived{TransferID: "T9", Lines: []domain.Movement{mv}}})
	got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 5 {
		t.Fatalf("balance = %v, want 5: an unknown transfer id must fall back to applying", got)
	}
}

// TestApplyStockSkipsTransferReceivedRelayAtOtherNodes proves the transfers table
// is actually consulted once populated: a TransferReceived relayed to a node that
// isn't the real destination must not move that node's own stock.
func TestApplyStockSkipsTransferReceivedRelayAtOtherNodes(t *testing.T) {
	l, set := openSet(t) // home is "wh-a"
	if _, err := l.DB().Exec(`CREATE TABLE transfers (id TEXT PRIMARY KEY, to_node TEXT NOT NULL)`); err != nil {
		t.Fatalf("create transfers: %v", err)
	}
	if _, err := l.DB().Exec(`INSERT INTO transfers (id, to_node) VALUES ('T1', 'wh-b')`); err != nil {
		t.Fatalf("seed transfers: %v", err)
	}
	mv := domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-01", Qty: 5}
	emit(t, l, set, domain.Event{Type: domain.TypeTransferReceived, AggregateID: "T1",
		Payload: domain.TransferReceived{TransferID: "T1", Lines: []domain.Movement{mv}}})
	got, err := set.Balance(domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 0 {
		t.Fatalf("balance = %v, want 0: a relay to a node that isn't the real destination must not move stock", got)
	}
}

func TestSetReservationStatusFailsWhenReservationsTableIsMissing(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set,
		received("WIDGET", "L1", "PICK-01", 10),
		domain.Event{Type: domain.TypeStockReserved, AggregateID: "RS1", Payload: domain.StockReserved{
			ReservationID: "RS1", Key: domain.StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}, Qty: 4}},
	)
	if _, err := l.DB().Exec(`DROP TABLE reservations`); err != nil {
		t.Fatalf("drop reservations: %v", err)
	}
	envs, err := l.Emit([]domain.Event{{Type: domain.TypeReservationReleased, AggregateID: "RS1",
		Payload: domain.ReservationReleased{ReservationID: "RS1"}}}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := set.Apply(envs[0]); err == nil {
		t.Fatal("Apply: expected an error updating a reservation with the table missing")
	}
}

// TestApplyFailsWhenRecordingAppliedIsBlocked hits the INSERT INTO projection_applied
// error branch specifically: a trigger blocks only the insert, leaving the earlier
// isApplied lookup (a plain SELECT on the same table) unaffected, so the two error
// paths through that table stay distinguishable.
func TestApplyFailsWhenRecordingAppliedIsBlocked(t *testing.T) {
	l, set := openSet(t)
	if _, err := l.DB().Exec(`CREATE TRIGGER block_applied BEFORE INSERT ON projection_applied
		BEGIN SELECT RAISE(ABORT, 'blocked'); END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	envs, err := l.Emit([]domain.Event{received("WIDGET", "L1", "RECV-01", 10)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := set.Apply(envs[0]); err == nil {
		t.Fatal("Apply: expected an error recording the applied event")
	}
}

// TestAddStockFailsOnTheCreditLeg hits addStock's second call specifically (the
// credit at Movement.To) by constraining the table so only a chosen location can
// violate it, proving the debit and credit legs are each error-checked rather than
// only the first.
func TestAddStockFailsOnTheCreditLeg(t *testing.T) {
	l, set := openSet(t)
	if _, err := l.DB().Exec(`DROP TABLE stock_on_hand`); err != nil {
		t.Fatalf("drop stock_on_hand: %v", err)
	}
	if _, err := l.DB().Exec(`CREATE TABLE stock_on_hand (
		sku TEXT NOT NULL, location TEXT NOT NULL CHECK (location <> 'FORBIDDEN'),
		lot_id TEXT NOT NULL, qty REAL NOT NULL, PRIMARY KEY (sku, location, lot_id))`); err != nil {
		t.Fatalf("recreate stock_on_hand: %v", err)
	}
	envs, err := l.Emit([]domain.Event{{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.PutAway{
		Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: "FORBIDDEN", Qty: 1}}}}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := set.Apply(envs[0]); err == nil {
		t.Fatal("Apply: expected an error crediting the forbidden location")
	}
}

// TestAddStockFailsPruningAZeroRow hits addStock's second statement (the
// zero-balance prune) specifically, distinct from the insert/update above it: the
// balance nets to zero, so the DELETE is attempted, and a trigger blocks it.
func TestAddStockFailsPruningAZeroRow(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 5))
	if _, err := l.DB().Exec(`CREATE TRIGGER block_delete BEFORE DELETE ON stock_on_hand
		BEGIN SELECT RAISE(ABORT, 'blocked'); END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	envs, err := l.Emit([]domain.Event{{Type: domain.TypePicked, AggregateID: "WIDGET", Payload: domain.Picked{
		Move: domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 5}}}}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := set.Apply(envs[0]); err == nil {
		t.Fatal("Apply: expected an error pruning the zero-balance row")
	}
}

func TestStockOnHandFailsToScanAMalformedRow(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 1))
	if _, err := l.DB().Exec(`UPDATE stock_on_hand SET qty = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt qty: %v", err)
	}
	if _, err := set.StockOnHand("", ""); err == nil {
		t.Fatal("StockOnHand: expected a scan error for a non-numeric qty")
	}
}

func TestReservationsFailsToScanAMalformedRow(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set,
		received("WIDGET", "L1", "PICK-01", 10),
		domain.Event{Type: domain.TypeStockReserved, AggregateID: "RS1", Payload: domain.StockReserved{
			ReservationID: "RS1", Key: domain.StockKey{SKU: "WIDGET", Location: "PICK-01", LotID: "L1"}, Qty: 4}},
	)
	if _, err := l.DB().Exec(`UPDATE reservations SET qty = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt qty: %v", err)
	}
	if _, err := set.Reservations(); err == nil {
		t.Fatal("Reservations: expected a scan error for a non-numeric qty")
	}
}

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
