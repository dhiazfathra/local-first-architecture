package projection

import (
	"testing"
	"time"

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

// TestReceiptLineReversalIsNotItsOwnException covers the paperwork half of a
// po_overreceipt rejection: central answers with both a StockAdjusted (the
// stock reversal) and a ReceiptLineRecorded (the receipt paperwork correction)
// sharing the same CausationID. Only the StockAdjusted is operator-visible —
// it carries the reason and the quantity actually reversed. Surfacing the
// paperwork correction too would show two exceptions for what is, from the
// operator's point of view, one rejection.
func TestReceiptLineReversalIsNotItsOwnException(t *testing.T) {
	l, set := openSet(t)
	origin := emit(t, l, set, received("WIDGET", "L1", "RECV-01", 10))[0]
	reversal := domain.Event{Type: domain.TypeReceiptLineRecorded, AggregateID: "R1",
		Payload: domain.ReceiptLineRecorded{ReceiptID: "R1", LineNo: 1, SKU: "WIDGET", LotID: "L1", QtyBase: -4}}
	comp := emitCaused(t, l, set, origin.ID, reversal)[0]

	rows, err := set.Exceptions()
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	for _, r := range rows {
		if r.ID == comp.ID.String() {
			t.Fatalf("exception row = %+v, want none for a paperwork-only compensation", r)
		}
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

// TestRecordCompensationUsesToKeyWhenFromIsExternal proves a compensation that
// credits stock (moving from the external sentinel into a real location) is keyed
// on the location it credits, not on the sentinel itself.
func TestRecordCompensationUsesToKeyWhenFromIsExternal(t *testing.T) {
	l, set := openSet(t)
	origin := emit(t, l, set, received("WIDGET", "L1", "RECV-01", 5))[0]
	comp := emitCaused(t, l, set, origin.ID,
		adjust("WIDGET", "L1", domain.External, "RECV-01", 3, domain.ReasonPOOverReceipt))[0]

	got := findException(t, set, comp.ID.String())
	want := domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}
	if got.Key != want {
		t.Errorf("Key = %+v, want %+v", got.Key, want)
	}
}

// TestApplyPropagatesAnExceptionProjectionFailure proves a failure recording a
// compensation is surfaced by Apply, not swallowed.
func TestApplyPropagatesAnExceptionProjectionFailure(t *testing.T) {
	l, set := openSet(t)
	origin := emit(t, l, set, received("WIDGET", "L1", "RECV-01", 5))[0]
	if _, err := l.DB().Exec(`DROP TABLE exceptions`); err != nil {
		t.Fatalf("drop exceptions: %v", err)
	}
	envs, err := l.Emit([]domain.Event{
		adjust("WIDGET", "L1", "RECV-01", domain.External, 5, domain.ReasonPOOverReceipt),
	}, &origin.ID)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := set.Apply(envs[0]); err == nil {
		t.Fatal("Apply: expected an error with the exceptions table missing")
	}
}

// TestRefreshNegativesPropagatesAPerKeyFailure proves a failure reading one key's
// balance stops the whole sweep rather than being swallowed.
func TestRefreshNegativesPropagatesAPerKeyFailure(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 5))
	if _, err := l.DB().Exec(`UPDATE stock_on_hand SET qty = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt qty: %v", err)
	}
	tx, err := l.DB().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	env := domain.Envelope{ID: domain.EventID{NodeID: "wh-a", Seq: 99}, RecordedAt: time.Now()}
	payload := domain.StockAdjusted{Move: domain.Movement{
		SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 1}}
	if err := refreshNegatives(tx, env, payload); err == nil {
		t.Fatal("refreshNegatives: expected an error reading a corrupted balance")
	}
}

// TestRefreshNegativeFailsWhenBalanceIsUnreadable proves refreshNegative surfaces a
// balance read failure that isn't the ordinary "no row yet" case.
func TestRefreshNegativeFailsWhenBalanceIsUnreadable(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 5))
	if _, err := l.DB().Exec(`UPDATE stock_on_hand SET qty = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt qty: %v", err)
	}
	tx, err := l.DB().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	key := domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}
	if err := refreshNegative(tx, domain.Envelope{RecordedAt: time.Now()}, key); err == nil {
		t.Fatal("refreshNegative: expected an error reading a corrupted balance")
	}
}

// TestRefreshNegativeFailsWhenExceptionsTableMissing covers both write paths inside
// refreshNegative: flagging a fresh negative balance, and resolving an existing one.
func TestRefreshNegativeFailsWhenExceptionsTableMissing(t *testing.T) {
	tests := []struct {
		name string
		qty  float64
	}{
		{"flagging a new negative balance", -5},
		{"resolving an existing flag", 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, set := openSet(t)
			emit(t, l, set, received("WIDGET", "L1", "RECV-01", 5))
			if _, err := l.DB().Exec(`UPDATE stock_on_hand SET qty = ?`, tt.qty); err != nil {
				t.Fatalf("set qty: %v", err)
			}
			if _, err := l.DB().Exec(`DROP TABLE exceptions`); err != nil {
				t.Fatalf("drop exceptions: %v", err)
			}
			tx, err := l.DB().Begin()
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			key := domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}
			if err := refreshNegative(tx, domain.Envelope{RecordedAt: time.Now()}, key); err == nil {
				t.Fatal("refreshNegative: expected an error with the exceptions table missing")
			}
		})
	}
}

// TestRefreshNegativeFailsWhenKeyTouchedTableMissing covers touchKey's error
// return and refreshNegative's propagation of it, the same way
// TestRefreshNegativeFailsWhenExceptionsTableMissing covers the exceptions
// table: drop the table touchKey writes to and confirm the failure surfaces.
func TestRefreshNegativeFailsWhenKeyTouchedTableMissing(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 5))
	if _, err := l.DB().Exec(`DROP TABLE key_touched`); err != nil {
		t.Fatalf("drop key_touched: %v", err)
	}
	tx, err := l.DB().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	key := domain.StockKey{SKU: "WIDGET", Location: "RECV-01", LotID: "L1"}
	if err := refreshNegative(tx, domain.Envelope{RecordedAt: time.Now()}, key); err == nil {
		t.Fatal("refreshNegative: expected an error with the key_touched table missing")
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
