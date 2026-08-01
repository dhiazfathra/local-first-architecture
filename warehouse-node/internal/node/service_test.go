package node

import (
	"path/filepath"
	"sync"
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

// TestConcurrentExecuteSerializesAgainstSameStock proves two commands racing the
// same available stock cannot both observe it available: Execute holds the
// service's single command mutex across validate-append-apply, so the second
// goroutine to run always sees the first's effect and is rejected for
// over-reservation instead of both succeeding and driving stock negative.
func TestConcurrentExecuteSerializesAgainstSameStock(t *testing.T) {
	svc := openService(t, "wh-a")
	if err := svc.RegisterLocation("PICK-01", domain.LocPick); err != nil {
		t.Fatalf("RegisterLocation: %v", err)
	}
	if _, err := svc.Execute(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReceive(s, domain.ReceiveCmd{ReceiptID: "R1", DeliveryNote: "DN-1", PORef: "PO-1",
			Line: domain.Line{SKU: "WIDGET", LotID: "L1", Qty: 10, UoM: "EA"}, To: "PICK-01"})
	}); err != nil {
		t.Fatalf("seed receipt: %v", err)
	}

	pick := func() (bool, error) {
		_, err := svc.Execute(func(s *domain.State) ([]domain.Event, error) {
			return domain.DoPick(s, domain.PickCmd{
				Line: domain.Line{SKU: "WIDGET", LotID: "L1", Qty: 6, UoM: "EA"},
				From: "PICK-01", OrderRef: "O1", At: time.Date(2026, 7, 30, 1, 0, 0, 0, time.UTC),
			})
		})
		return err == nil, err
	}

	var wg sync.WaitGroup
	results := make([]bool, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := pick()
			if err != nil && !domain.IsViolation(err, domain.RuleStockNonNegative) {
				t.Errorf("pick %d: unexpected error %v", i, err)
			}
			results[i] = ok
		}(i)
	}
	wg.Wait()

	succeeded := 0
	for _, ok := range results {
		if ok {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("succeeded picks = %d, want exactly 1 (two picks of 6 against a stock of 10 must not both succeed)", succeeded)
	}
	bal, err := svc.StockOnHand("WIDGET", "PICK-01")
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(bal) != 1 || bal[0].Qty != 4 {
		t.Fatalf("balance = %+v, want a single row of 4", bal)
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

func TestOpenFailsWhenProjectionVersionUnreadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	svc, err := Open(path, "wh-a", time.Now)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := svc.Log().DB().Exec(`UPDATE projection_version SET version = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt projection_version: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := Open(path, "wh-a", time.Now); err == nil {
		t.Fatal("Open: expected an error reading a corrupted projection_version")
	}
}

func TestOpenFailsWhenLogCannotBeReplayedIntoState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	svc, err := Open(path, "wh-a", time.Now)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := svc.RegisterLocation("RECV-01", domain.LocReceiving); err != nil {
		t.Fatalf("RegisterLocation: %v", err)
	}
	if _, err := svc.Log().DB().Exec(`UPDATE events SET payload = 'not-json' WHERE seq = 1`); err != nil {
		t.Fatalf("corrupt payload: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := Open(path, "wh-a", time.Now); err == nil {
		t.Fatal("Open: expected an error rebuilding state from a corrupted payload")
	}
}

func TestRegisterLocationRejectsUnknownType(t *testing.T) {
	svc := openService(t, "wh-a")
	err := svc.RegisterLocation("BAD-01", domain.LocationType("bogus"))
	if !domain.IsViolation(err, domain.RuleLocationExists) {
		t.Fatalf("err = %v, want a %s violation", err, domain.RuleLocationExists)
	}
}

func TestApplyPropagatesAnIsAppliedFailure(t *testing.T) {
	svc := openService(t, "wh-a")
	if _, err := svc.Log().DB().Exec(`DROP TABLE projection_applied`); err != nil {
		t.Fatalf("drop projection_applied: %v", err)
	}
	if _, err := svc.Execute(func(*domain.State) ([]domain.Event, error) {
		return []domain.Event{{Type: domain.TypeLocationRegistered, AggregateID: "X",
			Payload: domain.LocationRegistered{Code: "X-01", Type: domain.LocPick}}}, nil
	}); err == nil {
		t.Fatal("Execute: expected an error from apply() with projection_applied missing")
	}
}

func TestApplyPropagatesASetApplyFailure(t *testing.T) {
	svc := openService(t, "wh-a")
	if _, err := svc.Log().DB().Exec(`DROP TABLE stock_on_hand`); err != nil {
		t.Fatalf("drop stock_on_hand: %v", err)
	}
	if _, err := svc.Execute(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReceive(s, domain.ReceiveCmd{ReceiptID: "R2", DeliveryNote: "DN-2", PORef: "PO-2",
			Line: domain.Line{SKU: "WIDGET", LotID: "L1", Qty: 1, UoM: "EA"}, To: "RECV-01"})
	}); err == nil {
		t.Fatal("Execute: expected an error from apply() with stock_on_hand missing")
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
