package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
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

// failingStore wraps a real Store and forces one named method to fail, so
// applyBootstrap's and run's error-propagation branches can be exercised without a
// live Postgres.
type failingStore struct {
	central.Store
	fail string
}

var errInjected = errors.New("injected failure")

func (f *failingStore) RegisterNode(ctx context.Context, node domain.NodeID, rejects []string) error {
	if f.fail == "RegisterNode" {
		return errInjected
	}
	return f.Store.RegisterNode(ctx, node, rejects)
}

func (f *failingStore) UpsertPurchaseOrder(ctx context.Context, poRef, sku string, ordered float64) error {
	if f.fail == "UpsertPurchaseOrder" {
		return errInjected
	}
	return f.Store.UpsertPurchaseOrder(ctx, poRef, sku, ordered)
}

func (f *failingStore) UpsertItem(ctx context.Context, item domain.Item) error {
	if f.fail == "UpsertItem" {
		return errInjected
	}
	return f.Store.UpsertItem(ctx, item)
}

func (f *failingStore) EmitCentral(ctx context.Context, events []domain.Event, causation *domain.EventID,
	now time.Time) ([]domain.Envelope, error) {
	if f.fail == "EmitCentral" {
		return nil, errInjected
	}
	return f.Store.EmitCentral(ctx, events, causation, now)
}

func (f *failingStore) Enqueue(ctx context.Context, target domain.NodeID, envs []domain.Envelope) error {
	if f.fail == "Enqueue" {
		return errInjected
	}
	return f.Store.Enqueue(ctx, target, envs)
}

func (f *failingStore) OpenTransfers(ctx context.Context, dispatchedBefore time.Time) ([]central.InTransitRow, error) {
	if f.fail == "OpenTransfers" {
		return nil, errInjected
	}
	return f.Store.OpenTransfers(ctx, dispatchedBefore)
}

func TestApplyBootstrapPropagatesEachStoreError(t *testing.T) {
	for _, method := range []string{"RegisterNode", "UpsertPurchaseOrder", "UpsertItem", "EmitCentral", "Enqueue"} {
		t.Run(method, func(t *testing.T) {
			store := &failingStore{Store: central.NewMemory(), fail: method}
			if err := applyBootstrap(context.Background(), store, testBootstrap(), at); err == nil {
				t.Fatalf("applyBootstrap with a failing %s: expected an error", method)
			}
		})
	}
}

func TestRunPropagatesApplyBootstrapError(t *testing.T) {
	store := &failingStore{Store: central.NewMemory(), fail: "RegisterNode"}
	lis := bufconn.Listen(1 << 10)
	defer func() { _ = lis.Close() }()
	if err := run(context.Background(), store, testBootstrap(), time.Hour, time.Hour,
		func() time.Time { return at }, lis); err == nil {
		t.Fatal("run with a failing bootstrap: expected an error")
	}
}

func TestRunPropagatesServeError(t *testing.T) {
	lis := bufconn.Listen(1 << 10)
	_ = lis.Close()
	store := central.NewMemory()
	if err := run(context.Background(), store, Bootstrap{}, time.Hour, time.Hour,
		func() time.Time { return at }, lis); err == nil {
		t.Fatal("run with a closed listener: expected an error")
	}
}

func TestReportLoopLogsAndContinuesOnAFailedReport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	store := &failingStore{Store: central.NewMemory(), fail: "OpenTransfers"}
	done := make(chan struct{})
	go func() {
		reportLoop(ctx, store, time.Hour, time.Millisecond, func() time.Time { return at })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reportLoop did not stop with its context")
	}
}

func TestMainReportsErrorsAndExitsNonzero(t *testing.T) {
	oldArgs, oldExit, oldStderr := os.Args, osExit, os.Stderr
	defer func() { os.Args, osExit, os.Stderr = oldArgs, oldExit, oldStderr }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	os.Args = []string{"central", "--bootstrap", bad}
	var gotCode int
	osExit = func(code int) { gotCode = code }

	main()

	_ = w.Close()
	stderr, _ := io.ReadAll(r)
	if gotCode != 1 {
		t.Errorf("exit code = %d, want 1", gotCode)
	}
	if !strings.Contains(string(stderr), "central:") {
		t.Errorf("stderr = %q, want it prefixed with %q", stderr, "central:")
	}
}

func TestRealMainRejectsBadListenAddress(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"central", "--listen", "not-an-address"}
	if err := realMain(); err == nil {
		t.Fatal("realMain: expected an error from an unlistenable address")
	}
}

func TestRealMainRejectsAnUnparseableDSN(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"central", "--dsn", "postgres://%zz"}
	if err := realMain(); err == nil {
		t.Fatal("realMain: expected an error from an unparseable DSN")
	}
}
