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
	// A rejected over-receipt compensates with two events — the stock reversal and
	// the paperwork reversal — but only the stock reversal is operator-visible: the
	// paperwork correction carries no reason and no quantity of its own, so it
	// would only be a second, misleading row for the same rejection.
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
