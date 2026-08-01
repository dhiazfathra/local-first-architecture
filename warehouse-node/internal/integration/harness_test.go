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
		domain.LocationCode("RECV-" + string(id)): domain.LocReceiving,
		domain.LocationCode("PICK-" + string(id)): domain.LocPick,
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
