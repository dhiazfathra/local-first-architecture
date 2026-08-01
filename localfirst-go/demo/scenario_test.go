package demo_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/central"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/demo"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// --- the in-process environment -------------------------------------------
//
// Everything below is real: real SQLite logs, a real Postgres central, real
// gRPC sync over loopback. The one thing simulated is the network partition:
// gate=false makes Sync refuse to run, which is what a partition means from the
// scenario's point of view. `make demo` performs a genuine
// `docker network disconnect`; this twin exists to pin the numbers, not to
// re-test Docker.

type testNode struct {
	id, loc string
	inv     *domain.Inventory
	client  *lfsync.Client
	gate    bool
}

var errPartitioned = errors.New("demo: node is partitioned")

func (n *testNode) ID() string       { return n.id }
func (n *testNode) Location() string { return n.loc }

func (n *testNode) Receive(ctx context.Context, sku, location string, qty int64) error {
	return n.inv.Receive(ctx, sku, location, qty)
}

func (n *testNode) Issue(ctx context.Context, sku, location string, qty int64) error {
	return n.inv.Issue(ctx, sku, location, qty)
}

func (n *testNode) Balance(_ context.Context, sku, location string) (int64, error) {
	return n.inv.Balance(sku, location), nil
}

func (n *testNode) Sync(ctx context.Context) error {
	if !n.gate {
		return errPartitioned
	}
	_, err := n.client.SyncOnce(ctx)
	return err
}

type testEnv struct {
	nodes []demo.Node
	pg    *central.PGStore
}

func (e *testEnv) Nodes() []demo.Node    { return e.nodes }
func (e *testEnv) Central() demo.Central { return e.pg }

func (e *testEnv) Disconnect(_ context.Context, nodeID string) error {
	return e.setGate(nodeID, false)
}

func (e *testEnv) Reconnect(_ context.Context, nodeID string) error {
	return e.setGate(nodeID, true)
}

func (e *testEnv) setGate(nodeID string, open bool) error {
	for _, n := range e.nodes {
		if tn, ok := n.(*testNode); ok && tn.id == nodeID {
			tn.gate = open
			return nil
		}
	}
	return fmt.Errorf("demo: no such node %q", nodeID)
}

func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("LOCALFIRST_PG_DSN")
	if v == "" {
		t.Fatal("LOCALFIRST_PG_DSN is unset — run `make test`")
	}
	return v
}

// wipe gives the test a clean central. GlobalSum sums across every node in the
// table, so leftovers from another test would be indistinguishable from this
// test's own data.
func wipe(ctx context.Context, t *testing.T, url string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "TRUNCATE records, cursors"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func newEnv(ctx context.Context, t *testing.T) *testEnv {
	t.Helper()
	url := dsn(t)
	wipe(ctx, t, url)

	pg, err := central.OpenPG(ctx, url, "central")
	if err != nil {
		t.Fatalf("OpenPG: %v", err)
	}
	t.Cleanup(pg.Close)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, lfsync.NewServer(pg, nil))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	dir := t.TempDir()
	env := &testEnv{pg: pg}
	for i, loc := range []string{"A", "B", "C"} {
		id := fmt.Sprintf("node-%d", i+1)

		store, err := eventlog.Open(filepath.Join(dir, id+".db"), id)
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		t.Cleanup(func() { _ = store.Close() })

		inv, err := domain.NewInventory(ctx, store, clock.New(id, nil), []string{loc})
		if err != nil {
			t.Fatalf("NewInventory %s: %v", id, err)
		}

		cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial %s: %v", id, err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		env.nodes = append(env.nodes, &testNode{
			id: id, loc: loc, inv: inv,
			client: lfsync.NewClient(cc, store, nil, "central"),
			gate:   true,
		})
	}
	return env
}

// --- the test twin of `make demo` -----------------------------------------

// TestScenarioConvergesAfterAPartition is the integration test the demo cannot
// drift from: it runs the exact same seven steps and pins every printed number.
func TestScenarioConvergesAfterAPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var out strings.Builder
	res, err := demo.Run(ctx, newEnv(ctx, t), &out)
	if err != nil {
		t.Fatalf("Run = %v, want nil\n%s", err, out.String())
	}
	t.Log("\n" + out.String())

	checks := []struct {
		name string
		got  int64
		want int64
	}{
		{"seed total", res.SeedTotal, 180},
		{"sum after seeding", res.SumAfterSeed, 180},
		{"isolated node local balance", res.IsolatedLocal, 10},
		{"sum while node-3 was offline", res.SumWhileOffline, 180},
		{"sum after node-3 rejoined", res.SumAfterRejoin, 160},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
	if res.OfflineOK != demo.OfflineOps || res.OfflineFailed != 0 {
		t.Errorf("offline ops: %d ok / %d failed, want %d / 0", res.OfflineOK, res.OfflineFailed, demo.OfflineOps)
	}

	// The output is a deliverable too: a reader watching `make demo` must see
	// the numbers, not just a spinner.
	for _, want := range []string{"step 1/7", "step 7/7", "180", "160", "20 succeeded"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output is missing %q:\n%s", want, out.String())
		}
	}
}

// --- error paths ----------------------------------------------------------

type stubCentral struct {
	sum map[string]int64
	err error
}

func (s stubCentral) GlobalSum(context.Context) (map[string]int64, error) {
	return s.sum, s.err
}

// scriptedCentral returns a different total on each call, so a test can make
// central's sum move at a moment when the scenario insists it must not.
type scriptedCentral struct {
	sums []int64
	call int
}

func (s *scriptedCentral) GlobalSum(context.Context) (map[string]int64, error) {
	v := s.sums[len(s.sums)-1]
	if s.call < len(s.sums) {
		v = s.sums[s.call]
	}
	s.call++
	return map[string]int64{demo.SKU: v}, nil
}

type stubNode struct {
	id, loc    string
	balance    int64
	receiveErr error
	issueErr   error
	balanceErr error
	syncErr    error
}

func (n *stubNode) ID() string                                           { return n.id }
func (n *stubNode) Location() string                                     { return n.loc }
func (n *stubNode) Receive(context.Context, string, string, int64) error { return n.receiveErr }
func (n *stubNode) Issue(context.Context, string, string, int64) error   { return n.issueErr }
func (n *stubNode) Balance(context.Context, string, string) (int64, error) {
	return n.balance, n.balanceErr
}
func (n *stubNode) Sync(context.Context) error { return n.syncErr }

type stubEnv struct {
	nodes         []demo.Node
	central       demo.Central
	disconnectErr error
	reconnectErr  error
}

func (e *stubEnv) Nodes() []demo.Node                       { return e.nodes }
func (e *stubEnv) Central() demo.Central                    { return e.central }
func (e *stubEnv) Disconnect(context.Context, string) error { return e.disconnectErr }
func (e *stubEnv) Reconnect(context.Context, string) error  { return e.reconnectErr }

func threeStubs() []demo.Node {
	return []demo.Node{
		&stubNode{id: "node-1", loc: "A"},
		&stubNode{id: "node-2", loc: "B"},
		&stubNode{id: "node-3", loc: "C", balance: 10},
	}
}

var errBoom = errors.New("boom")

func TestRunErrorPaths(t *testing.T) {
	tests := []struct {
		name    string
		env     *stubEnv
		wantErr error
	}{
		{
			name:    "wrong number of nodes",
			env:     &stubEnv{nodes: threeStubs()[:2], central: stubCentral{}},
			wantErr: demo.ErrWantThreeNodes,
		},
		{
			name: "a seed receive fails",
			env: &stubEnv{
				nodes:   []demo.Node{&stubNode{id: "node-1", loc: "A", receiveErr: errBoom}, &stubNode{id: "node-2", loc: "B"}, &stubNode{id: "node-3", loc: "C"}},
				central: stubCentral{sum: map[string]int64{demo.SKU: 180}},
			},
			wantErr: errBoom,
		},
		{
			name: "central is unreadable",
			env: &stubEnv{
				nodes:   threeStubs(),
				central: stubCentral{err: errBoom},
			},
			wantErr: errBoom,
		},
		{
			name: "seeding did not converge",
			env: &stubEnv{
				nodes:   threeStubs(),
				central: stubCentral{sum: map[string]int64{demo.SKU: 7}},
			},
			wantErr: demo.ErrDidNotConverge,
		},
		{
			name: "the partition cannot be created",
			env: &stubEnv{
				nodes:         threeStubs(),
				central:       stubCentral{sum: map[string]int64{demo.SKU: 180}},
				disconnectErr: errBoom,
			},
			wantErr: errBoom,
		},
		{
			name: "an offline write is rejected",
			env: &stubEnv{
				nodes:   []demo.Node{&stubNode{id: "node-1", loc: "A"}, &stubNode{id: "node-2", loc: "B"}, &stubNode{id: "node-3", loc: "C", issueErr: errBoom}},
				central: stubCentral{sum: map[string]int64{demo.SKU: 180}},
			},
			wantErr: demo.ErrOfflineWriteRejected,
		},
		{
			name: "the isolated balance cannot be read",
			env: &stubEnv{
				nodes:   []demo.Node{&stubNode{id: "node-1", loc: "A"}, &stubNode{id: "node-2", loc: "B"}, &stubNode{id: "node-3", loc: "C", balanceErr: errBoom}},
				central: stubCentral{sum: map[string]int64{demo.SKU: 180}},
			},
			wantErr: errBoom,
		},
		{
			name: "central's sum moved during the partition",
			env: &stubEnv{
				nodes: threeStubs(),
				// 180 after seeding, then 200 during the partition: something
				// reached central while node-3 was supposed to be cut off.
				central: &scriptedCentral{sums: []int64{180, 200}},
			},
			wantErr: demo.ErrLeakedWhileOffline,
		},
		{
			name: "reconnect fails",
			env: &stubEnv{
				nodes:        threeStubs(),
				central:      stubCentral{sum: map[string]int64{demo.SKU: 180}},
				reconnectErr: errBoom,
			},
			wantErr: errBoom,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := demo.Run(context.Background(), tc.env, io.Discard)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Run = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// A stub central whose sum never changes reaches step 7 with 180 instead of 160,
// which is exactly the "did not converge" failure a broken sync would produce.
func TestRunDetectsAFailureToConverge(t *testing.T) {
	env := &stubEnv{nodes: threeStubs(), central: stubCentral{sum: map[string]int64{demo.SKU: 180}}}
	_, err := demo.Run(context.Background(), env, io.Discard)
	if !errors.Is(err, demo.ErrDidNotConverge) {
		t.Fatalf("Run = %v, want ErrDidNotConverge", err)
	}
}

// A node whose sync fails after reconnection must stop the demo: silently
// printing a converged number would be the one unforgivable bug in a
// demonstration of convergence.
func TestRunFailsWhenPostRejoinSyncFails(t *testing.T) {
	nodes := threeStubs()
	nodes[0].(*stubNode).syncErr = errBoom
	env := &stubEnv{nodes: nodes, central: stubCentral{sum: map[string]int64{demo.SKU: 180}}}
	if _, err := demo.Run(context.Background(), env, io.Discard); !errors.Is(err, errBoom) {
		t.Fatalf("Run = %v, want errBoom", err)
	}
}
