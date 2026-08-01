package demo_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/demo"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	transport "github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// serveNode stands up a real node service on loopback, so ComposeEnv can be
// tested against the same gRPC API the containers expose — no Docker required.
func serveNode(ctx context.Context, t *testing.T, id, loc string) string {
	t.Helper()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), id+".db"), id)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	inv, err := domain.NewInventory(ctx, store, clock.New(id, nil), []string{loc})
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	nodepb.RegisterNodeServer(srv, transport.NewNodeService(inv))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

// recorder captures the commands ComposeEnv would have run, so the partition
// step can be asserted exactly without touching a real network.
type recorder struct {
	calls []string
	err   error
}

func (r *recorder) exec(_ context.Context, name string, args ...string) error {
	r.calls = append(r.calls, name+" "+strings.Join(args, " "))
	return r.err
}

func newComposeEnv(ctx context.Context, t *testing.T, rec *recorder) *demo.ComposeEnv {
	t.Helper()
	url := dsn(t)
	wipe(ctx, t, url)

	var addrs []demo.NodeAddr
	for i, loc := range []string{"A", "B", "C"} {
		id := fmt.Sprintf("node-%d", i+1)
		addrs = append(addrs, demo.NodeAddr{
			ID:        id,
			Location:  loc,
			Container: "lf-" + id,
			Addr:      serveNode(ctx, t, id, loc),
		})
	}

	env, cleanup, err := demo.NewComposeEnv(ctx, demo.ComposeConfig{
		Network: "localfirst-go_lfnet",
		DSN:     url,
		Settle:  10 * time.Millisecond,
		Exec:    rec.exec,
		Nodes:   addrs,
	})
	if err != nil {
		t.Fatalf("NewComposeEnv: %v", err)
	}
	t.Cleanup(cleanup)
	return env
}

func TestComposeEnvTalksToNodesOverGRPC(t *testing.T) {
	ctx := context.Background()
	env := newComposeEnv(ctx, t, &recorder{})

	if err := env.WaitReady(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nodes := env.Nodes()
	if len(nodes) != 3 {
		t.Fatalf("Nodes() = %d, want 3", len(nodes))
	}
	if nodes[0].ID() != "node-1" || nodes[2].Location() != "C" {
		t.Fatalf("nodes are out of order: %s/%s", nodes[0].ID(), nodes[2].Location())
	}

	if err := nodes[0].Receive(ctx, demo.SKU, "A", 7); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := nodes[0].Issue(ctx, demo.SKU, "A", 2); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	got, err := nodes[0].Balance(ctx, demo.SKU, "A")
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got != 5 {
		t.Fatalf("balance = %d, want 5", got)
	}
	if err := nodes[0].Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

// A node that does not own a location must refuse the write, over gRPC, with the
// error surfacing through the transport rather than being swallowed.
func TestComposeEnvSurfacesDomainErrors(t *testing.T) {
	ctx := context.Background()
	env := newComposeEnv(ctx, t, &recorder{})
	if err := env.Nodes()[0].Receive(ctx, demo.SKU, "Z", 1); err == nil {
		t.Fatal("Receive into an unowned location must fail")
	}
}

func TestComposeEnvPartitionCommands(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{}
	env := newComposeEnv(ctx, t, rec)

	if err := env.Disconnect(ctx, "node-3"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := env.Reconnect(ctx, "node-3"); err != nil {
		t.Fatalf("Reconnect: %v", err)
	}

	want := []string{
		"docker network disconnect localfirst-go_lfnet lf-node-3",
		"docker network connect localfirst-go_lfnet lf-node-3",
	}
	if len(rec.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", rec.calls, want)
	}
	for i := range want {
		if rec.calls[i] != want[i] {
			t.Errorf("call %d = %q, want %q", i, rec.calls[i], want[i])
		}
	}
}

func TestComposeEnvRejectsAnUnknownNode(t *testing.T) {
	ctx := context.Background()
	env := newComposeEnv(ctx, t, &recorder{})
	if err := env.Disconnect(ctx, "node-9"); err == nil {
		t.Fatal("Disconnect must fail for a node it does not know")
	}
	if err := env.Reconnect(ctx, "node-9"); err == nil {
		t.Fatal("Reconnect must fail for a node it does not know")
	}
}

func TestComposeEnvReportsPartitionFailures(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{err: errBoom}
	env := newComposeEnv(ctx, t, rec)
	if err := env.Disconnect(ctx, "node-1"); err == nil {
		t.Fatal("Disconnect must report the exec failure")
	}
}

func TestComposeEnvCentralIsUsable(t *testing.T) {
	ctx := context.Background()
	env := newComposeEnv(ctx, t, &recorder{})
	if _, err := env.Central().GlobalSum(ctx); err != nil {
		t.Fatalf("GlobalSum: %v", err)
	}
}

func TestNewComposeEnvFailsOnABadDSN(t *testing.T) {
	_, _, err := demo.NewComposeEnv(context.Background(), demo.ComposeConfig{
		DSN:   "postgres://nobody@127.0.0.1:1/none",
		Nodes: demo.DefaultNodes(),
	})
	if err == nil {
		t.Fatal("NewComposeEnv must fail when Postgres is unreachable")
	}
}

func TestWaitReadyTimesOut(t *testing.T) {
	ctx := context.Background()
	url := dsn(t)
	wipe(ctx, t, url)

	// Port 1 on loopback refuses connections, so readiness can never arrive.
	env, cleanup, err := demo.NewComposeEnv(ctx, demo.ComposeConfig{
		DSN:    url,
		Settle: time.Millisecond,
		Exec:   (&recorder{}).exec,
		Nodes:  []demo.NodeAddr{{ID: "node-1", Location: "A", Container: "lf-node-1", Addr: "127.0.0.1:1"}},
	})
	if err != nil {
		t.Fatalf("NewComposeEnv: %v", err)
	}
	t.Cleanup(cleanup)

	if err := env.WaitReady(ctx, 150*time.Millisecond); err == nil {
		t.Fatal("WaitReady must time out against a dead address")
	}
}

func TestSyncStopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newComposeEnv(ctx, t, &recorder{})
	cancel()
	if err := env.Nodes()[0].Sync(ctx); err == nil {
		t.Fatal("Sync must return the context error")
	}
}

func TestDefaultNodesMatchTheComposeFile(t *testing.T) {
	nodes := demo.DefaultNodes()
	if len(nodes) != 3 {
		t.Fatalf("DefaultNodes() = %d, want 3", len(nodes))
	}
	want := []demo.NodeAddr{
		{ID: "node-1", Location: "A", Container: "lf-node-1", Addr: "127.0.0.1:8081"},
		{ID: "node-2", Location: "B", Container: "lf-node-2", Addr: "127.0.0.1:8082"},
		{ID: "node-3", Location: "C", Container: "lf-node-3", Addr: "127.0.0.1:8083"},
	}
	for i := range want {
		if nodes[i] != want[i] {
			t.Errorf("node %d = %+v, want %+v", i, nodes[i], want[i])
		}
	}
}
