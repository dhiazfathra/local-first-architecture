package main

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// freeAddr asks the OS for an unused port so parallel tests never collide.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

func TestRunServesCommandsAndShutsDownCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	addr := freeAddr(t)
	cfg := Config{
		NodeID:    "n1",
		DBPath:    filepath.Join(t.TempDir(), "n1.db"),
		Listen:    addr,
		Locations: []string{"A"},
		SyncEvery: time.Hour, // no central in this test
	}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = cc.Close() }()
	client := nodepb.NewNodeClient(cc)

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = client.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 4})
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	bal, err := client.Balance(ctx, &nodepb.BalanceRequest{Sku: "S", Location: "A"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if bal.GetQty() != 4 {
		t.Fatalf("Balance = %d, want 4", bal.GetQty())
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not shut down")
	}
}

func TestRunFailsOnAnUnusableDBPath(t *testing.T) {
	err := Run(context.Background(), Config{
		NodeID: "n1", DBPath: filepath.Join(t.TempDir(), "missing", "n1.db"),
		Listen: freeAddr(t), SyncEvery: time.Hour,
	})
	if err == nil {
		t.Fatal("Run must fail when the log cannot be opened")
	}
}

func TestRunFailsOnAnUnusableListenAddress(t *testing.T) {
	err := Run(context.Background(), Config{
		NodeID: "n1", DBPath: filepath.Join(t.TempDir(), "n1.db"),
		Listen: "256.256.256.256:1", SyncEvery: time.Hour,
	})
	if err == nil {
		t.Fatal("Run must fail when it cannot listen")
	}
}

func TestRunDialsCentralWhenConfigured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	// Central is not running: the node must still come up and serve.
	err := Run(ctx, Config{
		NodeID: "n1", DBPath: filepath.Join(t.TempDir(), "n1.db"),
		Listen: freeAddr(t), Locations: []string{"A"},
		CentralAddr: freeAddr(t), SyncEvery: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Run = %v, want nil: an unreachable central must not stop the node", err)
	}
}

// A Moved event is authored by the SOURCE node but credits a location the
// DESTINATION node owns. The destination learns of it only through sync, so
// this test is the one that proves Run passes its Inventory as the sync
// projector: with a nil projector the record lands in n2's log but never
// reaches its in-memory balances, and Balance stays 0 until a restart replays
// the log.
//
// n2 dials n1 as its "central". That works because the sync protocol is
// symmetric (see docs/adr/0004-symmetric-sync-protocol.md) — central is a peer
// with a bigger disk, not a different protocol.
func TestSyncedMovedEventUpdatesTheDestinationNodesBalances(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr1, addr2 := freeAddr(t), freeAddr(t)
	db1 := filepath.Join(t.TempDir(), "n1.db")
	db2 := filepath.Join(t.TempDir(), "n2.db")
	runErr := make(chan error, 2)
	go func() {
		runErr <- Run(ctx, Config{
			NodeID: "n1", DBPath: db1,
			Listen: addr1, Locations: []string{"A"}, SyncEvery: time.Hour,
		})
	}()
	go func() {
		runErr <- Run(ctx, Config{
			NodeID: "n2", DBPath: db2,
			Listen: addr2, Locations: []string{"B"},
			CentralAddr: addr1, SyncEvery: 20 * time.Millisecond,
		})
	}()

	dial := func(addr string) nodepb.NodeClient {
		cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("NewClient %s: %v", addr, err)
		}
		t.Cleanup(func() { _ = cc.Close() })
		return nodepb.NewNodeClient(cc)
	}
	n1, n2 := dial(addr1), dial(addr2)

	// n1 stocks its own location, then ships 4 units to B, which n2 owns.
	deadline := time.Now().Add(5 * time.Second)
	var err error
	for {
		_, err = n1.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 10})
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("n1.Receive: %v", err)
	}
	if _, err := n1.Move(ctx, &nodepb.MoveRequest{Sku: "S", From: "A", To: "B", Qty: 4}); err != nil {
		t.Fatalf("n1.Move: %v", err)
	}

	// n2 must converge to 4 at B purely through sync.
	deadline = time.Now().Add(5 * time.Second)
	var got int64
	for time.Now().Before(deadline) {
		select {
		case err := <-runErr:
			t.Fatalf("Run exited early: %v", err)
		default:
		}
		bal, err := n2.Balance(ctx, &nodepb.BalanceRequest{Sku: "S", Location: "B"})
		if err == nil {
			if got = bal.GetQty(); got == 4 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("n2 Balance(S, B) = %d, want 4 — is Run still passing nil as the sync projector?", got)
}
