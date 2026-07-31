package sync

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/node"
)

func TestGRPCEndToEnd(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")
	if _, err := a.Receive(ctx, "SKU-1", 10); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if _, err := b.Pick(ctx, "SKU-1", 4); err != nil {
		t.Fatalf("Pick() error = %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	gs := grpc.NewServer()
	Register(gs, NewServer(b, 4))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	dialer := NewGRPCDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	rep, err := NewClient(a, dialer, 4).SyncOnce(ctx, lis.Addr().String())
	if err != nil {
		t.Fatalf("SyncOnce() error = %v", err)
	}
	if rep.Sent != 1 || rep.Received != 1 {
		t.Fatalf("Report = %+v, want Sent 1 / Received 1", rep)
	}
	for _, n := range []*node.Node{a, b} {
		state, err := n.Get(ctx, "SKU-1")
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if state.Quantity() != 6 {
			t.Fatalf("node %q Quantity() = %d, want 6", n.ID(), state.Quantity())
		}
	}
}

func TestGRPCDialerRejectsBadTarget(t *testing.T) {
	if _, err := NewGRPCDialer().Dial(context.Background(), "!!!not a target!!!"); err == nil {
		t.Fatal("Dial() error = nil for an invalid target, want an error")
	}
}

// TestGRPCDialerReplicateFails exercises the branch where grpc.NewClient
// succeeds (it dials lazily) but opening the Replicate stream itself fails --
// here because the target resolves to zero addresses.
func TestGRPCDialerReplicateFails(t *testing.T) {
	dialer := NewGRPCDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	if _, err := dialer.Dial(context.Background(), "!!!not a target!!!"); err == nil {
		t.Fatal("Dial() error = nil, want the Replicate() failure to surface")
	}
}

// TestGRPCStreamCloseSendSurfacesConnCloseError closes the underlying
// connection out from under the stream, so CloseSend's own conn.Close() call
// hits an already-closing connection and returns an error.
func TestGRPCStreamCloseSendSurfacesConnCloseError(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	gs := grpc.NewServer()
	Register(gs, NewServer(newNode(t, "B"), 4))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	dialer := NewGRPCDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	st, err := dialer.Dial(context.Background(), lis.Addr().String())
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	gst, ok := st.(*grpcStream)
	if !ok {
		t.Fatalf("Dial() returned %T, want *grpcStream", st)
	}
	if err := gst.conn.Close(); err != nil {
		t.Fatalf("conn.Close() error = %v", err)
	}
	if err := gst.CloseSend(); err == nil {
		t.Fatal("CloseSend() error = nil, want the already-closed connection to surface")
	}
}

// TestGRPCStreamCloseSendSurfacesDrainError cancels the stream's context right
// after dialing, so CloseSend's own half-close succeeds but the drain loop's
// Recv (waiting for the server to finish) hits a non-EOF error, which must
// surface rather than be swallowed.
func TestGRPCStreamCloseSendSurfacesDrainError(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	gs := grpc.NewServer()
	Register(gs, NewServer(newNode(t, "B"), 4))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	dialer := NewGRPCDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	st, err := dialer.Dial(ctx, lis.Addr().String())
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	cancel()
	if err := st.CloseSend(); err == nil {
		t.Fatal("CloseSend() error = nil, want the canceled context to surface from the drain")
	}
}
