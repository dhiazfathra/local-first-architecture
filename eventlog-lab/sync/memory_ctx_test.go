package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// TestMemPipeUnblocksOnContextCancellation exercises memPipe's ctx.Done()
// branches directly. A dropped frame (the jepsenlite Injector's partition
// fault) never arrives on either channel, so without a context deadline both
// sides of the pipe block forever instead of the session failing -- exactly
// the deadlock jepsenlite's fault-combination matrix hit before SyncPair
// started bounding every session with a timeout. These cases build a bare
// memPipe with channels nobody ever reads or writes, so the only way any of
// the four blocking operations returns is via the already-cancelled context.
func TestMemPipeUnblocksOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &memPipe{
		ctx:      ctx,
		toServer: make(chan *syncpb.ClientFrame),
		toClient: make(chan *syncpb.ServerFrame),
		done:     make(chan struct{}),
	}
	cs := (*memClientSide)(p)
	ss := (*memServerSide)(p)

	if _, err := cs.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("memClientSide.Recv() error = %v, want context.Canceled", err)
	}
	hello := &syncpb.ClientFrame{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}}
	if err := cs.Send(hello); !errors.Is(err, context.Canceled) {
		t.Fatalf("memClientSide.Send() error = %v, want context.Canceled", err)
	}
	if _, err := ss.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("memServerSide.Recv() error = %v, want context.Canceled", err)
	}
	ack := &syncpb.ServerFrame{Body: &syncpb.ServerFrame_Ack{Ack: &syncpb.Ack{}}}
	if err := ss.Send(ack); !errors.Is(err, context.Canceled) {
		t.Fatalf("memServerSide.Send() error = %v, want context.Canceled", err)
	}
}

// TestCloseTerminatesAnAbandonedSession proves Close ends the server-side
// session goroutine even when the client never reaches CloseSend (e.g.
// SyncOnce failing partway through). Before Close cancelled a session-scoped
// context, an abandoned session left the server goroutine blocked in
// memServerSide.Recv for the lifetime of the caller's ctx.
func TestCloseTerminatesAnAbandonedSession(t *testing.T) {
	tr := NewMemoryTransport()
	tr.Serve("peer", NewServer(&fakeReplica{id: "B", log: &fakeLog{}}, 8))

	st, err := tr.Dial(context.Background(), "peer")
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	// Abandon the session without CloseSend, as SyncOnce does on an error
	// path partway through a session.
	if err := st.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	done := (*memPipe)(st.(*memClientSide)).done
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("server session did not finish after Close()")
	}
}
