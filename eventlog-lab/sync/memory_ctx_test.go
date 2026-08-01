package sync

import (
	"context"
	"errors"
	"testing"

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
