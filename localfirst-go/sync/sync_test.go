package sync_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

func store(t *testing.T, nodeID string) *eventlog.Store {
	t.Helper()
	s, err := eventlog.Open(filepath.Join(t.TempDir(), nodeID+".db"), nodeID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// serve starts an in-process gRPC server over bufconn — no ports, no Docker.
func serve(t *testing.T, log lfsync.Log, p eventlog.Projector) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, lfsync.NewServer(log, p))
	go func() { _ = srv.Serve(lis) }()
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close(); srv.Stop(); _ = lis.Close() })
	return cc
}

func appendN(t *testing.T, s *eventlog.Store, n int) {
	t.Helper()
	c := clock.New(s.NodeID(), nil)
	for i := 0; i < n; i++ {
		if _, err := s.Append(context.Background(), domain.TypeReceived,
			mustJSONBytes(t, domain.Received{SKU: "S", Location: s.NodeID(), Qty: 1}), c.Now(), nil, nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func mustJSONBytes(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestSyncIsBidirectionalInOnePass: each side ends up holding both logs.
func TestSyncIsBidirectionalInOnePass(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 3)
	appendN(t, central, 2)

	cc := serve(t, central, nil)
	client := lfsync.NewClient(cc, node, nil, "central")
	if _, err := client.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	want := eventlog.VersionVector{"n1": 3, "central": 2}
	for name, s := range map[string]*eventlog.Store{"node": node, "central": central} {
		vv, err := s.Version(ctx)
		if err != nil {
			t.Fatalf("%s Version: %v", name, err)
		}
		if !vv.Equal(want) {
			t.Fatalf("%s holds %v, want %v", name, vv, want)
		}
	}
	if cur, err := node.Cursor(ctx, "central"); err != nil || cur != 3 {
		t.Fatalf("cursor = (%d, %v), want (3, nil)", cur, err)
	}
}

// TestCatchUpAfterOfflineWindow is the local-first claim in test form: the node
// keeps appending while unreachable, then one sync closes the gap.
func TestCatchUpAfterOfflineWindow(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n3"), store(t, "central")

	cc := serve(t, central, nil)
	client := lfsync.NewClient(cc, node, nil, "central")
	appendN(t, node, 1)
	if _, err := client.SyncOnce(ctx); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	// Offline window: 20 local appends with no sync at all.
	appendN(t, node, 20)
	if got, err := central.Version(ctx); err != nil || got.Get("n3") != 1 {
		t.Fatalf("central saw %v during the offline window, want n3:1", got)
	}
	if _, err := client.SyncOnce(ctx); err != nil {
		t.Fatalf("catch-up sync: %v", err)
	}
	if got, err := central.Version(ctx); err != nil || got.Get("n3") != 21 {
		t.Fatalf("central holds n3:%d after catch-up, want 21", got.Get("n3"))
	}
}

// flakyStream fails the nth Send, simulating a mid-stream disconnect.
type flakyStream struct {
	lfsync.Stream
	failAfter int
	sent      int
}

func (f *flakyStream) Send(fr *syncpb.Frame) error {
	f.sent++
	if f.sent > f.failAfter {
		return errors.New("connection reset")
	}
	return f.Stream.Send(fr)
}

// TestResumeAfterMidStreamDisconnect: a broken exchange loses no data and the
// next exchange completes it, because Hello always restates the real vectors.
func TestResumeAfterMidStreamDisconnect(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 5)

	cc := serve(t, central, nil)
	raw, err := syncpb.NewSyncClient(cc).Replicate(ctx)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	// Fail on the very first frame we try to send: the Hello never lands.
	if _, err := lfsync.Exchange(ctx, node, nil, &flakyStream{Stream: raw, failAfter: 0}, true); err == nil {
		t.Fatal("Exchange must report the disconnect")
	}
	if got, err := central.Version(ctx); err != nil || len(got) != 0 {
		t.Fatalf("central holds %v after a failed exchange, want nothing", got)
	}

	client := lfsync.NewClient(cc, node, nil, "central")
	if _, err := client.SyncOnce(ctx); err != nil {
		t.Fatalf("resumed sync: %v", err)
	}
	if got, err := central.Version(ctx); err != nil || got.Get("n1") != 5 {
		t.Fatalf("central holds n1:%d, want 5", got.Get("n1"))
	}
}

// TestDuplicateBatchIsANoOp: syncing twice with nothing new changes nothing.
func TestDuplicateBatchIsANoOp(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 4)

	cc := serve(t, central, nil)
	client := lfsync.NewClient(cc, node, nil, "central")
	for i := 0; i < 3; i++ {
		if _, err := client.SyncOnce(ctx); err != nil {
			t.Fatalf("sync %d: %v", i, err)
		}
	}
	recs, err := central.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(recs) != 4 {
		t.Fatalf("central holds %d records after 3 syncs, want 4", len(recs))
	}
}

// raceLog wraps a store and injects a local append right after Since has
// already collected its results but before pushAndAck takes its version
// snapshot for the Ack — reproducing a local write racing with a push.
type raceLog struct {
	*eventlog.Store
	extra func()
}

func (r *raceLog) Since(ctx context.Context, vv eventlog.VersionVector) ([]eventlog.Record, error) {
	recs, err := r.Store.Since(ctx, vv)
	if r.extra != nil {
		f := r.extra
		r.extra = nil // race only once
		f()
	}
	return recs, err
}

// TestPushAndAckDoesNotAcknowledgeRecordsAppendedAfterTheSnapshot: a local
// append that lands between collecting records to send and acknowledging must
// not be reflected in the Ack, since the peer never actually received it.
func TestPushAndAckDoesNotAcknowledgeRecordsAppendedAfterTheSnapshot(t *testing.T) {
	ctx := context.Background()
	nodeStore, central := store(t, "n1"), store(t, "central")
	appendN(t, nodeStore, 2)
	node := &raceLog{Store: nodeStore, extra: func() { appendN(t, nodeStore, 1) }}

	cc := serve(t, central, nil)
	client := lfsync.NewClient(cc, node, nil, "central")
	vv, err := client.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if got := vv.Get("n1"); got != 2 {
		t.Fatalf("acked version = %d, want 2 — the raced-in 3rd record was never sent", got)
	}
	if got, err := central.Version(ctx); err != nil || got.Get("n1") != 2 {
		t.Fatalf("central holds n1:%d, want 2 — the raced-in record must not have been pushed", got.Get("n1"))
	}
}

// TestCentralProjectsAGlobalSum shows why Merge takes a Projector.
func TestCentralProjectsAGlobalSum(t *testing.T) {
	ctx := context.Background()
	n1, n2, central := store(t, "n1"), store(t, "n2"), store(t, "central")
	appendN(t, n1, 3) // 3 units at location "n1"
	appendN(t, n2, 2) // 2 units at location "n2"

	global, err := domain.NewInventory(ctx, central, clock.New("central", nil), nil)
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	cc := serve(t, central, global)
	for _, s := range []*eventlog.Store{n1, n2} {
		if _, err := lfsync.NewClient(cc, s, nil, "central").SyncOnce(ctx); err != nil {
			t.Fatalf("sync %s: %v", s.NodeID(), err)
		}
	}
	if got := global.Balance("S", "n1") + global.Balance("S", "n2"); got != 5 {
		t.Fatalf("global sum = %d, want 5", got)
	}
}

func TestExchangeRejectsAMissingHello(t *testing.T) {
	ctx := context.Background()
	central := store(t, "central")
	cc := serve(t, central, nil)
	stream, err := syncpb.NewSyncClient(cc).Replicate(ctx)
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	// Speak out of turn: a Batch where a Hello belongs.
	if err := stream.Send(&syncpb.Frame{Body: &syncpb.Frame_Batch{Batch: &syncpb.Batch{}}}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("server must reject a first frame that is not a Hello")
	}
}

func TestExchangeRejectsAnUnexpectedFrame(t *testing.T) {
	ctx := context.Background()
	node := store(t, "n1")
	// A stream that says Hello, then sends a second Hello where a Batch or Ack
	// belongs.
	s := &scriptedStream{frames: []*syncpb.Frame{
		{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
		{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
	}}
	if _, err := lfsync.Exchange(ctx, node, nil, s, true); !errors.Is(err, lfsync.ErrProtocol) {
		t.Fatalf("err = %v, want ErrProtocol", err)
	}
}

// scriptedStream replays canned frames and discards everything sent to it.
type scriptedStream struct {
	frames []*syncpb.Frame
	i      int
}

func (s *scriptedStream) Send(*syncpb.Frame) error { return nil }
func (s *scriptedStream) Recv() (*syncpb.Frame, error) {
	if s.i >= len(s.frames) {
		return nil, io.EOF
	}
	f := s.frames[s.i]
	s.i++
	return f, nil
}

func TestClientRunKeepsGoingWhenThePeerIsUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	node := store(t, "n1")
	appendN(t, node, 1)

	cc, err := grpc.NewClient("passthrough:///dead",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return nil, errors.New("network unreachable")
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = cc.Close() }()

	done := make(chan struct{})
	go func() { lfsync.NewClient(cc, node, nil, "central").Run(ctx, 10*time.Millisecond); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must return when its context is cancelled")
	}
	// The node stayed operational and its cursor was not advanced.
	if cur, err := node.Cursor(context.Background(), "central"); err != nil || cur != 0 {
		t.Fatalf("cursor = (%d, %v), want (0, nil) — a failed sync must not advance it", cur, err)
	}
}

func TestLargeLogIsSentInMultipleFrames(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 150) // > batchSize (100)

	cc := serve(t, central, nil)
	if _, err := lfsync.NewClient(cc, node, nil, "central").SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	recs, err := central.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(recs) != 150 {
		t.Fatalf("central holds %d records, want 150", len(recs))
	}
}

// failMergeLog wraps a store and makes every Merge fail, simulating a
// responder that rejects the initiator's final batch.
type failMergeLog struct{ *eventlog.Store }

func (failMergeLog) Merge(context.Context, []eventlog.Record, eventlog.Projector) (int, error) {
	return 0, errors.New("responder: merge exploded")
}

// TestSyncOnceFailsWhenTheResponderMergeFails proves the client waits for the
// responder's terminal RPC status instead of declaring success right after
// sending its own final Ack: the responder's Merge error must surface here,
// and the cursor must stay untouched.
func TestSyncOnceFailsWhenTheResponderMergeFails(t *testing.T) {
	ctx := context.Background()
	node, centralStore := store(t, "n1"), store(t, "central")
	appendN(t, node, 2)

	cc := serve(t, failMergeLog{centralStore}, nil)
	client := lfsync.NewClient(cc, node, nil, "central")
	if _, err := client.SyncOnce(ctx); err == nil {
		t.Fatal("SyncOnce must fail when the responder's merge fails")
	}
	if cur, err := node.Cursor(ctx, "central"); err != nil || cur != 0 {
		t.Fatalf("cursor = (%d, %v), want (0, nil) — a failed sync must not advance it", cur, err)
	}
}

// The sentinels below are shared between a fault wrapper and its test, so the
// tests can assert the exact underlying failure with errors.Is through the
// error wrapping that Exchange, pushAndAck and drain add.
var (
	errStoreVersion = errors.New("store: version exploded")
	errStoreSince   = errors.New("store: since exploded")
	errStoreCursor  = errors.New("store: cursor exploded")
	errStreamReset  = errors.New("stream: write reset")
	errStreamAbort  = errors.New("stream: aborted")
)

// failVersionLog fails Version from the nth call on, so a test can let the
// Hello's Version succeed and then fail pushAndAck's version snapshot.
type failVersionLog struct {
	*eventlog.Store
	failOnCall int
	calls      int
}

func (l *failVersionLog) Version(ctx context.Context) (eventlog.VersionVector, error) {
	l.calls++
	if l.calls >= l.failOnCall {
		return nil, errStoreVersion
	}
	return l.Store.Version(ctx)
}

// failSinceLog makes every Since fail, simulating a storage read error mid-push.
type failSinceLog struct{ *eventlog.Store }

func (l *failSinceLog) Since(context.Context, eventlog.VersionVector) ([]eventlog.Record, error) {
	return nil, errStoreSince
}

// failSetCursorLog makes every SetCursor fail, simulating a cursor write error
// after the exchange already delivered the records.
type failSetCursorLog struct{ *eventlog.Store }

func (l *failSetCursorLog) SetCursor(context.Context, string, uint64) error {
	return errStoreCursor
}

// failNthSendStream wraps a Stream and fails every Send from the nth call on,
// so a specific outgoing frame (hello, batch, or ack) can be made to fail.
type failNthSendStream struct {
	lfsync.Stream
	failOn int
	sent   int
}

func (s *failNthSendStream) Send(fr *syncpb.Frame) error {
	s.sent++
	if s.sent >= s.failOn {
		return errStreamReset
	}
	return s.Stream.Send(fr)
}

// abortStream returns a fixed error from Recv once its canned frames run out,
// simulating a peer that hangs up mid-exchange instead of finishing cleanly.
type abortStream struct {
	frames []*syncpb.Frame
	err    error
	i      int
}

func (s *abortStream) Send(*syncpb.Frame) error { return nil }
func (s *abortStream) Recv() (*syncpb.Frame, error) {
	if s.i >= len(s.frames) {
		return nil, s.err
	}
	f := s.frames[s.i]
	s.i++
	return f, nil
}

// TestSyncOnceFailsWhenTheInitiatorVersionFails: a client whose own log cannot
// answer Version cannot even send its Hello — SyncOnce must surface that, and
// neither peer may be left holding anything.
func TestSyncOnceFailsWhenTheInitiatorVersionFails(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 3)

	cc := serve(t, central, nil)
	client := lfsync.NewClient(cc, &failVersionLog{Store: node, failOnCall: 1}, nil, "central")
	if _, err := client.SyncOnce(ctx); !errors.Is(err, errStoreVersion) {
		t.Fatalf("SyncOnce err = %v, want the version error", err)
	}
	if got, err := central.Version(ctx); err != nil || len(got) != 0 {
		t.Fatalf("central holds %v after a failed sync, want nothing", got)
	}
	if cur, err := node.Cursor(ctx, "central"); err != nil || cur != 0 {
		t.Fatalf("cursor = (%d, %v), want (0, nil) — a failed sync must not advance it", cur, err)
	}
}

// TestSyncOnceFailsWhenTheCursorWriteFails: the exchange may have delivered the
// data, but a failed cursor write must still surface as an error so the caller
// knows the sync did not complete; the peer keeps the records either way.
func TestSyncOnceFailsWhenTheCursorWriteFails(t *testing.T) {
	ctx := context.Background()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 2)

	cc := serve(t, central, nil)
	client := lfsync.NewClient(cc, &failSetCursorLog{node}, nil, "central")
	if _, err := client.SyncOnce(ctx); !errors.Is(err, errStoreCursor) {
		t.Fatalf("SyncOnce err = %v, want the cursor error", err)
	}
	// The records were already delivered before the cursor write.
	recs, err := central.Since(ctx, nil)
	if err != nil || len(recs) != 2 {
		t.Fatalf("central holds %d records, want 2 — data must flow even if the cursor write fails", len(recs))
	}
	if cur, err := node.Cursor(ctx, "central"); err != nil || cur != 0 {
		t.Fatalf("cursor = (%d, %v), want (0, nil) — a failed cursor write must not advance it", cur, err)
	}
}

// TestClientRunSyncsOnAScheduleUntilCancelled: Run's success path resets the
// backoff and keeps looping, and cancelling the context stops it.
func TestClientRunSyncsOnAScheduleUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, central := store(t, "n1"), store(t, "central")
	appendN(t, node, 3)

	cc := serve(t, central, nil)
	done := make(chan struct{})
	go func() { lfsync.NewClient(cc, node, nil, "central").Run(ctx, 10*time.Millisecond); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cur, err := node.Cursor(context.Background(), "central"); err == nil && cur == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must return when its context is cancelled")
	}
	recs, err := central.Since(context.Background(), nil)
	if err != nil || len(recs) != 3 {
		t.Fatalf("central holds %d records, want 3", len(recs))
	}
	if cur, err := node.Cursor(context.Background(), "central"); err != nil || cur != 3 {
		t.Fatalf("cursor = (%d, %v), want (3, nil)", cur, err)
	}
}

// TestExchangeResponderFailsWhenItsOwnHelloCannotBeSent: a responder that
// cannot read its own version cannot answer the initiator's Hello.
func TestExchangeResponderFailsWhenItsOwnHelloCannotBeSent(t *testing.T) {
	ctx := context.Background()
	node := store(t, "n1")
	s := &scriptedStream{frames: []*syncpb.Frame{
		{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
	}}
	if _, err := lfsync.Exchange(ctx, &failVersionLog{Store: node, failOnCall: 1}, nil, s, false); !errors.Is(err, errStoreVersion) {
		t.Fatalf("Exchange err = %v, want the version error", err)
	}
}

// TestExchangeResponderFailsWhenItsPushFails: the responder's own push is the
// second Version call, so failing only that call must abort the exchange.
func TestExchangeResponderFailsWhenItsPushFails(t *testing.T) {
	ctx := context.Background()
	node := store(t, "n1")
	s := &scriptedStream{frames: []*syncpb.Frame{
		{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
	}}
	if _, err := lfsync.Exchange(ctx, &failVersionLog{Store: node, failOnCall: 2}, nil, s, false); !errors.Is(err, errStoreVersion) {
		t.Fatalf("Exchange err = %v, want the version error", err)
	}
}

// TestExchangeInitiatorFailsWhenSinceReadsFail: a storage read error while
// collecting records to push aborts the exchange. The peer's clean hang-up
// (io.EOF in drain) is also exercised here.
func TestExchangeInitiatorFailsWhenSinceReadsFail(t *testing.T) {
	ctx := context.Background()
	node := store(t, "n1")
	s := &scriptedStream{frames: []*syncpb.Frame{
		{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
	}}
	if _, err := lfsync.Exchange(ctx, &failSinceLog{node}, nil, s, true); !errors.Is(err, errStoreSince) {
		t.Fatalf("Exchange err = %v, want the since error", err)
	}
}

// TestExchangeInitiatorFailsWhenBatchSendFails: a mid-stream write error on a
// batch frame surfaces through the exchange.
func TestExchangeInitiatorFailsWhenBatchSendFails(t *testing.T) {
	ctx := context.Background()
	node := store(t, "n1")
	appendN(t, node, 1) // so a batch is actually sent
	s := &failNthSendStream{
		Stream: &scriptedStream{frames: []*syncpb.Frame{
			{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
		}},
		failOn: 2, // hello succeeds, the batch fails
	}
	if _, err := lfsync.Exchange(ctx, node, nil, s, true); !errors.Is(err, errStreamReset) {
		t.Fatalf("Exchange err = %v, want the write error", err)
	}
}

// TestExchangeInitiatorFailsWhenAckSendFails: a write error on the final Ack
// surfaces through the exchange just like one on a batch.
func TestExchangeInitiatorFailsWhenAckSendFails(t *testing.T) {
	ctx := context.Background()
	node := store(t, "n1")
	appendN(t, node, 1)
	s := &failNthSendStream{
		Stream: &scriptedStream{frames: []*syncpb.Frame{
			{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
		}},
		failOn: 3, // hello and batch succeed, the ack fails
	}
	if _, err := lfsync.Exchange(ctx, node, nil, s, true); !errors.Is(err, errStreamReset) {
		t.Fatalf("Exchange err = %v, want the write error", err)
	}
}

// TestExchangeInitiatorFailsWhenDrainReceiveFails: a peer that hangs up with an
// error instead of finishing cleanly aborts the exchange.
func TestExchangeInitiatorFailsWhenDrainReceiveFails(t *testing.T) {
	ctx := context.Background()
	node := store(t, "n1")
	s := &abortStream{
		frames: []*syncpb.Frame{
			{Body: &syncpb.Frame_Hello{Hello: &syncpb.Hello{NodeId: "central"}}},
		},
		err: errStreamAbort,
	}
	if _, err := lfsync.Exchange(ctx, node, nil, s, true); !errors.Is(err, errStreamAbort) {
		t.Fatalf("Exchange err = %v, want the receive error", err)
	}
}
