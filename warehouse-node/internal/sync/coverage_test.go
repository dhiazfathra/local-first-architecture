package syncrepl

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"; matches eventlog's own

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/arbiter"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// errBoom is returned by the fakes below whenever a test wants a call to fail.
var errBoom = errors.New("boom")

// errStore wraps a central.Store and forces one named method to fail on its Nth call
// (1-indexed; 0 means every call), delegating everything else through unchanged. It
// exercises the store-error branches inside server.go without a real closeable store
// for each one.
type errStore struct {
	central.Store
	method string
	failOn int
	calls  int
}

func (e *errStore) fail(method string) error {
	if e.method != method {
		return nil
	}
	e.calls++
	if e.failOn == 0 || e.calls == e.failOn {
		return errBoom
	}
	return nil
}

func (e *errStore) PushedSeq(ctx context.Context, node domain.NodeID) (uint64, error) {
	if err := e.fail("PushedSeq"); err != nil {
		return 0, err
	}
	return e.Store.PushedSeq(ctx, node)
}

func (e *errStore) SetPushedSeq(ctx context.Context, node domain.NodeID, seq uint64) error {
	if err := e.fail("SetPushedSeq"); err != nil {
		return err
	}
	return e.Store.SetPushedSeq(ctx, node, seq)
}

func (e *errStore) DeliveredOrd(ctx context.Context, node domain.NodeID) (uint64, error) {
	if err := e.fail("DeliveredOrd"); err != nil {
		return 0, err
	}
	return e.Store.DeliveredOrd(ctx, node)
}

func (e *errStore) SetDeliveredOrd(ctx context.Context, node domain.NodeID, ord uint64) error {
	if err := e.fail("SetDeliveredOrd"); err != nil {
		return err
	}
	return e.Store.SetDeliveredOrd(ctx, node, ord)
}

func (e *errStore) Outbound(ctx context.Context, target domain.NodeID, afterOrd uint64, limit int) ([]central.Outbound, error) {
	if err := e.fail("Outbound"); err != nil {
		return nil, err
	}
	return e.Store.Outbound(ctx, target, afterOrd, limit)
}

func (e *errStore) Decision(ctx context.Context, id domain.EventID) (central.Decision, bool, error) {
	if err := e.fail("Decision"); err != nil {
		return central.Decision{}, false, err
	}
	return e.Store.Decision(ctx, id)
}

// fakeServerStream is a scripted syncpb.Sync_ReplicateServer: it replays a fixed
// sequence of inbound frames (optionally erroring at a given index) and can be told
// to fail every outbound Send, so every Recv/Send error branch in server.go can be
// hit deterministically without a real network.
type fakeServerStream struct {
	ctx       context.Context
	recv      []*syncpb.NodeFrame
	recvErrAt int
	recvErr   error
	idx       int
	sendErr   error
	sent      []*syncpb.CentralFrame
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func (f *fakeServerStream) Recv() (*syncpb.NodeFrame, error) {
	if f.recvErr != nil && f.idx == f.recvErrAt {
		return nil, f.recvErr
	}
	if f.idx >= len(f.recv) {
		return nil, io.EOF
	}
	m := f.recv[f.idx]
	f.idx++
	return m, nil
}

func (f *fakeServerStream) Send(m *syncpb.CentralFrame) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) SendMsg(_ any) error          { return nil }
func (f *fakeServerStream) RecvMsg(_ any) error          { return nil }

var _ syncpb.Sync_ReplicateServer = (*fakeServerStream)(nil)

func newStore(t *testing.T) central.Store {
	t.Helper()
	store := central.NewMemory()
	seedCentral(t, store, 1000)
	return store
}

func helloFrame(id string) *syncpb.NodeFrame {
	return &syncpb.NodeFrame{Body: &syncpb.NodeFrame_Hello{Hello: &syncpb.Hello{NodeId: id}}}
}

func eventsFrame(more bool) *syncpb.NodeFrame {
	return &syncpb.NodeFrame{Body: &syncpb.NodeFrame_Events{Events: &syncpb.EventBatch{More: more}}}
}

func ackFrame(ord uint64) *syncpb.NodeFrame {
	return &syncpb.NodeFrame{Body: &syncpb.NodeFrame_Ack{Ack: &syncpb.Ack{Ord: ord}}}
}

func TestReplicateFirstRecvError(t *testing.T) {
	s := NewServer(newStore(t), arbiter.New(newStore(t), func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(), recvErr: errBoom, recvErrAt: 0}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestReplicatePushedSeqError(t *testing.T) {
	store := &errStore{Store: newStore(t), method: "PushedSeq"}
	s := NewServer(store, arbiter.New(store, func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(), recv: []*syncpb.NodeFrame{helloFrame("wh-a")}}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestReplicateWelcomeSendError(t *testing.T) {
	s := NewServer(newStore(t), arbiter.New(newStore(t), func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a")}, sendErr: errBoom}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestReplicatePushError(t *testing.T) {
	store := &errStore{Store: newStore(t), method: "DeliveredOrd"}
	s := NewServer(store, arbiter.New(store, func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false)}}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestReplicateFinalRecvEOFIsNotAnError(t *testing.T) {
	s := NewServer(newStore(t), arbiter.New(newStore(t), func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false)}}
	if err := s.Replicate(stream); err != nil {
		t.Fatalf("Replicate: %v, want nil (EOF on the final Recv costs nothing)", err)
	}
}

func TestReplicateFinalRecvError(t *testing.T) {
	s := NewServer(newStore(t), arbiter.New(newStore(t), func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv:      []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false)},
		recvErrAt: 2, recvErr: errBoom}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestReplicateFinalAckSetsDeliveredOrd(t *testing.T) {
	store := &errStore{Store: newStore(t), method: "SetDeliveredOrd"}
	s := NewServer(store, arbiter.New(store, func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false), ackFrame(5)}}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestReplicateFinalAckOfZeroSkipsSettingDeliveredOrd(t *testing.T) {
	s := NewServer(newStore(t), arbiter.New(newStore(t), func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false), ackFrame(0)}}
	if err := s.Replicate(stream); err != nil {
		t.Fatalf("Replicate: %v, want nil", err)
	}
}

func TestReceiveRejectsANonEventsFrame(t *testing.T) {
	s := NewServer(newStore(t), arbiter.New(newStore(t), func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"), ackFrame(0)}}
	if err := s.Replicate(stream); err == nil {
		t.Fatal("expected an error: a non-EventBatch frame while receiving")
	}
}

func TestReceivePushedSeqError(t *testing.T) {
	store := &errStore{Store: newStore(t), method: "PushedSeq", failOn: 2}
	s := NewServer(store, arbiter.New(store, func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false)}}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestReceiveArbitrateError(t *testing.T) {
	store := &errStore{Store: newStore(t), method: "Decision"}
	s := NewServer(store, arbiter.New(store, func() time.Time { return at }))
	raw, err := domain.NewEnvelope(domain.EventID{NodeID: "wh-a", Seq: 1}, domain.HLC{Wall: 1}, at, nil,
		domain.Event{Type: domain.TypeItemUpserted, AggregateID: "WIDGET", Payload: domain.ItemUpserted{Item: widget()}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	env := EncodeEnvelope(raw)
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"),
			{Body: &syncpb.NodeFrame_Events{Events: &syncpb.EventBatch{Events: []*syncpb.Event{env}, More: false}}}}}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestReceiveSetPushedSeqError(t *testing.T) {
	store := &errStore{Store: newStore(t), method: "SetPushedSeq"}
	s := NewServer(store, arbiter.New(store, func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false)}}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestReceiveAckSendError(t *testing.T) {
	s := NewServer(newStore(t), arbiter.New(newStore(t), func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false)}}
	// The Welcome send must succeed but the Ack send must fail: script Send to fail
	// only from the second call onward.
	stream.sendErr = nil
	orig := stream
	count := 0
	wrapped := &countingSendStream{fakeServerStream: orig, failFrom: 2, count: &count}
	if err := s.Replicate(wrapped); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

// countingSendStream fails Send from the Nth call onward (1-indexed), letting a test
// allow the Welcome send through while failing a later Ack or EventBatch send.
type countingSendStream struct {
	*fakeServerStream
	failFrom int
	count    *int
}

func (c *countingSendStream) Send(m *syncpb.CentralFrame) error {
	*c.count++
	if *c.count >= c.failFrom {
		return errBoom
	}
	return c.fakeServerStream.Send(m)
}

func TestReceiveEOFEndsTheLoop(t *testing.T) {
	s := NewServer(newStore(t), arbiter.New(newStore(t), func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a")}}
	// No EventBatch at all: receive's Recv hits EOF immediately and returns nil,
	// falling through to push and the final (EOF) Recv.
	if err := s.Replicate(stream); err != nil {
		t.Fatalf("Replicate: %v, want nil", err)
	}
}

func TestPushOutboundError(t *testing.T) {
	store := &errStore{Store: newStore(t), method: "Outbound"}
	s := NewServer(store, arbiter.New(store, func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false)}}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

func TestPushRemainingOutboundErrorAndSendError(t *testing.T) {
	ctx := context.Background()
	store := central.NewMemory()
	seedCentral(t, store, 1000)
	// Queue enough events that push must chunk and re-check "remaining" via a
	// second Outbound call, and enough that a Send failure on a later chunk is
	// reachable.
	events, err := store.EmitCentral(ctx, []domain.Event{
		{Type: domain.TypeItemUpserted, AggregateID: "WIDGET", Payload: domain.ItemUpserted{Item: widget()}},
	}, nil, at)
	if err != nil {
		t.Fatalf("EmitCentral: %v", err)
	}
	if err := store.Enqueue(ctx, "wh-a", events); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	failing := &errStore{Store: store, method: "Outbound", failOn: 2}
	s := NewServer(failing, arbiter.New(failing, func() time.Time { return at }))
	stream := &fakeServerStream{ctx: ctx, recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false)}}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom for the second Outbound call", err)
	}

	// Now force the Send of that EventBatch itself to fail: Welcome and the
	// upstream Ack must succeed first, so this is the third Send of the session.
	s2 := NewServer(store, arbiter.New(store, func() time.Time { return at }))
	stream2 := &fakeServerStream{ctx: ctx, recv: []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(false)}}
	count := 0
	wrapped := &countingSendStream{fakeServerStream: stream2, failFrom: 3, count: &count}
	if err := s2.Replicate(wrapped); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom for the EventBatch send", err)
	}
}

func TestReceiveGenericRecvError(t *testing.T) {
	s := NewServer(newStore(t), arbiter.New(newStore(t), func() time.Time { return at }))
	stream := &fakeServerStream{ctx: context.Background(),
		recv:      []*syncpb.NodeFrame{helloFrame("wh-a"), eventsFrame(true)},
		recvErrAt: 2, recvErr: errBoom}
	if err := s.Replicate(stream); !errors.Is(err, errBoom) {
		t.Fatalf("Replicate: got %v, want errBoom", err)
	}
}

// --- client-side fakes ---

// fakeClientStream is a scripted syncpb.Sync_ReplicateClient.
type fakeClientStream struct {
	ctx        context.Context
	recv       []*syncpb.CentralFrame
	idx        int
	recvErrAt  int
	recvErr    error
	sendErr    error
	closeErr   error
	sent       []*syncpb.NodeFrame
	closedSend bool
}

func (f *fakeClientStream) Context() context.Context { return f.ctx }

func (f *fakeClientStream) Send(m *syncpb.NodeFrame) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeClientStream) Recv() (*syncpb.CentralFrame, error) {
	if f.recvErr != nil && f.idx == f.recvErrAt {
		return nil, f.recvErr
	}
	if f.idx >= len(f.recv) {
		return nil, io.EOF
	}
	m := f.recv[f.idx]
	f.idx++
	return m, nil
}

func (f *fakeClientStream) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeClientStream) Trailer() metadata.MD         { return nil }
func (f *fakeClientStream) CloseSend() error {
	f.closedSend = true
	return f.closeErr
}
func (f *fakeClientStream) SendMsg(_ any) error { return nil }
func (f *fakeClientStream) RecvMsg(_ any) error { return nil }

var _ syncpb.Sync_ReplicateClient = (*fakeClientStream)(nil)

// fakeSyncClient hands back a pre-built stream or an error, so Client.Session's
// "open replication stream" failure is reachable without a real dial.
type fakeSyncClient struct {
	stream syncpb.Sync_ReplicateClient
	err    error
}

func (f *fakeSyncClient) Replicate(_ context.Context, _ ...grpc.CallOption) (syncpb.Sync_ReplicateClient, error) {
	return f.stream, f.err
}

func welcomeFrame(known uint64) *syncpb.CentralFrame {
	return &syncpb.CentralFrame{Body: &syncpb.CentralFrame_Welcome{Welcome: &syncpb.Welcome{KnownSeq: known}}}
}

func serverAckFrame(seq uint64) *syncpb.CentralFrame {
	return &syncpb.CentralFrame{Body: &syncpb.CentralFrame_Ack{Ack: &syncpb.Ack{Seq: seq}}}
}

func downstreamFrame(more bool, lastOrd uint64) *syncpb.CentralFrame {
	return &syncpb.CentralFrame{Body: &syncpb.CentralFrame_Events{Events: &syncpb.EventBatch{More: more, LastOrd: lastOrd}}}
}

func TestSessionOpenStreamError(t *testing.T) {
	svc := startNode(t, "wh-a")
	client := NewClient(svc, &fakeSyncClient{err: errBoom})
	if err := client.Session(context.Background()); err == nil {
		t.Fatal("expected an error opening the replication stream")
	}
}

func TestSessionHelloSendError(t *testing.T) {
	svc := startNode(t, "wh-a")
	stream := &fakeClientStream{ctx: context.Background(), sendErr: errBoom}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Session: got %v, want errBoom", err)
	}
}

func TestSessionWelcomeRecvError(t *testing.T) {
	svc := startNode(t, "wh-a")
	stream := &fakeClientStream{ctx: context.Background(), recvErr: errBoom, recvErrAt: 0}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Session: got %v, want errBoom", err)
	}
}

func TestSessionWithoutWelcomeIsRejected(t *testing.T) {
	svc := startNode(t, "wh-a")
	stream := &fakeClientStream{ctx: context.Background(), recv: []*syncpb.CentralFrame{serverAckFrame(0)}}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); err == nil {
		t.Fatal("expected an error: central answered Hello with something other than Welcome")
	}
}

// startNodeAtPath is startNode but also returns the sqlite file path, so a test can
// open a second raw connection to it and break one table without touching the
// others — the only way to force one specific Log call to fail while its neighbors
// keep working, since eventlog.Log is a concrete type with no seam to mock.
func startNodeAtPath(t *testing.T, id domain.NodeID) (*node.Service, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.db")
	n := 0
	svc, err := node.Open(path, id, func() time.Time {
		n++
		return at.Add(time.Duration(n) * time.Second)
	})
	if err != nil {
		t.Fatalf("node.Open: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc, path
}

// dropTable opens a second raw connection to the node's sqlite file and drops one
// table, so the next call through that table fails while every other table keeps
// working.
func dropTable(t *testing.T, path, table string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("DROP TABLE " + table); err != nil {
		t.Fatalf("drop table %s: %v", table, err)
	}
}

func TestSessionEventsTableMissingError(t *testing.T) {
	svc, path := startNodeAtPath(t, "wh-a")
	dropTable(t, path, "events")
	client := NewClient(svc, &fakeSyncClient{stream: &fakeClientStream{ctx: context.Background()}})
	if err := client.Session(context.Background()); err == nil {
		t.Fatal("expected an error: the events table is gone, so pushBacklog cannot read it")
	}
}

func TestSessionCursorPulledError(t *testing.T) {
	svc, path := startNodeAtPath(t, "wh-a")
	dropTable(t, path, "cursors")
	client := NewClient(svc, &fakeSyncClient{stream: &fakeClientStream{ctx: context.Background()}})
	if err := client.Session(context.Background()); err == nil {
		t.Fatal("expected an error: the cursors table is gone, so Cursor cannot read it")
	}
}

// corruptCursor writes a non-numeric value for one cursor name through a second raw
// connection, so reading that specific cursor fails to scan while every other
// cursor (and every other table) keeps working — the only way to fail the second of
// two sequential Cursor calls to the same table without a mockable seam.
func corruptCursor(t *testing.T, path, name string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`INSERT INTO cursors (name, value) VALUES (?, 'not-a-number')
		ON CONFLICT (name) DO UPDATE SET value = excluded.value`, name); err != nil {
		t.Fatalf("corrupt cursor %s: %v", name, err)
	}
}

func TestSessionCursorPushedError(t *testing.T) {
	svc, path := startNodeAtPath(t, "wh-a")
	corruptCursor(t, path, "pushed_to_central")
	stream := &fakeClientStream{ctx: context.Background(), recv: []*syncpb.CentralFrame{welcomeFrame(0)}}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); err == nil {
		t.Fatal("expected an error: the pushed cursor cannot be scanned")
	}
}

func TestSessionKnownLessThanFromUsesKnown(t *testing.T) {
	svc := startNode(t, "wh-a")
	if err := svc.Log().SetCursor("pushed_to_central", 100); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	// central admits to only 0: the client must resume from 0, not the stale 100 it
	// remembered pushing, because central's answer wins over a gap.
	stream := &fakeClientStream{ctx: context.Background(),
		recv: []*syncpb.CentralFrame{welcomeFrame(0), serverAckFrame(0), downstreamFrame(false, 0)}}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); err != nil {
		t.Fatalf("Session: %v", err)
	}
}

func TestPushBacklogReadOwnAfterError(t *testing.T) {
	svc, path := startNodeAtPath(t, "wh-a")
	dropTable(t, path, "events")
	c := &Client{svc: svc}
	stream := &fakeClientStream{ctx: context.Background()}
	if err := c.pushBacklog(context.Background(), stream, 0); err == nil {
		t.Fatal("expected an error: the events table is gone, so ReadOwnAfter cannot read it")
	}
}

func TestApplyDownstreamRecvEOFEndsCleanly(t *testing.T) {
	svc := startNode(t, "wh-a")
	c := &Client{svc: svc}
	stream := &fakeClientStream{ctx: context.Background()} // no scripted frames: Recv is EOF immediately
	if err := c.applyDownstream(stream); err != nil {
		t.Fatalf("applyDownstream: %v, want nil: the stream ending is not an error here", err)
	}
}

func TestApplyDownstreamIngestError(t *testing.T) {
	svc := startNode(t, "wh-a")
	c := &Client{svc: svc}
	raw, err := domain.NewEnvelope(domain.EventID{NodeID: "central", Seq: 1}, domain.HLC{Wall: 1}, at, nil,
		domain.Event{Type: domain.TypeItemUpserted, AggregateID: "WIDGET", Payload: domain.ItemUpserted{Item: widget()}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	ev := EncodeEnvelope(raw)
	ev.Payload = []byte("not json")
	stream := &fakeClientStream{ctx: context.Background(),
		recv: []*syncpb.CentralFrame{
			{Body: &syncpb.CentralFrame_Events{Events: &syncpb.EventBatch{Events: []*syncpb.Event{ev}, More: false}}}}}
	if err := c.applyDownstream(stream); err == nil {
		t.Fatal("expected an error: a garbled payload must fail Ingest")
	}
}

func TestApplyDownstreamSetCursorPulledError(t *testing.T) {
	svc, path := startNodeAtPath(t, "wh-a")
	dropTable(t, path, "cursors")
	c := &Client{svc: svc}
	stream := &fakeClientStream{ctx: context.Background(),
		recv: []*syncpb.CentralFrame{downstreamFrame(false, 5)}}
	if err := c.applyDownstream(stream); err == nil {
		t.Fatal("expected an error: the cursors table is gone, so SetCursor cannot write it")
	}
}

func TestApplyDownstreamFinalAckSendErrorDirect(t *testing.T) {
	svc := startNode(t, "wh-a")
	c := &Client{svc: svc}
	stream := &fakeClientStream{ctx: context.Background(),
		recv:    []*syncpb.CentralFrame{downstreamFrame(false, 0)},
		sendErr: errBoom}
	if err := c.applyDownstream(stream); !errors.Is(err, errBoom) {
		t.Fatalf("applyDownstream: got %v, want errBoom", err)
	}
}

func TestRunRetriesAfterSessionFailure(t *testing.T) {
	svc := startNode(t, "wh-a")
	client := NewClient(svc, &fakeSyncClient{err: errBoom})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := client.Run(ctx, time.Millisecond, time.Millisecond); err != nil {
		t.Fatalf("Run: %v, want nil: a failing session is swallowed and retried", err)
	}
}

func TestPushBacklogSetCursorError(t *testing.T) {
	svc, path := startNodeAtPath(t, "wh-a")
	if err := svc.RegisterLocation("RECV-01", domain.LocReceiving); err != nil {
		t.Fatalf("RegisterLocation: %v", err)
	}
	if _, err := svc.Execute(func(*domain.State) ([]domain.Event, error) {
		return []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "WIDGET",
			Payload: domain.ItemUpserted{Item: widget()}}}, nil
	}); err != nil {
		t.Fatalf("seed item master: %v", err)
	}
	if _, err := svc.Execute(receive("R1", "DN-1", 1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	dropTable(t, path, "cursors")
	c := &Client{svc: svc}
	stream := &fakeClientStream{ctx: context.Background(), recv: []*syncpb.CentralFrame{serverAckFrame(1)}}
	if err := c.pushBacklog(context.Background(), stream, 0); err == nil {
		t.Fatal("expected an error: the cursors table is gone, so SetCursor cannot write the pushed cursor")
	}
}

func TestPushBacklogAckSendError(t *testing.T) {
	svc := startNode(t, "wh-a")
	if _, err := svc.Execute(receive("R1", "DN-1", 1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stream := &fakeClientStream{ctx: context.Background(), recv: []*syncpb.CentralFrame{welcomeFrame(0)}}
	count := 0
	wrapped := &countingClientSendStream{fakeClientStream: stream, failFrom: 2, count: &count}
	client := NewClient(svc, &fakeSyncClient{stream: wrapped})
	if err := client.Session(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Session: got %v, want errBoom for the backlog batch send", err)
	}
}

// countingClientSendStream fails Send from the Nth call onward (1-indexed).
type countingClientSendStream struct {
	*fakeClientStream
	failFrom int
	count    *int
}

func (c *countingClientSendStream) Send(m *syncpb.NodeFrame) error {
	*c.count++
	if *c.count >= c.failFrom {
		return errBoom
	}
	return c.fakeClientStream.Send(m)
}

func TestPushBacklogAckRecvError(t *testing.T) {
	svc := startNode(t, "wh-a")
	if _, err := svc.Execute(receive("R1", "DN-1", 1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stream := &fakeClientStream{ctx: context.Background(),
		recv:      []*syncpb.CentralFrame{welcomeFrame(0)},
		recvErrAt: 1, recvErr: errBoom}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Session: got %v, want errBoom for the batch ack recv", err)
	}
}

func TestPushBacklogWithoutAckIsRejected(t *testing.T) {
	svc := startNode(t, "wh-a")
	if _, err := svc.Execute(receive("R1", "DN-1", 1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stream := &fakeClientStream{ctx: context.Background(),
		recv: []*syncpb.CentralFrame{welcomeFrame(0), downstreamFrame(false, 0)}}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); err == nil {
		t.Fatal("expected an error: central acked with something other than an Ack")
	}
}

func TestPushBacklogNothingToSendStillProbesForMore(t *testing.T) {
	svc := startNode(t, "wh-a")
	// No local events at all beyond the seeded item master, which is already at
	// cursor 0: pushBacklog must still send one empty, non-more batch.
	stream := &fakeClientStream{ctx: context.Background(),
		recv: []*syncpb.CentralFrame{welcomeFrame(1000), serverAckFrame(0), downstreamFrame(false, 0)}}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); err != nil {
		t.Fatalf("Session: %v", err)
	}
}

func TestApplyDownstreamNonEventsFrameIsRejected(t *testing.T) {
	svc := startNode(t, "wh-a")
	stream := &fakeClientStream{ctx: context.Background(),
		recv: []*syncpb.CentralFrame{welcomeFrame(0), serverAckFrame(0), welcomeFrame(0)}}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); err == nil {
		t.Fatal("expected an error: central sent a non-EventBatch frame downstream")
	}
}

func TestApplyDownstreamUndecodableEventIsRejected(t *testing.T) {
	svc := startNode(t, "wh-a")
	stream := &fakeClientStream{ctx: context.Background(),
		recv: []*syncpb.CentralFrame{welcomeFrame(0), serverAckFrame(0),
			{Body: &syncpb.CentralFrame_Events{Events: &syncpb.EventBatch{Events: []*syncpb.Event{{Type: "Nonsense"}}}}}}}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); err == nil {
		t.Fatal("expected an error: an undecodable event downstream must fail the session")
	}
}

func TestApplyDownstreamRecvError(t *testing.T) {
	svc := startNode(t, "wh-a")
	stream := &fakeClientStream{ctx: context.Background(),
		recv:      []*syncpb.CentralFrame{welcomeFrame(0), serverAckFrame(0)},
		recvErrAt: 2, recvErr: errBoom}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Session: got %v, want errBoom", err)
	}
}

func TestApplyDownstreamFinalAckSendError(t *testing.T) {
	svc := startNode(t, "wh-a")
	stream := &fakeClientStream{ctx: context.Background(),
		recv: []*syncpb.CentralFrame{welcomeFrame(0), serverAckFrame(0), downstreamFrame(false, 0)}}
	count := 0
	wrapped := &countingClientSendStream{fakeClientStream: stream, failFrom: 2, count: &count}
	client := NewClient(svc, &fakeSyncClient{stream: wrapped})
	if err := client.Session(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Session: got %v, want errBoom for the final Ack send", err)
	}
}

func TestApplyDownstreamCloseSendError(t *testing.T) {
	svc := startNode(t, "wh-a")
	stream := &fakeClientStream{ctx: context.Background(),
		recv:     []*syncpb.CentralFrame{welcomeFrame(0), serverAckFrame(0), downstreamFrame(false, 0)},
		closeErr: errBoom}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Session: got %v, want errBoom from CloseSend", err)
	}
}

func TestApplyDownstreamAppliesLastOrdAcrossChunks(t *testing.T) {
	svc := startNode(t, "wh-a")
	stream := &fakeClientStream{ctx: context.Background(),
		recv: []*syncpb.CentralFrame{welcomeFrame(0), serverAckFrame(0),
			downstreamFrame(true, 3), downstreamFrame(false, 7)}}
	client := NewClient(svc, &fakeSyncClient{stream: stream})
	if err := client.Session(context.Background()); err != nil {
		t.Fatalf("Session: %v", err)
	}
	pulled, err := svc.Log().Cursor("pulled_from_central")
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if pulled != 7 {
		t.Errorf("pulled cursor = %d, want 7: the highest LastOrd across every chunk", pulled)
	}
}

func TestRunAdvancesOnASuccessfulSession(t *testing.T) {
	store := central.NewMemory()
	seedCentral(t, store, 1000)
	svc := startNode(t, "wh-a")
	client := NewClient(svc, startCentral(t, store))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.Run(ctx, time.Millisecond, time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
