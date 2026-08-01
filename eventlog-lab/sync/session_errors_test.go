package sync

import (
	"context"
	"errors"
	"io"
	"iter"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// fakeLog is a minimal crdt.SQLLog double: every method is backed by an
// overridable func field, defaulting to an inert zero-value implementation.
// It exists to reach error branches (Cursor, SetCursor, Since) that a real
// SQLite-backed replica cannot be made to fail on demand.
type fakeLog struct {
	cursorFn    func(ctx context.Context, peer clock.NodeID) (eventlog.Seq, error)
	setCursorFn func(ctx context.Context, peer clock.NodeID, last eventlog.Seq) error
	sinceFn     func(ctx context.Context, vv eventlog.VersionVector) iter.Seq2[eventlog.Event, error]
}

func (f *fakeLog) Append(context.Context, eventlog.Event) error { return nil }
func (f *fakeLog) AppendLocal(context.Context, func(eventlog.Seq) eventlog.Event) (eventlog.Event, error) {
	return eventlog.Event{}, nil
}
func (f *fakeLog) Since(ctx context.Context, vv eventlog.VersionVector) iter.Seq2[eventlog.Event, error] {
	if f.sinceFn != nil {
		return f.sinceFn(ctx, vv)
	}
	return func(func(eventlog.Event, error) bool) {}
}
func (f *fakeLog) VersionVector(context.Context) (eventlog.VersionVector, error) {
	return eventlog.VersionVector{}, nil
}
func (f *fakeLog) LoadSnapshot(context.Context, string) ([]byte, eventlog.VersionVector, error) {
	return nil, nil, eventlog.ErrNoSnapshot
}
func (f *fakeLog) SaveSnapshot(context.Context, string, []byte, eventlog.VersionVector) error {
	return nil
}
func (f *fakeLog) Cursor(ctx context.Context, peer clock.NodeID) (eventlog.Seq, error) {
	if f.cursorFn != nil {
		return f.cursorFn(ctx, peer)
	}
	return 0, nil
}
func (f *fakeLog) SetCursor(ctx context.Context, peer clock.NodeID, last eventlog.Seq) error {
	if f.setCursorFn != nil {
		return f.setCursorFn(ctx, peer, last)
	}
	return nil
}
func (f *fakeLog) Compact(context.Context, eventlog.VersionVector) error { return nil }
func (f *fakeLog) Close() error                                          { return nil }
func (f *fakeLog) EventsForSKU(context.Context, string, eventlog.VersionVector) iter.Seq2[eventlog.Event, error] {
	return func(func(eventlog.Event, error) bool) {}
}
func (f *fakeLog) CountForSKU(context.Context, string) (int, error) { return 0, nil }

// fakeReplica is a Replica double whose VersionVector and Merge behaviour is
// overridable, so error handling in server.go/client.go can be exercised
// without a real SQLite log misbehaving on cue.
type fakeReplica struct {
	id      clock.NodeID
	log     *fakeLog
	vvFn    func(ctx context.Context) (eventlog.VersionVector, error)
	vvCall  int
	mergeFn func(ctx context.Context, events []eventlog.Event) (int, error)
}

func (r *fakeReplica) ID() clock.NodeID { return r.id }
func (r *fakeReplica) VersionVector(ctx context.Context) (eventlog.VersionVector, error) {
	r.vvCall++
	if r.vvFn != nil {
		return r.vvFn(ctx)
	}
	return eventlog.VersionVector{}, nil
}
func (r *fakeReplica) Merge(ctx context.Context, events []eventlog.Event) (int, error) {
	if r.mergeFn != nil {
		return r.mergeFn(ctx, events)
	}
	return len(events), nil
}
func (r *fakeReplica) Log() crdt.SQLLog { return r.log }

// failNTimesStream fails Send on the call index listed in failOn (0-based) and
// otherwise succeeds; Recv drains from in.
type failNTimesStream struct {
	in       []*syncpb.ClientFrame
	i        int
	sendN    int
	sendErr  error
	sendSeen int
}

func (s *failNTimesStream) Recv() (*syncpb.ClientFrame, error) {
	if s.i >= len(s.in) {
		return nil, io.EOF
	}
	f := s.in[s.i]
	s.i++
	return f, nil
}

func (s *failNTimesStream) Send(*syncpb.ServerFrame) error {
	s.sendSeen++
	if s.sendSeen == s.sendN {
		return s.sendErr
	}
	return nil
}

func TestNewServerAndClientDefaultBatchSize(t *testing.T) {
	srv := NewServer(&fakeReplica{id: "A", log: &fakeLog{}}, 0)
	if srv.batchSize != defaultBatchSize {
		t.Fatalf("Server batchSize = %d, want %d", srv.batchSize, defaultBatchSize)
	}
	cl := NewClient(&fakeReplica{id: "A", log: &fakeLog{}}, failingDialer{}, -1)
	if cl.batchSize != defaultBatchSize {
		t.Fatalf("Client batchSize = %d, want %d", cl.batchSize, defaultBatchSize)
	}
}

func TestServerSessionSurfacesReplicaErrors(t *testing.T) {
	boom := errors.New("boom")
	hello := []*syncpb.ClientFrame{{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}}}

	tests := []struct {
		name      string
		srv       func() *Server
		in        []*syncpb.ClientFrame
		sendFailN int
		recvFails bool
	}{
		{
			name: "cursor lookup fails",
			srv: func() *Server {
				return NewServer(&fakeReplica{id: "B", log: &fakeLog{
					cursorFn: func(context.Context, clock.NodeID) (eventlog.Seq, error) { return 0, boom },
				}}, 8)
			},
			in: hello,
		},
		{
			name: "version vector fails",
			srv: func() *Server {
				return NewServer(&fakeReplica{id: "B", log: &fakeLog{}, vvFn: func(context.Context) (eventlog.VersionVector, error) {
					return nil, boom
				}}, 8)
			},
			in: hello,
		},
		{
			name: "send welcome fails",
			srv: func() *Server {
				return NewServer(&fakeReplica{id: "B", log: &fakeLog{}}, 8)
			},
			in:        hello,
			sendFailN: 1,
		},
		{
			name: "stream events fails",
			srv: func() *Server {
				return NewServer(&fakeReplica{id: "B", log: &fakeLog{
					sinceFn: func(context.Context, eventlog.VersionVector) iter.Seq2[eventlog.Event, error] {
						return func(yield func(eventlog.Event, error) bool) {
							yield(eventlog.Event{}, boom)
						}
					},
				}}, 8)
			},
			in: hello,
		},
		{
			name: "send ack fails",
			srv: func() *Server {
				return NewServer(&fakeReplica{id: "B", log: &fakeLog{}}, 8)
			},
			in:        hello,
			sendFailN: 2,
		},
		{
			name: "merge fails",
			srv: func() *Server {
				return NewServer(&fakeReplica{id: "B", log: &fakeLog{}, mergeFn: func(context.Context, []eventlog.Event) (int, error) {
					return 0, boom
				}}, 8)
			},
			in: append(append([]*syncpb.ClientFrame{}, hello...),
				&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Events{Events: &syncpb.Events{Events: []*syncpb.Event{
					{NodeId: "A", Seq: 1, HlcWall: 10, Sku: "SKU-1", Kind: int32(eventlog.KindQuantityDelta), Delta: 1},
				}}}}),
		},
		{
			name: "set cursor fails",
			srv: func() *Server {
				return NewServer(&fakeReplica{id: "B", log: &fakeLog{
					setCursorFn: func(context.Context, clock.NodeID, eventlog.Seq) error { return boom },
				}}, 8)
			},
			in: append(append([]*syncpb.ClientFrame{}, hello...),
				&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Ack{Ack: &syncpb.Ack{VersionVector: map[string]uint64{"A": 1}}}}),
		},
		{
			name: "recv after hello fails",
			srv: func() *Server {
				return NewServer(&fakeReplica{id: "B", log: &fakeLog{}}, 8)
			},
			in:        hello,
			recvFails: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var st ServerStream
			if tt.sendFailN > 0 {
				st = &failNTimesStream{in: tt.in, sendN: tt.sendFailN, sendErr: boom}
			} else if tt.recvFails {
				st = &recvFailAfterStream{in: tt.in, failAfter: len(tt.in)}
			} else {
				st = &scriptedServerStream{in: tt.in}
			}
			if err := tt.srv().Session(context.Background(), st); err == nil {
				t.Fatal("Session() error = nil, want an error")
			}
		})
	}
}

// recvFailAfterStream drains in, then fails on the next Recv instead of EOF.
type recvFailAfterStream struct {
	in        []*syncpb.ClientFrame
	i         int
	failAfter int
}

func (s *recvFailAfterStream) Recv() (*syncpb.ClientFrame, error) {
	if s.i < len(s.in) {
		f := s.in[s.i]
		s.i++
		return f, nil
	}
	return nil, errors.New("transport broke")
}

func (s *recvFailAfterStream) Send(*syncpb.ServerFrame) error { return nil }

func TestServerStreamEventsRejectsInvalidLocalEvent(t *testing.T) {
	srv := NewServer(&fakeReplica{id: "B", log: &fakeLog{
		sinceFn: func(context.Context, eventlog.VersionVector) iter.Seq2[eventlog.Event, error] {
			return func(yield func(eventlog.Event, error) bool) {
				// Kind 99 is not eventlog.Valid(): encodeEvent's Validate()
				// call must reject it. In production this cannot happen --
				// a replica only ever holds events it validated on the way
				// in -- but the guard exists precisely so a corrupted local
				// row can never be replicated onward.
				yield(eventlog.Event{ID: eventlog.EventID{NodeID: "B", Seq: 1}, Kind: 99}, nil)
			}
		},
	}}, 8)
	hello := []*syncpb.ClientFrame{{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}}}
	if err := srv.Session(context.Background(), &scriptedServerStream{in: hello}); err == nil {
		t.Fatal("Session() error = nil, want the invalid local event to be rejected")
	}
}

func TestServerStreamEventsFlushMidLoopFails(t *testing.T) {
	boom := errors.New("boom")
	srv := NewServer(&fakeReplica{id: "B", log: &fakeLog{
		sinceFn: func(context.Context, eventlog.VersionVector) iter.Seq2[eventlog.Event, error] {
			return func(yield func(eventlog.Event, error) bool) {
				for i := 1; i <= 2; i++ {
					e := eventlog.Event{
						ID:  eventlog.EventID{NodeID: "B", Seq: eventlog.Seq(i)},
						HLC: clock.HLC{Wall: int64(i), NodeID: "B"},
						SKU: "SKU-1", Kind: eventlog.KindQuantityDelta, Delta: 1,
					}
					if !yield(e, nil) {
						return
					}
				}
			}
		},
	}}, 1)
	hello := []*syncpb.ClientFrame{{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}}}
	// batchSize 1 means the first Send (welcome) succeeds, the second Send
	// (the first Events batch) fails.
	st := &failNTimesStream{in: hello, sendN: 2, sendErr: boom}
	if err := srv.Session(context.Background(), st); err == nil {
		t.Fatal("Session() error = nil, want the mid-loop flush failure to propagate")
	}
}

// --- client-side error surfacing ---

type scriptedClientStream struct {
	in        []*syncpb.ServerFrame
	i         int
	sendFailN int
	sendSeen  int
	sendErr   error
	closeErr  error
}

func (s *scriptedClientStream) Send(*syncpb.ClientFrame) error {
	s.sendSeen++
	if s.sendFailN > 0 && s.sendSeen == s.sendFailN {
		return s.sendErr
	}
	return nil
}

func (s *scriptedClientStream) Recv() (*syncpb.ServerFrame, error) {
	if s.i >= len(s.in) {
		return nil, io.EOF
	}
	f := s.in[s.i]
	s.i++
	return f, nil
}

func (s *scriptedClientStream) CloseSend() error { return s.closeErr }
func (s *scriptedClientStream) Close() error     { return nil }

type stubDialer struct {
	st  Stream
	err error
}

func (d stubDialer) Dial(context.Context, string) (Stream, error) { return d.st, d.err }

func TestClientSyncOnceSurfacesReplicaAndStreamErrors(t *testing.T) {
	boom := errors.New("boom")
	welcome := &syncpb.ServerFrame{Body: &syncpb.ServerFrame_Welcome{Welcome: &syncpb.Welcome{NodeId: "B"}}}
	ack := &syncpb.ServerFrame{Body: &syncpb.ServerFrame_Ack{Ack: &syncpb.Ack{}}}

	tests := []struct {
		name    string
		replica *fakeReplica
		dialer  Dialer
	}{
		{
			name:    "initial version vector fails",
			replica: &fakeReplica{id: "A", log: &fakeLog{}, vvFn: func(context.Context) (eventlog.VersionVector, error) { return nil, boom }},
			dialer:  stubDialer{},
		},
		{
			name:    "loop recv fails after welcome",
			replica: &fakeReplica{id: "A", log: &fakeLog{}},
			dialer: stubDialer{st: &recvFailAfterClientStream{
				in: []*syncpb.ServerFrame{welcome},
			}},
		},
		{
			name:    "unexpected frame in loop",
			replica: &fakeReplica{id: "A", log: &fakeLog{}},
			dialer: stubDialer{st: &scriptedClientStream{in: []*syncpb.ServerFrame{
				welcome, {},
			}}},
		},
		{
			name:    "merge fails",
			replica: &fakeReplica{id: "A", log: &fakeLog{}, mergeFn: func(context.Context, []eventlog.Event) (int, error) { return 0, boom }},
			dialer: stubDialer{st: &scriptedClientStream{in: []*syncpb.ServerFrame{
				welcome,
				{Body: &syncpb.ServerFrame_Events{Events: &syncpb.Events{Events: []*syncpb.Event{
					{NodeId: "B", Seq: 1, HlcWall: 1, Sku: "SKU-1", Kind: int32(eventlog.KindQuantityDelta), Delta: 1},
				}}}},
			}}},
		},
		{
			name: "streamEvents send fails",
			replica: &fakeReplica{id: "A", log: &fakeLog{
				sinceFn: func(context.Context, eventlog.VersionVector) iter.Seq2[eventlog.Event, error] {
					return func(yield func(eventlog.Event, error) bool) {
						yield(eventlog.Event{
							ID:  eventlog.EventID{NodeID: "A", Seq: 1},
							HLC: clock.HLC{Wall: 1, NodeID: "A"},
							SKU: "SKU-1", Kind: eventlog.KindQuantityDelta, Delta: 1,
						}, nil)
					}
				},
			}},
			dialer: stubDialer{st: &scriptedClientStream{
				in:        []*syncpb.ServerFrame{welcome, ack},
				sendFailN: 2, // hello=1, then streamEvents' events send=2
				sendErr:   boom,
			}},
		},
		{
			name:    "send ack fails",
			replica: &fakeReplica{id: "A", log: &fakeLog{}},
			dialer: stubDialer{st: &scriptedClientStream{
				in:        []*syncpb.ServerFrame{welcome, ack},
				sendFailN: 2, // hello=1, ack=2 (nothing local to stream first)
				sendErr:   boom,
			}},
		},
		{
			name: "second version vector fails",
			replica: &fakeReplica{id: "A", log: &fakeLog{}, vvFn: func() func(context.Context) (eventlog.VersionVector, error) {
				calls := 0
				return func(context.Context) (eventlog.VersionVector, error) {
					calls++
					if calls == 2 {
						return nil, boom
					}
					return eventlog.VersionVector{}, nil
				}
			}()},
			dialer: stubDialer{st: &scriptedClientStream{in: []*syncpb.ServerFrame{welcome, ack}}},
		},
		{
			name:    "close send fails",
			replica: &fakeReplica{id: "A", log: &fakeLog{}},
			dialer:  stubDialer{st: &scriptedClientStream{in: []*syncpb.ServerFrame{welcome, ack}, closeErr: boom}},
		},
		{
			name: "set cursor fails",
			replica: &fakeReplica{id: "A", log: &fakeLog{
				setCursorFn: func(context.Context, clock.NodeID, eventlog.Seq) error { return boom },
			}},
			dialer: stubDialer{st: &scriptedClientStream{in: []*syncpb.ServerFrame{welcome, ack}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewClient(tt.replica, tt.dialer, 8).SyncOnce(context.Background(), "peer"); err == nil {
				t.Fatal("SyncOnce() error = nil, want an error")
			}
		})
	}
}

// recvFailAfterClientStream drains in, then fails on the next Recv.
type recvFailAfterClientStream struct {
	in []*syncpb.ServerFrame
	i  int
}

func (s *recvFailAfterClientStream) Send(*syncpb.ClientFrame) error { return nil }
func (s *recvFailAfterClientStream) CloseSend() error               { return nil }
func (s *recvFailAfterClientStream) Close() error                   { return nil }
func (s *recvFailAfterClientStream) Recv() (*syncpb.ServerFrame, error) {
	if s.i < len(s.in) {
		f := s.in[s.i]
		s.i++
		return f, nil
	}
	return nil, errors.New("transport broke")
}

func TestSyncWithBackoffUsesDefaultSleepOnRetry(t *testing.T) {
	a := &fakeReplica{id: "A", log: &fakeLog{}}
	flaky := &flakyDialer{inner: stubDialer{st: &scriptedClientStream{in: []*syncpb.ServerFrame{
		{Body: &syncpb.ServerFrame_Welcome{Welcome: &syncpb.Welcome{NodeId: "B"}}},
		{Body: &syncpb.ServerFrame_Ack{Ack: &syncpb.Ack{}}},
	}}}, failFirst: 1}
	rep, err := NewClient(a, flaky, 8).SyncWithBackoff(context.Background(), "central", 3, time.Nanosecond, nil)
	if err != nil {
		t.Fatalf("SyncWithBackoff() error = %v", err)
	}
	if rep.PeerID != "B" {
		t.Fatalf("PeerID = %q, want B", rep.PeerID)
	}
}

func TestMemoryTransportFilterHooks(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")
	if _, err := a.Receive(ctx, "SKU-1", 5); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	tr := NewMemoryTransport()
	tr.Serve("central", NewServer(b, 8))
	tr.SetFilter(func(_, _ clock.NodeID, frame any) []any {
		return []any{frame} // pass-through, but exercises the non-nil filter path
	})
	rep, err := NewClient(a, tr, 8).SyncOnce(ctx, "central")
	if err != nil {
		t.Fatalf("SyncOnce() error = %v", err)
	}
	if rep.Sent != 1 {
		t.Fatalf("Report = %+v, want Sent 1", rep)
	}

	// A filter returning the wrong frame type on each side must surface as
	// an error rather than panicking.
	tr2 := NewMemoryTransport()
	tr2.Serve("central", NewServer(newNode(t, "B2"), 8))
	tr2.SetFilter(func(_, _ clock.NodeID, _ any) []any { return []any{"not a frame"} })
	if _, err := NewClient(newNode(t, "A2"), tr2, 8).SyncOnce(ctx, "central"); err == nil {
		t.Fatal("SyncOnce() error = nil, want the client-side filter type mismatch to surface")
	}
}

func TestMemoryTransportServerSideBadFilterType(t *testing.T) {
	tr := NewMemoryTransport()
	tr.Serve("central", NewServer(newNode(t, "B"), 8))
	tr.SetFilter(func(_, _ clock.NodeID, frame any) []any {
		if _, ok := frame.(*syncpb.ClientFrame); ok {
			return []any{frame}
		}
		return []any{"not a server frame"}
	})
	if _, err := NewClient(newNode(t, "A"), tr, 8).SyncOnce(context.Background(), "central"); err == nil {
		t.Fatal("SyncOnce() error = nil, want the server-side filter type mismatch to surface")
	}
}

func TestMemoryTransportSendAfterSessionEnded(t *testing.T) {
	tr := NewMemoryTransport()
	tr.Serve("central", NewServer(newNode(t, "B"), 8))
	st, err := tr.Dial(context.Background(), "central")
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	// Send a malformed frame that makes the server session end immediately
	// (unknown body), then keep sending: the pipe's done channel should be
	// closed and further sends must report the session ended rather than
	// block forever.
	if err := st.Send(&syncpb.ClientFrame{}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let the server goroutine observe EOF-less error and exit
	for i := 0; i < 1100; i++ {
		if err := st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}}); err != nil {
			return
		}
	}
	t.Fatal("Send() never reported the ended session")
}

func TestMemoryTransportClientRecvAfterServerFailure(t *testing.T) {
	tr := NewMemoryTransport()
	tr.Serve("central", NewServer(newNode(t, "B"), 8))
	st, err := tr.Dial(context.Background(), "central")
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	if err := st.Send(&syncpb.ClientFrame{}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if _, err := st.Recv(); err == nil {
		t.Fatal("Recv() error = nil, want the server session failure surfaced")
	}
}

// TestMemoryTransportClientRecvAfterCleanServerEnd drives the wire protocol
// directly (bypassing Client) so a second Recv, issued after the server
// session ended successfully and closed toClient, hits the plain io.EOF
// branch rather than the "server session failed" branch.
func TestMemoryTransportClientRecvAfterCleanServerEnd(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()
	tr.Serve("central", NewServer(newNode(t, "B"), 8))
	st, err := tr.Dial(ctx, "central")
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	if err := st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}}); err != nil {
		t.Fatalf("Send(hello) error = %v", err)
	}
	// Drain Welcome then the server's Ack to let the session end cleanly.
	for {
		f, err := st.Recv()
		if err != nil {
			t.Fatalf("Recv() error = %v", err)
		}
		if _, ok := f.GetBody().(*syncpb.ServerFrame_Ack); ok {
			break
		}
	}
	if err := st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Ack{Ack: &syncpb.Ack{}}}); err != nil {
		t.Fatalf("Send(ack) error = %v", err)
	}
	if err := st.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}
	if _, err := st.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv() error = %v, want io.EOF", err)
	}
}

// TestMemoryTransportCloseSendAfterServerFailure covers memClientSide's
// CloseSend when the server session already ended in error.
func TestMemoryTransportCloseSendAfterServerFailure(t *testing.T) {
	tr := NewMemoryTransport()
	tr.Serve("central", NewServer(newNode(t, "B"), 8))
	st, err := tr.Dial(context.Background(), "central")
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	if err := st.Send(&syncpb.ClientFrame{}); err != nil { // unknown body: server errors immediately
		t.Fatalf("Send() error = %v", err)
	}
	if err := st.CloseSend(); err == nil {
		t.Fatal("CloseSend() error = nil, want the server session failure surfaced")
	}
}

// TestMemoryTransportServerRecvEOFWithoutAck covers memServerSide.Recv's
// plain io.EOF branch: the client closes its send side before ever sending
// an Ack, so the server's next Recv observes a closed channel with no
// error recorded.
func TestMemoryTransportServerRecvEOFWithoutAck(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTransport()
	tr.Serve("central", NewServer(newNode(t, "B"), 8))
	st, err := tr.Dial(ctx, "central")
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	if err := st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}}); err != nil {
		t.Fatalf("Send(hello) error = %v", err)
	}
	if err := st.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v, want the server to end cleanly on EOF before Ack", err)
	}
}

// TestServerRecvHelloTransportError makes the very first Recv (inside
// recvHello) fail with a hard transport error rather than a malformed frame.
func TestServerRecvHelloTransportError(t *testing.T) {
	srv := NewServer(&fakeReplica{id: "B", log: &fakeLog{}}, 8)
	st := &recvFailAfterStream{in: nil}
	if err := srv.Session(context.Background(), st); err == nil {
		t.Fatal("Session() error = nil, want the transport error from recvHello to surface")
	}
}

// TestServerConsumeEndsOnEOFWithoutAck exercises the plain io.EOF exit from
// consume(), reached when the stream ends before an Ack ever arrives (e.g. a
// dropped connection after Hello).
func TestServerConsumeEndsOnEOFWithoutAck(t *testing.T) {
	srv := NewServer(&fakeReplica{id: "B", log: &fakeLog{}}, 8)
	hello := []*syncpb.ClientFrame{{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}}}
	if err := srv.Session(context.Background(), &scriptedServerStream{in: hello}); err != nil {
		t.Fatalf("Session() error = %v, want a clean return on stream EOF before Ack", err)
	}
}

// TestWarnOnRegressionLogsWhenPeerClaimsLessThanItsCursor exercises the
// regression-warning branch directly: a peer's claimed version vector below
// what the server previously recorded as acked for it.
func TestWarnOnRegressionLogsWhenPeerClaimsLessThanItsCursor(t *testing.T) {
	srv := NewServer(&fakeReplica{id: "B", log: &fakeLog{
		cursorFn: func(context.Context, clock.NodeID) (eventlog.Seq, error) { return 5, nil },
	}}, 8)
	if err := srv.warnOnRegression(context.Background(), "A", eventlog.VersionVector{"A": 2}); err != nil {
		t.Fatalf("warnOnRegression() error = %v, want nil (a regression is logged, not fatal)", err)
	}
}

// TestServerConsumeRejectsUnexpectedFrame covers the consume() loop's default
// case: a frame that is neither Events, Ack, nor Hello.
func TestServerConsumeRejectsUnexpectedFrame(t *testing.T) {
	srv := NewServer(&fakeReplica{id: "B", log: &fakeLog{}}, 8)
	in := []*syncpb.ClientFrame{
		{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}},
		{},
	}
	if err := srv.Session(context.Background(), &scriptedServerStream{in: in}); err == nil {
		t.Fatal("Session() error = nil, want the unexpected frame in consume() to surface")
	}
}
