package sync

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/node"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

func newNode(t *testing.T, id clock.NodeID) *node.Node {
	t.Helper()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), string(id)+".db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	wall := int64(1000)
	n, err := node.New(node.Config{
		ID: id, Log: l, Wall: func() int64 { wall += 10; return wall }, SnapshotEvery: 4,
	})
	if err != nil {
		t.Fatalf("node.New() error = %v", err)
	}
	return n
}

func syncPair(t *testing.T, client, server *node.Node, batch int) Report {
	t.Helper()
	tr := NewMemoryTransport()
	tr.Serve("central", NewServer(server, batch))
	rep, err := NewClient(client, tr, batch).SyncOnce(context.Background(), "central")
	if err != nil {
		t.Fatalf("SyncOnce() error = %v", err)
	}
	return rep
}

func TestSyncOnceExchangesBothDirections(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")

	if _, err := a.Receive(ctx, "SKU-1", 10); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if _, err := b.Pick(ctx, "SKU-1", 4); err != nil {
		t.Fatalf("Pick() error = %v", err)
	}

	rep := syncPair(t, a, b, 8)
	if rep.Sent != 1 || rep.Received != 1 {
		t.Fatalf("Report = %+v, want Sent 1 / Received 1", rep)
	}
	if rep.PeerID != "B" {
		t.Fatalf("PeerID = %q, want B", rep.PeerID)
	}

	for _, n := range []*node.Node{a, b} {
		state, err := n.Get(ctx, "SKU-1")
		if err != nil {
			t.Fatalf("Get() on %q error = %v", n.ID(), err)
		}
		if state.Quantity() != 6 {
			t.Fatalf("node %q Quantity() = %d, want 6", n.ID(), state.Quantity())
		}
	}
}

func TestSyncOnceIsIdempotentAndBatchesCorrectly(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		batch     int
		aEvents   int
		bEvents   int
		wantSent  int
		wantRecvd int
	}{
		{"single batch", 8, 2, 3, 2, 3},
		{"batch size one forces many frames", 1, 3, 2, 3, 2},
		{"nothing to exchange", 8, 0, 0, 0, 0},
		{"one-sided", 8, 3, 0, 3, 0},
		{"exact multiple of batch size", 2, 4, 2, 4, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := newNode(t, "A"), newNode(t, "B")
			for i := 0; i < tt.aEvents; i++ {
				if _, err := a.Receive(ctx, "SKU-1", 1); err != nil {
					t.Fatalf("Receive() error = %v", err)
				}
			}
			for i := 0; i < tt.bEvents; i++ {
				if _, err := b.Receive(ctx, "SKU-1", 1); err != nil {
					t.Fatalf("Receive() error = %v", err)
				}
			}

			rep := syncPair(t, a, b, tt.batch)
			if rep.Sent != tt.wantSent || rep.Received != tt.wantRecvd {
				t.Fatalf("Report = %+v, want Sent %d / Received %d", rep, tt.wantSent, tt.wantRecvd)
			}

			// A second session must exchange nothing: both sides converged.
			again := syncPair(t, a, b, tt.batch)
			if again.Sent != 0 || again.Received != 0 {
				t.Fatalf("second session = %+v, want zero in both directions", again)
			}

			av, err := a.VersionVector(ctx)
			if err != nil {
				t.Fatalf("VersionVector() error = %v", err)
			}
			bv, err := b.VersionVector(ctx)
			if err != nil {
				t.Fatalf("VersionVector() error = %v", err)
			}
			if !av.Dominates(bv) || !bv.Dominates(av) {
				t.Fatalf("vectors diverged: A = %v, B = %v", av, bv)
			}
		})
	}
}

func TestSyncPersistsCursorsForResume(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")
	for i := 0; i < 3; i++ {
		if _, err := a.Receive(ctx, "SKU-1", 1); err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
	}
	syncPair(t, a, b, 8)

	got, err := b.Log().Cursor(ctx, "A")
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	if got != 3 {
		t.Fatalf("server cursor for A = %d, want 3", got)
	}
	if got, err = a.Log().Cursor(ctx, "B"); err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	if got != 0 {
		t.Fatalf("client cursor for B = %d, want 0 (B emitted nothing)", got)
	}
}

func TestSyncToleratesPeerVersionVectorRegression(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")
	for i := 0; i < 3; i++ {
		if _, err := a.Receive(ctx, "SKU-1", 1); err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
	}
	syncPair(t, a, b, 8)

	// B forgets everything it learned by coming back with an empty log under
	// the SAME peer identity "B" -- reusing the identity is what makes this a
	// genuine regression instead of a first-ever sync. A already has a
	// cursor for "B" recording seq 3 from the sync above; a brand-new peer
	// id (e.g. "B2") would have no prior cursor at all, and warnOnRegression
	// would never fire because there is nothing to regress from. With the
	// same identity, B's fresh Welcome reports vv={} -- below the cursor A
	// holds -- so A must re-send from the lower point, and idempotent
	// Append must absorb it.
	forgetful := newNode(t, "B")
	rep := syncPair(t, a, forgetful, 8)
	if rep.Sent != 3 {
		t.Fatalf("re-send after regression Sent = %d, want 3", rep.Sent)
	}
	state, err := forgetful.Get(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if state.Quantity() != 3 {
		t.Fatalf("Quantity() = %d, want 3", state.Quantity())
	}
}

func TestServerRejectsMalformedFrameAndContinuesSession(t *testing.T) {
	ctx := context.Background()
	server := newNode(t, "B")
	srv := NewServer(server, 8)

	valid := &syncpb.Event{
		NodeId: "A", Seq: 1, HlcWall: 10, Sku: "SKU-1", Kind: int32(eventlog.KindQuantityDelta), Delta: 5,
	}
	broken := &syncpb.Event{NodeId: "A", Seq: 2, HlcWall: 20, Sku: "SKU-1", Kind: 99}

	st := &scriptedServerStream{in: []*syncpb.ClientFrame{
		{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}},
		{Body: &syncpb.ClientFrame_Events{Events: &syncpb.Events{Events: []*syncpb.Event{valid, broken}}}},
		{Body: &syncpb.ClientFrame_Ack{Ack: &syncpb.Ack{VersionVector: map[string]uint64{"A": 2}}}},
	}}
	if err := srv.Session(ctx, st); err != nil {
		t.Fatalf("Session() error = %v, want the malformed frame to be skipped, not fatal", err)
	}
	vv, err := server.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["A"] != 1 {
		t.Fatalf("vv = %v; the malformed event must not be stored, the valid one must be", vv)
	}
}

func TestServerRejectsOutOfOrderHandshake(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(newNode(t, "B"), 8)
	tests := []struct {
		name string
		in   []*syncpb.ClientFrame
	}{
		{"events before hello", []*syncpb.ClientFrame{
			{Body: &syncpb.ClientFrame_Events{Events: &syncpb.Events{}}},
		}},
		{"empty hello node id", []*syncpb.ClientFrame{
			{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{}}},
		}},
		{"second hello", []*syncpb.ClientFrame{
			{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}},
			{Body: &syncpb.ClientFrame_Hello{Hello: &syncpb.Hello{NodeId: "A"}}},
		}},
		{"unknown body", []*syncpb.ClientFrame{{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := srv.Session(ctx, &scriptedServerStream{in: tt.in}); err == nil {
				t.Fatal("Session() error = nil, want a protocol error")
			}
		})
	}
}

func TestClientSurfacesTransportErrors(t *testing.T) {
	ctx := context.Background()
	a := newNode(t, "A")
	tests := []struct {
		name   string
		dialer Dialer
	}{
		{"dial fails", failingDialer{dialErr: errors.New("boom")}},
		{"send fails", failingDialer{sendErr: errors.New("boom")}},
		{"recv fails", failingDialer{recvErr: errors.New("boom")}},
		{"welcome missing", failingDialer{frames: []*syncpb.ServerFrame{
			{Body: &syncpb.ServerFrame_Ack{Ack: &syncpb.Ack{}}},
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewClient(a, tt.dialer, 8).SyncOnce(ctx, "peer"); err == nil {
				t.Fatal("SyncOnce() error = nil, want an error")
			}
		})
	}
}

// TestClientToleratesPeerSendingAMalformedEvent proves one bad event from a
// peer does not fail the whole session: same policy as the server, the event
// is rejected and counted, and the session keeps running so this pair can
// still converge.
func TestClientToleratesPeerSendingAMalformedEvent(t *testing.T) {
	ctx := context.Background()
	a := newNode(t, "A")
	dialer := failingDialer{frames: []*syncpb.ServerFrame{
		{Body: &syncpb.ServerFrame_Welcome{Welcome: &syncpb.Welcome{NodeId: "B"}}},
		{Body: &syncpb.ServerFrame_Events{Events: &syncpb.Events{Events: []*syncpb.Event{{NodeId: "B", Seq: 1, Sku: "S", Kind: 99}}}}},
		{Body: &syncpb.ServerFrame_Ack{Ack: &syncpb.Ack{VersionVector: map[string]uint64{"B": 1}}}},
	}}
	rep, err := NewClient(a, dialer, 8).SyncOnce(ctx, "peer")
	if err != nil {
		t.Fatalf("SyncOnce() error = %v, want the session to continue past the bad event", err)
	}
	if rep.Rejected != 1 {
		t.Errorf("Rejected = %d, want 1", rep.Rejected)
	}
	if rep.Received != 0 {
		t.Errorf("Received = %d, want 0", rep.Received)
	}
	// The rejected event was never applied locally, so the cursor must not
	// advance on the strength of the peer's advertised (unearned) vector.
	if got, err := a.Log().Cursor(ctx, "B"); err != nil || got != 0 {
		t.Fatalf("Cursor(B) = %d, %v; want 0, nil", got, err)
	}
}

func TestSyncWithBackoffRetriesThenSucceeds(t *testing.T) {
	ctx := context.Background()
	a, b := newNode(t, "A"), newNode(t, "B")
	if _, err := a.Receive(ctx, "SKU-1", 5); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	tr := NewMemoryTransport()
	flaky := &flakyDialer{inner: tr, failFirst: 2}
	tr.Serve("central", NewServer(b, 8))

	var sleeps []time.Duration
	rep, err := NewClient(a, flaky, 8).SyncWithBackoff(ctx, "central", 5, time.Millisecond,
		func(d time.Duration) time.Duration { sleeps = append(sleeps, d); return 0 })
	if err != nil {
		t.Fatalf("SyncWithBackoff() error = %v", err)
	}
	if rep.Sent != 1 {
		t.Fatalf("Report = %+v, want Sent 1", rep)
	}
	if len(sleeps) != 2 {
		t.Fatalf("backoff sleeps = %v, want 2 entries", sleeps)
	}
	if sleeps[1] <= sleeps[0] {
		t.Fatalf("backoff did not grow: %v", sleeps)
	}
}

func TestSyncWithBackoffGivesUp(t *testing.T) {
	a := newNode(t, "A")
	_, err := NewClient(a, failingDialer{dialErr: errors.New("boom")}, 8).
		SyncWithBackoff(context.Background(), "central", 2, time.Nanosecond, func(time.Duration) time.Duration { return 0 })
	if err == nil {
		t.Fatal("SyncWithBackoff() error = nil, want the final failure")
	}
}

func TestSyncWithBackoffHonoursContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := newNode(t, "A")
	_, err := NewClient(a, failingDialer{dialErr: errors.New("boom")}, 8).
		SyncWithBackoff(ctx, "central", 5, time.Hour, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SyncWithBackoff() error = %v, want context.Canceled", err)
	}
}

func TestMemoryTransportUnknownAddress(t *testing.T) {
	if _, err := NewMemoryTransport().Dial(context.Background(), "nope"); err == nil {
		t.Fatal("Dial() error = nil for an unserved address, want an error")
	}
}

// --- test doubles ---

// scriptedServerStream feeds a fixed sequence of client frames to a Server and
// records what the Server sent back.
type scriptedServerStream struct {
	in   []*syncpb.ClientFrame
	i    int
	sent []*syncpb.ServerFrame
}

func (s *scriptedServerStream) Recv() (*syncpb.ClientFrame, error) {
	if s.i >= len(s.in) {
		return nil, io.EOF
	}
	f := s.in[s.i]
	s.i++
	return f, nil
}

func (s *scriptedServerStream) Send(f *syncpb.ServerFrame) error {
	s.sent = append(s.sent, f)
	return nil
}

// failingDialer injects failures at each stage of a session.
type failingDialer struct {
	dialErr, sendErr, recvErr error
	frames                    []*syncpb.ServerFrame
}

func (d failingDialer) Dial(context.Context, string) (Stream, error) {
	if d.dialErr != nil {
		return nil, d.dialErr
	}
	return &failingStream{d: d}, nil
}

type failingStream struct {
	d failingDialer
	i int
}

func (s *failingStream) Send(*syncpb.ClientFrame) error { return s.d.sendErr }
func (s *failingStream) CloseSend() error               { return nil }
func (s *failingStream) Close() error                   { return nil }
func (s *failingStream) Recv() (*syncpb.ServerFrame, error) {
	if s.d.recvErr != nil {
		return nil, s.d.recvErr
	}
	if s.i >= len(s.d.frames) {
		return nil, io.EOF
	}
	f := s.d.frames[s.i]
	s.i++
	return f, nil
}

// flakyDialer fails the first failFirst dials, then delegates.
type flakyDialer struct {
	inner     Dialer
	failFirst int
	calls     int
}

func (d *flakyDialer) Dial(ctx context.Context, addr string) (Stream, error) {
	d.calls++
	if d.calls <= d.failFirst {
		return nil, errors.New("transport unavailable")
	}
	return d.inner.Dial(ctx, addr)
}
