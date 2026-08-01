package eventlog

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// fixedClock returns a clock that advances by one second per call, so HLC
// behaviour is deterministic in tests.
func fixedClock(start time.Time) func() time.Time {
	n := 0
	return func() time.Time {
		n++
		return start.Add(time.Duration(n) * time.Second)
	}
}

func openLog(t *testing.T, node domain.NodeID) *Log {
	t.Helper()
	l, err := Open(filepath.Join(t.TempDir(), "node.db"), node, fixedClock(time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return l
}

func putAway(qty float64) domain.Event {
	return domain.Event{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.PutAway{
		Move: domain.Movement{SKU: "WIDGET", From: "RECV-01", To: "PICK-01", Qty: qty}}}
}

func TestEmitAssignsSequentialIdentityAndMonotonicHLC(t *testing.T) {
	l := openLog(t, "wh-a")
	envs, err := l.Emit([]domain.Event{putAway(1), putAway(2)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(envs) != 2 {
		t.Fatalf("got %d envelopes, want 2", len(envs))
	}
	for i, e := range envs {
		if e.ID != (domain.EventID{NodeID: "wh-a", Seq: uint64(i + 1)}) {
			t.Fatalf("envelope %d id = %+v", i, e.ID)
		}
		if e.HLC.Node != "wh-a" {
			t.Fatalf("envelope %d hlc node = %q", i, e.HLC.Node)
		}
	}
	if envs[0].HLC.Compare(envs[1].HLC) >= 0 {
		t.Fatalf("hlc did not advance: %+v then %+v", envs[0].HLC, envs[1].HLC)
	}

	more, err := l.Emit([]domain.Event{putAway(3)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if more[0].ID.Seq != 3 {
		t.Fatalf("seq = %d, want 3", more[0].ID.Seq)
	}
}

func TestEmitCarriesCausationID(t *testing.T) {
	l := openLog(t, "central")
	cause := domain.EventID{NodeID: "wh-a", Seq: 7}
	envs, err := l.Emit([]domain.Event{{Type: domain.TypeStockAdjusted, AggregateID: "WIDGET",
		Payload: domain.StockAdjusted{Reason: domain.ReasonUnknownSKU}}}, &cause)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	round, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(round) != 1 || round[0].CausationID == nil || *round[0].CausationID != cause {
		t.Fatalf("causation did not survive storage: %+v", round)
	}
	if envs[0].CausationID == nil || *envs[0].CausationID != cause {
		t.Fatalf("returned envelope lost causation: %+v", envs[0])
	}
}

func TestEmitIsAtomicSoAPartialFailureWritesNothing(t *testing.T) {
	// The second event cannot be JSON-encoded. Nothing at all must be appended:
	// a crash or error mid-transaction leaves the log exactly as it was.
	l := openLog(t, "wh-a")
	_, err := l.Emit([]domain.Event{putAway(1), {Type: domain.TypePutAway, AggregateID: "X", Payload: math.Inf(1)}}, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	all, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("log holds %d events after a failed emit, want 0", len(all))
	}
	// The in-memory sequence must not have advanced either, or the next emit
	// would leave a hole.
	envs, err := l.Emit([]domain.Event{putAway(9)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if envs[0].ID.Seq != 1 {
		t.Fatalf("seq after failed emit = %d, want 1", envs[0].ID.Seq)
	}
}

func TestIngestIsIdempotent(t *testing.T) {
	l := openLog(t, "wh-a")
	remote := remoteEnvelopes(t, "wh-b", 3)

	n, err := l.Ingest(remote)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if n != 3 {
		t.Fatalf("first ingest reported %d new, want 3", n)
	}
	n, err = l.Ingest(remote)
	if err != nil {
		t.Fatalf("second Ingest: %v", err)
	}
	if n != 0 {
		t.Fatalf("second ingest reported %d new, want 0", n)
	}
	all, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("log holds %d events, want 3", len(all))
	}
	// Overlapping batches, the normal case on a resumed stream, must also be safe.
	n, err = l.Ingest(append(remote, remoteEnvelopes(t, "wh-b", 5)[3:]...))
	if err != nil {
		t.Fatalf("overlapping Ingest: %v", err)
	}
	if n != 2 {
		t.Fatalf("overlapping ingest reported %d new, want 2", n)
	}
}

func TestIngestAdvancesLocalHLCPastRemote(t *testing.T) {
	l := openLog(t, "wh-a")
	far := domain.Envelope{
		ID: domain.EventID{NodeID: "wh-b", Seq: 1}, AggregateID: "WIDGET", Type: domain.TypePutAway,
		HLC:        domain.HLC{Wall: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), Counter: 4, Node: "wh-b"},
		RecordedAt: time.Unix(0, 0).UTC(), Payload: []byte(`{"move":{"sku":"WIDGET","from":"RECV-01","to":"PICK-01","qty":1}}`),
	}
	if _, err := l.Ingest([]domain.Envelope{far}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	envs, err := l.Emit([]domain.Event{putAway(1)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if envs[0].HLC.Compare(far.HLC) <= 0 {
		t.Fatalf("local hlc %+v did not overtake remote %+v", envs[0].HLC, far.HLC)
	}
}

func TestIngestRejectsAnEnvelopeItCannotStore(t *testing.T) {
	l := openLog(t, "wh-a")
	// An empty node id would collide with nothing and corrupt the version vector,
	// so it must be refused loudly rather than skipped.
	_, err := l.Ingest([]domain.Envelope{{ID: domain.EventID{Seq: 1}, Type: domain.TypePutAway, Payload: []byte(`{}`)}})
	if err == nil {
		t.Fatal("expected an error for an envelope with no node id")
	}
}

func TestReadAllIsInTotalHLCOrderRegardlessOfArrivalOrder(t *testing.T) {
	l := openLog(t, "wh-a")
	late := remoteEnvelopes(t, "wh-b", 2)
	// Arrive out of order: highest HLC first.
	if _, err := l.Ingest([]domain.Envelope{late[1], late[0]}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	all, err := l.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].HLC.Compare(all[i].HLC) >= 0 {
			t.Fatalf("event %d (%+v) not after %d (%+v)", i, all[i].HLC, i-1, all[i-1].HLC)
		}
	}
}

func TestReadOwnAfterRespectsCursorAndLimit(t *testing.T) {
	l := openLog(t, "wh-a")
	if _, err := l.Emit([]domain.Event{putAway(1), putAway(2), putAway(3), putAway(4)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := l.Ingest(remoteEnvelopes(t, "wh-b", 2)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	tests := []struct {
		name     string
		after    uint64
		limit    int
		wantSeqs []uint64
	}{
		{name: "from the start, bounded chunk", after: 0, limit: 2, wantSeqs: []uint64{1, 2}},
		{name: "resumed after two", after: 2, limit: 2, wantSeqs: []uint64{3, 4}},
		{name: "caught up", after: 4, limit: 2, wantSeqs: nil},
		{name: "limit larger than the tail", after: 3, limit: 10, wantSeqs: []uint64{4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := l.ReadOwnAfter(tt.after, tt.limit)
			if err != nil {
				t.Fatalf("ReadOwnAfter: %v", err)
			}
			if len(got) != len(tt.wantSeqs) {
				t.Fatalf("got %d events, want %d", len(got), len(tt.wantSeqs))
			}
			for i, seq := range tt.wantSeqs {
				if got[i].ID != (domain.EventID{NodeID: "wh-a", Seq: seq}) {
					t.Fatalf("event %d id = %+v, want wh-a/%d", i, got[i].ID, seq)
				}
			}
		})
	}
}

func TestVersionVectorAndHighestSeq(t *testing.T) {
	l := openLog(t, "wh-a")
	if _, err := l.Emit([]domain.Event{putAway(1), putAway(2)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := l.Ingest(remoteEnvelopes(t, "wh-b", 5)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	vv, err := l.VersionVector()
	if err != nil {
		t.Fatalf("VersionVector: %v", err)
	}
	if vv["wh-a"] != 2 || vv["wh-b"] != 5 || len(vv) != 2 {
		t.Fatalf("version vector = %+v", vv)
	}
	for node, want := range map[domain.NodeID]uint64{"wh-a": 2, "wh-b": 5, "wh-z": 0} {
		got, err := l.HighestSeq(node)
		if err != nil {
			t.Fatalf("HighestSeq(%s): %v", node, err)
		}
		if got != want {
			t.Fatalf("HighestSeq(%s) = %d, want %d", node, got, want)
		}
	}
}

func TestCursors(t *testing.T) {
	l := openLog(t, "wh-a")
	got, err := l.Cursor(CursorPushed)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	if got != 0 {
		t.Fatalf("fresh cursor = %d, want 0", got)
	}
	if err := l.SetCursor(CursorPushed, 12); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	if err := l.SetCursor(CursorPushed, 34); err != nil {
		t.Fatalf("SetCursor overwrite: %v", err)
	}
	if err := l.SetCursor(CursorPulled, 7); err != nil {
		t.Fatalf("SetCursor other: %v", err)
	}
	for name, want := range map[string]uint64{CursorPushed: 34, CursorPulled: 7} {
		got, err := l.Cursor(name)
		if err != nil {
			t.Fatalf("Cursor(%s): %v", name, err)
		}
		if got != want {
			t.Fatalf("Cursor(%s) = %d, want %d", name, got, want)
		}
	}
}

func TestProjectionVersionRoundTrip(t *testing.T) {
	l := openLog(t, "wh-a")
	v, err := l.ProjectionVersion()
	if err != nil {
		t.Fatalf("ProjectionVersion: %v", err)
	}
	if v != 0 {
		t.Fatalf("fresh projection version = %d, want 0", v)
	}
	if err := l.SetProjectionVersion(3); err != nil {
		t.Fatalf("SetProjectionVersion: %v", err)
	}
	if err := l.SetProjectionVersion(4); err != nil {
		t.Fatalf("SetProjectionVersion again: %v", err)
	}
	if v, err = l.ProjectionVersion(); err != nil || v != 4 {
		t.Fatalf("ProjectionVersion = %d, %v; want 4, nil", v, err)
	}
}

func TestReopenRecoversSequenceAndClock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.db")
	clock := fixedClock(time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC))

	first, err := Open(path, "wh-a", clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	before, err := first.Emit([]domain.Event{putAway(1), putAway(2)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with a clock stuck in the past: the recovered HLC must still move
	// forward, so a restart cannot produce duplicate or out-of-order readings.
	second, err := Open(path, "wh-a", func() time.Time { return time.Unix(0, 0).UTC() })
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := second.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	after, err := second.Emit([]domain.Event{putAway(3)}, nil)
	if err != nil {
		t.Fatalf("Emit after reopen: %v", err)
	}
	if after[0].ID.Seq != 3 {
		t.Fatalf("seq after reopen = %d, want 3", after[0].ID.Seq)
	}
	if after[0].HLC.Compare(before[1].HLC) <= 0 {
		t.Fatalf("hlc %+v did not advance past %+v after reopen", after[0].HLC, before[1].HLC)
	}
	if got := second.NodeID(); got != "wh-a" {
		t.Fatalf("NodeID = %q", got)
	}
}

func TestOpenFailsOnAnUnusablePath(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "no-such-dir", "node.db"), "wh-a", time.Now); err == nil {
		t.Fatal("expected an error opening a database in a missing directory")
	}
}

func TestReplayDeterminism(t *testing.T) {
	// Two logs receive the same events in opposite orders. Folding ReadAll into a
	// fresh domain state must produce identical stock maps.
	remote := remoteEnvelopes(t, "wh-b", 6)
	forward, backward := openLog(t, "wh-a"), openLog(t, "wh-a")
	if _, err := forward.Ingest(remote); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	reversed := make([]domain.Envelope, len(remote))
	for i, e := range remote {
		reversed[len(remote)-1-i] = e
	}
	if _, err := backward.Ingest(reversed); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	fold := func(l *Log) map[domain.StockKey]float64 {
		all, err := l.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		s := domain.NewState()
		for _, e := range all {
			if err := s.Apply(e); err != nil {
				t.Fatalf("Apply: %v", err)
			}
		}
		return s.Stock
	}
	a, b := fold(forward), fold(backward)
	if len(a) != len(b) {
		t.Fatalf("different key counts: %d vs %d", len(a), len(b))
	}
	for k, v := range a {
		if b[k] != v {
			t.Fatalf("key %+v: %v vs %v", k, v, b[k])
		}
	}
}

func TestOperationsFailAfterClose(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "node.db"), "wh-a", time.Now)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := l.ReadAll(); err == nil {
		t.Error("ReadAll after close: expected an error")
	}
	if _, err := l.VersionVector(); err == nil {
		t.Error("VersionVector after close: expected an error")
	}
	if _, err := l.HighestSeq("wh-a"); err == nil {
		t.Error("HighestSeq after close: expected an error")
	}
	if _, err := l.Cursor(CursorPushed); err == nil {
		t.Error("Cursor after close: expected an error")
	}
	if err := l.SetCursor(CursorPushed, 1); err == nil {
		t.Error("SetCursor after close: expected an error")
	}
	if _, err := l.ProjectionVersion(); err == nil {
		t.Error("ProjectionVersion after close: expected an error")
	}
	if err := l.SetProjectionVersion(1); err == nil {
		t.Error("SetProjectionVersion after close: expected an error")
	}
	if _, err := l.Emit([]domain.Event{putAway(1)}, nil); err == nil {
		t.Error("Emit after close: expected an error")
	}
	if _, err := l.Ingest(remoteEnvelopes(t, "wh-b", 1)); err == nil {
		t.Error("Ingest after close: expected an error")
	}
}

func TestReadAllFailsOnCorruptRecordedAt(t *testing.T) {
	l := openLog(t, "wh-a")
	if _, err := l.Emit([]domain.Event{putAway(1)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := l.DB().Exec(`UPDATE events SET recorded_at = 'not-a-time'`); err != nil {
		t.Fatalf("corrupt row: %v", err)
	}
	if _, err := l.ReadAll(); err == nil {
		t.Fatal("expected an error parsing a corrupt recorded_at")
	}
}

func TestReadAllFailsOnCorruptHLCColumn(t *testing.T) {
	l := openLog(t, "wh-a")
	if _, err := l.Emit([]domain.Event{putAway(1)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	// hlc_wall is declared INTEGER but SQLite's dynamic typing lets a TEXT value
	// that cannot convert to a number through, so Scan into an int64 must fail
	// loudly rather than silently coercing to zero.
	if _, err := l.DB().Exec(`UPDATE events SET hlc_wall = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt row: %v", err)
	}
	if _, err := l.ReadAll(); err == nil {
		t.Fatal("expected an error scanning a corrupt hlc_wall")
	}
}

func TestVersionVectorFailsOnCorruptSeqColumn(t *testing.T) {
	l := openLog(t, "wh-a")
	if _, err := l.Emit([]domain.Event{putAway(1)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := l.DB().Exec(`UPDATE events SET seq = 'not-a-number' WHERE seq = 1`); err != nil {
		t.Fatalf("corrupt row: %v", err)
	}
	if _, err := l.VersionVector(); err == nil {
		t.Fatal("expected an error scanning a corrupt seq")
	}
}

func TestIngestOwnEventsAdvancesLocalSequence(t *testing.T) {
	// A resend of this node's own events (echoed back by central) must not leave
	// the local sequence counter behind, or the next Emit would collide.
	l := openLog(t, "wh-a")
	own, err := domain.NewEnvelope(domain.EventID{NodeID: "wh-a", Seq: 5}, domain.HLC{Wall: 1, Node: "wh-a"},
		time.Unix(1, 0).UTC(), nil, putAway(1))
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if _, err := l.Ingest([]domain.Envelope{own}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	envs, err := l.Emit([]domain.Event{putAway(2)}, nil)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if envs[0].ID.Seq != 6 {
		t.Fatalf("seq after ingesting own event = %d, want 6", envs[0].ID.Seq)
	}
}

func TestRecoverFailsOnCorruptClockColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	clockFn := fixedClock(time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC))
	l, err := Open(path, "wh-a", clockFn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := l.Emit([]domain.Event{putAway(1)}, nil); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if _, err := l.DB().Exec(`UPDATE events SET hlc_wall = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt row: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := Open(path, "wh-a", clockFn); err == nil {
		t.Fatal("expected recover to fail scanning a corrupt hlc_wall on reopen")
	}
}

func TestInsertFailsWhenPrepareFailsIndependentlyOfBegin(t *testing.T) {
	l := openLog(t, "wh-a")
	if _, err := l.DB().Exec(`DROP TABLE events`); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := l.Emit([]domain.Event{putAway(1)}, nil); err == nil {
		t.Fatal("expected Emit to fail preparing insert against a dropped table")
	}
}

func TestEmitFailsRatherThanDiscardOnConflict(t *testing.T) {
	// Emit assigns a fresh local sequence and should never collide. If the target
	// row is somehow already occupied, Emit must error rather than silently drop
	// the event via ON CONFLICT DO NOTHING (which is Ingest's replay semantics).
	l := openLog(t, "wh-a")
	if _, err := l.DB().Exec(`INSERT INTO events
		(node_id, seq, aggregate_id, type, hlc_wall, hlc_counter, hlc_node, recorded_at, payload)
		VALUES ('wh-a', 1, 'WIDGET', ?, 0, 0, 'wh-a', ?, '{}')`,
		string(domain.TypePutAway), time.Unix(0, 0).UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed conflicting row: %v", err)
	}
	if _, err := l.Emit([]domain.Event{putAway(1)}, nil); err == nil {
		t.Fatal("expected Emit to error on a conflicting (node_id, seq)")
	}
}

func TestOpenFailsOnACorruptDatabaseFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	if err := os.WriteFile(path, []byte("not a sqlite database"), 0o600); err != nil {
		t.Fatalf("write garbage file: %v", err)
	}
	if _, err := Open(path, "wh-a", time.Now); err == nil {
		t.Fatal("expected an error applying schema to a corrupt file")
	}
}

// remoteEnvelopes builds n well-formed envelopes as if node had emitted them,
// with strictly increasing HLC readings.
func remoteEnvelopes(t *testing.T, node domain.NodeID, n int) []domain.Envelope {
	t.Helper()
	out := make([]domain.Envelope, 0, n)
	for i := 1; i <= n; i++ {
		env, err := domain.NewEnvelope(
			domain.EventID{NodeID: node, Seq: uint64(i)},
			domain.HLC{Wall: int64(i) * 1000, Node: node},
			time.Unix(int64(i), 0).UTC(), nil,
			domain.Event{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.PutAway{
				Move: domain.Movement{SKU: "WIDGET", From: "RECV-01", To: "PICK-01", Qty: float64(i)}}},
		)
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		out = append(out, env)
	}
	return out
}
