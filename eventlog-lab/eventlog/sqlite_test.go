package eventlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// newTestLog returns an isolated on-disk log; on-disk rather than :memory: so
// tests can reopen it and prove durability.
func newTestLog(t *testing.T) *SQLiteLog {
	t.Helper()
	l, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return l
}

func qty(node clock.NodeID, seq Seq, wall int64, sku string, delta int64) Event {
	return Event{
		ID:    EventID{NodeID: node, Seq: seq},
		HLC:   clock.HLC{Wall: wall, NodeID: node},
		SKU:   sku,
		Kind:  KindQuantityDelta,
		Delta: delta,
	}
}

func TestAppendIsIdempotent(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	e := qty("A", 1, 100, "SKU-1", 5)

	for i := 0; i < 3; i++ {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() call %d error = %v", i, err)
		}
	}

	var got []Event
	for ev, err := range l.Since(ctx, VersionVector{}) {
		if err != nil {
			t.Fatalf("Since() error = %v", err)
		}
		got = append(got, ev)
	}
	if len(got) != 1 {
		t.Fatalf("len(events) = %d after 3 identical appends, want 1", len(got))
	}
	if got[0].ID != e.ID || got[0].Delta != e.Delta || got[0].HLC != e.HLC || got[0].SKU != e.SKU {
		t.Fatalf("round trip = %+v, want %+v", got[0], e)
	}
}

func TestAppendRejectsMalformed(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	tests := []struct {
		name  string
		event Event
	}{
		{"unknown kind", Event{ID: EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{NodeID: "A"}, SKU: "S", Kind: 42}},
		{"empty sku", Event{ID: EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{NodeID: "A"}, Kind: KindQuantityDelta, Delta: 1}},
		{"zero seq", Event{ID: EventID{NodeID: "A"}, HLC: clock.HLC{NodeID: "A"}, SKU: "S", Kind: KindQuantityDelta, Delta: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := l.Append(ctx, tt.event); !errors.Is(err, ErrMalformedEvent) {
				t.Fatalf("Append() error = %v, want ErrMalformedEvent", err)
			}
		})
	}
	vv, err := l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if len(vv) != 0 {
		t.Fatalf("malformed events reached storage: %v", vv)
	}
}

func TestAppendLocalAllocatesGapFreeSeq(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	for i := Seq(1); i <= 3; i++ {
		got, err := l.AppendLocal(ctx, func(s Seq) Event {
			return qty("A", s, int64(100+i), "SKU-1", 1)
		})
		if err != nil {
			t.Fatalf("AppendLocal() error = %v", err)
		}
		if got.ID.Seq != i {
			t.Fatalf("AppendLocal() seq = %d, want %d", got.ID.Seq, i)
		}
	}
	vv, err := l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["A"] != 3 {
		t.Fatalf("vv[A] = %d, want 3", vv["A"])
	}
}

// TestVersionVectorReportsContiguousPrefixNotRawMax is a regression test for
// reordered delivery: if node A's event 2 is stored before its event 1 (the
// harness's Reorder fault can produce exactly this, since a single Events
// batch can interleave more than one origin), VersionVector must not report
// A as covered through 2 -- that would make Contains(A:1) lie, and a peer
// deciding what to send this replica would never send A:1 again.
func TestVersionVectorReportsContiguousPrefixNotRawMax(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)

	// Seq 2 arrives and is stored; seq 1 has not arrived yet.
	if err := l.Append(ctx, qty("A", 2, 100, "SKU-1", 1)); err != nil {
		t.Fatalf("Append(A:2) error = %v", err)
	}
	vv, err := l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if got, ok := vv["A"]; ok {
		t.Fatalf(`vv["A"] = %d, want absent -- A:1 is missing, so nothing is covered yet`, got)
	}
	if got := vv.Contains(EventID{NodeID: "A", Seq: 1}); got {
		t.Fatal("Contains(A:1) = true before A:1 ever arrived -- a peer would wrongly believe it was already delivered")
	}

	// Seq 1 arrives late; the gap closes and the vector advances.
	if err := l.Append(ctx, qty("A", 1, 100, "SKU-1", 1)); err != nil {
		t.Fatalf("Append(A:1) error = %v", err)
	}
	vv, err = l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["A"] != 2 {
		t.Fatalf(`vv["A"] = %d, want 2 once the gap is filled`, vv["A"])
	}
}

func TestAppendLocalRejectsMalformedWithoutBurningSeq(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if _, err := l.AppendLocal(ctx, func(s Seq) Event {
		return Event{ID: EventID{NodeID: "A", Seq: s}, HLC: clock.HLC{NodeID: "A"}, SKU: "S", Kind: 42}
	}); !errors.Is(err, ErrMalformedEvent) {
		t.Fatalf("AppendLocal() error = %v, want ErrMalformedEvent", err)
	}
	got, err := l.AppendLocal(ctx, func(s Seq) Event { return qty("A", s, 100, "SKU-1", 1) })
	if err != nil {
		t.Fatalf("AppendLocal() error = %v", err)
	}
	if got.ID.Seq != 1 {
		t.Fatalf("seq = %d after a rejected append, want 1 (no gap)", got.ID.Seq)
	}
}

func TestSinceFiltersByVersionVectorAndOrdersByHLC(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	all := []Event{
		qty("A", 1, 300, "SKU-1", 1),
		qty("A", 2, 100, "SKU-1", 2),
		qty("B", 1, 200, "SKU-2", 3),
	}
	for _, e := range all {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append(%+v) error = %v", e.ID, err)
		}
	}

	tests := []struct {
		name string
		vv   VersionVector
		want []EventID
	}{
		{"empty vector yields all in HLC order", VersionVector{}, []EventID{
			{NodeID: "A", Seq: 2}, {NodeID: "B", Seq: 1}, {NodeID: "A", Seq: 1},
		}},
		{"partial vector skips covered", VersionVector{"A": 2}, []EventID{{NodeID: "B", Seq: 1}}},
		{"full vector yields nothing", VersionVector{"A": 2, "B": 1}, nil},
		{"unknown peer in vector is ignored", VersionVector{"Z": 9}, []EventID{
			{NodeID: "A", Seq: 2}, {NodeID: "B", Seq: 1}, {NodeID: "A", Seq: 1},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []EventID
			for e, err := range l.Since(ctx, tt.vv) {
				if err != nil {
					t.Fatalf("Since() error = %v", err)
				}
				got = append(got, e.ID)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ids = %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("ids = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestSinceBreaksEarly(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	for i := Seq(1); i <= 5; i++ {
		if err := l.Append(ctx, qty("A", i, int64(i), "SKU-1", 1)); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	n := 0
	for range l.Since(ctx, VersionVector{}) {
		n++
		break
	}
	if n != 1 {
		t.Fatalf("visited %d events, want 1 (yield=false must stop iteration)", n)
	}
}

func TestSinceSurfacesDecodeError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if err := l.Append(ctx, qty("A", 1, 100, "SKU-1", 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := l.db.ExecContext(ctx, `UPDATE events SET payload = ? WHERE node_id = 'A'`, []byte("{bad")); err != nil {
		t.Fatalf("corrupting payload: %v", err)
	}
	var sawErr error
	for _, err := range l.Since(ctx, VersionVector{}) {
		sawErr = err
	}
	if !errors.Is(sawErr, ErrMalformedEvent) {
		t.Fatalf("Since() error = %v, want ErrMalformedEvent", sawErr)
	}
}

func TestEventsForSKU(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	for _, e := range []Event{
		qty("A", 1, 100, "SKU-1", 1),
		qty("A", 2, 200, "SKU-2", 1),
		qty("B", 1, 300, "SKU-1", 1),
	} {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	tests := []struct {
		name  string
		sku   string
		after VersionVector
		want  int
	}{
		{"all for sku", "SKU-1", VersionVector{}, 2},
		{"excludes covered", "SKU-1", VersionVector{"A": 1}, 1},
		{"other sku", "SKU-2", VersionVector{}, 1},
		{"unknown sku", "SKU-9", VersionVector{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := 0
			for _, err := range l.EventsForSKU(ctx, tt.sku, tt.after) {
				if err != nil {
					t.Fatalf("EventsForSKU() error = %v", err)
				}
				n++
			}
			if n != tt.want {
				t.Fatalf("count = %d, want %d", n, tt.want)
			}
		})
	}
}

func TestOpenSQLiteFailsOnBadPath(t *testing.T) {
	if _, err := OpenSQLite(filepath.Join(t.TempDir(), "no-such-dir", "x.db")); err == nil {
		t.Fatal("OpenSQLite() error = nil, want a failure for an unwritable path")
	}
}

func TestCursorRoundTrip(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	got, err := l.Cursor(ctx, "B")
	if err != nil || got != 0 {
		t.Fatalf("Cursor() = %d, %v; want 0, nil for an unknown peer", got, err)
	}
	if err := l.SetCursor(ctx, "B", 7); err != nil {
		t.Fatalf("SetCursor() error = %v", err)
	}
	if err := l.SetCursor(ctx, "B", 9); err != nil {
		t.Fatalf("SetCursor() error = %v", err)
	}
	if got, err = l.Cursor(ctx, "B"); err != nil || got != 9 {
		t.Fatalf("Cursor() = %d, %v; want 9, nil", got, err)
	}
	// A peer reporting a regressed vector must not shrink our record.
	if err := l.SetCursor(ctx, "B", 4); err != nil {
		t.Fatalf("SetCursor() error = %v", err)
	}
	if got, err = l.Cursor(ctx, "B"); err != nil || got != 9 {
		t.Fatalf("Cursor() = %d, %v after a regressed report; want 9, nil", got, err)
	}
}

func TestClosedLogReturnsErrors(t *testing.T) {
	ctx := context.Background()
	l, err := OpenSQLite(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := l.Append(ctx, qty("A", 1, 100, "SKU-1", 1)); err == nil {
		t.Error("Append() on a closed log error = nil, want an error")
	}
	if _, err := l.AppendLocal(ctx, func(s Seq) Event { return qty("A", s, 100, "SKU-1", 1) }); err == nil {
		t.Error("AppendLocal() on a closed log error = nil, want an error")
	}
	if _, err := l.VersionVector(ctx); err == nil {
		t.Error("VersionVector() on a closed log error = nil, want an error")
	}
	if _, err := l.Cursor(ctx, "B"); err == nil {
		t.Error("Cursor() on a closed log error = nil, want an error")
	}
	if err := l.SetCursor(ctx, "B", 1); err == nil {
		t.Error("SetCursor() on a closed log error = nil, want an error")
	}
	var sawErr error
	for _, err := range l.Since(ctx, VersionVector{}) {
		sawErr = err
	}
	if sawErr == nil {
		t.Error("Since() on a closed log yielded no error, want one")
	}
	if err := l.Compact(ctx, VersionVector{}); err == nil {
		t.Error("Compact() on a closed log error = nil, want an error")
	}
	if _, err := l.CountForSKU(ctx, "SKU-1"); err == nil {
		t.Error("CountForSKU() on a closed log error = nil, want an error")
	}
}

func TestDSNReportsOpenPath(t *testing.T) {
	l := newTestLog(t)
	if l.DSN() == "" {
		t.Fatal("DSN() = \"\", want the path passed to OpenSQLite")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("LoadSnapshot() error = %v, want ErrNoSnapshot", err)
	}
	covers := VersionVector{"A": 3}
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte("state-v1"), covers); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	state, gotCovers, err := l.LoadSnapshot(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if string(state) != "state-v1" || gotCovers["A"] != 3 {
		t.Fatalf("LoadSnapshot() = %q, %v, want state-v1, %v", state, gotCovers, covers)
	}
	// Overwriting must replace, not duplicate.
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte("state-v2"), VersionVector{"A": 5}); err != nil {
		t.Fatalf("SaveSnapshot() overwrite error = %v", err)
	}
	state, gotCovers, err = l.LoadSnapshot(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if string(state) != "state-v2" || gotCovers["A"] != 5 {
		t.Fatalf("LoadSnapshot() after overwrite = %q, %v, want state-v2, A:5", state, gotCovers)
	}
}

func TestLoadSnapshotSurfacesQueryError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if _, err := l.db.ExecContext(ctx, `DROP TABLE snapshots`); err != nil {
		t.Fatalf("dropping snapshots table: %v", err)
	}
	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); err == nil || errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("LoadSnapshot() error = %v, want a non-ErrNoSnapshot failure", err)
	}
}

func TestLoadSnapshotSurfacesDecodeError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte("state"), VersionVector{"A": 1}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if _, err := l.db.ExecContext(ctx, `UPDATE snapshots SET covers = ? WHERE sku = 'SKU-1'`, []byte("{bad")); err != nil {
		t.Fatalf("corrupting covers: %v", err)
	}
	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); err == nil {
		t.Fatal("LoadSnapshot() with a corrupt covers blob error = nil, want an error")
	}
}

func TestSaveSnapshotSurfacesExecError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if _, err := l.db.ExecContext(ctx, `DROP TABLE snapshots`); err != nil {
		t.Fatalf("dropping snapshots table: %v", err)
	}
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte("state"), VersionVector{"A": 1}); err == nil {
		t.Error("SaveSnapshot() with no snapshots table error = nil, want an error")
	}
}

func TestScanEventSurfacesScanError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if err := l.Append(ctx, qty("A", 1, 1, "SKU-1", 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := l.db.ExecContext(ctx, `UPDATE events SET hlc_wall = 'notanumber'`); err != nil {
		t.Fatalf("corrupting hlc_wall: %v", err)
	}
	var sawErr error
	for _, err := range l.Since(ctx, VersionVector{}) {
		sawErr = err
	}
	if sawErr == nil {
		t.Fatal("Since() over a row with an unscannable column yielded no error, want one")
	}
}

func TestScanEventSurfacesValidateError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if err := l.Append(ctx, qty("A", 1, 1, "SKU-1", 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := l.db.ExecContext(ctx, `UPDATE events SET kind = 99`); err != nil {
		t.Fatalf("corrupting kind: %v", err)
	}
	var sawErr error
	for _, err := range l.Since(ctx, VersionVector{}) {
		sawErr = err
	}
	if !errors.Is(sawErr, ErrMalformedEvent) {
		t.Fatalf("Since() over a row with an invalid kind = %v, want ErrMalformedEvent", sawErr)
	}
}

func TestAppendOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l := newTestLog(t)
	if err := l.Append(ctx, qty("A", 1, 1, "SKU-1", 1)); err == nil {
		t.Error("Append() on a canceled context error = nil, want an error")
	}
}

func TestAppendLocalFailsWhenSeqAllocationQueryErrors(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if _, err := l.db.ExecContext(ctx, `DROP TABLE events`); err != nil {
		t.Fatalf("dropping events table: %v", err)
	}
	if _, err := l.AppendLocal(ctx, func(s Seq) Event { return qty("A", s, 1, "SKU-1", 1) }); err == nil {
		t.Error("AppendLocal() with no events table error = nil, want an error")
	}
}

func TestAppendLocalFailsWhenInsertErrors(t *testing.T) {
	ctx := context.Background()
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission bits, so a read-only database still accepts writes")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "ro.db")
	l, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatalf("chmod db file: %v", err)
	}
	if _, err := l.AppendLocal(ctx, func(s Seq) Event { return qty("A", s, 1, "SKU-1", 1) }); err == nil {
		t.Error("AppendLocal() against a read-only database error = nil, want an error")
	}
}

// SQLiteLog must satisfy Log.
var _ Log = (*SQLiteLog)(nil)
