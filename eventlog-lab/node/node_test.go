package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

func ptr[T any](v T) *T { return &v }

func newTestNode(t *testing.T, id clock.NodeID) *Node {
	t.Helper()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), string(id)+".db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	wall := int64(1000)
	n, err := New(Config{
		ID:            id,
		Log:           l,
		Wall:          func() int64 { wall++; return wall },
		SnapshotEvery: 3,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return n
}

func TestNewValidatesConfig(t *testing.T) {
	l, err := eventlog.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"valid", Config{ID: "A", Log: l}, false},
		{"empty id", Config{Log: l}, true},
		{"nil log", Config{ID: "A"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if err == nil && got.ID() != tt.cfg.ID {
				t.Fatalf("ID() = %q, want %q", got.ID(), tt.cfg.ID)
			}
		})
	}
}

func TestOpsProjectToState(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")

	if _, err := n.Receive(ctx, "SKU-1", 10); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if _, err := n.Pick(ctx, "SKU-1", 3); err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if _, err := n.SetMeta(ctx, "SKU-1", ptr("Widget"), ptr(int64(4))); err != nil {
		t.Fatalf("SetMeta() error = %v", err)
	}
	if _, err := n.Delete(ctx, "SKU-1", true); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, err := n.Get(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Quantity() != 7 {
		t.Errorf("Quantity() = %d, want 7", got.Quantity())
	}
	if got.Name.Value != "Widget" {
		t.Errorf("Name = %q, want %q", got.Name.Value, "Widget")
	}
	if got.ReorderPoint.Value != 4 {
		t.Errorf("ReorderPoint = %d, want 4", got.ReorderPoint.Value)
	}
	if !got.Deleted.Value {
		t.Error("Deleted = false, want true")
	}
}

func TestOpsAllocateSequentialIDs(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")
	for i := eventlog.Seq(1); i <= 3; i++ {
		id, err := n.Receive(ctx, "SKU-1", 1)
		if err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
		if id != (eventlog.EventID{NodeID: "A", Seq: i}) {
			t.Fatalf("Receive() id = %+v, want {A %d}", id, i)
		}
	}
}

func TestOpsRejectBadArguments(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")
	tests := []struct {
		name string
		call func() error
	}{
		{"receive zero", func() error { _, err := n.Receive(ctx, "SKU-1", 0); return err }},
		{"receive negative", func() error { _, err := n.Receive(ctx, "SKU-1", -1); return err }},
		{"pick zero", func() error { _, err := n.Pick(ctx, "SKU-1", 0); return err }},
		{"pick negative", func() error { _, err := n.Pick(ctx, "SKU-1", -1); return err }},
		{"empty sku on receive", func() error { _, err := n.Receive(ctx, "", 1); return err }},
		{"empty sku on setmeta", func() error { _, err := n.SetMeta(ctx, "", ptr("x"), nil); return err }},
		{"empty sku on delete", func() error { _, err := n.Delete(ctx, "", true); return err }},
		{"empty sku on get", func() error { _, err := n.Get(ctx, ""); return err }},
		{"setmeta with nothing to set", func() error { _, err := n.SetMeta(ctx, "SKU-1", nil, nil); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); err == nil {
				t.Fatal("error = nil, want a rejection")
			}
		})
	}
	vv, err := n.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if len(vv) != 0 {
		t.Fatalf("rejected ops reached the log: %v", vv)
	}
}

func TestOpsAdvanceTheClock(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")
	var prev clock.HLC
	for i := 0; i < 3; i++ {
		if _, err := n.Receive(ctx, "SKU-1", 1); err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
		got := n.Clock().Last()
		if i > 0 && !prev.Before(got) {
			t.Fatalf("stamp %+v did not advance past %+v", got, prev)
		}
		prev = got
	}
}

func TestMergeObservesRemoteClockAndSkipsMalformed(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")

	good := eventlog.Event{
		ID:    eventlog.EventID{NodeID: "B", Seq: 1},
		HLC:   clock.HLC{Wall: 999_000, NodeID: "B"},
		SKU:   "SKU-1",
		Kind:  eventlog.KindQuantityDelta,
		Delta: 5,
	}
	bad := eventlog.Event{
		ID:   eventlog.EventID{NodeID: "B", Seq: 2},
		HLC:  clock.HLC{Wall: 999_001, NodeID: "B"},
		SKU:  "SKU-1",
		Kind: 99, // unknown kind: must be rejected, never persisted
	}

	accepted, err := n.Merge(ctx, []eventlog.Event{good, bad, good})
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if accepted != 2 {
		t.Fatalf("Merge() accepted = %d, want 2 (duplicate counts as accepted, malformed does not)", accepted)
	}

	vv, err := n.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["B"] != 1 {
		t.Fatalf("vv = %v; the malformed event must not be stored", vv)
	}

	// A local write after the merge must sort AFTER the remote event, even
	// though the remote wall clock is far ahead of ours.
	if _, err := n.Receive(ctx, "SKU-1", 1); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if !good.HLC.Before(n.Clock().Last()) {
		t.Fatalf("local stamp %+v does not follow observed remote %+v", n.Clock().Last(), good.HLC)
	}

	state, err := n.Get(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if state.Quantity() != 6 {
		t.Fatalf("Quantity() = %d, want 6", state.Quantity())
	}
}

func TestMergeEmptyBatch(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, "A")
	got, err := n.Merge(ctx, nil)
	if err != nil || got != 0 {
		t.Fatalf("Merge(nil) = %d, %v; want 0, nil", got, err)
	}
}

func TestOpsPropagateAppendFailure(t *testing.T) {
	ctx := context.Background()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	n, err := New(Config{ID: "A", Log: l})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	// Every op must surface the failure so the caller does not acknowledge it.
	if _, err := n.Receive(ctx, "SKU-1", 1); err == nil {
		t.Error("Receive() error = nil on a broken log, want an error")
	}
	if _, err := n.Pick(ctx, "SKU-1", 1); err == nil {
		t.Error("Pick() error = nil on a broken log, want an error")
	}
	if _, err := n.SetMeta(ctx, "SKU-1", ptr("x"), nil); err == nil {
		t.Error("SetMeta() error = nil on a broken log, want an error")
	}
	if _, err := n.Delete(ctx, "SKU-1", true); err == nil {
		t.Error("Delete() error = nil on a broken log, want an error")
	}
	if _, err := n.Get(ctx, "SKU-1"); err == nil {
		t.Error("Get() error = nil on a broken log, want an error")
	}
	if _, err := n.VersionVector(ctx); err == nil {
		t.Error("VersionVector() error = nil on a broken log, want an error")
	}
	if _, err := n.Merge(ctx, []eventlog.Event{{
		ID: eventlog.EventID{NodeID: "B", Seq: 1}, HLC: clock.HLC{Wall: 1, NodeID: "B"},
		SKU: "SKU-1", Kind: eventlog.KindQuantityDelta, Delta: 1,
	}}); err == nil {
		t.Error("Merge() error = nil on a broken log, want an error")
	}
	if n.Log() == nil {
		t.Error("Log() = nil, want the configured log")
	}
}

// failCountLog wraps a real log but forces CountForSKU to fail, so
// MaybeSnapshot's error branch is reachable even though the underlying store
// is otherwise healthy -- Append succeeds, then the snapshot check fails.
type failCountLog struct {
	crdt.SQLLog
}

func (failCountLog) CountForSKU(context.Context, string) (int, error) {
	return 0, errors.New("forced CountForSKU failure")
}

func TestEmitSnapshotFailure(t *testing.T) {
	ctx := context.Background()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	n, err := New(Config{ID: "A", Log: failCountLog{l}, SnapshotEvery: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := n.Receive(ctx, "SKU-1", 1); err == nil {
		t.Fatal("Receive() error = nil, want a snapshot failure")
	}
}

func TestMergeSnapshotFailure(t *testing.T) {
	ctx := context.Background()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	n, err := New(Config{ID: "A", Log: failCountLog{l}, SnapshotEvery: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	good := eventlog.Event{
		ID:    eventlog.EventID{NodeID: "B", Seq: 1},
		HLC:   clock.HLC{Wall: 999_000, NodeID: "B"},
		SKU:   "SKU-1",
		Kind:  eventlog.KindQuantityDelta,
		Delta: 5,
	}
	accepted, err := n.Merge(ctx, []eventlog.Event{good})
	if err == nil {
		t.Fatal("Merge() error = nil, want a snapshot failure")
	}
	if accepted != 1 {
		t.Fatalf("Merge() accepted = %d, want 1 (append succeeded before the snapshot failed)", accepted)
	}
}

// sinkLog wraps a real log and implements projectionSink, exercising Merge's
// optional central-reporting branch (added for Task 10's Postgres store; no
// concrete type in this codebase implements it yet).
type sinkLog struct {
	crdt.SQLLog
	failUpsert  bool
	failProject bool
	upserts     int
}

func (s *sinkLog) UpsertProjection(context.Context, string, int64, string, int64, bool) error {
	s.upserts++
	if s.failUpsert {
		return errors.New("forced UpsertProjection failure")
	}
	return nil
}

func (s *sinkLog) LoadSnapshot(ctx context.Context, sku string) ([]byte, eventlog.VersionVector, error) {
	if s.failProject {
		return nil, nil, errors.New("forced LoadSnapshot failure")
	}
	return s.SQLLog.LoadSnapshot(ctx, sku)
}

func TestMergeUpsertsProjectionSink(t *testing.T) {
	ctx := context.Background()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "sink.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	sink := &sinkLog{SQLLog: l}
	n, err := New(Config{ID: "A", Log: sink})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	good := eventlog.Event{
		ID:    eventlog.EventID{NodeID: "B", Seq: 1},
		HLC:   clock.HLC{Wall: 999_000, NodeID: "B"},
		SKU:   "SKU-1",
		Kind:  eventlog.KindQuantityDelta,
		Delta: 5,
	}
	accepted, err := n.Merge(ctx, []eventlog.Event{good})
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if accepted != 1 || sink.upserts != 1 {
		t.Fatalf("accepted = %d, upserts = %d, want 1, 1", accepted, sink.upserts)
	}
}

func TestMergeProjectionSinkProjectFailure(t *testing.T) {
	ctx := context.Background()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "sink.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	sink := &sinkLog{SQLLog: l, failProject: true}
	n, err := New(Config{ID: "A", Log: sink})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	good := eventlog.Event{
		ID:    eventlog.EventID{NodeID: "B", Seq: 1},
		HLC:   clock.HLC{Wall: 999_000, NodeID: "B"},
		SKU:   "SKU-1",
		Kind:  eventlog.KindQuantityDelta,
		Delta: 5,
	}
	accepted, err := n.Merge(ctx, []eventlog.Event{good})
	if err == nil {
		t.Fatal("Merge() error = nil, want a project failure")
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1 (append and snapshot succeeded first)", accepted)
	}
}

func TestMergeProjectionSinkUpsertFailure(t *testing.T) {
	ctx := context.Background()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "sink.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	sink := &sinkLog{SQLLog: l, failUpsert: true}
	n, err := New(Config{ID: "A", Log: sink})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	good := eventlog.Event{
		ID:    eventlog.EventID{NodeID: "B", Seq: 1},
		HLC:   clock.HLC{Wall: 999_000, NodeID: "B"},
		SKU:   "SKU-1",
		Kind:  eventlog.KindQuantityDelta,
		Delta: 5,
	}
	accepted, err := n.Merge(ctx, []eventlog.Event{good})
	if err == nil {
		t.Fatal("Merge() error = nil, want an upsert failure")
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1", accepted)
	}
}
