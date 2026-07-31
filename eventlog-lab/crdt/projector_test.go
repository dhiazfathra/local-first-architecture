package crdt

import (
	"context"
	"errors"
	"iter"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

func newProjector(t *testing.T, every int) (*Projector, *eventlog.SQLiteLog) {
	t.Helper()
	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "p.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return &Projector{Log: l, SnapshotEvery: every}, l
}

func TestProjectFoldsWholeLogWithoutSnapshot(t *testing.T) {
	ctx := context.Background()
	p, l := newProjector(t, 0)
	for _, e := range []eventlog.Event{qty("A", 1, 10, 10), qty("B", 1, 20, -4)} {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	got, err := p.Project(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if got.Quantity() != 6 {
		t.Fatalf("Quantity() = %d, want 6", got.Quantity())
	}
}

func TestProjectSnapshotPlusRemainderEqualsFullFold(t *testing.T) {
	ctx := context.Background()
	p, l := newProjector(t, 2)

	events := []eventlog.Event{
		qty("A", 1, 10, 10),
		qty("A", 2, 20, -1),
		meta("A", 3, 30, &eventlog.MetaSet{Name: ptr("Widget")}),
		qty("B", 1, 40, 5),
		del("B", 2, 50, true),
	}
	for i, e := range events {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		// Snapshot midway so later reads take the snapshot path.
		if i == 1 {
			if err := p.MaybeSnapshot(ctx, "SKU-1"); err != nil {
				t.Fatalf("MaybeSnapshot() error = %v", err)
			}
		}
	}
	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); err != nil {
		t.Fatalf("expected a snapshot to exist: %v", err)
	}

	got, err := p.Project(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if want := applyAll(events); !got.Equal(want) {
		t.Fatalf("snapshot path = %+v, want %+v", got, want)
	}
}

func TestMaybeSnapshotHonoursThreshold(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		every        int
		events       int
		wantSnapshot bool
	}{
		{"disabled", 0, 5, false},
		{"below threshold", 4, 3, false},
		{"at threshold", 3, 3, true},
		{"above threshold", 2, 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, l := newProjector(t, tt.every)
			for i := 1; i <= tt.events; i++ {
				if err := l.Append(ctx, qty("A", eventlog.Seq(i), int64(i*10), 1)); err != nil {
					t.Fatalf("Append() error = %v", err)
				}
			}
			if err := p.MaybeSnapshot(ctx, "SKU-1"); err != nil {
				t.Fatalf("MaybeSnapshot() error = %v", err)
			}
			_, _, err := l.LoadSnapshot(ctx, "SKU-1")
			if got := err == nil; got != tt.wantSnapshot {
				t.Fatalf("snapshot exists = %v, want %v (err = %v)", got, tt.wantSnapshot, err)
			}
		})
	}
}

// TestMaybeSnapshotClampsCoversToGapFreePrefix guards against the silent-loss
// risk in which Compact durably raises a node's watermark off a gapped
// covers vector: if node A's seq 2 was never delivered (reorder/async
// faults), covers["A"] must clamp to 1, never jump to 3 on the strength of
// seq 3 alone.
func TestMaybeSnapshotClampsCoversToGapFreePrefix(t *testing.T) {
	ctx := context.Background()
	p, l := newProjector(t, 2)
	if err := l.Append(ctx, qty("A", 1, 10, 10)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := l.Append(ctx, qty("A", 3, 30, 5)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := p.MaybeSnapshot(ctx, "SKU-1"); err != nil {
		t.Fatalf("MaybeSnapshot() error = %v", err)
	}
	_, covers, err := l.LoadSnapshot(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := covers["A"]; got != 1 {
		t.Fatalf("covers[%q] = %d, want 1 (gap-free prefix, seq 2 was never delivered)", "A", got)
	}
}

func TestProjectRejectsCorruptSnapshotState(t *testing.T) {
	ctx := context.Background()
	p, l := newProjector(t, 0)
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte("{bad"), eventlog.VersionVector{}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if _, err := p.Project(ctx, "SKU-1"); err == nil {
		t.Fatal("Project() error = nil for a corrupt snapshot, want an error")
	}
}

// eventsForSKUFailsLog wraps a real log but forces EventsForSKU to yield an
// error, exercising MaybeSnapshot's refold-loop error path without touching
// the unexported sqlite internals from outside the eventlog package.
type eventsForSKUFailsLog struct {
	*eventlog.SQLiteLog
}

func (eventsForSKUFailsLog) EventsForSKU(context.Context, string, eventlog.VersionVector) iter.Seq2[eventlog.Event, error] {
	return func(yield func(eventlog.Event, error) bool) {
		yield(eventlog.Event{}, errors.New("boom"))
	}
}

func TestProjectSurfacesFoldError(t *testing.T) {
	ctx := context.Background()
	_, l := newProjector(t, 0)
	if err := l.Append(ctx, qty("A", 1, 10, 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	// No snapshot exists, so LoadSnapshot succeeds with ErrNoSnapshot and
	// Project reaches the Fold call, where the wrapped EventsForSKU fails.
	p := &Projector{Log: eventsForSKUFailsLog{l}}
	if _, err := p.Project(ctx, "SKU-1"); err == nil {
		t.Fatal("Project() error = nil when EventsForSKU fails, want an error")
	}
}

func TestMaybeSnapshotSurfacesEventsForSKUError(t *testing.T) {
	ctx := context.Background()
	_, l := newProjector(t, 1)
	if err := l.Append(ctx, qty("A", 1, 10, 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	p := &Projector{Log: eventsForSKUFailsLog{l}, SnapshotEvery: 1}
	if err := p.MaybeSnapshot(ctx, "SKU-1"); err == nil {
		t.Fatal("MaybeSnapshot() error = nil when EventsForSKU fails, want an error")
	}
}

// versionVectorFailsLog wraps a real log but forces VersionVector to error,
// exercising MaybeSnapshot's frontier-lookup error path (added when
// MaybeSnapshot started consulting the log's true gap-free frontier to
// decide what's safe to fold, rather than folding everything and clamping
// covers after the fact).
type versionVectorFailsLog struct {
	*eventlog.SQLiteLog
}

func (versionVectorFailsLog) VersionVector(context.Context) (eventlog.VersionVector, error) {
	return nil, errors.New("boom")
}

func TestMaybeSnapshotSurfacesVersionVectorError(t *testing.T) {
	ctx := context.Background()
	_, l := newProjector(t, 1)
	if err := l.Append(ctx, qty("A", 1, 10, 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	p := &Projector{Log: versionVectorFailsLog{l}, SnapshotEvery: 1}
	if err := p.MaybeSnapshot(ctx, "SKU-1"); err == nil {
		t.Fatal("MaybeSnapshot() error = nil when VersionVector fails, want an error")
	}
}

func TestProjectPropagatesLogErrors(t *testing.T) {
	ctx := context.Background()
	p, l := newProjector(t, 1)
	if err := l.Append(ctx, qty("A", 1, 10, 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := p.Project(ctx, "SKU-1"); err == nil {
		t.Error("Project() on a closed log error = nil, want an error")
	}
	if err := p.MaybeSnapshot(ctx, "SKU-1"); err == nil {
		t.Error("MaybeSnapshot() on a closed log error = nil, want an error")
	}
}
