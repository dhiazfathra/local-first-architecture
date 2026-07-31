package eventlog

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
)

func TestCompactRequiresBothDominanceAndSnapshotCoverage(t *testing.T) {
	ctx := context.Background()

	all := []EventID{{"A", 1}, {"A", 2}, {"B", 1}}

	tests := []struct {
		name        string
		snapshotFor string // "" means save no snapshot
		snapshotVV  VersionVector
		upTo        VersionVector
		wantRemain  []EventID
	}{
		{
			name:       "no snapshot deletes nothing",
			upTo:       VersionVector{"A": 2, "B": 1},
			wantRemain: all,
		},
		{
			name:        "snapshot covers all and peers acked all deletes all",
			snapshotFor: "SKU-1",
			snapshotVV:  VersionVector{"A": 2, "B": 1},
			upTo:        VersionVector{"A": 2, "B": 1},
			wantRemain:  nil,
		},
		{
			name:        "peer behind keeps its uncovered events",
			snapshotFor: "SKU-1",
			snapshotVV:  VersionVector{"A": 2, "B": 1},
			upTo:        VersionVector{"A": 1, "B": 1},
			wantRemain:  []EventID{{"A", 1}, {"A", 2}},
		},
		{
			name:        "snapshot behind keeps events it does not fold in",
			snapshotFor: "SKU-1",
			snapshotVV:  VersionVector{"A": 1},
			upTo:        VersionVector{"A": 2, "B": 1},
			wantRemain:  all,
		},
		{
			name:        "empty upTo deletes nothing",
			snapshotFor: "SKU-1",
			snapshotVV:  VersionVector{"A": 2, "B": 1},
			upTo:        VersionVector{},
			wantRemain:  all,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newTestLog(t)
			for _, e := range []Event{
				qty("A", 1, 100, "SKU-1", 5),
				qty("A", 2, 200, "SKU-1", -1),
				qty("B", 1, 300, "SKU-1", 2),
			} {
				if err := l.Append(ctx, e); err != nil {
					t.Fatalf("Append() error = %v", err)
				}
			}
			if tt.snapshotFor != "" {
				if err := l.SaveSnapshot(ctx, tt.snapshotFor, []byte(`{}`), tt.snapshotVV); err != nil {
					t.Fatalf("SaveSnapshot() error = %v", err)
				}
			}
			if err := l.Compact(ctx, tt.upTo); err != nil {
				t.Fatalf("Compact() error = %v", err)
			}

			var remain []EventID
			for e, err := range l.Since(ctx, VersionVector{}) {
				if err != nil {
					t.Fatalf("Since() error = %v", err)
				}
				remain = append(remain, e.ID)
			}
			slices.SortFunc(remain, func(a, b EventID) int {
				return cmp.Or(cmp.Compare(a.NodeID, b.NodeID), cmp.Compare(a.Seq, b.Seq))
			})
			if !slices.Equal(remain, tt.wantRemain) {
				t.Fatalf("remaining rows = %v, want %v", remain, tt.wantRemain)
			}

			// Compaction never means "this event stopped happening": whatever
			// it deleted, the version vector must still report it as held.
			got, err := l.VersionVector(ctx)
			if err != nil {
				t.Fatalf("VersionVector() error = %v", err)
			}
			if got["A"] != 2 || got["B"] != 1 || len(got) != 2 {
				t.Fatalf("vv after compaction = %v, want map[A:2 B:1]", got)
			}
		})
	}
}

func TestCompactOnlyConsidersSnapshottedSKUs(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	for _, e := range []Event{
		qty("A", 1, 100, "SKU-1", 5),
		qty("A", 2, 200, "SKU-2", 5),
	} {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	// Only SKU-1 has a snapshot, so SKU-2's event must survive even though the
	// peer vector dominates it.
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{}`), VersionVector{"A": 2}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if err := l.Compact(ctx, VersionVector{"A": 2}); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	n, err := l.CountForSKU(ctx, "SKU-2")
	if err != nil {
		t.Fatalf("CountForSKU() error = %v", err)
	}
	if n != 1 {
		t.Fatalf("SKU-2 events after compaction = %d, want 1", n)
	}
	if n, err = l.CountForSKU(ctx, "SKU-1"); err != nil || n != 0 {
		t.Fatalf("SKU-1 events after compaction = %d, %v; want 0, nil", n, err)
	}
}

func TestCompactRejectsCorruptSnapshotCovers(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if _, err := l.db.ExecContext(ctx,
		`INSERT INTO snapshots (sku, state, covers) VALUES ('SKU-1', ?, ?)`,
		[]byte(`{}`), []byte("{bad")); err != nil {
		t.Fatalf("seeding bad snapshot: %v", err)
	}
	if err := l.Compact(ctx, VersionVector{"A": 1}); err == nil {
		t.Fatal("Compact() error = nil, want a decode failure rather than a wrong deletion")
	}
}

func TestSnapshotRoundTripAndErrors(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)

	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); !errorsIsNoSnapshot(err) {
		t.Fatalf("LoadSnapshot() error = %v, want ErrNoSnapshot", err)
	}
	state, _ := json.Marshal(map[string]int{"x": 1})
	if err := l.SaveSnapshot(ctx, "SKU-1", state, VersionVector{"A": 3}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	// Overwrite -- SaveSnapshot must replace, not fail.
	if err := l.SaveSnapshot(ctx, "SKU-1", state, VersionVector{"A": 4}); err != nil {
		t.Fatalf("SaveSnapshot() overwrite error = %v", err)
	}
	gotState, covers, err := l.LoadSnapshot(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if string(gotState) != string(state) || covers["A"] != 4 {
		t.Fatalf("LoadSnapshot() = %s, %v; want %s, {A:4}", gotState, covers, state)
	}
	if _, err := l.db.ExecContext(ctx, `UPDATE snapshots SET covers = ? WHERE sku = 'SKU-1'`, []byte("{bad")); err != nil {
		t.Fatalf("corrupting covers: %v", err)
	}
	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); err == nil {
		t.Fatal("LoadSnapshot() error = nil for corrupt covers, want an error")
	}
}

func errorsIsNoSnapshot(err error) bool { return err != nil && errors.Is(err, ErrNoSnapshot) }

func TestCompactSurfacesSnapshotQueryError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if _, err := l.db.ExecContext(ctx, `DROP TABLE snapshots`); err != nil {
		t.Fatalf("dropping snapshots table: %v", err)
	}
	if err := l.Compact(ctx, VersionVector{"A": 1}); err == nil {
		t.Fatal("Compact() with no snapshots table error = nil, want an error")
	}
}

func TestCompactSurfacesStrandingCheckError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if err := l.Append(ctx, qty("A", 1, 100, "SKU-1", 5)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{}`), VersionVector{"A": 1}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if _, err := l.db.ExecContext(ctx, `DROP TABLE events`); err != nil {
		t.Fatalf("dropping events table: %v", err)
	}
	if err := l.Compact(ctx, VersionVector{"A": 1}); err == nil {
		t.Fatal("Compact() with no events table error = nil, want an error")
	}
}

func TestCompactSurfacesDeleteError(t *testing.T) {
	ctx := context.Background()
	l := newTestLog(t)
	if err := l.Append(ctx, qty("A", 1, 100, "SKU-1", 5)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{}`), VersionVector{"A": 1}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	// A trigger that rejects the delete Compact is about to issue -- this
	// exercises the DELETE's error path without disturbing the stranding
	// check, which must still see the events table intact and answer "no".
	if _, err := l.db.ExecContext(ctx, `
		CREATE TRIGGER reject_delete BEFORE DELETE ON events
		BEGIN SELECT RAISE(ABORT, 'delete rejected for test'); END`); err != nil {
		t.Fatalf("creating trigger: %v", err)
	}
	if err := l.Compact(ctx, VersionVector{"A": 1}); err == nil {
		t.Fatal("Compact() with a delete-rejecting trigger error = nil, want an error")
	}
}

// appendLocalSeqAfterCompact drives the exact regression: mint four local
// events, snapshot+compact them all away, then mint one more. Before the
// seq_watermark fix the fifth event was minted as A:1 -- a seq the node had
// already emitted and every peer already held, so its own snapshot's covers
// vector made the read path skip it and every peer's idempotent Append
// dropped it. The event vanished with no error anywhere.
func appendLocalSeqAfterCompact(ctx context.Context, t *testing.T, l Log) {
	t.Helper()
	mint := func(wall int64) func(Seq) Event {
		return func(s Seq) Event { return qty("A", s, wall, "SKU-1", 1) }
	}
	for i := int64(1); i <= 4; i++ {
		if _, err := l.AppendLocal(ctx, mint(100*i)); err != nil {
			t.Fatalf("AppendLocal() error = %v", err)
		}
	}
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{}`), VersionVector{"A": 4}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if err := l.Compact(ctx, VersionVector{"A": 4}); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	// Nothing survives, yet the vector must still report the node's real past.
	vv, err := l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["A"] != 4 {
		t.Fatalf("VersionVector() after compaction = %v, want A:4", vv)
	}

	e, err := l.AppendLocal(ctx, mint(500))
	if err != nil {
		t.Fatalf("AppendLocal() after compaction error = %v", err)
	}
	if e.ID.Seq != 5 {
		t.Fatalf("AppendLocal() after compaction seq = %d, want 5 (a reused seq is a silently lost event)", e.ID.Seq)
	}
	if vv, err = l.VersionVector(ctx); err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["A"] != 5 {
		t.Fatalf("VersionVector() = %v, want A:5", vv)
	}
}

func TestAppendLocalDoesNotReuseSeqAfterCompact(t *testing.T) {
	appendLocalSeqAfterCompact(context.Background(), t, newTestLog(t))
}
