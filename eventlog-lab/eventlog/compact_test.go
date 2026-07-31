package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestCompactRequiresBothDominanceAndSnapshotCoverage(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		snapshotFor  string // "" means save no snapshot
		snapshotVV   VersionVector
		upTo         VersionVector
		wantRemainVV VersionVector
	}{
		{
			name:         "no snapshot deletes nothing",
			upTo:         VersionVector{"A": 2, "B": 1},
			wantRemainVV: VersionVector{"A": 2, "B": 1},
		},
		{
			name:         "snapshot covers all and peers acked all deletes all",
			snapshotFor:  "SKU-1",
			snapshotVV:   VersionVector{"A": 2, "B": 1},
			upTo:         VersionVector{"A": 2, "B": 1},
			wantRemainVV: VersionVector{},
		},
		{
			name:         "peer behind keeps its uncovered events",
			snapshotFor:  "SKU-1",
			snapshotVV:   VersionVector{"A": 2, "B": 1},
			upTo:         VersionVector{"A": 1, "B": 1},
			wantRemainVV: VersionVector{"A": 2},
		},
		{
			name:         "snapshot behind keeps events it does not fold in",
			snapshotFor:  "SKU-1",
			snapshotVV:   VersionVector{"A": 1},
			upTo:         VersionVector{"A": 2, "B": 1},
			wantRemainVV: VersionVector{"A": 2, "B": 1},
		},
		{
			name:         "empty upTo deletes nothing",
			snapshotFor:  "SKU-1",
			snapshotVV:   VersionVector{"A": 2, "B": 1},
			upTo:         VersionVector{},
			wantRemainVV: VersionVector{"A": 2, "B": 1},
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
			got, err := l.VersionVector(ctx)
			if err != nil {
				t.Fatalf("VersionVector() error = %v", err)
			}
			if len(got) != len(tt.wantRemainVV) {
				t.Fatalf("remaining vv = %v, want %v", got, tt.wantRemainVV)
			}
			for k, v := range tt.wantRemainVV {
				if got[k] != v {
					t.Fatalf("remaining vv = %v, want %v", got, tt.wantRemainVV)
				}
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
