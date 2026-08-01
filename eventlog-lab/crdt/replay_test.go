package crdt

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

// TestFullLogReplayIsDeterministic folds the same log into two independent
// fresh states and requires identical results. This is the actual idempotence
// guarantee: re-deriving projected state from the deduplicated log is free and
// repeatable. It is NOT a claim that Apply itself absorbs a duplicate event --
// folding the same events into an already-populated state would double-count
// (see TestDuplicateThroughLogIsAbsorbedButDirectApplyDoubleCounts below for
// where the real dedup boundary is).
func TestFullLogReplayIsDeterministic(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		events []eventlog.Event
	}{
		{name: "empty log", events: nil},
		{name: "one delta", events: buildEvents(1)},
		{name: "mixed kinds", events: buildEvents(9)},
		{name: "long log", events: buildEvents(60)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "replay.db"))
			if err != nil {
				t.Fatalf("OpenSQLite() error = %v", err)
			}
			defer func() { _ = l.Close() }()
			for _, e := range tt.events {
				if err := l.Append(ctx, e); err != nil {
					t.Fatalf("Append(%v) error = %v", e.ID, err)
				}
			}

			first := NewItemState()
			if err := first.Fold(l.Since(ctx, eventlog.VersionVector{})); err != nil {
				t.Fatalf("first Fold() error = %v", err)
			}
			// Second replay into a FRESH state must match exactly -- re-deriving
			// projected state from the log is deterministic and repeatable.
			second := NewItemState()
			if err := second.Fold(l.Since(ctx, eventlog.VersionVector{})); err != nil {
				t.Fatalf("second Fold() error = %v", err)
			}
			if !second.Equal(first) {
				t.Errorf("replay diverged: quantity %d, want %d",
					second.Quantity(), first.Quantity())
			}
		})
	}
}

// TestDuplicateThroughLogIsAbsorbedButDirectApplyDoubleCounts documents where
// idempotence actually lives: in Append's primary key, not in Apply.
func TestDuplicateThroughLogIsAbsorbedButDirectApplyDoubleCounts(t *testing.T) {
	ctx := context.Background()
	e := buildEvents(1)[0]

	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "dup.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	for i := 0; i < 5; i++ {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() attempt %d error = %v", i, err)
		}
	}
	viaLog := NewItemState()
	if err := viaLog.Fold(l.Since(ctx, eventlog.VersionVector{})); err != nil {
		t.Fatalf("Fold() error = %v", err)
	}

	direct := NewItemState()
	for i := 0; i < 5; i++ {
		direct.Apply(e)
	}

	if viaLog.Quantity() != e.Delta {
		t.Errorf("log path quantity = %d, want %d (duplicates absorbed by the primary key)",
			viaLog.Quantity(), e.Delta)
	}
	if direct.Quantity() != 5*e.Delta {
		t.Errorf("direct path quantity = %d, want %d -- Apply is deliberately NOT idempotent; "+
			"only the log path is safe",
			direct.Quantity(), 5*e.Delta)
	}
}
