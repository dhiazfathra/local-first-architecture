//go:build integration

// This file is excluded from the default `go test ./...` gate by the
// integration build tag -- it is a separate, explicitly required gate, not a
// task allowed to skip. Run it with:
//
//	go test -tags=integration ./eventlog/ -run TestPostgres -v
//
// against a live Postgres (EVENTLOG_LAB_PG_DSN set). See Task 10 domain notes.
package eventlog

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// truncateAll empties every table. Test helper only -- defined here (rather
// than in postgres.go) so it only exists in the integration build; a method
// used solely by tests behind this build tag would otherwise trip the
// unused-method lint check in the default build.
func (l *PostgresLog) truncateAll(ctx context.Context) error {
	if _, err := l.pool.Exec(ctx, `TRUNCATE events, snapshots, sync_cursors, seq_watermark, projections`); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	return nil
}

// newPGLog returns a Postgres log with empty tables. EVENTLOG_LAB_PG_DSN must
// be set -- this file only builds under -tags=integration, so there is no
// silent-skip path: a missing DSN is a hard test failure, not a skip.
func newPGLog(t *testing.T) *PostgresLog {
	t.Helper()
	dsn := os.Getenv("EVENTLOG_LAB_PG_DSN")
	if dsn == "" {
		t.Fatal("EVENTLOG_LAB_PG_DSN not set; start Postgres and export it (see Task 10)")
	}
	ctx := context.Background()
	l, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgres() error = %v", err)
	}
	if err := l.truncateAll(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestPostgresSatisfiesTheSameContractAsSQLite(t *testing.T) {
	ctx := context.Background()
	l := newPGLog(t)

	e := qty("A", 1, 100, "SKU-1", 5)
	for i := 0; i < 3; i++ {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() call %d error = %v", i, err)
		}
	}
	vv, err := l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if vv["A"] != 1 {
		t.Fatalf("vv = %v, want {A:1} -- Append must be idempotent on (node_id, seq)", vv)
	}

	if err := l.Append(ctx, Event{ID: EventID{NodeID: "A", Seq: 2}, HLC: clock.HLC{NodeID: "A"}, SKU: "S", Kind: 99}); err == nil {
		t.Fatal("Append() error = nil for an unknown kind, want ErrMalformedEvent")
	}

	local, err := l.AppendLocal(ctx, func(s Seq) Event { return qty("C", s, 200, "SKU-1", 2) })
	if err != nil {
		t.Fatalf("AppendLocal() error = %v", err)
	}
	if local.ID.Seq != 1 {
		t.Fatalf("AppendLocal() seq = %d, want 1", local.ID.Seq)
	}

	if err := l.Append(ctx, qty("B", 1, 50, "SKU-2", 1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	var ids []EventID
	for ev, err := range l.Since(ctx, VersionVector{}) {
		if err != nil {
			t.Fatalf("Since() error = %v", err)
		}
		ids = append(ids, ev.ID)
	}
	want := []EventID{{NodeID: "B", Seq: 1}, {NodeID: "A", Seq: 1}, {NodeID: "C", Seq: 1}}
	if len(ids) != len(want) {
		t.Fatalf("Since() ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("Since() ids = %v, want %v", ids, want)
		}
	}

	n, err := l.CountForSKU(ctx, "SKU-1")
	if err != nil || n != 2 {
		t.Fatalf("CountForSKU() = %d, %v; want 2, nil", n, err)
	}

	if _, _, err := l.LoadSnapshot(ctx, "SKU-1"); err == nil {
		t.Fatal("LoadSnapshot() error = nil with no snapshot, want ErrNoSnapshot")
	}
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{"pos":{}}`), VersionVector{"A": 1, "C": 1}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{"pos":{}}`), VersionVector{"A": 1, "C": 1}); err != nil {
		t.Fatalf("SaveSnapshot() overwrite error = %v", err)
	}
	blob, covers, err := l.LoadSnapshot(ctx, "SKU-1")
	if err != nil || string(blob) != `{"pos":{}}` || covers["A"] != 1 {
		t.Fatalf("LoadSnapshot() = %s, %v, %v", blob, covers, err)
	}

	if got, err := l.Cursor(ctx, "A"); err != nil || got != 0 {
		t.Fatalf("Cursor() = %d, %v; want 0, nil", got, err)
	}
	if err := l.SetCursor(ctx, "A", 1); err != nil {
		t.Fatalf("SetCursor() error = %v", err)
	}
	if got, err := l.Cursor(ctx, "A"); err != nil || got != 1 {
		t.Fatalf("Cursor() = %d, %v; want 1, nil", got, err)
	}

	// Compaction: A and C are covered by both the snapshot and the peer
	// vector; B's event has no snapshot and must survive.
	if err := l.Compact(ctx, VersionVector{"A": 1, "C": 1}); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if n, err = l.CountForSKU(ctx, "SKU-1"); err != nil || n != 0 {
		t.Fatalf("SKU-1 count after compaction = %d, %v; want 0, nil", n, err)
	}
	if n, err = l.CountForSKU(ctx, "SKU-2"); err != nil || n != 1 {
		t.Fatalf("SKU-2 count after compaction = %d, %v; want 1, nil", n, err)
	}
}

func TestPostgresEventsForSKU(t *testing.T) {
	ctx := context.Background()
	l := newPGLog(t)
	for _, e := range []Event{qty("A", 1, 10, "SKU-1", 1), qty("A", 2, 20, "SKU-2", 1)} {
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
		{"all", "SKU-1", VersionVector{}, 1},
		{"covered", "SKU-1", VersionVector{"A": 1}, 0},
		{"unknown", "SKU-9", VersionVector{}, 0},
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

func TestPostgresOpenRejectsBadDSN(t *testing.T) {
	if _, err := OpenPostgres(context.Background(), "not-a-dsn"); err == nil {
		t.Fatal("OpenPostgres() error = nil for a bad DSN, want an error")
	}
}

// TestPostgresAppendLocalSerializesConcurrentSeqAllocation guards the
// pg_advisory_xact_lock fix: without it, concurrent AppendLocal calls for the
// same node can silently drop an event under ON CONFLICT DO NOTHING.
func TestPostgresAppendLocalSerializesConcurrentSeqAllocation(t *testing.T) {
	ctx := context.Background()
	l := newPGLog(t)

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = l.AppendLocal(ctx, func(s Seq) Event {
				return qty("A", s, int64(1000+i), "SKU-1", 1)
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("AppendLocal() goroutine %d error = %v", i, err)
		}
	}

	seqs := map[Seq]bool{}
	count := 0
	for e, err := range l.Since(ctx, VersionVector{}) {
		if err != nil {
			t.Fatalf("Since() error = %v", err)
		}
		if seqs[e.ID.Seq] {
			t.Fatalf("seq %d allocated to more than one event -- lost a concurrent append", e.ID.Seq)
		}
		seqs[e.ID.Seq] = true
		count++
	}
	if count != n {
		t.Fatalf("stored %d events, want %d -- a concurrent append was silently dropped", count, n)
	}
}

// TestPostgresCompactDoesNotStrandLaterEventsAcrossSKUs reproduces the
// stranding bug fixed in Task 10 follow-up: seq is a per-node counter shared
// across SKUs, so compacting the covered prefix of one SKU while a later,
// uncovered event from the same node lives under a *different* SKU would
// strand that later event -- versionVectorQuery's gap-free-prefix computation
// partitions by node_id across the whole events table, not per-sku, so the
// gap left behind makes the node's frontier disappear entirely.
func TestPostgresCompactDoesNotStrandLaterEventsAcrossSKUs(t *testing.T) {
	ctx := context.Background()
	l := newPGLog(t)

	// Node A writes seq 1 under SKU-1, then seq 2 under SKU-2.
	if err := l.Append(ctx, qty("A", 1, 100, "SKU-1", 5)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := l.Append(ctx, qty("A", 2, 200, "SKU-2", 3)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Only SKU-1 is snapshotted and it only covers seq 1 -- SKU-2's later
	// event is not covered by any snapshot.
	if err := l.SaveSnapshot(ctx, "SKU-1", []byte(`{}`), VersionVector{"A": 1}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	// The peer has acked everything, so the naive (unguarded) compact would
	// delete SKU-1's seq-1 event, leaving only SKU-2's seq-2 event behind --
	// a gap at seq 1 that makes node A vanish from the version vector.
	if err := l.Compact(ctx, VersionVector{"A": 2}); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	got, err := l.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() error = %v", err)
	}
	if got["A"] != 2 {
		t.Fatalf("VersionVector()[A] = %d, want 2 -- SKU-1's seq-1 event was stranded, corrupting sync", got["A"])
	}

	// Both events must still be physically present: the guard should have
	// skipped the delete entirely rather than stranding SKU-2's event.
	n1, err := l.CountForSKU(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("CountForSKU(SKU-1) error = %v", err)
	}
	if n1 != 1 {
		t.Fatalf("SKU-1 events after compaction = %d, want 1 -- stranding guard did not fire", n1)
	}
	n2, err := l.CountForSKU(ctx, "SKU-2")
	if err != nil {
		t.Fatalf("CountForSKU(SKU-2) error = %v", err)
	}
	if n2 != 1 {
		t.Fatalf("SKU-2 events after compaction = %d, want 1", n2)
	}
}

func TestPostgresAppendLocalDoesNotReuseSeqAfterCompact(t *testing.T) {
	appendLocalSeqAfterCompact(context.Background(), t, newPGLog(t))
}
