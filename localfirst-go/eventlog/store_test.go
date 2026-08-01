package eventlog_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// countProjector records what it was asked to project and whether the store
// committed it, so tests can assert the two-phase contract.
type countProjector struct {
	staged, committed []string
	err               error
}

func (c *countProjector) Project(r eventlog.Record) (func(), error) {
	if c.err != nil {
		return nil, c.err
	}
	c.staged = append(c.staged, r.Type)
	return func() { c.committed = append(c.committed, r.Type) }, nil
}

type rejectAll struct{ err error }

func (r rejectAll) Check(eventlog.Record) error { return r.err }

func open(t *testing.T, nodeID string) *eventlog.Store {
	t.Helper()
	s, err := eventlog.Open(filepath.Join(t.TempDir(), nodeID+".db"), nodeID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenRejectsBadPath(t *testing.T) {
	if _, err := eventlog.Open(filepath.Join(t.TempDir(), "nope", "x.db"), "n1"); err == nil {
		t.Fatal("Open into a missing directory must fail")
	}
}

func TestAppendAssignsDenseSequences(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	c := clock.New("n1", nil)
	p := &countProjector{}
	for i := uint64(1); i <= 3; i++ {
		got, err := s.Append(ctx, "T", []byte(`{}`), c.Now(), nil, p)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if got.Seq != i || got.NodeID != "n1" {
			t.Fatalf("Append = %+v, want seq %d node n1", got, i)
		}
	}
	if len(p.committed) != 3 {
		t.Fatalf("committed %d projections, want 3", len(p.committed))
	}
	if s.NodeID() != "n1" {
		t.Fatalf("NodeID = %q", s.NodeID())
	}
}

func TestAppendResumesSequenceAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "n1.db")
	first, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := first.Append(ctx, "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	second, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close() }()
	got, err := second.Append(ctx, "T", nil, clock.HLC{Wall: 2, NodeID: "n1"}, nil, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got.Seq != 2 {
		t.Fatalf("Seq after reopen = %d, want 2", got.Seq)
	}
}

func TestValidatorRejectionLeavesNoRecord(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	boom := errors.New("balance would go negative")
	p := &countProjector{}

	_, err := s.Append(ctx, "T", []byte(`{}`), clock.HLC{Wall: 1, NodeID: "n1"}, rejectAll{boom}, p)
	if !errors.Is(err, boom) {
		t.Fatalf("Append error = %v, want %v", err, boom)
	}
	recs, err := s.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("rejected append left %d records", len(recs))
	}
	if len(p.staged) != 0 || len(p.committed) != 0 {
		t.Fatal("rejected append must not reach the projector")
	}
	// The sequence number must not be burned either.
	next, err := s.Append(ctx, "T", nil, clock.HLC{Wall: 2, NodeID: "n1"}, nil, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if next.Seq != 1 {
		t.Fatalf("Seq = %d after rejection, want 1", next.Seq)
	}
}

func TestProjectorFailureRollsBackTheAppend(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	boom := errors.New("unknown type")
	_, err := s.Append(ctx, "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, &countProjector{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("Append error = %v, want %v", err, boom)
	}
	recs, _ := s.Since(ctx, nil)
	if len(recs) != 0 {
		t.Fatalf("projector failure left %d records", len(recs))
	}
}

func TestMergeIsIdempotent(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	remote := []eventlog.Record{
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 10, NodeID: "n2"}, Type: "T", Payload: []byte("a")},
		{NodeID: "n2", Seq: 2, Clock: clock.HLC{Wall: 11, NodeID: "n2"}, Type: "T", Payload: []byte("b")},
	}
	p := &countProjector{}
	n, err := s.Merge(ctx, remote, p)
	if err != nil || n != 2 {
		t.Fatalf("Merge = (%d, %v), want (2, nil)", n, err)
	}
	// Re-delivering the same batch, plus one new record, inserts only the new one.
	n, err = s.Merge(ctx, append(remote, eventlog.Record{
		NodeID: "n2", Seq: 3, Clock: clock.HLC{Wall: 12, NodeID: "n2"}, Type: "T",
	}), p)
	if err != nil || n != 1 {
		t.Fatalf("re-merge = (%d, %v), want (1, nil)", n, err)
	}
	recs, _ := s.Since(ctx, nil)
	if len(recs) != 3 {
		t.Fatalf("held %d records, want 3", len(recs))
	}
	if len(p.committed) != 3 {
		t.Fatalf("projected %d records, want 3 (duplicates must not re-project)", len(p.committed))
	}
}

func TestMergeAdvancesOwnSequence(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	// Central-style merge of records authored by this node id (e.g. restored
	// from a peer): the local counter must jump past them.
	if _, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n1", Seq: 7, Clock: clock.HLC{Wall: 1, NodeID: "n1"}, Type: "T"},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	got, err := s.Append(ctx, "T", nil, clock.HLC{Wall: 2, NodeID: "n1"}, nil, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got.Seq != 8 {
		t.Fatalf("Seq = %d, want 8", got.Seq)
	}
}

func TestMergeEmptyBatchIsANoOp(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	if n, err := s.Merge(ctx, nil, nil); n != 0 || err != nil {
		t.Fatalf("Merge(nil) = (%d, %v), want (0, nil)", n, err)
	}
}

func TestMergeProjectorFailureRollsBack(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	boom := errors.New("unknown type")
	_, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n2"}, Type: "?"},
	}, &countProjector{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("Merge error = %v, want %v", err, boom)
	}
	if recs, _ := s.Since(ctx, nil); len(recs) != 0 {
		t.Fatalf("rolled-back merge left %d records", len(recs))
	}
}

func TestSinceFiltersByVersionVectorAndReplaysDeterministically(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	if _, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n2", Seq: 2, Clock: clock.HLC{Wall: 30, NodeID: "n2"}, Type: "B"},
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 10, NodeID: "n2"}, Type: "A"},
		{NodeID: "n3", Seq: 1, Clock: clock.HLC{Wall: 20, NodeID: "n3"}, Type: "C"},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	all, err := s.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	var order []string
	for _, r := range all {
		order = append(order, r.Type)
	}
	if want := []string{"A", "C", "B"}; !equal(order, want) {
		t.Fatalf("replay order = %v, want %v (HLC order, insertion order irrelevant)", order, want)
	}
	// Replaying twice gives byte-identical results.
	again, _ := s.Since(ctx, nil)
	for i := range all {
		if all[i].Type != again[i].Type || all[i].Seq != again[i].Seq {
			t.Fatalf("replay is not deterministic at %d", i)
		}
	}

	partial, err := s.Since(ctx, eventlog.VersionVector{"n2": 1})
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(partial) != 2 || partial[0].Type != "C" || partial[1].Type != "B" {
		t.Fatalf("Since(vv) = %+v, want C then B", partial)
	}
	if got, err := s.Since(ctx, eventlog.VersionVector{"n2": 99, "n3": 99}); err != nil || len(got) != 0 {
		t.Fatalf("Since with a vector ahead of us = (%d, %v), want (0, nil)", len(got), err)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestVersion(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	vv, err := s.Version(ctx)
	if err != nil || len(vv) != 0 {
		t.Fatalf("Version on empty log = (%v, %v), want empty", vv, err)
	}
	if _, err := s.Append(ctx, "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n2", Seq: 4, Clock: clock.HLC{Wall: 2, NodeID: "n2"}, Type: "T"},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	vv, err = s.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if !vv.Equal(eventlog.VersionVector{"n1": 1, "n2": 4}) {
		t.Fatalf("Version = %v, want {n1:1, n2:4}", vv)
	}
}

func TestCursors(t *testing.T) {
	ctx, s := context.Background(), open(t, "n1")
	if got, err := s.Cursor(ctx, "central"); got != 0 || err != nil {
		t.Fatalf("unknown cursor = (%d, %v), want (0, nil)", got, err)
	}
	if err := s.SetCursor(ctx, "central", 5); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	if err := s.SetCursor(ctx, "central", 9); err != nil {
		t.Fatalf("SetCursor upsert: %v", err)
	}
	if got, err := s.Cursor(ctx, "central"); got != 9 || err != nil {
		t.Fatalf("Cursor = (%d, %v), want (9, nil)", got, err)
	}
}

func TestOperationsFailAfterClose(t *testing.T) {
	ctx := context.Background()
	s, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := s.Append(ctx, "T", nil, clock.HLC{}, nil, nil); err == nil {
		t.Error("Append after Close must fail")
	}
	if _, err := s.Merge(ctx, []eventlog.Record{{NodeID: "n2", Seq: 1}}, nil); err == nil {
		t.Error("Merge after Close must fail")
	}
	if _, err := s.Since(ctx, nil); err == nil {
		t.Error("Since after Close must fail")
	}
	if _, err := s.Version(ctx); err == nil {
		t.Error("Version after Close must fail")
	}
	if _, err := s.Cursor(ctx, "central"); err == nil {
		t.Error("Cursor after Close must fail")
	}
	if err := s.SetCursor(ctx, "central", 1); err == nil {
		t.Error("SetCursor after Close must fail")
	}
}

// rawDB opens a second connection to the same file for tests that must corrupt
// or restrict the schema underneath the store.
func rawDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("raw Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOpenSurfacesReadOwnSeqError(t *testing.T) {
	// Pre-create the records table without the seq column so the schema exec is
	// a no-op but the MAX(seq) probe fails.
	path := filepath.Join(t.TempDir(), "n1.db")
	db := rawDB(t, path)
	if _, err := db.Exec(`CREATE TABLE records (node_id TEXT NOT NULL, foo INTEGER NOT NULL)`); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	if _, err := eventlog.Open(path, "n1"); err == nil {
		t.Fatal("Open must fail when the own-sequence probe cannot scan")
	}
}

func TestOpenSurfacesSchemaError(t *testing.T) {
	// A read-only directory makes the schema exec fail (modernc creates the
	// file lazily on first statement).
	path := filepath.Join(t.TempDir(), "ro", "x.db")
	if err := os.MkdirAll(filepath.Dir(path), 0o500); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := eventlog.Open(path, "n1"); err == nil {
		t.Fatal("Open must fail when the schema exec cannot create the file")
	}
}

// boomTrigger makes every INSERT INTO records fail, so the insert path inside
// Append and Merge (and the shared exec helper) reports an error.
func boomTrigger(t *testing.T, path string) {
	t.Helper()
	db := rawDB(t, path)
	if _, err := db.Exec(`CREATE TRIGGER boom BEFORE INSERT ON records BEGIN SELECT RAISE(FAIL, 'boom'); END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
}

func TestAppendSurfacesInsertError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.db")
	s, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	boomTrigger(t, path)
	if _, err := s.Append(context.Background(), "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, nil); err == nil {
		t.Fatal("Append must fail when the insert is rejected")
	}
}

func TestMergeSurfacesExecError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.db")
	s, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	boomTrigger(t, path)
	if _, err := s.Merge(context.Background(), []eventlog.Record{
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n2"}, Type: "T"},
	}, nil); err == nil {
		t.Fatal("Merge must fail when the insert is rejected")
	}
}

// cancelProjector cancels the transaction's context from inside Project, so the
// subsequent tx.Commit must fail and roll the write back.
type cancelProjector struct{ cancel context.CancelFunc }

func (c cancelProjector) Project(eventlog.Record) (func(), error) {
	c.cancel()
	return func() {}, nil
}

func TestAppendSurfacesCommitError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := open(t, "n1")
	if _, err := s.Append(ctx, "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, cancelProjector{cancel}); err == nil {
		t.Fatal("Append must fail when Commit is interrupted")
	}
	if recs, _ := s.Since(context.Background(), nil); len(recs) != 0 {
		t.Fatalf("interrupted commit left %d records", len(recs))
	}
}

func TestMergeSurfacesCommitError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := open(t, "n1")
	if _, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n2"}, Type: "T"},
	}, cancelProjector{cancel}); err == nil {
		t.Fatal("Merge must fail when Commit is interrupted")
	}
	if recs, _ := s.Since(context.Background(), nil); len(recs) != 0 {
		t.Fatalf("interrupted commit left %d records", len(recs))
	}
}

func TestSinceSurfacesScanError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.db")
	s, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.Append(context.Background(), "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// hlc_wall is INTEGER; stuffing text into it breaks the Scan.
	db := rawDB(t, path)
	if _, err := db.Exec(`UPDATE records SET hlc_wall = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if _, err := s.Since(context.Background(), nil); err == nil {
		t.Fatal("Since must fail when a column cannot be scanned")
	}
}

func TestVersionSurfacesScanError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.db")
	s, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.Append(context.Background(), "T", nil, clock.HLC{Wall: 1, NodeID: "n1"}, nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	db := rawDB(t, path)
	if _, err := db.Exec(`UPDATE records SET seq = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if _, err := s.Version(context.Background()); err == nil {
		t.Fatal("Version must fail when a column cannot be scanned")
	}
}

// bulkFill fills records with n rows so an iteration can be cancelled mid-flight
// and surface rows.Err. distinct gives each row its own node_id (Version needs
// distinct groups to iterate every row).
func bulkFill(t *testing.T, path string, n int, distinct bool) {
	t.Helper()
	db := rawDB(t, path)
	node := "'n1'"
	if distinct {
		node = "'n' || printf('%d', s)"
	}
	sql := fmt.Sprintf(`INSERT INTO records (node_id, seq, hlc_wall, hlc_logical, type, payload)
		SELECT %s, s, s, 0, 'T', X'' FROM (WITH RECURSIVE c(s) AS (SELECT 1 UNION ALL SELECT s+1 FROM c WHERE s < %d) SELECT s FROM c)`,
		node, n)
	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("bulk fill: %v", err)
	}
}

// cancelMidIteration returns a context that cancels delay after the query has
// started, interrupting the rows iteration.
func cancelMidIteration(delay time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(delay); cancel() }()
	return ctx, cancel
}

// requireRowsErr runs fn (Since/Version) with fresh contexts until the
// cancelled iteration surfaces via rows.Err rather than rows.Scan. Go's
// database/sql races the two: a mid-iteration cancel sets contextDone, and
// either the next Next() reports it (rows.Err) or close() lands in the gap
// before Scan (scan error). Probes measured rows.Err at ~30% per attempt; the
// cap of 50 puts P(never fired) below ~1.8e-8 per test.
func requireRowsErr(t *testing.T, what string, fn func(context.Context) error) {
	t.Helper()
	const attempts = 50
	for attempt := 0; attempt < attempts; attempt++ {
		ctx, cancel := cancelMidIteration(10 * time.Millisecond)
		err := fn(ctx)
		cancel()
		if err != nil && strings.Contains(err.Error(), "eventlog: rows:") {
			return
		}
	}
	t.Fatalf("%s: rows.Err() branch never fired in %d attempts", what, attempts)
}

func TestSinceSurfacesRowsErr(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.db")
	s, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	bulkFill(t, path, 200_000, false)
	requireRowsErr(t, "Since", func(ctx context.Context) error {
		_, err := s.Since(ctx, nil)
		return err
	})
}

func TestVersionSurfacesRowsErr(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.db")
	s, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	bulkFill(t, path, 200_000, true)
	requireRowsErr(t, "Version", func(ctx context.Context) error {
		_, err := s.Version(ctx)
		return err
	})
}
