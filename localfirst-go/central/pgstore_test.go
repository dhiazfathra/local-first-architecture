package central_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/central"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

// pgStore returns a store on a schema-isolated database. It FAILS rather than
// skips when Postgres is absent: run `make test`, which starts it.
func pgStore(t *testing.T) *central.PGStore {
	t.Helper()
	dsn := os.Getenv("LOCALFIRST_PG_DSN")
	if dsn == "" {
		t.Fatal("LOCALFIRST_PG_DSN is unset — run `make test`, which starts Postgres")
	}
	s, err := central.OpenPG(context.Background(), dsn, "central")
	if err != nil {
		t.Fatalf("OpenPG: %v", err)
	}
	if err := s.TruncateForTest(context.Background()); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func received(t *testing.T, node string, seq uint64, loc string, qty int64) eventlog.Record {
	t.Helper()
	payload, err := json.Marshal(domain.Received{SKU: "S", Location: loc, Qty: qty})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return eventlog.Record{
		NodeID: node, Seq: seq, Clock: clock.HLC{Wall: int64(seq), NodeID: node},
		Type: domain.TypeReceived, Payload: payload,
	}
}

func TestPGStoreSatisfiesTheSameContractAsSQLite(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)

	if s.NodeID() != "central" {
		t.Fatalf("NodeID = %q", s.NodeID())
	}
	batch := []eventlog.Record{received(t, "n1", 1, "A", 5), received(t, "n2", 1, "B", 3)}
	n, err := s.Merge(ctx, batch, nil)
	if err != nil || n != 2 {
		t.Fatalf("Merge = (%d, %v), want (2, nil)", n, err)
	}
	if n, err := s.Merge(ctx, batch, nil); err != nil || n != 0 {
		t.Fatalf("re-Merge = (%d, %v), want (0, nil): duplicates must be no-ops", n, err)
	}
	if n, err := s.Merge(ctx, nil, nil); err != nil || n != 0 {
		t.Fatalf("Merge(nil) = (%d, %v), want (0, nil)", n, err)
	}

	vv, err := s.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if !vv.Equal(eventlog.VersionVector{"n1": 1, "n2": 1}) {
		t.Fatalf("Version = %v", vv)
	}
	all, err := s.Since(ctx, nil)
	if err != nil || len(all) != 2 {
		t.Fatalf("Since(nil) = (%d, %v), want 2", len(all), err)
	}
	partial, err := s.Since(ctx, eventlog.VersionVector{"n1": 1})
	if err != nil || len(partial) != 1 || partial[0].NodeID != "n2" {
		t.Fatalf("Since(vv) = (%+v, %v), want only n2's record", partial, err)
	}

	if got, err := s.Cursor(ctx, "n1"); got != 0 || err != nil {
		t.Fatalf("Cursor = (%d, %v), want (0, nil)", got, err)
	}
	if err := s.SetCursor(ctx, "n1", 4); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	if err := s.SetCursor(ctx, "n1", 6); err != nil {
		t.Fatalf("SetCursor upsert: %v", err)
	}
	if got, err := s.Cursor(ctx, "n1"); got != 6 || err != nil {
		t.Fatalf("Cursor = (%d, %v), want (6, nil)", got, err)
	}
}

// TestGlobalSumIsASumNotAMerge is the conflict model in test form.
func TestGlobalSumIsASumNotAMerge(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	if _, err := s.Merge(ctx, []eventlog.Record{
		received(t, "n1", 1, "A", 5), // node 1 owns A
		received(t, "n2", 1, "B", 3), // node 2 owns B
		received(t, "n3", 1, "C", 7), // node 3 owns C
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	sum, err := s.GlobalSum(ctx)
	if err != nil {
		t.Fatalf("GlobalSum: %v", err)
	}
	if sum["S"] != 15 {
		t.Fatalf("global sum = %d, want 15 (5+3+7, summed because no key is shared)", sum["S"])
	}
}

func TestGlobalSumFailsHardOnAnUnknownRecordType(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	if _, err := s.Merge(ctx, []eventlog.Record{
		{NodeID: "n1", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n1"}, Type: "inventory.Teleported", Payload: []byte("{}")},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if _, err := s.GlobalSum(ctx); err == nil {
		t.Fatal("GlobalSum must fail on an unknown type, not skip it")
	}
}

func TestOpenPGRejectsABadDSN(t *testing.T) {
	if _, err := central.OpenPG(context.Background(), "postgres://nobody@127.0.0.1:1/none", "central"); err == nil {
		t.Fatal("OpenPG must fail against an unreachable database")
	}
}

func TestPGStoreFailsAfterClose(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	s.Close()
	if _, err := s.Version(ctx); err == nil {
		t.Error("Version after Close must fail")
	}
	if _, err := s.Since(ctx, nil); err == nil {
		t.Error("Since after Close must fail")
	}
	if _, err := s.Merge(ctx, []eventlog.Record{received(t, "n1", 1, "A", 1)}, nil); err == nil {
		t.Error("Merge after Close must fail")
	}
	if _, err := s.Cursor(ctx, "n1"); err == nil {
		t.Error("Cursor after Close must fail")
	}
	if err := s.SetCursor(ctx, "n1", 1); err == nil {
		t.Error("SetCursor after Close must fail")
	}
	if _, err := s.GlobalSum(ctx); err == nil {
		t.Error("GlobalSum after Close must fail")
	}
}

// TestMergeProjectsWhenGivenAProjector covers the projector path central uses
// to keep a live in-memory view.
func TestMergeProjectsWhenGivenAProjector(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	p := &recorder{}
	if _, err := s.Merge(ctx, []eventlog.Record{received(t, "n1", 1, "A", 5)}, p); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if len(p.committed) != 1 {
		t.Fatalf("projected %d records, want 1", len(p.committed))
	}
}

type recorder struct{ committed []eventlog.Record }

func (r *recorder) Project(rec eventlog.Record) (func(), error) {
	return func() { r.committed = append(r.committed, rec) }, nil
}

// TestMergeMultiRecordBatchProjectsEveryRecordThroughARealProjector uses a
// real stateful projector (domain.Inventory) rather than recorder (which only
// records, and would not catch cross-record ordering bugs). It reproduces the
// bug where Merge ran every record's Project before any commit: the second
// and third records would see the first's pre-commit balance, and only the
// last commit would stick, silently losing the earlier records' effects.
func TestMergeMultiRecordBatchProjectsEveryRecordThroughARealProjector(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "inv.db"), "central")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	inv, err := domain.NewInventory(ctx, store, clock.New("central", nil), nil)
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}

	batch := []eventlog.Record{
		received(t, "n1", 1, "A", 5),
		received(t, "n2", 1, "A", 3),
		received(t, "n3", 1, "A", 2),
	}
	n, err := s.Merge(ctx, batch, inv)
	if err != nil || n != 3 {
		t.Fatalf("Merge = (%d, %v), want (3, nil)", n, err)
	}
	if got := inv.Balance("S", "A"); got != 10 {
		t.Fatalf("Balance = %d, want 10 (5+3+2): a buggy Merge that projects "+
			"all records before committing any of them would leave only the "+
			"last record's effect", got)
	}
}

// TestOpenPGRejectsAMalformedDSN reaches pgxpool.New's synchronous error path.
// A bad DSN normally fails lazily on first use; only a URL that fails to parse
// makes the pool constructor itself return an error.
func TestOpenPGRejectsAMalformedDSN(t *testing.T) {
	if _, err := central.OpenPG(context.Background(), "postgres://%zz@127.0.0.1:5433/db", "central"); err == nil {
		t.Fatal("OpenPG must fail when the DSN cannot be parsed")
	}
}

type errProjector struct{}

func (errProjector) Project(eventlog.Record) (func(), error) { return nil, errors.New("boom") }

// TestMergeFailsWhenProjectionFails covers the projector error path inside
// Merge, and proves the transaction rolled back: the record must not persist.
func TestMergeFailsWhenProjectionFails(t *testing.T) {
	ctx, s := context.Background(), pgStore(t)
	if _, err := s.Merge(ctx, []eventlog.Record{received(t, "n1", 1, "A", 1)}, errProjector{}); err == nil {
		t.Fatal("Merge must fail when the projector errors")
	}
	all, err := s.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("Merge did not roll back: %d records persisted, want 0", len(all))
	}
}
