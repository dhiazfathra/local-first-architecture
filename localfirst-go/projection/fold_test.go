package projection_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// staticLog is an in-memory eventlog.Reader: Fold must not need a database.
type staticLog struct {
	recs []eventlog.Record
	err  error
}

func (s staticLog) Since(context.Context, eventlog.VersionVector) ([]eventlog.Record, error) {
	return s.recs, s.err
}

// --- reducer 1: state is an int64 total ---

type sumReducer struct{}

func (sumReducer) Zero() int64 { return 0 }
func (sumReducer) Apply(total int64, r eventlog.Record) (int64, error) {
	switch r.Type {
	case "add":
		return total + int64(len(r.Payload)), nil
	case "reset":
		return 0, nil
	default:
		return total, eventlog.ErrUnknownType
	}
}

// --- reducer 2: state is a map, a completely different shape ---

type countReducer struct{}

func (countReducer) Zero() map[string]int { return map[string]int{} }
func (countReducer) Apply(m map[string]int, r eventlog.Record) (map[string]int, error) {
	if r.Type != "add" && r.Type != "reset" {
		return m, eventlog.ErrUnknownType
	}
	m[r.Type]++
	return m, nil
}

func fixture() []eventlog.Record {
	mk := func(seq uint64, typ, payload string) eventlog.Record {
		return eventlog.Record{
			NodeID: "n1", Seq: seq, Clock: clock.HLC{Wall: int64(seq), NodeID: "n1"},
			Type: typ, Payload: []byte(payload),
		}
	}
	return []eventlog.Record{mk(1, "add", "abc"), mk(2, "add", "de"), mk(3, "reset", ""), mk(4, "add", "f")}
}

// TestFoldIsGenericOverUnrelatedReducers is the proof of the seam: one Fold,
// two reducers, two different state types, same records.
func TestFoldIsGenericOverUnrelatedReducers(t *testing.T) {
	ctx, log := context.Background(), staticLog{recs: fixture()}

	total, err := projection.Fold[int64](ctx, log, sumReducer{})
	if err != nil {
		t.Fatalf("Fold(sumReducer): %v", err)
	}
	if total != 1 {
		t.Fatalf("total = %d, want 1 (reset clears the 5 bytes before it)", total)
	}

	counts, err := projection.Fold[map[string]int](ctx, log, countReducer{})
	if err != nil {
		t.Fatalf("Fold(countReducer): %v", err)
	}
	if counts["add"] != 3 || counts["reset"] != 1 {
		t.Fatalf("counts = %v, want add:3 reset:1", counts)
	}
}

func TestFoldOnEmptyLogReturnsZero(t *testing.T) {
	got, err := projection.Fold[int64](context.Background(), staticLog{}, sumReducer{})
	if err != nil || got != 0 {
		t.Fatalf("Fold(empty) = (%d, %v), want (0, nil)", got, err)
	}
}

func TestFoldPropagatesReadError(t *testing.T) {
	boom := errors.New("disk gone")
	_, err := projection.Fold[int64](context.Background(), staticLog{err: boom}, sumReducer{})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

// TestFoldFailsHardOnUnknownType pins the error-handling rule: an unrecognised
// record type stops replay. Skipping it would teach silent divergence.
func TestFoldFailsHardOnUnknownType(t *testing.T) {
	log := staticLog{recs: []eventlog.Record{
		{NodeID: "n1", Seq: 1, Type: "add", Payload: []byte("x")},
		{NodeID: "n1", Seq: 2, Type: "FutureEvent"},
	}}
	_, err := projection.Fold[int64](context.Background(), log, sumReducer{})
	if !errors.Is(err, eventlog.ErrUnknownType) {
		t.Fatalf("err = %v, want ErrUnknownType", err)
	}
	if got := err.Error(); !contains(got, "n1/2") || !contains(got, "FutureEvent") {
		t.Fatalf("error %q must name the offending record and type", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestFoldRecords(t *testing.T) {
	got, err := projection.FoldRecords[int64](sumReducer{}, fixture())
	if err != nil || got != 1 {
		t.Fatalf("FoldRecords = (%d, %v), want (1, nil)", got, err)
	}
}
