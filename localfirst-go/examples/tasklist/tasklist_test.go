package tasklist_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/examples/tasklist"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// rec builds a record the way the engine would, with an already-encoded payload.
func rec(t *testing.T, seq uint64, typ string, payload any) eventlog.Record {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return eventlog.Record{
		NodeID:  "n1",
		Seq:     seq,
		Clock:   clock.HLC{Wall: int64(seq), Logical: 0, NodeID: "n1"},
		Type:    typ,
		Payload: body,
	}
}

func TestReducerApply(t *testing.T) {
	tests := []struct {
		name    string
		records []eventlog.Record
		want    tasklist.State
		wantErr error
	}{
		{
			name: "empty log folds to an empty list",
			want: tasklist.State{},
		},
		{
			name:    "one add",
			records: []eventlog.Record{rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "write the ADR"})},
			want:    tasklist.State{"t1": {Title: "write the ADR"}},
		},
		{
			name: "add then complete",
			records: []eventlog.Record{
				rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "write the ADR"}),
				rec(t, 2, tasklist.TypeCompleted, tasklist.Completed{ID: "t1"}),
			},
			want: tasklist.State{"t1": {Title: "write the ADR", Done: true}},
		},
		{
			name: "two independent tasks",
			records: []eventlog.Record{
				rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "a"}),
				rec(t, 2, tasklist.TypeAdded, tasklist.Added{ID: "t2", Title: "b"}),
				rec(t, 3, tasklist.TypeCompleted, tasklist.Completed{ID: "t2"}),
			},
			want: tasklist.State{"t1": {Title: "a"}, "t2": {Title: "b", Done: true}},
		},
		{
			name:    "completing an unknown task is an error, not a no-op",
			records: []eventlog.Record{rec(t, 1, tasklist.TypeCompleted, tasklist.Completed{ID: "ghost"})},
			wantErr: tasklist.ErrNoSuchTask,
		},
		{
			name: "completing twice is an error",
			records: []eventlog.Record{
				rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "a"}),
				rec(t, 2, tasklist.TypeCompleted, tasklist.Completed{ID: "t1"}),
				rec(t, 3, tasklist.TypeCompleted, tasklist.Completed{ID: "t1"}),
			},
			wantErr: tasklist.ErrAlreadyDone,
		},
		{
			name: "adding the same id twice is an error",
			records: []eventlog.Record{
				rec(t, 1, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "a"}),
				rec(t, 2, tasklist.TypeAdded, tasklist.Added{ID: "t1", Title: "a again"}),
			},
			wantErr: tasklist.ErrDuplicateTask,
		},
		{
			name:    "an unknown record type is a hard error",
			records: []eventlog.Record{rec(t, 1, "tasklist.Renamed", map[string]string{"id": "t1"})},
			wantErr: eventlog.ErrUnknownType,
		},
		{
			name: "a corrupt payload is a hard error",
			records: []eventlog.Record{{
				NodeID: "n1", Seq: 1, Type: tasklist.TypeAdded, Payload: []byte("{not json"),
			}},
			wantErr: tasklist.ErrBadPayload,
		},
		{
			name: "a corrupt completed payload is a hard error",
			records: []eventlog.Record{{
				NodeID: "n1", Seq: 1, Type: tasklist.TypeCompleted, Payload: []byte("{not json"),
			}},
			wantErr: tasklist.ErrBadPayload,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := projection.FoldRecords[tasklist.State](tasklist.Reducer{}, tc.records)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FoldRecords = %v, want nil", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("state = %v, want %v", got, tc.want)
			}
			for id, want := range tc.want {
				if got[id] != want {
					t.Errorf("state[%q] = %+v, want %+v", id, got[id], want)
				}
			}
		})
	}
}

func TestCloneDoesNotAliasTheOriginal(t *testing.T) {
	s := tasklist.State{"t1": {Title: "a"}}
	c := s.Clone()
	c["t1"] = tasklist.Task{Title: "mutated", Done: true}
	if s["t1"].Title != "a" || s["t1"].Done {
		t.Fatalf("Clone aliased the original: %+v", s["t1"])
	}
}

func newList(t *testing.T) (*tasklist.List, *eventlog.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tasks.db")
	return openList(t, path)
}

func openList(t *testing.T, path string) (*tasklist.List, *eventlog.Store, string) {
	t.Helper()
	store, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	wall := time.Unix(0, 0)
	clk := clock.New("n1", func() time.Time { wall = wall.Add(time.Millisecond); return wall })
	list, err := tasklist.NewList(context.Background(), store, clk)
	if err != nil {
		t.Fatalf("NewList: %v", err)
	}
	return list, store, path
}

func TestAddAndCompleteThroughTheEngine(t *testing.T) {
	list, _, _ := newList(t)
	ctx := context.Background()

	if err := list.Add(ctx, "t1", "write the ADR"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := list.Complete(ctx, "t1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got := list.Tasks()["t1"]; got.Title != "write the ADR" || !got.Done {
		t.Fatalf("task = %+v, want {write the ADR true}", got)
	}
}

func TestAddRejectsAnEmptyID(t *testing.T) {
	list, _, _ := newList(t)
	if err := list.Add(context.Background(), "", "x"); !errors.Is(err, tasklist.ErrEmptyID) {
		t.Fatalf("Add(\"\") = %v, want ErrEmptyID", err)
	}
}

func TestCompleteRejectsAnEmptyID(t *testing.T) {
	list, _, _ := newList(t)
	if err := list.Complete(context.Background(), ""); !errors.Is(err, tasklist.ErrEmptyID) {
		t.Fatalf("Complete(\"\") = %v, want ErrEmptyID", err)
	}
}

// The whole point of the Validator seam: a rejected command must leave the log
// exactly as it was.
func TestRejectedCommandAppendsNothing(t *testing.T) {
	list, store, _ := newList(t)
	ctx := context.Background()

	before, err := store.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if err := list.Complete(ctx, "ghost"); !errors.Is(err, tasklist.ErrNoSuchTask) {
		t.Fatalf("Complete(ghost) = %v, want ErrNoSuchTask", err)
	}
	after, err := store.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if !before.Equal(after) {
		t.Fatalf("version moved from %v to %v: a rejected command must append nothing", before, after)
	}
	if len(list.Tasks()) != 0 {
		t.Fatalf("tasks = %v, want empty", list.Tasks())
	}
}

func TestDuplicateAddIsRejected(t *testing.T) {
	list, _, _ := newList(t)
	ctx := context.Background()
	if err := list.Add(ctx, "t1", "a"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := list.Add(ctx, "t1", "a again"); !errors.Is(err, tasklist.ErrDuplicateTask) {
		t.Fatalf("second Add = %v, want ErrDuplicateTask", err)
	}
}

// Reopening the file and replaying must reconstruct identical state: the log is
// the truth, the map is a cache.
func TestStateSurvivesAReopen(t *testing.T) {
	list, store, path := newList(t)
	ctx := context.Background()
	if err := list.Add(ctx, "t1", "a"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := list.Add(ctx, "t2", "b"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := list.Complete(ctx, "t1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	want := list.Tasks()
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, _, _ := openList(t, path)
	got := reopened.Tasks()
	if len(got) != len(want) {
		t.Fatalf("replayed %v, want %v", got, want)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("replayed[%q] = %+v, want %+v", id, got[id], w)
		}
	}
}

// Fold reads through eventlog.Reader, so the same reducer works against the
// store as well as against a slice.
func TestFoldOverTheStore(t *testing.T) {
	list, store, _ := newList(t)
	ctx := context.Background()
	if err := list.Add(ctx, "t1", "a"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := projection.Fold[tasklist.State](ctx, store, tasklist.Reducer{})
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	if got["t1"].Title != "a" {
		t.Fatalf("folded %v, want t1 titled a", got)
	}
}
