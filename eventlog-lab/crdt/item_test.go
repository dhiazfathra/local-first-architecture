package crdt

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

func ptr[T any](v T) *T { return &v }

func qty(node clock.NodeID, seq eventlog.Seq, wall int64, delta int64) eventlog.Event {
	return eventlog.Event{
		ID:    eventlog.EventID{NodeID: node, Seq: seq},
		HLC:   clock.HLC{Wall: wall, NodeID: node},
		SKU:   "SKU-1",
		Kind:  eventlog.KindQuantityDelta,
		Delta: delta,
	}
}

func meta(node clock.NodeID, seq eventlog.Seq, wall int64, m *eventlog.MetaSet) eventlog.Event {
	return eventlog.Event{
		ID:   eventlog.EventID{NodeID: node, Seq: seq},
		HLC:  clock.HLC{Wall: wall, NodeID: node},
		SKU:  "SKU-1",
		Kind: eventlog.KindMetaSet,
		Meta: m,
	}
}

func del(node clock.NodeID, seq eventlog.Seq, wall int64, to bool) eventlog.Event {
	return eventlog.Event{
		ID:        eventlog.EventID{NodeID: node, Seq: seq},
		HLC:       clock.HLC{Wall: wall, NodeID: node},
		SKU:       "SKU-1",
		Kind:      eventlog.KindDeleteSet,
		DeletedTo: ptr(to),
	}
}

func applyAll(events []eventlog.Event) *ItemState {
	s := NewItemState()
	for _, e := range events {
		s.Apply(e)
	}
	return s
}

func TestApplyQuantity(t *testing.T) {
	tests := []struct {
		name    string
		events  []eventlog.Event
		wantQty int64
		wantPos map[clock.NodeID]int64
		wantNeg map[clock.NodeID]int64
	}{
		{"single increment", []eventlog.Event{qty("A", 1, 1, 10)}, 10,
			map[clock.NodeID]int64{"A": 10}, map[clock.NodeID]int64{}},
		{"single decrement", []eventlog.Event{qty("A", 1, 1, -4)}, -4,
			map[clock.NodeID]int64{}, map[clock.NodeID]int64{"A": 4}},
		{"per-node halves accumulate", []eventlog.Event{
			qty("A", 1, 1, 10), qty("A", 2, 2, -3), qty("B", 1, 3, -8),
		}, -1, map[clock.NodeID]int64{"A": 10}, map[clock.NodeID]int64{"A": 3, "B": 8}},
		{"concurrent overdraw goes negative and that is correct", []eventlog.Event{
			qty("C", 1, 1, 10), qty("A", 1, 2, -8), qty("B", 1, 2, -8),
		}, -6, map[clock.NodeID]int64{"C": 10}, map[clock.NodeID]int64{"A": 8, "B": 8}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := applyAll(tt.events)
			if got := s.Quantity(); got != tt.wantQty {
				t.Fatalf("Quantity() = %d, want %d", got, tt.wantQty)
			}
			assertCounter(t, "Pos", s.Pos, tt.wantPos)
			assertCounter(t, "Neg", s.Neg, tt.wantNeg)
		})
	}
}

func assertCounter(t *testing.T, name string, got, want map[clock.NodeID]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s[%q] = %d, want %d", name, k, got[k], v)
		}
	}
}

func TestApplyMetaAndDelete(t *testing.T) {
	tests := []struct {
		name             string
		events           []eventlog.Event
		wantName         string
		wantReorderPoint int64
		wantDeleted      bool
	}{
		{"meta sets both", []eventlog.Event{
			meta("A", 1, 1, &eventlog.MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(5))}),
		}, "Widget", 5, false},
		{"nil field does not clobber", []eventlog.Event{
			meta("A", 1, 1, &eventlog.MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(5))}),
			meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("Gadget")}),
		}, "Gadget", 5, false},
		{"later wall wins regardless of arrival order", []eventlog.Event{
			meta("B", 1, 9, &eventlog.MetaSet{Name: ptr("Late")}),
			meta("A", 1, 1, &eventlog.MetaSet{Name: ptr("Early")}),
		}, "Late", 0, false},
		{"delete sets tombstone", []eventlog.Event{del("A", 1, 1, true)}, "", 0, true},
		{"later undelete resurrects (LWW, not add-wins)", []eventlog.Event{
			del("A", 1, 1, true), del("B", 1, 2, false),
		}, "", 0, false},
		{"earlier undelete loses", []eventlog.Event{
			del("A", 1, 2, true), del("B", 1, 1, false),
		}, "", 0, true},
		{"nil meta payload is ignored, not a panic", []eventlog.Event{
			{ID: eventlog.EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{Wall: 1, NodeID: "A"}, SKU: "SKU-1", Kind: eventlog.KindMetaSet},
		}, "", 0, false},
		{"nil delete payload is ignored, not a panic", []eventlog.Event{
			{ID: eventlog.EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{Wall: 1, NodeID: "A"}, SKU: "SKU-1", Kind: eventlog.KindDeleteSet},
		}, "", 0, false},
		{"unknown kind is ignored, not a panic", []eventlog.Event{
			{ID: eventlog.EventID{NodeID: "A", Seq: 1}, HLC: clock.HLC{Wall: 1, NodeID: "A"}, SKU: "SKU-1", Kind: 99},
		}, "", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := applyAll(tt.events)
			if s.Name.Value != tt.wantName {
				t.Errorf("Name = %q, want %q", s.Name.Value, tt.wantName)
			}
			if s.ReorderPoint.Value != tt.wantReorderPoint {
				t.Errorf("ReorderPoint = %d, want %d", s.ReorderPoint.Value, tt.wantReorderPoint)
			}
			if s.Deleted.Value != tt.wantDeleted {
				t.Errorf("Deleted = %v, want %v", s.Deleted.Value, tt.wantDeleted)
			}
		})
	}
}

// TestApplyIsNotIdempotentButTheLogPathIs is the test the spec demands: it
// shows the direct path double-counts a duplicate, and the log path does not,
// because idempotence comes from Append's primary key, never from Apply.
func TestApplyIsNotIdempotentButTheLogPathIs(t *testing.T) {
	ctx := context.Background()
	e := qty("A", 1, 100, 7)

	direct := NewItemState()
	direct.Apply(e)
	direct.Apply(e)
	if direct.Quantity() != 14 {
		t.Fatalf("direct double-apply Quantity() = %d, want 14 -- Apply is documented as NOT idempotent", direct.Quantity())
	}

	l, err := eventlog.OpenSQLite(filepath.Join(t.TempDir(), "dup.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	for i := 0; i < 2; i++ {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	viaLog := NewItemState()
	if err := viaLog.Fold(l.EventsForSKU(ctx, "SKU-1", eventlog.VersionVector{})); err != nil {
		t.Fatalf("Fold() error = %v", err)
	}
	if viaLog.Quantity() != 7 {
		t.Fatalf("log-path Quantity() = %d after duplicate delivery, want 7", viaLog.Quantity())
	}
}

func TestFoldSurfacesIterationError(t *testing.T) {
	wantErr := context.Canceled
	s := NewItemState()
	err := s.Fold(func(yield func(eventlog.Event, error) bool) {
		yield(eventlog.Event{}, wantErr)
	})
	if err == nil {
		t.Fatal("Fold() error = nil, want the iteration error")
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	s := applyAll([]eventlog.Event{
		qty("A", 1, 1, 10),
		qty("B", 1, 2, -3),
		meta("A", 2, 3, &eventlog.MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(4))}),
		del("A", 3, 4, true),
	})
	b, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !got.Equal(s) {
		t.Fatalf("round trip = %+v, want %+v", got, s)
	}
	if _, err := Unmarshal([]byte("{bad")); err == nil {
		t.Fatal("Unmarshal(garbage) error = nil, want an error")
	}
}

func TestUnmarshalNormalisesNilMaps(t *testing.T) {
	got, err := Unmarshal([]byte(`{"pos":null,"neg":null}`))
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.Pos == nil || got.Neg == nil {
		t.Fatalf("Unmarshal() left nil maps: %+v", got)
	}
}

func TestEqual(t *testing.T) {
	base := func() *ItemState {
		return applyAll([]eventlog.Event{qty("A", 1, 1, 5), meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("W")})})
	}
	tests := []struct {
		name  string
		other *ItemState
		want  bool
	}{
		{"identical", base(), true},
		{"different quantity", applyAll([]eventlog.Event{qty("A", 1, 1, 6), meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("W")})}), false},
		{"different node in counter", applyAll([]eventlog.Event{qty("B", 1, 1, 5), meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("W")})}), false},
		{"different name", applyAll([]eventlog.Event{qty("A", 1, 1, 5), meta("A", 2, 2, &eventlog.MetaSet{Name: ptr("X")})}), false},
		{"empty", NewItemState(), false},
		{"nil other", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := base().Equal(tt.other); got != tt.want {
				t.Fatalf("Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}

// permute calls fn with every permutation of events when the count is small
// enough to enumerate (<= 6! = 720), otherwise with `samples` random shuffles.
func permute(t *testing.T, events []eventlog.Event, samples int, fn func([]eventlog.Event)) {
	t.Helper()
	if len(events) <= 6 {
		buf := make([]eventlog.Event, len(events))
		copy(buf, events)
		var rec func(k int)
		rec = func(k int) {
			if k == len(buf) {
				out := make([]eventlog.Event, len(buf))
				copy(out, buf)
				fn(out)
				return
			}
			for i := k; i < len(buf); i++ {
				buf[k], buf[i] = buf[i], buf[k]
				rec(k + 1)
				buf[k], buf[i] = buf[i], buf[k]
			}
		}
		rec(0)
		return
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < samples; i++ {
		out := make([]eventlog.Event, len(events))
		copy(out, events)
		rng.Shuffle(len(out), func(a, b int) { out[a], out[b] = out[b], out[a] })
		fn(out)
	}
}

func TestApplyIsCommutative(t *testing.T) {
	tests := []struct {
		name   string
		events []eventlog.Event
	}{
		{"small mixed set, exhaustive", []eventlog.Event{
			qty("A", 1, 10, 10),
			qty("B", 1, 20, -3),
			meta("A", 2, 30, &eventlog.MetaSet{Name: ptr("Widget")}),
			del("B", 2, 40, true),
		}},
		{"conflicting lww writes, exhaustive", []eventlog.Event{
			meta("A", 1, 10, &eventlog.MetaSet{Name: ptr("A1"), ReorderPoint: ptr(int64(1))}),
			meta("B", 1, 10, &eventlog.MetaSet{Name: ptr("B1"), ReorderPoint: ptr(int64(2))}),
			meta("A", 2, 11, &eventlog.MetaSet{Name: ptr("A2")}),
			del("A", 3, 12, true),
			del("B", 2, 12, false),
		}},
		{"large set, sampled", func() []eventlog.Event {
			var out []eventlog.Event
			for i := 1; i <= 12; i++ {
				node := clock.NodeID("A")
				if i%3 == 0 {
					node = "B"
				}
				out = append(out, qty(node, eventlog.Seq(i), int64(i), int64(i%5)-2+1))
			}
			out = append(out, meta("A", 20, 50, &eventlog.MetaSet{Name: ptr("Z")}))
			return out
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := applyAll(tt.events)
			n := 0
			permute(t, tt.events, 200, func(perm []eventlog.Event) {
				n++
				if got := applyAll(perm); !got.Equal(want) {
					t.Fatalf("permutation %d diverged:\n got %+v\nwant %+v\nperm %v", n, got, want, ids(perm))
				}
			})
			if n < 2 {
				t.Fatalf("only %d permutations exercised", n)
			}
		})
	}
}

func ids(events []eventlog.Event) []eventlog.EventID {
	out := make([]eventlog.EventID, len(events))
	for i, e := range events {
		out[i] = e.ID
	}
	return out
}

func TestApplyIsAssociative(t *testing.T) {
	events := []eventlog.Event{
		qty("A", 1, 10, 10),
		qty("B", 1, 20, -3),
		meta("A", 2, 30, &eventlog.MetaSet{Name: ptr("Widget")}),
		qty("C", 1, 40, 5),
	}
	whole := applyAll(events)
	for split := 0; split <= len(events); split++ {
		t.Run(fmt.Sprintf("split at %d", split), func(t *testing.T) {
			left := applyAll(events[:split])
			for _, e := range events[split:] {
				left.Apply(e)
			}
			if !left.Equal(whole) {
				t.Fatalf("split %d = %+v, want %+v", split, left, whole)
			}
		})
	}
}
