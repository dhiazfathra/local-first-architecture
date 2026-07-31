package eventlog

import (
	"errors"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

func ptr[T any](v T) *T { return &v }

func TestEventPayloadRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   Event
	}{
		{"quantity delta positive", Event{Kind: KindQuantityDelta, Delta: 10}},
		{"quantity delta negative", Event{Kind: KindQuantityDelta, Delta: -3}},
		{"meta both fields", Event{Kind: KindMetaSet, Meta: &MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(5))}}},
		{"meta name only", Event{Kind: KindMetaSet, Meta: &MetaSet{Name: ptr("Widget")}}},
		{"meta reorder only", Event{Kind: KindMetaSet, Meta: &MetaSet{ReorderPoint: ptr(int64(5))}}},
		{"delete true", Event{Kind: KindDeleteSet, DeletedTo: ptr(true)}},
		{"delete false", Event{Kind: KindDeleteSet, DeletedTo: ptr(false)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := tt.in.MarshalPayload()
			if err != nil {
				t.Fatalf("MarshalPayload() error = %v", err)
			}
			got := Event{Kind: tt.in.Kind}
			if err := got.UnmarshalPayload(b); err != nil {
				t.Fatalf("UnmarshalPayload() error = %v", err)
			}
			if got.Delta != tt.in.Delta {
				t.Errorf("Delta = %d, want %d", got.Delta, tt.in.Delta)
			}
			switch {
			case tt.in.Meta == nil && got.Meta != nil:
				t.Errorf("Meta = %+v, want nil", got.Meta)
			case tt.in.Meta != nil:
				if got.Meta == nil {
					t.Fatalf("Meta = nil, want %+v", tt.in.Meta)
				}
				if !eqPtr(got.Meta.Name, tt.in.Meta.Name) || !eqPtr(got.Meta.ReorderPoint, tt.in.Meta.ReorderPoint) {
					t.Errorf("Meta = %+v/%+v, want %+v/%+v", got.Meta.Name, got.Meta.ReorderPoint, tt.in.Meta.Name, tt.in.Meta.ReorderPoint)
				}
			}
			if !eqPtr(got.DeletedTo, tt.in.DeletedTo) {
				t.Errorf("DeletedTo = %v, want %v", got.DeletedTo, tt.in.DeletedTo)
			}
		})
	}
}

func eqPtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func TestEventUnmarshalPayloadRejectsGarbage(t *testing.T) {
	e := Event{Kind: KindQuantityDelta}
	if err := e.UnmarshalPayload([]byte("{not json")); !errors.Is(err, ErrMalformedEvent) {
		t.Fatalf("UnmarshalPayload() error = %v, want ErrMalformedEvent", err)
	}
}

func TestEventValidate(t *testing.T) {
	base := func(mut func(*Event)) Event {
		e := Event{
			ID:    EventID{NodeID: "A", Seq: 1},
			HLC:   clock.HLC{Wall: 1, NodeID: "A"},
			SKU:   "SKU-1",
			Kind:  KindQuantityDelta,
			Delta: 1,
		}
		mut(&e)
		return e
	}
	tests := []struct {
		name    string
		event   Event
		wantErr bool
	}{
		{"valid quantity delta", base(func(*Event) {}), false},
		{"valid meta", base(func(e *Event) { e.Kind = KindMetaSet; e.Delta = 0; e.Meta = &MetaSet{Name: ptr("W")} }), false},
		{"valid delete", base(func(e *Event) { e.Kind = KindDeleteSet; e.Delta = 0; e.DeletedTo = ptr(true) }), false},
		{"empty node id", base(func(e *Event) { e.ID.NodeID = "" }), true},
		{"zero seq", base(func(e *Event) { e.ID.Seq = 0 }), true},
		{"empty sku", base(func(e *Event) { e.SKU = "" }), true},
		{"unknown kind", base(func(e *Event) { e.Kind = 99 }), true},
		{"zero delta on quantity kind", base(func(e *Event) { e.Delta = 0 }), true},
		{"meta kind without meta", base(func(e *Event) { e.Kind = KindMetaSet; e.Delta = 0 }), true},
		{"meta kind with empty meta", base(func(e *Event) { e.Kind = KindMetaSet; e.Delta = 0; e.Meta = &MetaSet{} }), true},
		{"delete kind without value", base(func(e *Event) { e.Kind = KindDeleteSet; e.Delta = 0 }), true},
		{"delta set on meta kind", base(func(e *Event) { e.Kind = KindMetaSet; e.Meta = &MetaSet{Name: ptr("W")} }), true},
		{"hlc node mismatch", base(func(e *Event) { e.HLC.NodeID = "B" }), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrMalformedEvent) {
				t.Fatalf("Validate() error = %v, want wrapped ErrMalformedEvent", err)
			}
		})
	}
}

func TestKindValid(t *testing.T) {
	for _, k := range []Kind{KindQuantityDelta, KindMetaSet, KindDeleteSet} {
		if !k.Valid() {
			t.Errorf("Kind(%d).Valid() = false, want true", k)
		}
	}
	for _, k := range []Kind{0, 4, -1} {
		if k.Valid() {
			t.Errorf("Kind(%d).Valid() = true, want false", k)
		}
	}
}

func TestVersionVector(t *testing.T) {
	t.Run("observe keeps the highest seq per node", func(t *testing.T) {
		vv := VersionVector{}
		vv.Observe(EventID{NodeID: "A", Seq: 5})
		vv.Observe(EventID{NodeID: "A", Seq: 3})
		vv.Observe(EventID{NodeID: "B", Seq: 1})
		if vv["A"] != 5 || vv["B"] != 1 {
			t.Fatalf("vv = %v, want {A:5, B:1}", vv)
		}
	})

	t.Run("contains", func(t *testing.T) {
		vv := VersionVector{"A": 5}
		cases := []struct {
			id   EventID
			want bool
		}{
			{EventID{NodeID: "A", Seq: 5}, true},
			{EventID{NodeID: "A", Seq: 4}, true},
			{EventID{NodeID: "A", Seq: 6}, false},
			{EventID{NodeID: "B", Seq: 1}, false},
		}
		for _, c := range cases {
			if got := vv.Contains(c.id); got != c.want {
				t.Errorf("Contains(%+v) = %v, want %v", c.id, got, c.want)
			}
		}
	})

	t.Run("dominates and merge", func(t *testing.T) {
		tests := []struct {
			name          string
			a, b          VersionVector
			wantDominates bool
			wantMerge     VersionVector
		}{
			{"equal", VersionVector{"A": 1}, VersionVector{"A": 1}, true, VersionVector{"A": 1}},
			{"strictly ahead", VersionVector{"A": 2}, VersionVector{"A": 1}, true, VersionVector{"A": 2}},
			{"strictly behind", VersionVector{"A": 1}, VersionVector{"A": 2}, false, VersionVector{"A": 2}},
			{"missing node", VersionVector{"A": 1}, VersionVector{"B": 1}, false, VersionVector{"A": 1, "B": 1}},
			{"superset", VersionVector{"A": 1, "B": 1}, VersionVector{"A": 1}, true, VersionVector{"A": 1, "B": 1}},
			{"empty dominated by anything", VersionVector{"A": 1}, VersionVector{}, true, VersionVector{"A": 1}},
			{"nil receiver", nil, VersionVector{"A": 1}, false, VersionVector{"A": 1}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := tt.a.Dominates(tt.b); got != tt.wantDominates {
					t.Errorf("Dominates() = %v, want %v", got, tt.wantDominates)
				}
				got := tt.a.Merge(tt.b)
				if len(got) != len(tt.wantMerge) {
					t.Fatalf("Merge() = %v, want %v", got, tt.wantMerge)
				}
				for k, v := range tt.wantMerge {
					if got[k] != v {
						t.Fatalf("Merge()[%q] = %d, want %d", k, got[k], v)
					}
				}
			})
		}
	})

	t.Run("clone is independent", func(t *testing.T) {
		vv := VersionVector{"A": 1}
		c := vv.Clone()
		c["A"] = 9
		c["B"] = 2
		if vv["A"] != 1 || len(vv) != 1 {
			t.Fatalf("original mutated: %v", vv)
		}
		if got := VersionVector(nil).Clone(); got == nil || len(got) != 0 {
			t.Fatalf("nil.Clone() = %v, want empty non-nil", got)
		}
	})
}
