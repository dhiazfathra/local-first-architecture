package sync

import (
	"errors"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

func ptr[T any](v T) *T { return &v }

func TestEventCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   eventlog.Event
	}{
		{"quantity delta", eventlog.Event{
			ID: eventlog.EventID{NodeID: "A", Seq: 7}, HLC: clock.HLC{Wall: 100, Logical: 2, NodeID: "A"},
			SKU: "SKU-1", Kind: eventlog.KindQuantityDelta, Delta: -3,
		}},
		{"meta both", eventlog.Event{
			ID: eventlog.EventID{NodeID: "B", Seq: 1}, HLC: clock.HLC{Wall: 100, NodeID: "B"},
			SKU: "SKU-1", Kind: eventlog.KindMetaSet,
			Meta: &eventlog.MetaSet{Name: ptr("Widget"), ReorderPoint: ptr(int64(5))},
		}},
		{"meta name only", eventlog.Event{
			ID: eventlog.EventID{NodeID: "B", Seq: 2}, HLC: clock.HLC{Wall: 101, NodeID: "B"},
			SKU: "SKU-1", Kind: eventlog.KindMetaSet, Meta: &eventlog.MetaSet{Name: ptr("Widget")},
		}},
		{"meta reorder only", eventlog.Event{
			ID: eventlog.EventID{NodeID: "B", Seq: 3}, HLC: clock.HLC{Wall: 102, NodeID: "B"},
			SKU: "SKU-1", Kind: eventlog.KindMetaSet, Meta: &eventlog.MetaSet{ReorderPoint: ptr(int64(9))},
		}},
		{"delete", eventlog.Event{
			ID: eventlog.EventID{NodeID: "C", Seq: 1}, HLC: clock.HLC{Wall: 103, NodeID: "C"},
			SKU: "SKU-2", Kind: eventlog.KindDeleteSet, DeletedTo: ptr(true),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pe, err := encodeEvent(tt.in)
			if err != nil {
				t.Fatalf("encodeEvent() error = %v", err)
			}
			got, err := decodeEvent(pe)
			if err != nil {
				t.Fatalf("decodeEvent() error = %v", err)
			}
			if got.ID != tt.in.ID || got.HLC != tt.in.HLC || got.SKU != tt.in.SKU ||
				got.Kind != tt.in.Kind || got.Delta != tt.in.Delta {
				t.Fatalf("round trip = %+v, want %+v", got, tt.in)
			}
			switch tt.in.Kind {
			case eventlog.KindMetaSet:
				if got.Meta == nil {
					t.Fatalf("Meta = nil, want %+v", tt.in.Meta)
				}
				if !eqPtr(got.Meta.Name, tt.in.Meta.Name) || !eqPtr(got.Meta.ReorderPoint, tt.in.Meta.ReorderPoint) {
					t.Fatalf("Meta = %+v, want %+v", got.Meta, tt.in.Meta)
				}
			case eventlog.KindDeleteSet:
				if !eqPtr(got.DeletedTo, tt.in.DeletedTo) {
					t.Fatalf("DeletedTo = %v, want %v", got.DeletedTo, tt.in.DeletedTo)
				}
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

func TestEncodeEventRejectsMalformed(t *testing.T) {
	if _, err := encodeEvent(eventlog.Event{ID: eventlog.EventID{NodeID: "A", Seq: 1}, Kind: 42}); !errors.Is(err, eventlog.ErrMalformedEvent) {
		t.Fatalf("encodeEvent() error = %v, want ErrMalformedEvent", err)
	}
}

func TestDecodeEventRejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		in   *syncpb.Event
	}{
		{"nil frame", nil},
		{"unknown kind", &syncpb.Event{NodeId: "A", Seq: 1, Sku: "S", Kind: 42}},
		{"empty sku", &syncpb.Event{NodeId: "A", Seq: 1, Kind: 1, Delta: 1}},
		{"zero seq", &syncpb.Event{NodeId: "A", Kind: 1, Sku: "S", Delta: 1}},
		{"meta kind with no fields", &syncpb.Event{NodeId: "A", Seq: 1, Sku: "S", Kind: 2}},
		{"delete kind with no value", &syncpb.Event{NodeId: "A", Seq: 1, Sku: "S", Kind: 3}},
		{"delta on wrong kind", &syncpb.Event{NodeId: "A", Seq: 1, Sku: "S", Kind: 3, Delta: 1, DeletedTo: ptr(true)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeEvent(tt.in); !errors.Is(err, eventlog.ErrMalformedEvent) {
				t.Fatalf("decodeEvent() error = %v, want ErrMalformedEvent", err)
			}
		})
	}
}

func TestVersionVectorCodec(t *testing.T) {
	tests := []struct {
		name string
		vv   eventlog.VersionVector
	}{
		{"empty", eventlog.VersionVector{}},
		{"populated", eventlog.VersionVector{"A": 3, "B": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decodeVV(encodeVV(tt.vv))
			if len(got) != len(tt.vv) {
				t.Fatalf("round trip = %v, want %v", got, tt.vv)
			}
			for k, v := range tt.vv {
				if got[k] != v {
					t.Fatalf("round trip = %v, want %v", got, tt.vv)
				}
			}
		})
	}
	if got := decodeVV(nil); got == nil || len(got) != 0 {
		t.Fatalf("decodeVV(nil) = %v, want empty non-nil", got)
	}
}
