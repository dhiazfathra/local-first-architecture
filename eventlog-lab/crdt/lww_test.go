package crdt

import (
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

func TestLWWSet(t *testing.T) {
	tests := []struct {
		name       string
		startAt    clock.HLC
		startValue string
		writeAt    clock.HLC
		writeValue string
		wantOK     bool
		wantValue  string
	}{
		{"first write always wins", clock.HLC{}, "", clock.HLC{Wall: 1, NodeID: "A"}, "new", true, "new"},
		{"later wall wins", clock.HLC{Wall: 1, NodeID: "A"}, "old", clock.HLC{Wall: 2, NodeID: "A"}, "new", true, "new"},
		{"earlier wall loses", clock.HLC{Wall: 2, NodeID: "A"}, "old", clock.HLC{Wall: 1, NodeID: "A"}, "new", false, "old"},
		{"same stamp loses (idempotent replay)", clock.HLC{Wall: 1, NodeID: "A"}, "old", clock.HLC{Wall: 1, NodeID: "A"}, "new", false, "old"},
		{"logical breaks wall tie", clock.HLC{Wall: 1, NodeID: "A"}, "old", clock.HLC{Wall: 1, Logical: 1, NodeID: "A"}, "new", true, "new"},
		{"node id breaks full tie, higher wins", clock.HLC{Wall: 1, NodeID: "A"}, "old", clock.HLC{Wall: 1, NodeID: "B"}, "new", true, "new"},
		{"node id breaks full tie, lower loses", clock.HLC{Wall: 1, NodeID: "B"}, "old", clock.HLC{Wall: 1, NodeID: "A"}, "new", false, "old"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := LWW[string]{Value: tt.startValue, At: tt.startAt}
			if got := r.Set(tt.writeValue, tt.writeAt); got != tt.wantOK {
				t.Fatalf("Set() = %v, want %v", got, tt.wantOK)
			}
			if r.Value != tt.wantValue {
				t.Fatalf("Value = %q, want %q", r.Value, tt.wantValue)
			}
		})
	}
}
