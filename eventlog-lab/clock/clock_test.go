package clock

import "testing"

func TestHLCCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b HLC
		want int
	}{
		{"wall dominates", HLC{Wall: 1}, HLC{Wall: 2}, -1},
		{"wall dominates reversed", HLC{Wall: 2}, HLC{Wall: 1}, 1},
		{"logical breaks wall tie", HLC{Wall: 1, Logical: 1}, HLC{Wall: 1, Logical: 2}, -1},
		{"logical tie reversed", HLC{Wall: 1, Logical: 2}, HLC{Wall: 1, Logical: 1}, 1},
		{"node breaks logical tie", HLC{Wall: 1, Logical: 1, NodeID: "A"}, HLC{Wall: 1, Logical: 1, NodeID: "B"}, -1},
		{"node tie reversed", HLC{Wall: 1, Logical: 1, NodeID: "B"}, HLC{Wall: 1, Logical: 1, NodeID: "A"}, 1},
		{"fully equal", HLC{Wall: 1, Logical: 1, NodeID: "A"}, HLC{Wall: 1, Logical: 1, NodeID: "A"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Fatalf("Compare() = %d, want %d", got, tt.want)
			}
			wantBefore := tt.want < 0
			if got := tt.a.Before(tt.b); got != wantBefore {
				t.Fatalf("Before() = %v, want %v", got, wantBefore)
			}
		})
	}
}
