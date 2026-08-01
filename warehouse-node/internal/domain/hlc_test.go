package domain

import (
	"math"
	"testing"
)

func TestTick(t *testing.T) {
	tests := []struct {
		name string
		prev HLC
		now  int64
		want HLC
	}{
		{
			name: "wall clock moved forward resets counter",
			prev: HLC{Wall: 100, Counter: 7, Node: "a"},
			now:  200,
			want: HLC{Wall: 200, Counter: 0, Node: "a"},
		},
		{
			name: "wall clock equal bumps counter",
			prev: HLC{Wall: 100, Counter: 7, Node: "a"},
			now:  100,
			want: HLC{Wall: 100, Counter: 8, Node: "a"},
		},
		{
			name: "wall clock went backwards keeps prev wall and bumps counter",
			prev: HLC{Wall: 100, Counter: 7, Node: "a"},
			now:  50,
			want: HLC{Wall: 100, Counter: 8, Node: "a"},
		},
		{
			name: "zero prev adopts now",
			prev: HLC{},
			now:  10,
			want: HLC{Wall: 10, Counter: 0, Node: "a"},
		},
		{
			// A saturated counter must carry into the wall component. Wrapping to
			// zero would move the clock backwards and break the total order.
			name: "saturated counter carries into the wall",
			prev: HLC{Wall: 100, Counter: math.MaxUint32, Node: "a"},
			now:  50,
			want: HLC{Wall: 101, Counter: 0, Node: "a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Tick(tt.prev, tt.now, "a"); got != tt.want {
				t.Fatalf("Tick() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name          string
		local, remote HLC
		now           int64
		want          HLC
	}{
		{
			name:   "now dominates both",
			local:  HLC{Wall: 100, Counter: 3, Node: "a"},
			remote: HLC{Wall: 120, Counter: 9, Node: "b"},
			now:    200,
			want:   HLC{Wall: 200, Counter: 0, Node: "a"},
		},
		{
			name:   "remote ahead of local and now",
			local:  HLC{Wall: 100, Counter: 3, Node: "a"},
			remote: HLC{Wall: 300, Counter: 9, Node: "b"},
			now:    200,
			want:   HLC{Wall: 300, Counter: 10, Node: "a"},
		},
		{
			name:   "local ahead of remote and now",
			local:  HLC{Wall: 400, Counter: 3, Node: "a"},
			remote: HLC{Wall: 300, Counter: 9, Node: "b"},
			now:    200,
			want:   HLC{Wall: 400, Counter: 4, Node: "a"},
		},
		{
			name:   "equal walls takes max counter plus one",
			local:  HLC{Wall: 300, Counter: 3, Node: "a"},
			remote: HLC{Wall: 300, Counter: 9, Node: "b"},
			now:    200,
			want:   HLC{Wall: 300, Counter: 10, Node: "a"},
		},
		{
			name:   "saturated counter carries into the wall",
			local:  HLC{Wall: 300, Counter: math.MaxUint32, Node: "a"},
			remote: HLC{Wall: 300, Counter: 1, Node: "b"},
			now:    200,
			want:   HLC{Wall: 301, Counter: 0, Node: "a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Merge(tt.local, tt.remote, tt.now, "a"); got != tt.want {
				t.Fatalf("Merge() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestHLCCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b HLC
		want int
	}{
		{"wall less", HLC{Wall: 1}, HLC{Wall: 2}, -1},
		{"wall greater", HLC{Wall: 3}, HLC{Wall: 2}, 1},
		{"counter less", HLC{Wall: 1, Counter: 1}, HLC{Wall: 1, Counter: 2}, -1},
		{"counter greater", HLC{Wall: 1, Counter: 5}, HLC{Wall: 1, Counter: 2}, 1},
		{"node breaks tie less", HLC{Wall: 1, Counter: 1, Node: "a"}, HLC{Wall: 1, Counter: 1, Node: "b"}, -1},
		{"node breaks tie greater", HLC{Wall: 1, Counter: 1, Node: "c"}, HLC{Wall: 1, Counter: 1, Node: "b"}, 1},
		{"fully equal", HLC{Wall: 1, Counter: 1, Node: "a"}, HLC{Wall: 1, Counter: 1, Node: "a"}, 0},
		// Extreme walls must still order correctly: subtracting them would overflow
		// and report the opposite answer.
		{"extreme walls do not overflow", HLC{Wall: math.MinInt64}, HLC{Wall: math.MaxInt64}, -1},
		{"extreme walls reversed", HLC{Wall: math.MaxInt64}, HLC{Wall: math.MinInt64}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Fatalf("Compare() = %d, want %d", got, tt.want)
			}
		})
	}
}
