package clock

import (
	"math"
	"testing"
	"time"
)

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

// fakeWall returns a WallFunc reading from a mutable slice of readings; the
// last reading repeats once exhausted.
func fakeWall(readings ...int64) WallFunc {
	i := 0
	return func() int64 {
		v := readings[i]
		if i < len(readings)-1 {
			i++
		}
		return v
	}
}

func TestClockNowMonotonic(t *testing.T) {
	tests := []struct {
		name     string
		readings []int64
		calls    int
		want     []HLC
	}{
		{
			name:     "advancing wall resets logical",
			readings: []int64{100, 101, 102},
			calls:    3,
			want: []HLC{
				{Wall: 100, Logical: 0, NodeID: "A"},
				{Wall: 101, Logical: 0, NodeID: "A"},
				{Wall: 102, Logical: 0, NodeID: "A"},
			},
		},
		{
			name:     "stalled wall bumps logical",
			readings: []int64{100, 100, 100},
			calls:    3,
			want: []HLC{
				{Wall: 100, Logical: 0, NodeID: "A"},
				{Wall: 100, Logical: 1, NodeID: "A"},
				{Wall: 100, Logical: 2, NodeID: "A"},
			},
		},
		{
			name:     "backwards jump clamps and bumps logical",
			readings: []int64{100, 50, 50},
			calls:    3,
			want: []HLC{
				{Wall: 100, Logical: 0, NodeID: "A"},
				{Wall: 100, Logical: 1, NodeID: "A"},
				{Wall: 100, Logical: 2, NodeID: "A"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wall := fakeWall(tt.readings...)
			c := New("A", wall)
			var prev HLC
			for i := 0; i < tt.calls; i++ {
				got := c.Now()
				if got != tt.want[i] {
					t.Fatalf("call %d: Now() = %+v, want %+v", i, got, tt.want[i])
				}
				if i > 0 && !prev.Before(got) {
					t.Fatalf("call %d: %+v did not advance past %+v", i, got, prev)
				}
				prev = got
			}
			if c.Last() != tt.want[tt.calls-1] {
				t.Fatalf("Last() = %+v, want %+v", c.Last(), tt.want[tt.calls-1])
			}
		})
	}
}

func TestClockObserve(t *testing.T) {
	tests := []struct {
		name   string
		wall   int64
		remote HLC
		want   HLC
	}{
		{
			name:   "remote in the future pulls local forward",
			wall:   100,
			remote: HLC{Wall: 500, Logical: 3, NodeID: "B"},
			want:   HLC{Wall: 500, Logical: 4, NodeID: "A"},
		},
		{
			name:   "remote in the past leaves local wall, uses wall reading",
			wall:   100,
			remote: HLC{Wall: 50, Logical: 9, NodeID: "B"},
			want:   HLC{Wall: 100, Logical: 0, NodeID: "A"},
		},
		{
			name:   "remote equal wall bumps logical past remote",
			wall:   100,
			remote: HLC{Wall: 100, Logical: 7, NodeID: "B"},
			want:   HLC{Wall: 100, Logical: 8, NodeID: "A"},
		},
		{
			name:   "remote logical at max uint32 advances wall instead of wrapping",
			wall:   100,
			remote: HLC{Wall: 100, Logical: math.MaxUint32, NodeID: "B"},
			want:   HLC{Wall: 101, Logical: 0, NodeID: "A"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wall := fakeWall(tt.wall)
			c := New("A", wall)
			got := c.Observe(tt.remote)
			if got != tt.want {
				t.Fatalf("Observe() = %+v, want %+v", got, tt.want)
			}
			if !tt.remote.Before(got) && tt.remote.Wall >= tt.wall {
				t.Fatalf("Observe() = %+v does not sort after remote %+v", got, tt.remote)
			}
			next := c.Now()
			if !got.Before(next) {
				t.Fatalf("Now() = %+v did not advance past Observe() = %+v", next, got)
			}
		})
	}
}

func TestClockDefaultWallUsesRealTime(t *testing.T) {
	c := New("A", nil)
	before := time.Now().UnixMilli()
	got := c.Now()
	after := time.Now().UnixMilli()
	if got.Wall < before || got.Wall > after {
		t.Fatalf("Now().Wall = %d, want within [%d, %d]", got.Wall, before, after)
	}
}
