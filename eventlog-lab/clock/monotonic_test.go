package clock

import "testing"

// TestNowIsMonotonicUnderBackwardsWallJumps is the single most important clock
// property. If Now() ever regresses, LWW resolves backwards and convergence
// dies with no error at all -- just wrong numbers.
func TestNowIsMonotonicUnderBackwardsWallJumps(t *testing.T) {
	tests := []struct {
		name     string
		readings []int64
	}{
		{name: "monotonic wall", readings: []int64{100, 101, 102, 103}},
		{name: "stalled wall", readings: []int64{100, 100, 100, 100}},
		{name: "single backwards step", readings: []int64{100, 101, 50, 51}},
		{name: "large backwards jump", readings: []int64{1_000_000, 1_000_001, 1, 2}},
		{name: "repeated backwards jumps", readings: []int64{100, 90, 80, 70, 60}},
		{name: "zigzag", readings: []int64{100, 50, 200, 60, 300, 70}},
		{name: "negative readings", readings: []int64{10, -100, -200, 5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := 0
			wall := func() int64 {
				v := tt.readings[i]
				if i < len(tt.readings)-1 {
					i++
				}
				return v
			}
			c := New("A", wall)

			prev := c.Now()
			for step := 1; step < len(tt.readings)+3; step++ {
				got := c.Now()
				if got.Before(prev) {
					t.Fatalf("step %d: Now() = %+v regressed before %+v", step, got, prev)
				}
				if got == prev {
					t.Fatalf("step %d: Now() = %+v repeated; timestamps must be distinct",
						step, got)
				}
				if got.Wall == prev.Wall && got.Logical <= prev.Logical {
					t.Fatalf("step %d: Wall stalled at %d but Logical did not advance (%d -> %d)",
						step, got.Wall, prev.Logical, got.Logical)
				}
				prev = got
			}
			if last := c.Last(); last != prev {
				t.Errorf("Last() = %+v, want %+v", last, prev)
			}
		})
	}
}

// TestObserveKeepsCausalityAcrossABackwardsJumpingClock asserts the reason
// Observe exists: a local write made after receiving a remote event must sort
// after it, even if the local wall clock is behind and jumping backwards.
func TestObserveKeepsCausalityAcrossABackwardsJumpingClock(t *testing.T) {
	tests := []struct {
		name   string
		remote HLC
	}{
		{name: "remote slightly ahead", remote: HLC{Wall: 105, NodeID: "B"}},
		{name: "remote far ahead", remote: HLC{Wall: 9_000_000, NodeID: "B"}},
		{name: "remote equal wall", remote: HLC{Wall: 100, Logical: 7, NodeID: "B"}},
		{name: "remote behind", remote: HLC{Wall: 1, NodeID: "B"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readings := []int64{100, 99, 98, 97}
			i := 0
			c := New("A", func() int64 {
				v := readings[i]
				if i < len(readings)-1 {
					i++
				}
				return v
			})
			_ = c.Now()

			observed := c.Observe(tt.remote)
			if tt.remote.Before(observed) == false && observed != tt.remote {
				t.Fatalf("Observe(%+v) = %+v, which does not sort after the remote",
					tt.remote, observed)
			}
			next := c.Now()
			if !tt.remote.Before(next) {
				t.Errorf("after Observe(%+v), Now() = %+v does not sort after the remote; "+
					"a later local write would lose the LWW conflict it causally follows",
					tt.remote, next)
			}
			if !observed.Before(next) {
				t.Errorf("Now() = %+v does not follow Observe's result %+v", next, observed)
			}
		})
	}
}
