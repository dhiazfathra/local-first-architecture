package clock_test

import (
	"sync"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
)

// fakeNow returns times from a slice, repeating the last one forever.
func fakeNow(ms ...int64) func() time.Time {
	i := 0
	return func() time.Time {
		v := ms[i]
		if i < len(ms)-1 {
			i++
		}
		return time.UnixMilli(v)
	}
}

func TestNowAdvancesWithWallClock(t *testing.T) {
	c := clock.New("n1", fakeNow(1000, 1001))
	first, second := c.Now(), c.Now()
	if first.Wall != 1000 || first.Logical != 0 {
		t.Fatalf("first = %v, want wall 1000 logical 0", first)
	}
	if second.Wall != 1001 || second.Logical != 0 {
		t.Fatalf("second = %v, want wall 1001 logical 0", second)
	}
}

func TestNowUsesLogicalCounterWhenWallStalls(t *testing.T) {
	c := clock.New("n1", fakeNow(1000))
	for i, want := range []uint32{0, 1, 2} {
		got := c.Now()
		if got.Wall != 1000 || got.Logical != want {
			t.Fatalf("call %d = %v, want wall 1000 logical %d", i, got, want)
		}
	}
}

func TestNowIsMonotonicAcrossBackwardsJump(t *testing.T) {
	c := clock.New("n1", fakeNow(2000, 1000, 1000))
	first := c.Now()
	for i := 0; i < 2; i++ {
		got := c.Now()
		if !first.Before(got) {
			t.Fatalf("after backwards jump got %v, not after %v", got, first)
		}
		if got.Wall != 2000 {
			t.Fatalf("wall regressed to %d", got.Wall)
		}
		first = got
	}
}

func TestObserve(t *testing.T) {
	tests := []struct {
		name        string
		localWall   int64
		remote      clock.HLC
		wantWall    int64
		wantLogical uint32
	}{
		{"remote ahead adopts remote wall", 1000, clock.HLC{Wall: 5000, Logical: 3, NodeID: "n2"}, 5000, 4},
		{"remote equal bumps logical", 1000, clock.HLC{Wall: 1000, Logical: 7, NodeID: "n2"}, 1000, 8},
		{"remote behind keeps local", 9000, clock.HLC{Wall: 10, Logical: 1, NodeID: "n2"}, 9000, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := clock.New("n1", fakeNow(tc.localWall))
			got := c.Observe(tc.remote)
			if got.Wall != tc.wantWall || got.Logical != tc.wantLogical {
				t.Fatalf("got %v, want wall %d logical %d", got, tc.wantWall, tc.wantLogical)
			}
			if got.NodeID != "n1" {
				t.Fatalf("NodeID = %q, want n1", got.NodeID)
			}
			if next := c.Now(); !got.Before(next) {
				t.Fatalf("Now() after Observe returned %v, not after %v", next, got)
			}
		})
	}
}

func TestCompareTotalOrder(t *testing.T) {
	a := clock.HLC{Wall: 1, Logical: 0, NodeID: "a"}
	b := clock.HLC{Wall: 1, Logical: 0, NodeID: "b"}
	c := clock.HLC{Wall: 1, Logical: 1, NodeID: "a"}
	d := clock.HLC{Wall: 2, Logical: 0, NodeID: "a"}
	tests := []struct {
		name string
		x, y clock.HLC
		want int
	}{
		{"equal", a, a, 0},
		{"node id tiebreak", a, b, -1},
		{"node id tiebreak reversed", b, a, 1},
		{"logical beats equal wall", a, c, -1},
		{"wall dominates", c, d, -1},
		{"wall dominates reversed", d, c, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.x.Compare(tc.y); got != tc.want {
				t.Fatalf("Compare = %d, want %d", got, tc.want)
			}
			if got := tc.x.Before(tc.y); got != (tc.want < 0) {
				t.Fatalf("Before = %v, want %v", got, tc.want < 0)
			}
		})
	}
}

func TestString(t *testing.T) {
	got := clock.HLC{Wall: 1700000000000, Logical: 2, NodeID: "n1"}.String()
	if want := "1700000000000.2@n1"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestClockIsSafeForConcurrentUse(t *testing.T) {
	c := clock.New("n1", time.Now)
	var wg sync.WaitGroup
	seen := make(chan clock.HLC, 200)
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); seen <- c.Now() }()
		go func() { defer wg.Done(); seen <- c.Observe(clock.HLC{Wall: 1, NodeID: "n2"}) }()
	}
	wg.Wait()
	close(seen)
	uniq := map[clock.HLC]bool{}
	for h := range seen {
		if uniq[h] {
			t.Fatalf("duplicate timestamp %v issued", h)
		}
		uniq[h] = true
	}
}

func TestNewDefaultsToWallClock(t *testing.T) {
	c := clock.New("n1", nil)
	if got := c.Now(); got.Wall == 0 {
		t.Fatal("New(nil) must default to the system clock")
	}
}
