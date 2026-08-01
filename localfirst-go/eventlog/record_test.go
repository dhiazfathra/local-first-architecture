package eventlog_test

import (
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

func TestVersionVector(t *testing.T) {
	vv := eventlog.VersionVector{}
	if got := vv.Get("n1"); got != 0 {
		t.Fatalf("Get on empty = %d, want 0", got)
	}
	vv.Observe("n1", 5)
	vv.Observe("n1", 3) // regression must be ignored: vectors only grow
	if got := vv.Get("n1"); got != 5 {
		t.Fatalf("Get = %d, want 5", got)
	}
	vv.Observe("n2", 1)

	clone := vv.Clone()
	if !clone.Equal(vv) {
		t.Fatal("clone must equal source")
	}
	clone.Observe("n2", 9)
	if vv.Get("n2") != 1 {
		t.Fatal("mutating the clone must not touch the source")
	}
	if clone.Equal(vv) {
		t.Fatal("diverged vectors must not be equal")
	}
	if (eventlog.VersionVector{"n1": 5}).Equal(vv) {
		t.Fatal("vectors of different length must not be equal")
	}
	var nilVV eventlog.VersionVector
	if nilVV.Get("n1") != 0 {
		t.Fatal("Get on a nil vector must be 0")
	}
	if !nilVV.Equal(eventlog.VersionVector{}) {
		t.Fatal("nil and empty vectors are equal")
	}
}

func TestRecordLess(t *testing.T) {
	mk := func(node string, seq uint64, wall int64) eventlog.Record {
		return eventlog.Record{NodeID: node, Seq: seq, Clock: clock.HLC{Wall: wall, NodeID: node}}
	}
	tests := []struct {
		name string
		a, b eventlog.Record
		want bool
	}{
		{"earlier clock first", mk("n1", 1, 10), mk("n2", 1, 20), true},
		{"later clock not first", mk("n2", 1, 20), mk("n1", 1, 10), false},
		{"same clock falls back to seq", mk("n1", 1, 10), mk("n1", 2, 10), true},
		{"identical is not less", mk("n1", 1, 10), mk("n1", 1, 10), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Less(tc.b); got != tc.want {
				t.Fatalf("Less = %v, want %v", got, tc.want)
			}
		})
	}
}
