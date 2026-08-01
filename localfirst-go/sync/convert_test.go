package sync_test

import (
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

func TestRecordRoundTrip(t *testing.T) {
	in := eventlog.Record{
		NodeID: "n1", Seq: 9, Clock: clock.HLC{Wall: 42, Logical: 3, NodeID: "n1"},
		Type: "T", Payload: []byte("hello"),
	}
	got := lfsync.RecordFromPB(lfsync.RecordToPB(in))
	if string(got.Payload) != string(in.Payload) {
		t.Fatalf("payload = %q, want %q", got.Payload, in.Payload)
	}
	if got.NodeID != in.NodeID || got.Seq != in.Seq || got.Clock != in.Clock || got.Type != in.Type {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
}

func TestVersionVectorRoundTrip(t *testing.T) {
	in := eventlog.VersionVector{"n1": 3, "n2": 7}
	if got := lfsync.VersionFromPB(lfsync.VersionToPB(in)); !got.Equal(in) {
		t.Fatalf("round trip = %v, want %v", got, in)
	}
	if got := lfsync.VersionFromPB(nil); len(got) != 0 {
		t.Fatalf("nil map = %v, want empty", got)
	}
}

func TestRecordFromPBRestoresClockNodeID(t *testing.T) {
	got := lfsync.RecordFromPB(&syncpb.Record{NodeId: "n5", Seq: 1})
	if got.Clock.NodeID != "n5" {
		t.Fatalf("Clock.NodeID = %q, want n5", got.Clock.NodeID)
	}
}
