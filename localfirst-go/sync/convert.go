// Package sync replicates records between peers over one symmetric gRPC stream.
// It is domain-agnostic: it moves opaque records and never interprets them.
package sync

import (
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// RecordToPB converts a log record to the wire form.
func RecordToPB(r eventlog.Record) *syncpb.Record {
	return &syncpb.Record{
		NodeId: r.NodeID, Seq: r.Seq, HlcWall: r.Clock.Wall,
		HlcLogical: r.Clock.Logical, Type: r.Type, Payload: r.Payload,
	}
}

// RecordFromPB converts a wire record back. The HLC's node id is derived from
// the record's author, so it is never sent twice.
func RecordFromPB(p *syncpb.Record) eventlog.Record {
	return eventlog.Record{
		NodeID: p.GetNodeId(), Seq: p.GetSeq(), Type: p.GetType(), Payload: p.GetPayload(),
		Clock: clock.HLC{Wall: p.GetHlcWall(), Logical: p.GetHlcLogical(), NodeID: p.GetNodeId()},
	}
}

// VersionToPB converts a version vector to the wire form.
func VersionToPB(v eventlog.VersionVector) map[string]uint64 { return map[string]uint64(v.Clone()) }

// VersionFromPB converts a wire version vector back.
func VersionFromPB(m map[string]uint64) eventlog.VersionVector {
	return eventlog.VersionVector(m).Clone()
}
