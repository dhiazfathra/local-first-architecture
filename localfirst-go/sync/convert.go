// Package sync replicates records between peers over one symmetric gRPC stream.
// It is domain-agnostic: it moves opaque records and never interprets them.
package sync

import (
	"errors"
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// ErrInvalidPeerRecord means a batch record could not have been produced
// honestly by any peer and must not reach Merge.
var ErrInvalidPeerRecord = errors.New("sync: invalid peer record")

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

// ValidatePeerRecord rejects records a peer could not honestly have
// authored: an empty node_id, a node_id claiming to be our own (this node
// receiving its own records back), or a seq outside the valid range (peer
// sequence counters start at one, so zero is never legitimate).
func ValidatePeerRecord(r eventlog.Record, localNodeID string) error {
	if r.NodeID == "" {
		return fmt.Errorf("%w: empty node_id", ErrInvalidPeerRecord)
	}
	if r.NodeID == localNodeID {
		return fmt.Errorf("%w: self-authored node_id %q", ErrInvalidPeerRecord, r.NodeID)
	}
	if r.Seq == 0 {
		return fmt.Errorf("%w: seq 0 for node_id %q", ErrInvalidPeerRecord, r.NodeID)
	}
	return nil
}

// VersionToPB converts a version vector to the wire form.
func VersionToPB(v eventlog.VersionVector) map[string]uint64 { return map[string]uint64(v.Clone()) }

// VersionFromPB converts a wire version vector back.
func VersionFromPB(m map[string]uint64) eventlog.VersionVector {
	return eventlog.VersionVector(m).Clone()
}
