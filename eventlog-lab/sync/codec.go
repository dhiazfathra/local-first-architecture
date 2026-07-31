// Package sync replicates events between two logs over a bidirectional stream.
// Both sides merge; neither arbitrates. The central store runs the same code
// with the same crdt.Apply, so it is a merge participant, not an authority.
package sync

//go:generate protoc --proto_path=. --go_out=.. --go_opt=module=github.com/dhiazfathra/local-first-architecture/eventlog-lab --go-grpc_out=.. --go-grpc_opt=module=github.com/dhiazfathra/local-first-architecture/eventlog-lab sync.proto

import (
	"fmt"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// encodeEvent converts a stored event to its wire form. It validates first so a
// node can never emit something its peer would have to reject.
func encodeEvent(e eventlog.Event) (*syncpb.Event, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	pe := &syncpb.Event{
		NodeId:     string(e.ID.NodeID),
		Seq:        uint64(e.ID.Seq),
		HlcWall:    e.HLC.Wall,
		HlcLogical: e.HLC.Logical,
		Sku:        e.SKU,
		Kind:       int32(e.Kind),
		Delta:      e.Delta,
	}
	if e.Meta != nil {
		pe.Name, pe.ReorderPoint = e.Meta.Name, e.Meta.ReorderPoint
	}
	pe.DeletedTo = e.DeletedTo
	return pe, nil
}

// decodeEvent converts a wire event to its stored form, validating it. This is
// the trust boundary for remote input: an event that fails here is never
// persisted, because storing something crdt cannot apply breaks convergence
// silently.
func decodeEvent(pe *syncpb.Event) (eventlog.Event, error) {
	if pe == nil {
		return eventlog.Event{}, fmt.Errorf("%w: nil event frame", eventlog.ErrMalformedEvent)
	}
	node := clock.NodeID(pe.GetNodeId())
	e := eventlog.Event{
		ID:        eventlog.EventID{NodeID: node, Seq: eventlog.Seq(pe.GetSeq())},
		HLC:       clock.HLC{Wall: pe.GetHlcWall(), Logical: pe.GetHlcLogical(), NodeID: node},
		SKU:       pe.GetSku(),
		Kind:      eventlog.Kind(pe.GetKind()),
		Delta:     pe.GetDelta(),
		DeletedTo: pe.DeletedTo,
	}
	if pe.Name != nil || pe.ReorderPoint != nil {
		e.Meta = &eventlog.MetaSet{Name: pe.Name, ReorderPoint: pe.ReorderPoint}
	}
	if err := e.Validate(); err != nil {
		return eventlog.Event{}, err
	}
	return e, nil
}

// encodeVV converts a version vector to its wire form.
func encodeVV(vv eventlog.VersionVector) map[string]uint64 {
	out := make(map[string]uint64, len(vv))
	for k, v := range vv {
		out[string(k)] = uint64(v)
	}
	return out
}

// decodeVV converts a wire version vector back, never returning nil.
func decodeVV(m map[string]uint64) eventlog.VersionVector {
	out := make(eventlog.VersionVector, len(m))
	for k, v := range m {
		out[clock.NodeID(k)] = eventlog.Seq(v)
	}
	return out
}
