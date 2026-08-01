// Package syncrepl implements bidirectional replication between a warehouse node and
// central. It is named syncrepl rather than sync so it does not shadow the standard
// library's sync package at its call sites.
package syncrepl

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// Batch caps. A node offline for a week has a large backlog, and sending it in one
// message would blow both the gRPC message limit and the receiver's memory. Both
// caps are enforced, whichever bites first.
const (
	MaxBatchEvents = 200
	MaxBatchBytes  = 1 << 20
)

// EncodeEnvelope converts a stored envelope to its wire form. The payload travels as
// opaque bytes: only the node and central's projections interpret it, and the wire
// format deliberately does not need to know the payload types.
func EncodeEnvelope(env domain.Envelope) *syncpb.Event {
	e := &syncpb.Event{
		Id:                 &syncpb.EventID{NodeId: string(env.ID.NodeID), Seq: env.ID.Seq},
		AggregateId:        env.AggregateID,
		Type:               env.Type,
		Hlc:                &syncpb.HLC{Wall: env.HLC.Wall, Counter: env.HLC.Counter, Node: string(env.HLC.Node)},
		RecordedAtUnixNano: env.RecordedAt.UnixNano(),
		Payload:            env.Payload,
	}
	if env.CausationID != nil {
		e.CausationId = &syncpb.EventID{NodeId: string(env.CausationID.NodeID), Seq: env.CausationID.Seq}
	}
	return e
}

// DecodeEnvelope converts a wire event back to an envelope. Every structural field is
// required: an event that cannot be decoded must fail the session loudly rather than
// be skipped, because skipping an event silently forks state.
func DecodeEnvelope(e *syncpb.Event) (domain.Envelope, error) {
	switch {
	case e == nil:
		return domain.Envelope{}, fmt.Errorf("decode event: frame is nil")
	case e.GetId() == nil || e.GetId().GetNodeId() == "" || e.GetId().GetSeq() == 0:
		return domain.Envelope{}, fmt.Errorf("decode event: identity is missing or incomplete")
	case e.GetType() == "":
		return domain.Envelope{}, fmt.Errorf("decode event %s/%d: type is empty",
			e.GetId().GetNodeId(), e.GetId().GetSeq())
	case e.GetHlc() == nil:
		return domain.Envelope{}, fmt.Errorf("decode event %s/%d: hlc is missing",
			e.GetId().GetNodeId(), e.GetId().GetSeq())
	}
	env := domain.Envelope{
		ID:          domain.EventID{NodeID: domain.NodeID(e.GetId().GetNodeId()), Seq: e.GetId().GetSeq()},
		AggregateID: e.GetAggregateId(),
		Type:        e.GetType(),
		HLC: domain.HLC{Wall: e.GetHlc().GetWall(), Counter: e.GetHlc().GetCounter(),
			Node: domain.NodeID(e.GetHlc().GetNode())},
		RecordedAt: time.Unix(0, e.GetRecordedAtUnixNano()).UTC(),
		Payload:    json.RawMessage(e.GetPayload()),
	}
	if c := e.GetCausationId(); c != nil {
		env.CausationID = &domain.EventID{NodeID: domain.NodeID(c.GetNodeId()), Seq: c.GetSeq()}
	}
	return env, nil
}

// EncodeBatch wraps envelopes in a batch frame. Callers pass a chunk from Chunk, so
// this does not itself enforce the caps.
func EncodeBatch(envs []domain.Envelope) *syncpb.EventBatch {
	out := make([]*syncpb.Event, 0, len(envs))
	for _, env := range envs {
		out = append(out, EncodeEnvelope(env))
	}
	return &syncpb.EventBatch{Events: out}
}

// DecodeBatch converts a batch frame. One undecodable event fails the whole batch.
func DecodeBatch(b *syncpb.EventBatch) ([]domain.Envelope, error) {
	out := make([]domain.Envelope, 0, len(b.GetEvents()))
	for _, e := range b.GetEvents() {
		env, err := DecodeEnvelope(e)
		if err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, nil
}

// Chunk splits envelopes into batches within both caps, preserving order. A single
// envelope larger than MaxBatchBytes still gets its own chunk rather than being
// dropped: a chunk is never empty, because an empty batch would stall the stream.
func Chunk(envs []domain.Envelope) [][]domain.Envelope {
	var chunks [][]domain.Envelope
	var current []domain.Envelope
	var bytes int
	for _, env := range envs {
		size := len(env.Payload) + len(env.Type) + len(env.AggregateID) + 64
		if len(current) > 0 && (len(current) >= MaxBatchEvents || bytes+size > MaxBatchBytes) {
			chunks = append(chunks, current)
			current, bytes = nil, 0
		}
		current = append(current, env)
		bytes += size
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}
