package syncrepl

import (
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/arbiter"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// Server is central's side of the replication stream.
type Server struct {
	syncpb.UnimplementedSyncServer
	store central.Store
	arb   *arbiter.Arbiter
}

// NewServer builds the sync server over a store and an arbiter.
func NewServer(store central.Store, arb *arbiter.Arbiter) *Server {
	return &Server{store: store, arb: arb}
}

// Replicate runs one session: Hello, Welcome, the node's events upstream, then
// everything queued downstream, then the node's acknowledgement. Cursors are
// persisted as it goes, so a session that dies halfway costs nothing but a retry.
func (s *Server) Replicate(stream syncpb.Sync_ReplicateServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil || hello.GetNodeId() == "" {
		return status.Error(codes.InvalidArgument, "a session must open with Hello naming the node")
	}
	node := domain.NodeID(hello.GetNodeId())

	known, err := s.store.PushedSeq(ctx, node)
	if err != nil {
		return err
	}
	if err := stream.Send(&syncpb.CentralFrame{Body: &syncpb.CentralFrame_Welcome{
		Welcome: &syncpb.Welcome{KnownSeq: known}}}); err != nil {
		return err
	}

	if err := s.receive(stream, node); err != nil {
		return err
	}
	if err := s.push(stream, node); err != nil {
		return err
	}

	// One final Ack tells us how far the node applied what we sent. EOF instead means
	// the node dropped out: nothing is lost, the cursor simply does not advance.
	last, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	if ack := last.GetAck(); ack != nil && ack.GetOrd() > 0 {
		return s.store.SetDeliveredOrd(ctx, node, ack.GetOrd())
	}
	return nil
}

// receive consumes the node's upstream batches, arbitrating every event, until a batch
// arrives with more = false.
func (s *Server) receive(stream syncpb.Sync_ReplicateServer, node domain.NodeID) error {
	ctx := stream.Context()
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		batch := frame.GetEvents()
		if batch == nil {
			return status.Error(codes.InvalidArgument, "expected an EventBatch while receiving")
		}
		envs, err := DecodeBatch(batch)
		if err != nil {
			// Never skip. A malformed event means the session fails and is retried;
			// accepting the rest would fork central's history from the node's.
			return status.Error(codes.InvalidArgument, err.Error())
		}
		highest, err := s.store.PushedSeq(ctx, node)
		if err != nil {
			return err
		}
		for _, env := range envs {
			if _, _, err := s.arb.Arbitrate(ctx, env); err != nil {
				return err
			}
			if env.ID.NodeID == node && env.ID.Seq > highest {
				highest = env.ID.Seq
			}
		}
		if err := s.store.SetPushedSeq(ctx, node, highest); err != nil {
			return err
		}
		if err := stream.Send(&syncpb.CentralFrame{Body: &syncpb.CentralFrame_Ack{
			Ack: &syncpb.Ack{Seq: highest}}}); err != nil {
			return err
		}
		if !batch.GetMore() {
			return nil
		}
	}
}

// push sends everything queued for this node — compensations, item-master updates and
// forwarded transfer events — in chunks bounded by MaxBatchEvents.
//
// Outbound already bounds one page to MaxBatchEvents rows, but a page of large
// envelopes can still exceed MaxBatchBytes on the wire: Chunk splits that page
// further before each piece is sent, so no single frame ever breaks the byte cap
// EncodeBatch itself does not enforce.
func (s *Server) push(stream syncpb.Sync_ReplicateServer, node domain.NodeID) error {
	ctx := stream.Context()
	cursor, err := s.store.DeliveredOrd(ctx, node)
	if err != nil {
		return err
	}
	for {
		rows, err := s.store.Outbound(ctx, node, cursor, MaxBatchEvents)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return stream.Send(&syncpb.CentralFrame{Body: &syncpb.CentralFrame_Events{
				Events: &syncpb.EventBatch{More: false, LastOrd: cursor}}})
		}
		envs := make([]domain.Envelope, 0, len(rows))
		for _, row := range rows {
			envs = append(envs, row.Env)
		}
		chunks := Chunk(envs)
		consumed := 0
		for i, chunk := range chunks {
			consumed += len(chunk)
			cursor = rows[consumed-1].Ord
			more := i < len(chunks)-1
			if !more {
				remaining, err := s.store.Outbound(ctx, node, cursor, 1)
				if err != nil {
					return err
				}
				more = len(remaining) > 0
			}
			batch := EncodeBatch(chunk)
			batch.LastOrd, batch.More = cursor, more
			if err := stream.Send(&syncpb.CentralFrame{Body: &syncpb.CentralFrame_Events{Events: batch}}); err != nil {
				return err
			}
			if !more {
				return nil
			}
		}
	}
}
