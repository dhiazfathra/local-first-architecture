package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// defaultBatchSize is used when a caller passes a non-positive size.
const defaultBatchSize = 64

// Server serves replication sessions. It merges everything valid it is given
// and never rejects an event on policy grounds -- there are no arbitration
// frames in the protocol, by design.
type Server struct {
	syncpb.UnimplementedSyncServer
	replica   Replica
	batchSize int
}

// NewServer wraps a replica as the serving end of replication.
func NewServer(r Replica, batchSize int) *Server {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return &Server{replica: r, batchSize: batchSize}
}

// Replicate adapts the gRPC stream to Session.
func (s *Server) Replicate(gs syncpb.Sync_ReplicateServer) error {
	return s.Session(gs.Context(), gs)
}

// Session runs one replication session: read Hello, reply Welcome, stream what
// the peer lacks, ack, then merge what the peer streams and record a cursor.
func (s *Server) Session(ctx context.Context, st ServerStream) error {
	hello, err := s.recvHello(st)
	if err != nil {
		return err
	}
	peer := clock.NodeID(hello.GetNodeId())
	peerVV := decodeVV(hello.GetVersionVector())

	if err := s.warnOnRegression(ctx, peer, peerVV); err != nil {
		return err
	}

	mine, err := s.replica.VersionVector(ctx)
	if err != nil {
		return fmt.Errorf("sync server: %w", err)
	}
	if err := st.Send(&syncpb.ServerFrame{Body: &syncpb.ServerFrame_Welcome{
		Welcome: &syncpb.Welcome{NodeId: string(s.replica.ID()), VersionVector: encodeVV(mine)},
	}}); err != nil {
		return fmt.Errorf("sync server: send welcome: %w", err)
	}

	if err := streamEvents(ctx, s.replica, peerVV, s.batchSize, func(batch []*syncpb.Event) error {
		return st.Send(&syncpb.ServerFrame{Body: &syncpb.ServerFrame_Events{Events: &syncpb.Events{Events: batch}}})
	}); err != nil {
		return err
	}
	if err := st.Send(&syncpb.ServerFrame{Body: &syncpb.ServerFrame_Ack{
		Ack: &syncpb.Ack{VersionVector: encodeVV(mine)},
	}}); err != nil {
		return fmt.Errorf("sync server: send ack: %w", err)
	}

	return s.consume(ctx, st, peer)
}

// recvHello reads the mandatory opening frame.
func (s *Server) recvHello(st ServerStream) (*syncpb.Hello, error) {
	frame, err := st.Recv()
	if err != nil {
		return nil, fmt.Errorf("sync server: recv hello: %w", err)
	}
	hello, ok := frame.GetBody().(*syncpb.ClientFrame_Hello)
	if !ok {
		return nil, fmt.Errorf("sync server: expected hello, got %T", frame.GetBody())
	}
	if hello.Hello.GetNodeId() == "" {
		return nil, errors.New("sync server: hello with empty node id")
	}
	return hello.Hello, nil
}

// warnOnRegression logs when a peer claims less than it previously acked. The
// session continues from the peer's lower point: re-sending is harmless because
// Append is idempotent.
func (s *Server) warnOnRegression(ctx context.Context, peer clock.NodeID, peerVV eventlog.VersionVector) error {
	cursor, err := s.replica.Log().Cursor(ctx, peer)
	if err != nil {
		return fmt.Errorf("sync server: %w", err)
	}
	if peerVV[peer] < cursor {
		slog.Warn("peer version vector regressed; re-sending from the lower point",
			"peer", peer, "acked", cursor, "claimed", peerVV[peer])
	}
	return nil
}

// consume merges the peer's event batches until its Ack, then records a cursor.
func (s *Server) consume(ctx context.Context, st ServerStream, peer clock.NodeID) error {
	for {
		frame, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("sync server: recv from %q: %w", peer, err)
		}
		switch body := frame.GetBody().(type) {
		case *syncpb.ClientFrame_Events:
			if _, err := mergeFrame(ctx, s.replica, body.Events.GetEvents()); err != nil {
				return fmt.Errorf("sync server: %w", err)
			}
		case *syncpb.ClientFrame_Ack:
			acked := decodeVV(body.Ack.GetVersionVector())
			if err := s.replica.Log().SetCursor(ctx, peer, acked[peer]); err != nil {
				return fmt.Errorf("sync server: %w", err)
			}
			return nil
		case *syncpb.ClientFrame_Hello:
			return errors.New("sync server: duplicate hello mid-session")
		default:
			return fmt.Errorf("sync server: unexpected frame %T", frame.GetBody())
		}
	}
}

// streamEvents sends everything the replica holds that peerVV lacks, in batches.
func streamEvents(ctx context.Context, r Replica, peerVV eventlog.VersionVector, batchSize int,
	send func([]*syncpb.Event) error) error {
	batch := make([]*syncpb.Event, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := send(batch); err != nil {
			return fmt.Errorf("send events: %w", err)
		}
		// A fresh backing array, not batch[:0]: the memory transport is
		// asynchronous, so the consumer may still hold a reference to the
		// slice just sent when the next append would otherwise overwrite it.
		batch = make([]*syncpb.Event, 0, batchSize)
		return nil
	}
	for e, err := range r.Log().Since(ctx, peerVV) {
		if err != nil {
			return fmt.Errorf("read local events: %w", err)
		}
		pe, err := encodeEvent(e)
		if err != nil {
			return err
		}
		batch = append(batch, pe)
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// mergeFrame decodes and merges one Events frame, skipping malformed events.
// Per the spec: reject the bad event, log loudly, continue the session. Never
// persist something crdt cannot apply.
func mergeFrame(ctx context.Context, r Replica, pes []*syncpb.Event) (int, error) {
	events := make([]eventlog.Event, 0, len(pes))
	for _, pe := range pes {
		e, err := decodeEvent(pe)
		if err != nil {
			slog.Error("rejecting malformed remote event frame",
				"replica", r.ID(), "node", pe.GetNodeId(), "seq", pe.GetSeq(), "err", err)
			continue
		}
		events = append(events, e)
	}
	return r.Merge(ctx, events)
}
