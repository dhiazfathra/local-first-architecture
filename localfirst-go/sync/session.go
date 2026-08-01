package sync

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// ErrProtocol reports a peer that spoke out of turn.
var ErrProtocol = errors.New("sync: protocol violation")

// batchSize caps records per frame, so one exchange streams instead of building
// a single enormous message.
const batchSize = 100

// Log is the storage contract sync needs. Both *eventlog.Store (SQLite, nodes)
// and central's Postgres store satisfy it — which is what makes central a peer
// rather than a special case.
type Log interface {
	NodeID() string
	Since(ctx context.Context, vv eventlog.VersionVector) ([]eventlog.Record, error)
	Merge(ctx context.Context, recs []eventlog.Record, p eventlog.Projector) (int, error)
	Version(ctx context.Context) (eventlog.VersionVector, error)
	Cursor(ctx context.Context, peer string) (uint64, error)
	SetCursor(ctx context.Context, peer string, seq uint64) error
}

// Stream is the half of a gRPC bidirectional stream both roles share. The
// generated client and server stream types both satisfy it, which is how one
// Exchange function serves both ends.
type Stream interface {
	Send(*syncpb.Frame) error
	Recv() (*syncpb.Frame, error)
}

// Exchange runs one full replication pass. For the initiator it returns the
// version vector it acknowledged to the peer — the peer's view of this node's
// records — which is what SyncOnce needs to set its cursor. The responder's
// return value is unused (the server ignores it). initiator selects who speaks
// first; nothing else differs between the two roles.
func Exchange(
	ctx context.Context, log Log, p eventlog.Projector, s Stream, initiator bool,
) (eventlog.VersionVector, error) {
	if initiator {
		if err := sendHello(ctx, log, s); err != nil {
			return nil, err
		}
	}
	frame, err := s.Recv()
	if err != nil {
		return nil, fmt.Errorf("sync: awaiting hello: %w", err)
	}
	hello := frame.GetHello()
	if hello == nil {
		return nil, fmt.Errorf("%w: first frame was %T, want Hello", ErrProtocol, frame.GetBody())
	}
	peerVV := VersionFromPB(hello.GetVersion())

	if !initiator {
		if err := sendHello(ctx, log, s); err != nil {
			return nil, err
		}
		if _, err := pushAndAck(ctx, log, s, peerVV); err != nil {
			return nil, err
		}
	}
	if err := drain(ctx, log, p, s); err != nil {
		return nil, err
	}
	if initiator {
		vv, err := pushAndAck(ctx, log, s, peerVV)
		if err != nil {
			return nil, err
		}
		return vv, nil
	}
	return log.Version(ctx)
}

func sendHello(ctx context.Context, log Log, s Stream) error {
	vv, err := log.Version(ctx)
	if err != nil {
		return err
	}
	if err := s.Send(&syncpb.Frame{Body: &syncpb.Frame_Hello{
		Hello: &syncpb.Hello{NodeId: log.NodeID(), Version: VersionToPB(vv)},
	}}); err != nil {
		return fmt.Errorf("sync: send hello: %w", err)
	}
	return nil
}

// pushAndAck sends everything the peer lacks, then an Ack meaning "that is all
// from me". It returns the version snapshot that Ack carries — the version this
// node asserted the peer now holds.
//
// The version snapshot is taken *before* Since collects records, and records
// above that snapshot are dropped from the batch. Without this, a local
// append racing with the push could land between Since and Version and be
// acknowledged to the peer despite never having been sent — the peer would
// then believe it holds a record it does not.
func pushAndAck(ctx context.Context, log Log, s Stream, peerVV eventlog.VersionVector) (eventlog.VersionVector, error) {
	vv, err := log.Version(ctx)
	if err != nil {
		return nil, err
	}
	recs, err := log.Since(ctx, peerVV)
	if err != nil {
		return nil, err
	}
	bounded := make([]eventlog.Record, 0, len(recs))
	for _, r := range recs {
		if r.Seq <= vv.Get(r.NodeID) {
			bounded = append(bounded, r)
		}
	}
	for start := 0; start < len(bounded); start += batchSize {
		end := min(start+batchSize, len(bounded))
		batch := &syncpb.Batch{Records: make([]*syncpb.Record, 0, end-start)}
		for _, r := range bounded[start:end] {
			batch.Records = append(batch.Records, RecordToPB(r))
		}
		if err := s.Send(&syncpb.Frame{Body: &syncpb.Frame_Batch{Batch: batch}}); err != nil {
			return nil, fmt.Errorf("sync: send batch: %w", err)
		}
	}
	if err := s.Send(&syncpb.Frame{Body: &syncpb.Frame_Ack{Ack: &syncpb.Ack{Version: VersionToPB(vv)}}}); err != nil {
		return nil, fmt.Errorf("sync: send ack: %w", err)
	}
	return vv, nil
}

// drain applies the peer's batches until its Ack (or a clean EOF).
func drain(ctx context.Context, log Log, p eventlog.Projector, s Stream) error {
	for {
		frame, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return nil // peer hung up after sending everything it had
		}
		if err != nil {
			return fmt.Errorf("sync: receive: %w", err)
		}
		switch body := frame.GetBody().(type) {
		case *syncpb.Frame_Batch:
			for _, pb := range body.Batch.GetRecords() {
				// ponytail: merge one record per call. Store.Merge defers its
				// projection commit funcs until after the transaction, so a
				// single multi-record Merge projects every next against the
				// pre-batch state and the last commit overwrites the earlier
				// records' effects. Per-record merges chain them correctly.
				if _, err := log.Merge(ctx, []eventlog.Record{RecordFromPB(pb)}, p); err != nil {
					return err
				}
			}
		case *syncpb.Frame_Ack:
			return nil
		default:
			return fmt.Errorf("%w: unexpected %T mid-stream", ErrProtocol, body)
		}
	}
}
