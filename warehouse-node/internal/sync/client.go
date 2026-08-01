package syncrepl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/eventlog"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// maxBackoff caps Run's reconnect wait once every doubling has topped out, so an
// extended central outage never leaves a node waiting hours to retry.
const maxBackoff = 5 * time.Minute

// Client is the node's side of the replication stream. The transport is injected as a
// syncpb.SyncClient, so tests drive a real gRPC stream over an in-process listener.
type Client struct {
	svc    *node.Service
	remote syncpb.SyncClient
}

// NewClient builds a sync client for one node.
func NewClient(svc *node.Service, remote syncpb.SyncClient) *Client {
	return &Client{svc: svc, remote: remote}
}

// Run keeps syncing until ctx is done. A failed session is not an error the operator
// sees: central being unreachable is a normal state for a warehouse, so the failure is
// logged by the caller at most and retried after backoff. The backoff grows and jitters
// on repeated failure so nodes reconnecting after a shared outage do not all retry on
// the same tick, and resets once a session succeeds.
func (c *Client) Run(ctx context.Context, every, backoff time.Duration) error {
	current := backoff
	for {
		wait := every
		if err := c.Session(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			wait = current + rand.N(current/2+1)
			if current < maxBackoff {
				current *= 2
			}
		} else {
			current = backoff
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// Session runs one full round: push this node's backlog, then apply everything central
// has for it. Both cursors are persisted as they advance, so the next session resumes
// rather than restarting.
func (c *Client) Session(ctx context.Context) error {
	stream, err := c.remote.Replicate(ctx)
	if err != nil {
		return fmt.Errorf("open replication stream: %w", err)
	}

	pulled, err := c.svc.Log().Cursor(eventlog.CursorPulled)
	if err != nil {
		return err
	}
	hello := &syncpb.Hello{NodeId: string(c.svc.NodeID()), PulledCursor: pulled}
	if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Hello{Hello: hello}}); err != nil {
		return err
	}
	welcome, err := stream.Recv()
	if err != nil {
		return err
	}
	if welcome.GetWelcome() == nil {
		return errors.New("central did not answer Hello with Welcome")
	}

	// Resume from whichever is lower: what we think we pushed, or what central admits
	// to holding. Central's answer wins, because re-sending is free and idempotent
	// while a gap is not.
	from, err := c.svc.Log().Cursor(eventlog.CursorPushed)
	if err != nil {
		return err
	}
	if known := welcome.GetWelcome().GetKnownSeq(); known < from {
		from = known
	}
	if err := c.pushBacklog(ctx, stream, from); err != nil {
		return err
	}
	return c.applyDownstream(stream)
}

// pushBacklog sends this node's own events above from, in bounded chunks, updating the
// push cursor from each acknowledgement. A week of backlog is many chunks, never one
// enormous message.
//
// ReadOwnAfter already bounds one page to MaxBatchEvents events, but a page of large
// envelopes can still exceed MaxBatchBytes on the wire: Chunk splits that page
// further before each piece is sent, matching the same discipline central's push
// applies, so neither side of the stream can send a frame past the byte cap.
func (c *Client) pushBacklog(ctx context.Context, stream syncpb.Sync_ReplicateClient, from uint64) error {
	for {
		envs, err := c.svc.Log().ReadOwnAfter(from, MaxBatchEvents)
		if err != nil {
			return err
		}
		chunks := Chunk(envs)
		if len(chunks) == 0 {
			chunks = [][]domain.Envelope{nil}
		}
		for i, chunk := range chunks {
			batch := EncodeBatch(chunk)
			more := i < len(chunks)-1
			if len(chunk) > 0 {
				from = chunk[len(chunk)-1].ID.Seq
			}
			if !more {
				next, err := c.svc.Log().ReadOwnAfter(from, 1)
				if err != nil {
					return err
				}
				more = len(next) > 0
			}
			batch.More = more
			if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Events{Events: batch}}); err != nil {
				return err
			}
			ack, err := stream.Recv()
			if err != nil {
				return err
			}
			if ack.GetAck() == nil {
				return errors.New("central did not acknowledge the batch")
			}
			if seq := ack.GetAck().GetSeq(); seq > 0 {
				if err := c.svc.Log().SetCursor(eventlog.CursorPushed, seq); err != nil {
					return err
				}
			}
			if !batch.GetMore() {
				return ctx.Err()
			}
		}
	}
}

// applyDownstream ingests central's batches. An undecodable event fails the session
// and leaves the cursor where it was, so it is retried: skipping an event from the
// authority silently forks this node's state, which is the one outcome that must never
// happen.
func (c *Client) applyDownstream(stream syncpb.Sync_ReplicateClient) error {
	var highest uint64
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
			return errors.New("central sent a frame that was not an EventBatch")
		}
		envs, err := DecodeBatch(batch)
		if err != nil {
			return err
		}
		if len(envs) > 0 {
			if _, err := c.svc.Ingest(envs); err != nil {
				return err
			}
		}
		if batch.GetLastOrd() > highest {
			highest = batch.GetLastOrd()
		}
		if batch.GetMore() {
			continue
		}
		if highest > 0 {
			if err := c.svc.Log().SetCursor(eventlog.CursorPulled, highest); err != nil {
				return err
			}
		}
		if err := stream.Send(&syncpb.NodeFrame{Body: &syncpb.NodeFrame_Ack{
			Ack: &syncpb.Ack{Ord: highest}}}); err != nil {
			return err
		}
		return stream.CloseSend()
	}
}
