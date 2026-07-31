package sync

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// Report summarises one session.
type Report struct {
	Sent     int
	Received int
	PeerID   clock.NodeID
	PeerVV   eventlog.VersionVector
}

// Client drives replication sessions from a replica to a peer address.
type Client struct {
	replica   Replica
	dialer    Dialer
	batchSize int
}

// NewClient wraps a replica as the dialing end of replication.
func NewClient(r Replica, d Dialer, batchSize int) *Client {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return &Client{replica: r, dialer: d, batchSize: batchSize}
}

// SyncOnce runs a single session: Hello, consume the peer's events until its
// Ack, then stream ours and Ack. A node is fully usable offline, so a failure
// here is never fatal to the node -- the caller decides whether to retry.
func (c *Client) SyncOnce(ctx context.Context, addr string) (Report, error) {
	mine, err := c.replica.VersionVector(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("sync client: %w", err)
	}
	st, err := c.dialer.Dial(ctx, addr)
	if err != nil {
		return Report{}, fmt.Errorf("sync client: dial %q: %w", addr, err)
	}
	if err := st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Hello{
		Hello: &syncpb.Hello{NodeId: string(c.replica.ID()), VersionVector: encodeVV(mine)},
	}}); err != nil {
		return Report{}, fmt.Errorf("sync client: send hello: %w", err)
	}

	rep, err := c.consume(ctx, st)
	if err != nil {
		return Report{}, err
	}

	if err := streamEvents(ctx, c.replica, rep.PeerVV, c.batchSize, func(batch []*syncpb.Event) error {
		rep.Sent += len(batch)
		return st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Events{Events: &syncpb.Events{Events: batch}}})
	}); err != nil {
		return Report{}, fmt.Errorf("sync client: %w", err)
	}

	after, err := c.replica.VersionVector(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("sync client: %w", err)
	}
	if err := st.Send(&syncpb.ClientFrame{Body: &syncpb.ClientFrame_Ack{
		Ack: &syncpb.Ack{VersionVector: encodeVV(after)},
	}}); err != nil {
		return Report{}, fmt.Errorf("sync client: send ack: %w", err)
	}
	if err := st.CloseSend(); err != nil {
		return Report{}, fmt.Errorf("sync client: close send: %w", err)
	}
	if err := c.replica.Log().SetCursor(ctx, rep.PeerID, rep.PeerVV[rep.PeerID]); err != nil {
		return Report{}, fmt.Errorf("sync client: %w", err)
	}
	return rep, nil
}

// consume reads Welcome, then the peer's event batches, until the peer's Ack.
func (c *Client) consume(ctx context.Context, st Stream) (Report, error) {
	frame, err := st.Recv()
	if err != nil {
		return Report{}, fmt.Errorf("sync client: recv welcome: %w", err)
	}
	welcome, ok := frame.GetBody().(*syncpb.ServerFrame_Welcome)
	if !ok {
		return Report{}, fmt.Errorf("sync client: expected welcome, got %T", frame.GetBody())
	}
	rep := Report{
		PeerID: clock.NodeID(welcome.Welcome.GetNodeId()),
		PeerVV: decodeVV(welcome.Welcome.GetVersionVector()),
	}

	for {
		frame, err := st.Recv()
		if err != nil {
			return Report{}, fmt.Errorf("sync client: recv from %q: %w", rep.PeerID, err)
		}
		switch body := frame.GetBody().(type) {
		case *syncpb.ServerFrame_Events:
			n, err := mergeFrame(ctx, c.replica, body.Events.GetEvents())
			if err != nil {
				return Report{}, fmt.Errorf("sync client: %w", err)
			}
			if n != len(body.Events.GetEvents()) {
				return Report{}, fmt.Errorf("sync client: peer %q sent an unapplicable event", rep.PeerID)
			}
			rep.Received += n
		case *syncpb.ServerFrame_Ack:
			return rep, nil
		default:
			return Report{}, fmt.Errorf("sync client: unexpected frame %T", frame.GetBody())
		}
	}
}

// SyncWithBackoff retries SyncOnce with exponential backoff and jitter. sleep
// may be nil, meaning time.Sleep with full jitter; tests pass a recorder.
// A node stays fully usable offline indefinitely, so exhausting attempts is a
// reportable outcome, not a crash.
func (c *Client) SyncWithBackoff(ctx context.Context, addr string, attempts int, base time.Duration,
	sleep func(time.Duration) time.Duration) (Report, error) {
	if sleep == nil {
		sleep = func(d time.Duration) time.Duration {
			jittered := time.Duration(rand.Int64N(int64(d) + 1))
			time.Sleep(jittered)
			return jittered
		}
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Report{}, fmt.Errorf("sync client: %w", err)
		}
		rep, err := c.SyncOnce(ctx, addr)
		if err == nil {
			return rep, nil
		}
		lastErr = err
		if attempt < attempts-1 {
			sleep(base << attempt)
		}
	}
	return Report{}, fmt.Errorf("sync client: gave up after %d attempts: %w", attempts, lastErr)
}
