package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// Client is the initiator half.
type Client struct {
	rpc  syncpb.SyncClient
	log  Log
	proj eventlog.Projector
	peer string // cursor key: which peer this client talks to
}

// NewClient returns a client that replicates log with peer over cc.
func NewClient(cc grpc.ClientConnInterface, log Log, p eventlog.Projector, peer string) *Client {
	return &Client{rpc: syncpb.NewSyncClient(cc), log: log, proj: p, peer: peer}
}

// SyncOnce runs one exchange and, on success, advances the peer cursor to the
// highest own-record sequence the peer now holds. On failure the cursor is left
// untouched: the node stays fully operational and simply retries.
//
// Sending our final Ack only closes our send side — it says nothing about
// whether the responder finished applying our batches. After CloseSend, the
// responder is still merging the last batch server-side; its Merge error (if
// any) only surfaces as this stream's terminal RPC status. We must read that
// status before advancing the cursor, or a failed remote merge would be
// recorded as a successful sync.
func (c *Client) SyncOnce(ctx context.Context) (eventlog.VersionVector, error) {
	stream, err := c.rpc.Replicate(ctx)
	if err != nil {
		return nil, err
	}
	vv, err := Exchange(ctx, c.log, c.proj, stream, true)
	if err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	if _, err := stream.Recv(); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("sync: responder: %w", err)
	}
	if err := c.log.SetCursor(ctx, c.peer, vv.Get(c.log.NodeID())); err != nil {
		return nil, err
	}
	return vv, nil
}

// Run syncs every interval until ctx is done. A failure is logged and retried
// with a capped backoff; it never stops the node from accepting local commands,
// because nothing here is on the command path.
func (c *Client) Run(ctx context.Context, every time.Duration) {
	backoff := every
	const maxBackoff = 30 * time.Second
	for {
		if _, err := c.SyncOnce(ctx); err != nil {
			slog.WarnContext(ctx, "sync failed, node still operational",
				"peer", c.peer, "retry_in", backoff, "err", err)
			backoff = min(backoff*2, maxBackoff)
		} else {
			backoff = every
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}
