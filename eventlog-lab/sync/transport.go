package sync

import (
	"context"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// Stream is the client half of a replication session. gRPC's generated client
// stream satisfies it directly; the fault harness supplies a deterministic
// in-memory implementation. That second implementation is the only reason this
// interface exists.
type Stream interface {
	Send(*syncpb.ClientFrame) error
	Recv() (*syncpb.ServerFrame, error)
	CloseSend() error
}

// ServerStream is the server half of a session.
type ServerStream interface {
	Send(*syncpb.ServerFrame) error
	Recv() (*syncpb.ClientFrame, error)
}

// Dialer opens a session to a peer address.
type Dialer interface {
	Dial(ctx context.Context, addr string) (Stream, error)
}

// Replica is what a session needs from either end. *node.Node satisfies it, for
// both a SQLite-backed node and the Postgres-backed central store.
type Replica interface {
	ID() clock.NodeID
	VersionVector(ctx context.Context) (eventlog.VersionVector, error)
	Merge(ctx context.Context, events []eventlog.Event) (int, error)
	Log() crdt.SQLLog
}
