package sync

import (
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// Server is the responder half. It is only three lines because the protocol is
// symmetric: all the logic lives in Exchange.
type Server struct {
	syncpb.UnimplementedSyncServer
	log Log
	// A node passes its *domain.Inventory so merged peer records update its
	// in-memory balances. Central passes nil: its global sum is a SQL query
	// over the records table, not an in-memory projection.
	proj eventlog.Projector
}

// NewServer returns a Sync service backed by log.
func NewServer(log Log, p eventlog.Projector) *Server { return &Server{log: log, proj: p} }

// Replicate handles one peer's replication stream.
func (s *Server) Replicate(stream syncpb.Sync_ReplicateServer) error {
	_, err := Exchange(stream.Context(), s.log, s.proj, stream, false)
	return err
}
