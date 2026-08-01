// Package node ties a warehouse node's event log, in-memory domain state and
// projections together. It is the only place that knows all three, and it is what
// every transport — the operator gRPC API, the sync client — talks to.
package node

import (
	"fmt"
	"sync"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/eventlog"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/projection"
)

// Command is a domain command bound to its arguments. Every domain.Do* function has
// this shape once its command struct is supplied, so no per-command wrapper is
// needed:
//
//	svc.Execute(func(s *domain.State) ([]domain.Event, error) {
//	    return domain.DoPutAway(s, cmd)
//	})
type Command func(*domain.State) ([]domain.Event, error)

// Service is one warehouse node.
type Service struct {
	mu    sync.Mutex
	log   *eventlog.Log
	set   *projection.Set
	state *domain.State
}

// Open opens (or creates) the node at path, brings its projections up to date, and
// rebuilds the in-memory domain state by replaying the log. now supplies wall time
// and is injected so tests are deterministic.
func Open(path string, id domain.NodeID, now func() time.Time) (*Service, error) {
	log, err := eventlog.Open(path, id, now)
	if err != nil {
		return nil, err
	}
	set, err := projection.Open(log)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	s := &Service{log: log, set: set, state: domain.NewState()}
	s.state.SetHome(id)
	if err := s.rebuildState(); err != nil {
		_ = log.Close()
		return nil, err
	}
	return s, nil
}

// rebuildState replays the entire log into a fresh in-memory domain.State. Unlike
// the projections, state has no durable per-event applied marker of its own — it is
// an in-memory derivation, so the only way to guarantee it matches the log is to
// derive it wholesale rather than mutate it incrementally. Open uses this to build
// state the first time; apply calls it every time projections successfully catch up,
// so state is always a fresh, idempotent replay of exactly what the log and the
// projections' applied markers agree has happened — never state.Apply(env) run
// against a state that might already reflect env from an earlier partial retry.
func (s *Service) rebuildState() error {
	envs, err := s.log.ReadAll()
	if err != nil {
		return err
	}
	fresh := domain.NewState()
	fresh.SetHome(s.log.NodeID())
	for _, env := range envs {
		if err := fresh.Apply(env); err != nil {
			return fmt.Errorf("rebuild state: %w", err)
		}
	}
	s.state = fresh
	return nil
}

// Close releases the underlying database.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
}

// NodeID is this node's identity.
func (s *Service) NodeID() domain.NodeID { return s.log.NodeID() }

// Log exposes the event log for the sync client, which needs cursors and raw reads.
func (s *Service) Log() *eventlog.Log { return s.log }

// Execute validates a command against current state and, only if it passes, appends
// its events and folds them into state and the projections. A rejected command
// writes nothing at all: the operator sees the violated rule immediately and the log
// is untouched.
func (s *Service) Execute(cmd Command) ([]domain.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events, err := cmd(s.state)
	if err != nil {
		return nil, err
	}
	envs, err := s.log.Emit(events, nil)
	if err != nil {
		return nil, err
	}
	return envs, s.apply(envs)
}

// Ingest folds in envelopes produced elsewhere: compensations from central,
// item-master updates, and transfer events forwarded from another node. It returns
// how many were new. There is no validation: a node cannot refuse a compensation,
// which is what makes central authoritative.
//
// Ingest does not pre-filter which envelopes still need applying — apply itself
// asks the projections' durable per-event applied marker (Task 11) for each one
// and skips what is already there, so passing the full batch on every retry is
// safe. This is deliberately not a version-vector comparison: a version vector
// only tells you what the log gained in *this* call, and if a previous call
// persisted an event but crashed or errored before applying it, that event is
// already reflected in the log's version vector on the next call and would never
// be recognised as still-pending. Asking the projections directly, inside apply,
// is what gives ingestion a recovery path.
func (s *Service) Ingest(envs []domain.Envelope) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, err := s.log.Ingest(envs)
	if err != nil {
		return 0, err
	}
	return n, s.apply(envs)
}

// RegisterLocation declares a node-owned stock location. Locations are node-owned —
// no other node has authority over them — so this is a local command, not something
// replicated down from central.
func (s *Service) RegisterLocation(code domain.LocationCode, typ domain.LocationType) error {
	_, err := s.Execute(func(*domain.State) ([]domain.Event, error) {
		if !typ.Valid() {
			return nil, domain.Violation(domain.RuleLocationExists, "unknown location type %q", typ)
		}
		return []domain.Event{{Type: domain.TypeLocationRegistered, AggregateID: string(code),
			Payload: domain.LocationRegistered{Code: code, Type: typ}}}, nil
	})
	return err
}

// apply folds envelopes into the projections, then rebuilds state from the log.
//
// Projections come first because they carry a durable per-event applied marker
// (Task 11): applying the same envelope to them twice is a safe no-op, so a retry
// that re-submits an envelope already reflected in the projections is harmless.
// State has no such marker — it is a plain in-memory struct — so it is never
// mutated incrementally here. Instead, once every envelope in this call has
// cleared the projections, state is rebuilt wholesale from the persisted log via
// rebuildState, which is itself idempotent: replaying the same log twice always
// produces the same state.
//
// This ordering is what rules out double-applying a non-idempotent effect (a
// stock movement) after a partial failure. Consider the alternative this replaced:
// apply state first, then projections, and rebuild state from the log only when
// projections fail. If state.Apply(env) succeeds but set.Apply(env) then fails,
// rebuildState makes state correct again (the log already has env, so the replay
// includes it) — but the projections' applied marker for env is still unset. A
// retry recomputes "still needs applying" from that marker, sees env as pending
// again, and calls apply([env]) a second time: state.Apply(env) now runs against
// a state that already reflects env via the earlier rebuild, applying it twice.
// Doing projections first and always deriving state by full rebuild removes the
// possibility entirely: state is never asked to apply an envelope it might
// already contain, because it is never asked to apply envelopes one at a time.
func (s *Service) apply(envs []domain.Envelope) error {
	for _, env := range envs {
		applied, err := s.set.IsApplied(env.ID)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		if err := s.set.Apply(env); err != nil {
			return err
		}
	}
	return s.rebuildState()
}

// StockOnHand returns operator-visible balances. Empty arguments mean "no filter".
func (s *Service) StockOnHand(sku string, location domain.LocationCode) ([]projection.StockRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.set.StockOnHand(sku, location)
}

// Reservations returns every reservation this node knows about.
func (s *Service) Reservations() ([]projection.ReservationRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.set.Reservations()
}

// Exceptions returns what central reversed and why, plus any balance a compensation
// drove negative.
func (s *Service) Exceptions() ([]projection.ExceptionRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.set.Exceptions()
}

// Transfers returns this node's view of every inter-node transfer.
func (s *Service) Transfers() ([]projection.TransferRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.set.Transfers()
}
