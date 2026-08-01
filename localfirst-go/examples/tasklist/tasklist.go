// Package tasklist is the worked example behind docs/swapping-the-domain.md: a
// second, unrelated domain running on the same engine as inventory. It imports
// eventlog, projection and clock, and nothing else of ours — in particular it
// does not import domain. Compare it side by side with domain/ and note that no
// engine file differs between the two.
package tasklist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

// Record types. The engine treats these as opaque strings; only this package
// knows what they mean.
const (
	TypeAdded     = "tasklist.Added"
	TypeCompleted = "tasklist.Completed"
)

// Added and Completed are the entire event vocabulary of this domain.
type Added struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

// Completed marks an existing task done.
type Completed struct {
	ID string `json:"id"`
}

// Task is the projected form of one task.
type Task struct {
	Title string
	Done  bool
}

// State is the projection: every task this node knows about, by id.
type State map[string]Task

// Clone returns an independent copy. The reducer never mutates the state it is
// given, so a failed Apply cannot leave a half-updated projection behind.
func (s State) Clone() State {
	out := make(State, len(s))
	for id, t := range s {
		out[id] = t
	}
	return out
}

// Domain errors. Each names the rule it enforces, because the engine cannot.
var (
	ErrNoSuchTask    = errors.New("tasklist: no such task")
	ErrAlreadyDone   = errors.New("tasklist: task already completed")
	ErrDuplicateTask = errors.New("tasklist: task id already exists")
	ErrEmptyID       = errors.New("tasklist: task id must not be empty")
	ErrBadPayload    = errors.New("tasklist: undecodable payload")
)

// Reducer folds records into State. This is one of the two seams: it satisfies
// projection.Reducer[State], so projection.Fold works over it unchanged.
type Reducer struct{}

// Zero is the state of a node that has seen no records.
func (Reducer) Zero() State { return State{} }

// Apply interprets one record. It is a pure function: same state plus same
// record always yields the same result, which is what makes replay
// deterministic.
func (Reducer) Apply(state State, r eventlog.Record) (State, error) {
	switch r.Type {
	case TypeAdded:
		var e Added
		if err := decode(r, &e); err != nil {
			return nil, err
		}
		if _, exists := state[e.ID]; exists {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateTask, e.ID)
		}
		out := state.Clone()
		out[e.ID] = Task{Title: e.Title}
		return out, nil

	case TypeCompleted:
		var e Completed
		if err := decode(r, &e); err != nil {
			return nil, err
		}
		t, exists := state[e.ID]
		if !exists {
			return nil, fmt.Errorf("%w: %s", ErrNoSuchTask, e.ID)
		}
		if t.Done {
			return nil, fmt.Errorf("%w: %s", ErrAlreadyDone, e.ID)
		}
		out := state.Clone()
		t.Done = true
		out[e.ID] = t
		return out, nil

	default:
		// Never skip: a node that silently ignores records it does not
		// understand diverges from its peers without saying so.
		return nil, fmt.Errorf("%w: %s", eventlog.ErrUnknownType, r.Type)
	}
}

func decode(r eventlog.Record, into any) error {
	if err := json.Unmarshal(r.Payload, into); err != nil {
		return fmt.Errorf("%w: %s seq %d: %w", ErrBadPayload, r.Type, r.Seq, err)
	}
	return nil
}

// List is the command side: it owns the projected State, encodes commands into
// records, and serves as both eventlog.Validator and eventlog.Projector — the
// second seam.
type List struct {
	store *eventlog.Store
	clk   *clock.Clock

	// mu guards state itself, independently of anything that guards calling
	// Store.Append. A background eventlog.Merge (a synced record arriving from
	// another node) calls Check and Project through this same Validator/
	// Projector seam, concurrently with a local command — so the state access
	// inside Check and Project must defend itself, exactly like
	// domain.Inventory does with its own mu.
	mu    sync.RWMutex
	state State
}

// NewList rebuilds state by replaying the whole log, then accepts commands.
func NewList(ctx context.Context, store *eventlog.Store, clk *clock.Clock) (*List, error) {
	state, err := projection.Fold[State](ctx, store, Reducer{})
	if err != nil {
		return nil, err
	}
	return &List{store: store, clk: clk, state: state}, nil
}

// Check is eventlog.Validator: the store calls it inside the append
// transaction. "Would my reducer accept this?" is the whole invariant, so the
// rules live in exactly one place.
func (l *List) Check(r eventlog.Record) error {
	l.mu.RLock()
	trial := l.state
	l.mu.RUnlock()
	_, err := Reducer{}.Apply(trial, r)
	return err
}

// Project is eventlog.Projector: compute the next state, and hand back a
// closure the store runs only after the transaction commits. If the commit
// fails, l.state is untouched.
func (l *List) Project(r eventlog.Record) (func(), error) {
	l.mu.RLock()
	trial := l.state
	l.mu.RUnlock()
	next, err := Reducer{}.Apply(trial, r)
	if err != nil {
		return nil, err
	}
	return func() {
		l.mu.Lock()
		l.state = next
		l.mu.Unlock()
	}, nil
}

// Add appends a tasklist.Added. Note the shape of a command in this
// architecture: validate what you can locally, encode, append. Three lines of
// domain logic and no network.
func (l *List) Add(ctx context.Context, id, title string) error {
	if id == "" {
		return ErrEmptyID
	}
	return l.append(ctx, TypeAdded, Added{ID: id, Title: title})
}

// Complete appends a tasklist.Completed. Whether the task exists is decided by
// Check inside the transaction, against the state at that moment.
func (l *List) Complete(ctx context.Context, id string) error {
	if id == "" {
		return ErrEmptyID
	}
	return l.append(ctx, TypeCompleted, Completed{ID: id})
}

// append does not hold l.mu around Store.Append: Check and Project already
// guard their own access to l.state, and Store.Append calls them
// synchronously inside its own transaction lock (Task 4). Wrapping this call
// in l.mu too would only add a second lock with no correctness benefit.
func (l *List) append(ctx context.Context, typ string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("tasklist: encode %s: %w", typ, err)
	}
	_, err = l.store.Append(ctx, typ, payload, l.clk.Now(), l, l)
	return err
}

// Tasks returns a snapshot of projected state.
func (l *List) Tasks() State {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.state.Clone()
}
