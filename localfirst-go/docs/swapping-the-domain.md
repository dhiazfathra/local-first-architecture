# Swapping the domain

This repo claims its engine is domain-agnostic. Here is the claim cashed in:
inventory replaced, end to end, by a domain that shares nothing with it — no
quantities, no arithmetic, string-keyed entities, and an invariant about
existence rather than about a number.

Everything below is real code. It lives at
[`examples/tasklist`](../examples/tasklist) and its tests run in CI, so this
guide cannot rot into a description of something that no longer compiles.

## The shape of the job

Three things to write. Nothing else.

| You write | Engine interface | Where it is called |
| --- | --- | --- |
| A reducer | `projection.Reducer[S]` | `projection.Fold` on startup, and on every replay |
| A validator/projector | `eventlog.Validator`, `eventlog.Projector` | inside `eventlog.Store.Append`'s transaction |
| A command encoder | none — it is yours | your own command methods |

Files you do **not** touch: `clock/`, `eventlog/`, `projection/`, `sync/`, and
the proto definitions. Verify that claim rather than trusting it —
`git diff --stat` after the swap should show no changes in any of them.

## Step 1 — name the events

Inventory had three. The task list has two, and they carry strings instead of
integers:

```go
const (
	TypeAdded     = "tasklist.Added"
	TypeCompleted = "tasklist.Completed"
)

type Added struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type Completed struct {
	ID string `json:"id"`
}
```

The `Type` string is the only thing the engine sees, and it treats it as an
opaque label. `Payload` is whatever bytes you put there; this example uses JSON
because it is legible in `sqlite3` output.

## Step 2 — define the projected state

```go
type Task struct {
	Title string
	Done  bool
}

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
```

Note that `State` is a completely different type from `domain.State`
(`map[domain.Key]int64`). The engine never names either of them: `Fold[S any]`
is generic precisely so that it cannot.

## Step 3 — write the reducer

This is the whole reading seam. It is a pure function — same state plus same
record always gives the same answer — which is what makes replay deterministic
and makes the table-driven test in
[`tasklist_test.go`](../examples/tasklist/tasklist_test.go) possible.

```go
type Reducer struct{}

func (Reducer) Zero() State { return State{} }

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
```

Two conventions worth copying:

- The `default` branch returns `eventlog.ErrUnknownType` and **never skips**.
  A record you cannot interpret means your binary is older than the fact; a
  loud failure is the only honest response. See
  [ADR 0005](adr/0005-domain-agnostic-engine.md).
- Every rule of the domain lives here, including rules you might have been
  tempted to put in the command handler. That pays off in the next step.

## Step 4 — reuse the reducer as the validator

The engine has to ask "is this append allowed?" without knowing what the answer
depends on. `eventlog.Validator` is that question, and it is called inside the
append transaction against current projected state. Because every rule is in
`Apply`, the validator is two lines:

```go
// Check is eventlog.Validator: the store calls it inside the append
// transaction.
func (l *List) Check(r eventlog.Record) error {
	l.mu.RLock()
	trial := l.state
	l.mu.RUnlock()
	_, err := Reducer{}.Apply(trial, r)
	return err
}

// Project is eventlog.Projector: compute the next state, and hand back a
// closure the store runs only after the transaction commits.
func (l *List) Project(r eventlog.Record) (func(), error) {
	l.mu.RLock()
	trial := l.state
	l.mu.RUnlock()
	next, err := Reducer{}.Apply(trial, r)
	if err != nil {
		return nil, err
	}
	l.clk.Observe(r.Clock)
	return func() {
		l.mu.Lock()
		l.state = next
		l.mu.Unlock()
	}, nil
}
```

"Would my reducer accept this?" *is* the invariant. `domain.Inventory` does the
same thing for negative balances. The deferred `commit func()` is why a
transaction that fails to commit cannot leave the in-memory projection ahead of
the log.

`Project` is also the required place to call `l.clk.Observe(r.Clock)` — it runs
for every record the node ever accepts, local or synced, and is what makes
ADR 0003's clock-monotonicity promise hold for this domain too (see
`domain.Inventory.Project` for the same pattern).

Note the lock is `l.mu`, held only inside `Check` and `Project` themselves —
not around the call to `Store.Append`. A background `eventlog.Merge` (a record
synced in from another node) calls `Check`/`Project` through this same seam,
concurrently with a local command, so the state access has to defend itself
rather than rely on whatever lock the caller happens to hold.

## Step 5 — write the command encoder

There is no engine interface here; commands are yours. The pattern is: validate
what is purely local, encode, append.

```go
func NewList(ctx context.Context, store *eventlog.Store, clk *clock.Clock) (*List, error) {
	state, err := projection.Fold[State](ctx, store, Reducer{})
	if err != nil {
		return nil, err
	}
	return &List{store: store, clk: clk, state: state}, nil
}

func (l *List) Add(ctx context.Context, id, title string) error {
	if id == "" {
		return ErrEmptyID
	}
	return l.append(ctx, TypeAdded, Added{ID: id, Title: title})
}

func (l *List) Complete(ctx context.Context, id string) error {
	if id == "" {
		return ErrEmptyID
	}
	return l.append(ctx, TypeCompleted, Completed{ID: id})
}

func (l *List) append(ctx context.Context, typ string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("tasklist: encode %s: %w", typ, err)
	}
	_, err = l.store.Append(ctx, typ, payload, l.clk.Now(), l, l)
	return err
}
```

`store.Append(ctx, typ, payload, ts, validator, projector)` is the only engine
call a command makes. Passing `l` twice is not a trick — `List` satisfies both
interfaces, which is the natural result of keeping the rules in one place. Note
`append` does not take `l.mu` itself: `Check` and `Project` already guard their
own reads and writes of `l.state`, so a second lock around `Append` would add
nothing but a chance to deadlock against a concurrent `Merge`.

## Step 6 — the swap is done

That is everything. To run this domain instead of inventory in a node, replace
`domain.NewInventory` with `tasklist.NewList` in `cmd/node/node.go` and swap the
`transport/grpc` service for one that exposes `Add`/`Complete` instead of
`Receive`/`Issue`/`Move`. `sync`, `eventlog`, `clock`, `projection`, the
`Frame` protocol and `central`'s replication all work untouched, because none of
them ever looked at a payload.

Two consequences worth stating plainly:

- **Sync needs no changes at all.** `sync.Exchange` moves `eventlog.Record`s. A
  task list replicates and converges under exactly the same protocol as
  inventory, with the same idempotent append and the same resumption behaviour.
- **`central.PGStore.GlobalSum` does not carry over**, and that is correct.
  Summing balances is an inventory question. A task list would ask something
  else — "how many tasks are open across all nodes?" — and that query, like the
  reducer, belongs to the domain.

## What still needs your judgement

The engine is generic; ownership is not. This architecture removes conflict by
partitioning write authority ([ADR 0001](adr/0001-per-node-ownership.md)), and
that partition has to mean something in *your* domain. In inventory it is
locations. In this task list, the natural rule is that a node may only complete
tasks it created — that is the version to build if you take this further. Skip
that question and you have a system where two nodes can complete the same task
offline, which per-node ownership does not cover and
[limitations](limitations.md) rules out.
