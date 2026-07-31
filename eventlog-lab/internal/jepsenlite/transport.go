// Package jepsenlite is a deterministic fault-injection simulator for the
// eventlog-lab replication stack. It is the actual product of this project:
// everything else exists so that this package can try to break it.
//
// Every random decision comes from a single seeded *rand.Rand, so a failing run
// is reproducible from its seed alone.
package jepsenlite

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand"
	"sort"
	"strings"
	stdsync "sync"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// Faults enumerates the seven fault kinds from the spec. Each is independently
// togglable and they compose freely -- the zero value injects nothing.
type Faults struct {
	Partition      bool // transport drops frames between a node pair
	AsymPartition  bool // transport drops one direction only
	ClockSkew      bool // per-node wall offset, including backwards jumps
	Duplicate      bool // transport re-sends a prior Events frame
	Reorder        bool // transport buffers and permutes Events frames
	CrashMidAppend bool // log aborts the transaction, then reopens the DB
	SlowPeer       bool // one node's acks land after the next batch
}

// faultFields maps the canonical CLI name of each fault to its field, so
// parsing, naming, and iteration all share one table (DRY).
var faultFields = []struct {
	name string
	get  func(*Faults) *bool
}{
	{"asym", func(f *Faults) *bool { return &f.AsymPartition }},
	{"crash", func(f *Faults) *bool { return &f.CrashMidAppend }},
	{"dup", func(f *Faults) *bool { return &f.Duplicate }},
	{"partition", func(f *Faults) *bool { return &f.Partition }},
	{"reorder", func(f *Faults) *bool { return &f.Reorder }},
	{"skew", func(f *Faults) *bool { return &f.ClockSkew }},
	{"slow", func(f *Faults) *bool { return &f.SlowPeer }},
}

// ParseFaults parses a comma-separated fault list. "" means no faults, "all"
// means every fault.
func ParseFaults(csv string) (Faults, error) {
	var f Faults
	for _, raw := range strings.Split(csv, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if name == "all" {
			for _, ff := range faultFields {
				*ff.get(&f) = true
			}
			continue
		}
		matched := false
		for _, ff := range faultFields {
			if ff.name == name {
				*ff.get(&f) = true
				matched = true
				break
			}
		}
		if !matched {
			known := make([]string, 0, len(faultFields))
			for _, ff := range faultFields {
				known = append(known, ff.name)
			}
			return Faults{}, fmt.Errorf("unknown fault %q; known: %s, all",
				name, strings.Join(known, ", "))
		}
	}
	return f, nil
}

// Names returns the sorted canonical names of the enabled faults.
func (f Faults) Names() []string {
	out := []string{}
	for _, ff := range faultFields {
		if *ff.get(&f) {
			out = append(out, ff.name)
		}
	}
	sort.Strings(out)
	return out
}

// Stats counts what the transport seam actually did during a run.
type Stats struct {
	Dropped    int
	Duplicated int
	Reordered  int
}

type link struct{ from, to clock.NodeID }

// Injector is the transport-seam fault source. Install it with
// sync.MemoryTransport.SetFilter(in.Filter); MemoryTransport addresses are node
// IDs verbatim, so Filter's from/to arguments are node IDs.
//
// Only Events frames are duplicated or reordered. Duplicating a Hello or
// withholding an Ack desynchronises the half-duplex session into a deadlock,
// which would be a harness bug rather than a discovered system bug. A partition
// drops every frame kind, because that is what a real partition does.
type Injector struct {
	mu      stdsync.Mutex
	rng     *rand.Rand
	faults  Faults
	blocked map[link]bool
	prior   map[link]any
	held    map[link]any
	stats   Stats
}

// NewInjector returns an Injector drawing every decision from rng.
func NewInjector(rng *rand.Rand, f Faults) *Injector {
	return &Injector{
		rng:     rng,
		faults:  f,
		blocked: map[link]bool{},
		prior:   map[link]any{},
		held:    map[link]any{},
	}
}

// Partition drops frames in both directions between a and b. It is a no-op
// unless Faults.Partition is set.
func (in *Injector) Partition(a, b clock.NodeID) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if !in.faults.Partition {
		return
	}
	in.blocked[link{a, b}] = true
	in.blocked[link{b, a}] = true
}

// PartitionOneWay drops frames from -> to only. It is a no-op unless
// Faults.AsymPartition is set.
func (in *Injector) PartitionOneWay(from, to clock.NodeID) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if !in.faults.AsymPartition {
		return
	}
	in.blocked[link{from, to}] = true
}

// Heal removes every partition. Quiescence requires a healed network.
func (in *Injector) Heal() {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.blocked = map[link]bool{}
}

// Stats returns a snapshot of the injection counters.
func (in *Injector) Stats() Stats {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.stats
}

// Filter implements sync.FrameFilter: it maps one outbound frame to the frames
// actually delivered. Returning an empty slice drops (or holds) the frame.
func (in *Injector) Filter(from, to clock.NodeID, frame any) []any {
	in.mu.Lock()
	defer in.mu.Unlock()

	l := link{from, to}
	if in.blocked[l] {
		in.stats.Dropped++
		return nil
	}

	out := []any{frame}

	if in.faults.Duplicate && isEvents(frame) && in.rng.Intn(3) == 0 {
		if p, ok := in.prior[l]; ok {
			out = append(out, p)
			in.stats.Duplicated++
		}
	}
	if isEvents(frame) {
		in.prior[l] = frame
	}

	if in.faults.Reorder {
		// Release any held frame first, regardless of what kind of frame
		// just arrived. The protocol always sends another frame in the same
		// direction after an Events batch (a further batch, or -- most
		// commonly for the *final* batch -- the terminating Ack). An earlier
		// version of this check only released the held frame when the new
		// frame was itself an Events frame, so a held frame followed by an
		// Ack passed the Ack through and left the held Events batch buffered
		// forever: a silently dropped batch, not a reorder. Checking
		// unconditionally here is what makes the held frame release in the
		// same turn regardless of which frame follows it.
		if h, ok := in.held[l]; ok {
			delete(in.held, l)
			out = append(out, h)
		} else if isEvents(frame) && in.rng.Intn(3) == 0 {
			in.held[l] = frame
			in.stats.Reordered++
			return nil
		}
	}
	return out
}

// isEvents reports whether frame carries an Events batch, for either direction.
func isEvents(frame any) bool {
	switch f := frame.(type) {
	case *syncpb.ClientFrame:
		return f.GetEvents() != nil
	case *syncpb.ServerFrame:
		return f.GetEvents() != nil
	default:
		return false
	}
}

// ErrCrash is returned by an armed CrashLog's AppendLocal.
var ErrCrash = errors.New("jepsenlite: injected crash mid-append")

// CrashLog wraps a SQLite log and can abort exactly one local append, then
// reopen the database -- the storage-seam model of a process dying between Seq
// allocation and insert. Because eventlog allocates Seq inside the insert
// transaction, the reopened log must show no gap and no partial row.
//
// It implements crdt.SQLLog, so a node cannot tell it apart from a real log.
type CrashLog struct {
	mu      stdsync.Mutex
	dsn     string
	inner   *eventlog.SQLiteLog
	armed   bool
	crashes int
}

// OpenCrashLog opens dsn as a SQLite log wrapped for crash injection. dsn must
// be a file path: an in-memory database cannot survive the reopen.
func OpenCrashLog(dsn string) (*CrashLog, error) {
	l, err := eventlog.OpenSQLite(dsn)
	if err != nil {
		return nil, fmt.Errorf("open crash log %q: %w", dsn, err)
	}
	return &CrashLog{dsn: dsn, inner: l}, nil
}

// Arm makes the next AppendLocal crash. One-shot.
func (c *CrashLog) Arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
}

// Crashes returns how many injected crashes have fired.
func (c *CrashLog) Crashes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashes
}

// AppendLocal aborts and reopens the database when armed, otherwise delegates.
func (c *CrashLog) AppendLocal(ctx context.Context, mint func(eventlog.Seq) eventlog.Event) (eventlog.Event, error) {
	c.mu.Lock()
	if c.armed {
		c.armed = false
		c.crashes++
		// Never reached the insert: close and reopen, exactly as a restarted
		// process would.
		closeErr := c.inner.Close()
		l, err := eventlog.OpenSQLite(c.dsn)
		c.mu.Unlock()
		if err != nil {
			return eventlog.Event{}, fmt.Errorf("reopen after injected crash: %w", err)
		}
		c.mu.Lock()
		c.inner = l
		c.mu.Unlock()
		if closeErr != nil {
			return eventlog.Event{}, fmt.Errorf("%w (close: %w)", ErrCrash, closeErr)
		}
		return eventlog.Event{}, ErrCrash
	}
	inner := c.inner
	c.mu.Unlock()
	return inner.AppendLocal(ctx, mint)
}

// current returns the live inner log under the lock.
func (c *CrashLog) current() *eventlog.SQLiteLog {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner
}

// Append delegates to the live log.
func (c *CrashLog) Append(ctx context.Context, e eventlog.Event) error {
	return c.current().Append(ctx, e)
}

// Since delegates to the live log.
func (c *CrashLog) Since(ctx context.Context, vv eventlog.VersionVector) iter.Seq2[eventlog.Event, error] {
	return c.current().Since(ctx, vv)
}

// EventsForSKU delegates to the live log.
func (c *CrashLog) EventsForSKU(ctx context.Context, sku string, after eventlog.VersionVector) iter.Seq2[eventlog.Event, error] {
	return c.current().EventsForSKU(ctx, sku, after)
}

// CountForSKU delegates to the live log.
func (c *CrashLog) CountForSKU(ctx context.Context, sku string) (int, error) {
	return c.current().CountForSKU(ctx, sku)
}

// VersionVector delegates to the live log.
func (c *CrashLog) VersionVector(ctx context.Context) (eventlog.VersionVector, error) {
	return c.current().VersionVector(ctx)
}

// LoadSnapshot delegates to the live log.
func (c *CrashLog) LoadSnapshot(ctx context.Context, sku string) ([]byte, eventlog.VersionVector, error) {
	return c.current().LoadSnapshot(ctx, sku)
}

// SaveSnapshot delegates to the live log.
func (c *CrashLog) SaveSnapshot(ctx context.Context, sku string, state []byte, covers eventlog.VersionVector) error {
	return c.current().SaveSnapshot(ctx, sku, state, covers)
}

// Cursor delegates to the live log.
func (c *CrashLog) Cursor(ctx context.Context, peer clock.NodeID) (eventlog.Seq, error) {
	return c.current().Cursor(ctx, peer)
}

// SetCursor delegates to the live log.
func (c *CrashLog) SetCursor(ctx context.Context, peer clock.NodeID, last eventlog.Seq) error {
	return c.current().SetCursor(ctx, peer, last)
}

// Compact delegates to the live log.
func (c *CrashLog) Compact(ctx context.Context, upTo eventlog.VersionVector) error {
	return c.current().Compact(ctx, upTo)
}

// Close delegates to the live log.
func (c *CrashLog) Close() error {
	return c.current().Close()
}

// CrashLog must be substitutable for a real log wherever a node expects one.
var _ crdt.SQLLog = (*CrashLog)(nil)

// NewWall returns a deterministic clock.WallFunc for the clock seam. It starts
// at start+skew and advances one millisecond per call. When jumpEvery > 0, every
// jumpEvery-th call jumps backwards by jumpBy milliseconds -- the NTP-correction
// fault. clock.Clock.Now must stay monotonic across such a jump.
func NewWall(start, skew int64, jumpEvery int, jumpBy int64) clock.WallFunc {
	now := start + skew
	calls := 0
	return func() int64 {
		calls++
		if jumpEvery > 0 && calls%jumpEvery == 0 {
			now -= jumpBy
		}
		v := now
		now++
		return v
	}
}
