package jepsenlite

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/crdt"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/node"
	syncpkg "github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync"
)

// snapshotEvery is how many events per SKU trigger a snapshot in simulated
// nodes. Deliberately small so the snapshot read path is exercised constantly.
const snapshotEvery = 4

// syncBatchSize keeps batches small so reorder and duplicate faults have
// several frames per session to act on.
const syncBatchSize = 3

// Cluster is a set of in-process nodes wired to the in-memory transport with
// the fault injector installed. Addresses on the transport are node IDs.
type Cluster struct {
	IDs     []clock.NodeID
	Nodes   map[clock.NodeID]*node.Node
	Logs    map[clock.NodeID]*CrashLog
	Servers map[clock.NodeID]*syncpkg.Server
	Trans   *syncpkg.MemoryTransport
	Inj     *Injector
	slow    clock.NodeID
}

// NewCluster builds a cluster under dir following sch's node list, clock skews,
// and slow-peer choice.
func NewCluster(dir string, sch Schedule, f Faults) (*Cluster, error) {
	c := &Cluster{
		Nodes:   map[clock.NodeID]*node.Node{},
		Logs:    map[clock.NodeID]*CrashLog{},
		Servers: map[clock.NodeID]*syncpkg.Server{},
		Trans:   syncpkg.NewMemoryTransport(),
		Inj:     NewInjector(rand.New(rand.NewSource(sch.Seed)), f),
		slow:    sch.Slow,
	}
	c.Trans.SetFilter(c.Inj.Filter)

	for _, id := range sch.Nodes {
		// A file-backed DB, because the crash fault reopens it.
		l, err := OpenCrashLog(filepath.Join(dir, string(id)+".db"))
		if err != nil {
			return nil, err
		}
		n, err := node.New(node.Config{
			ID:            id,
			Log:           l,
			Wall:          NewWall(1_700_000_000_000, sch.Skews[id], sch.Jumps[id], 25),
			SnapshotEvery: snapshotEvery,
		})
		if err != nil {
			return nil, fmt.Errorf("build node %q: %w", id, err)
		}
		srv := syncpkg.NewServer(n, syncBatchSize)
		c.Trans.Serve(string(id), srv)

		c.IDs = append(c.IDs, id)
		c.Nodes[id] = n
		c.Logs[id] = l
		c.Servers[id] = srv
	}
	return c, nil
}

// Close closes every log, returning the first error.
func (c *Cluster) Close() error {
	var first error
	for _, id := range c.IDs {
		if err := c.Logs[id].Close(); err != nil && first == nil {
			first = fmt.Errorf("close %q: %w", id, err)
		}
	}
	return first
}

// ApplyOp drives one scheduled op against its node. A returned error means the
// op was not acknowledged, so the no-lost-event property does not cover it.
func (c *Cluster) ApplyOp(ctx context.Context, op Op) (eventlog.EventID, error) {
	n, ok := c.Nodes[op.Node]
	if !ok {
		return eventlog.EventID{}, fmt.Errorf("unknown node %q", op.Node)
	}
	switch op.Kind {
	case OpReceive:
		return n.Receive(ctx, op.SKU, op.Qty)
	case OpPick:
		return n.Pick(ctx, op.SKU, op.Qty)
	case OpSetMeta:
		name, rp := op.Name, op.ReorderPoint
		return n.SetMeta(ctx, op.SKU, &name, &rp)
	case OpDelete:
		return n.Delete(ctx, op.SKU, op.Deleted)
	default:
		return eventlog.EventID{}, fmt.Errorf("unknown op kind %v", op.Kind)
	}
}

// syncSessionTimeout bounds one client session. The in-memory transport has no
// real network deadline: a dropped frame (a partition) otherwise blocks both
// ends of the pipe forever instead of failing the session, since nothing ever
// arrives on the channel it's waiting on. This is what makes "a partition
// should break the stream" (see SyncRound) actually true rather than a
// deadlock.
const syncSessionTimeout = 500 * time.Millisecond

// SyncPair runs one client session from a to b. A session failing during a
// partition is expected, so the error is returned for the caller to ignore.
func (c *Cluster) SyncPair(ctx context.Context, from, to clock.NodeID) error {
	ctx, cancel := context.WithTimeout(ctx, syncSessionTimeout)
	defer cancel()
	cl := syncpkg.NewClient(c.Nodes[from], c.Trans, syncBatchSize)
	if _, err := cl.SyncOnce(ctx, string(to)); err != nil {
		return fmt.Errorf("sync %q -> %q: %w", from, to, err)
	}
	return nil
}

// SyncRound runs a full mesh of sessions and returns how many succeeded. The
// slow peer's sessions run last: that is the "slow peer" fault, realised as
// scheduling rather than as a withheld ack (withholding an ack the peer is
// synchronously waiting for deadlocks instead of delaying).
func (c *Cluster) SyncRound(ctx context.Context) int {
	ok := 0
	for _, from := range c.syncOrder() {
		for _, to := range c.IDs {
			if from == to {
				continue
			}
			if err := c.SyncPair(ctx, from, to); err == nil {
				ok++
			}
		}
	}
	return ok
}

// syncOrder returns node IDs with the slow peer moved to the end.
func (c *Cluster) syncOrder() []clock.NodeID {
	if c.slow == "" {
		return c.IDs
	}
	out := make([]clock.NodeID, 0, len(c.IDs))
	for _, id := range c.IDs {
		if id != c.slow {
			out = append(out, id)
		}
	}
	return append(out, c.slow)
}

// Quiesce heals every partition and syncs full mesh until all nodes hold the
// same version vector. It returns an error rather than giving up silently: a
// harness that quietly stops syncing reports false convergence.
func (c *Cluster) Quiesce(ctx context.Context, maxRounds int) error {
	c.Inj.Heal()
	for round := 0; round < maxRounds; round++ {
		c.SyncRound(ctx)
		same, err := c.vectorsAgree(ctx)
		if err != nil {
			return err
		}
		if same {
			return nil
		}
	}
	return fmt.Errorf("no quiescence after %d rounds", maxRounds)
}

// vectorsAgree reports whether every node holds the same version vector.
func (c *Cluster) vectorsAgree(ctx context.Context) (bool, error) {
	var first eventlog.VersionVector
	for i, id := range c.IDs {
		vv, err := c.Nodes[id].VersionVector(ctx)
		if err != nil {
			return false, fmt.Errorf("version vector of %q: %w", id, err)
		}
		if i == 0 {
			first = vv
			continue
		}
		if !vv.Dominates(first) || !first.Dominates(vv) {
			return false, nil
		}
	}
	return true, nil
}

// Project returns a node's merged state for a SKU, via the snapshot read path.
func (c *Cluster) Project(ctx context.Context, id clock.NodeID, sku string) (*crdt.ItemState, error) {
	n, ok := c.Nodes[id]
	if !ok {
		return nil, fmt.Errorf("unknown node %q", id)
	}
	return n.Get(ctx, sku)
}

// Snapshot forces a snapshot of sku on one node, whatever its event count.
func (c *Cluster) Snapshot(ctx context.Context, id clock.NodeID, sku string) error {
	p := &crdt.Projector{Log: c.Logs[id], SnapshotEvery: 1}
	if err := p.MaybeSnapshot(ctx, sku); err != nil {
		return fmt.Errorf("snapshot %q/%q: %w", id, sku, err)
	}
	return nil
}

// AckedFloor returns the greatest version vector every *peer* of id has acked
// holding -- the only safe upper bound for Compact. Compacting past this
// deletes events a peer has never seen, which is unrecoverable.
func (c *Cluster) AckedFloor(ctx context.Context, id clock.NodeID) (eventlog.VersionVector, error) {
	floor := eventlog.VersionVector{}
	first := true
	for _, peer := range c.IDs {
		if peer == id {
			continue
		}
		vv, err := c.Nodes[peer].VersionVector(ctx)
		if err != nil {
			return nil, fmt.Errorf("version vector of peer %q: %w", peer, err)
		}
		if first {
			floor, first = vv.Clone(), false
			continue
		}
		for n, seq := range floor {
			if vv[n] < seq {
				floor[n] = vv[n]
			}
		}
		for n := range floor {
			if _, ok := vv[n]; !ok {
				delete(floor, n)
			}
		}
	}
	return floor, nil
}

// CheckConvergence asserts property 1: every node computes an identical
// ItemState for every SKU. Compared with ItemState.Equal, not by quantity --
// two states can agree on quantity and disagree on Name or Deleted.
func (c *Cluster) CheckConvergence(ctx context.Context, skus []string) error {
	for _, sku := range skus {
		var want *crdt.ItemState
		var wantID clock.NodeID
		for _, id := range c.IDs {
			got, err := c.Project(ctx, id, sku)
			if err != nil {
				return fmt.Errorf("project %q/%q: %w", id, sku, err)
			}
			if want == nil {
				want, wantID = got, id
				continue
			}
			if !got.Equal(want) {
				return fmt.Errorf(
					"convergence violated for %q: %q has quantity %d name %q deleted %v, "+
						"but %q has quantity %d name %q deleted %v",
					sku,
					wantID, want.Quantity(), want.Name.Value, want.Deleted.Value,
					id, got.Quantity(), got.Name.Value, got.Deleted.Value)
			}
		}
	}
	return nil
}

// CheckNoLostEvent asserts property 2: every acknowledged event is present in
// every replica's log. Only ops that returned a nil error are acknowledged --
// per the spec, a failed local Append carries no guarantee.
func (c *Cluster) CheckNoLostEvent(ctx context.Context, acked []eventlog.EventID) error {
	for _, id := range c.IDs {
		vv, err := c.Nodes[id].VersionVector(ctx)
		if err != nil {
			return fmt.Errorf("version vector of %q: %w", id, err)
		}
		for _, want := range acked {
			if !vv.Contains(want) {
				return fmt.Errorf(
					"lost event: %q/%d acknowledged but missing from %q (its vector holds %d)",
					want.NodeID, want.Seq, id, vv[want.NodeID])
			}
		}
	}
	return nil
}

// CheckOrderIndependence asserts property 3: folding a node's own events for a
// SKU in a random permutation reproduces that node's projection exactly.
//
// This check requires an uncompacted log. Compaction deletes events on purpose,
// so folding from scratch is no longer possible -- the property that must hold
// against a compacted log is convergence, checked separately.
func (c *Cluster) CheckOrderIndependence(ctx context.Context, rng *rand.Rand, skus []string) error {
	for _, id := range c.IDs {
		for _, sku := range skus {
			var events []eventlog.Event
			for e, err := range c.Logs[id].EventsForSKU(ctx, sku, eventlog.VersionVector{}) {
				if err != nil {
					return fmt.Errorf("read %q/%q: %w", id, sku, err)
				}
				events = append(events, e)
			}
			if len(events) == 0 {
				continue
			}
			want, err := c.Project(ctx, id, sku)
			if err != nil {
				return fmt.Errorf("project %q/%q: %w", id, sku, err)
			}
			rng.Shuffle(len(events), func(i, j int) {
				events[i], events[j] = events[j], events[i]
			})
			got := crdt.NewItemState()
			for _, e := range events {
				got.Apply(e)
			}
			if !got.Equal(want) {
				return fmt.Errorf(
					"order dependence for %q/%q: permuted fold gives quantity %d name %q, "+
						"projection gives quantity %d name %q",
					id, sku, got.Quantity(), got.Name.Value,
					want.Quantity(), want.Name.Value)
			}
		}
	}
	return nil
}

// Result is a run report. It is what `lab sim` prints.
type Result struct {
	Seed       int64
	Faults     []string
	Ops        int
	Acked      int
	Failed     int
	Rounds     int
	Crashes    int
	Stats      Stats
	Quantities map[string]int64
	Anomalies  []string
}

// String renders the convergence report.
func (r Result) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed %d  faults [%s]\n", r.Seed, strings.Join(r.Faults, ","))
	fmt.Fprintf(&b, "ops %d  acked %d  failed %d  sync rounds %d\n",
		r.Ops, r.Acked, r.Failed, r.Rounds)
	fmt.Fprintf(&b, "injected: dropped %d  duplicated %d  reordered %d  crashes %d\n",
		r.Stats.Dropped, r.Stats.Duplicated, r.Stats.Reordered, r.Crashes)
	b.WriteString("converged state:\n")
	skus := make([]string, 0, len(r.Quantities))
	for sku := range r.Quantities {
		skus = append(skus, sku)
	}
	sort.Strings(skus)
	for _, sku := range skus {
		fmt.Fprintf(&b, "  %s quantity %d\n", sku, r.Quantities[sku])
	}
	if len(r.Anomalies) > 0 {
		b.WriteString("anomalies (accepted, not rejected -- see README):\n")
		for _, a := range r.Anomalies {
			fmt.Fprintf(&b, "  %s\n", a)
		}
	}
	b.WriteString("properties: convergence OK, no lost event OK, order independence OK\n")
	return b.String()
}

// Run executes a whole schedule: ops with faults firing, then quiescence, then
// the three properties. Every returned error names the seed, so a red run is a
// one-command local reproduction.
func Run(ctx context.Context, dir string, sch Schedule, f Faults) (Result, error) {
	c, err := NewCluster(dir, sch, f)
	if err != nil {
		return Result{}, fmt.Errorf("seed %d: %w", sch.Seed, err)
	}
	defer func() { _ = c.Close() }()

	res := Result{
		Seed:       sch.Seed,
		Faults:     f.Names(),
		Ops:        len(sch.Ops),
		Quantities: map[string]int64{},
	}
	var acked []eventlog.EventID
	fi := 0
	for i, op := range sch.Ops {
		for fi < len(sch.Faults) && sch.Faults[fi].At == i {
			c.fire(sch.Faults[fi])
			fi++
		}
		id, err := c.ApplyOp(ctx, op)
		if err != nil {
			// Not acknowledged, so no guarantee attaches to it. This is the
			// crash fault's normal outcome, not a failure.
			res.Failed++
			continue
		}
		res.Acked++
		acked = append(acked, id)
		if sch.SyncEvery > 0 && i%sch.SyncEvery == sch.SyncEvery-1 {
			c.SyncRound(ctx)
			res.Rounds++
		}
	}

	quiesceRounds := 4 * (len(c.IDs) + 1)
	if err := c.Quiesce(ctx, quiesceRounds); err != nil {
		return res, fmt.Errorf("seed %d: %w", sch.Seed, err)
	}
	res.Rounds += quiesceRounds

	for _, id := range c.IDs {
		res.Crashes += c.Logs[id].Crashes()
	}
	res.Stats = c.Inj.Stats()

	if err := c.CheckConvergence(ctx, sch.SKUs); err != nil {
		return res, fmt.Errorf("seed %d: %w", sch.Seed, err)
	}
	if err := c.CheckNoLostEvent(ctx, acked); err != nil {
		return res, fmt.Errorf("seed %d: %w", sch.Seed, err)
	}
	orderRNG := rand.New(rand.NewSource(sch.Seed ^ 0x5eed))
	if err := c.CheckOrderIndependence(ctx, orderRNG, sch.SKUs); err != nil {
		return res, fmt.Errorf("seed %d: %w", sch.Seed, err)
	}

	for _, sku := range sch.SKUs {
		st, err := c.Project(ctx, c.IDs[0], sku)
		if err != nil {
			return res, fmt.Errorf("seed %d: project %q: %w", sch.Seed, sku, err)
		}
		q := st.Quantity()
		res.Quantities[sku] = q
		if q < 0 {
			res.Anomalies = append(res.Anomalies,
				fmt.Sprintf("%s quantity %d", sku, q))
		}
	}
	return res, nil
}

// fire applies one scheduled fault event.
func (c *Cluster) fire(fe FaultEvent) {
	switch fe.Kind {
	case "partition":
		c.Inj.Partition(fe.A, fe.B)
	case "asym":
		c.Inj.PartitionOneWay(fe.A, fe.B)
	case "heal":
		c.Inj.Heal()
	case "crash":
		if l, ok := c.Logs[fe.A]; ok {
			l.Arm()
		}
	}
}
