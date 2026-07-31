package jepsenlite

import (
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

func eventsFrame() *syncpb.ClientFrame {
	return &syncpb.ClientFrame{Body: &syncpb.ClientFrame_Events{
		Events: &syncpb.Events{Events: []*syncpb.Event{{NodeId: "A", Seq: 1}}},
	}}
}

func helloFrame() *syncpb.ClientFrame {
	return &syncpb.ClientFrame{Body: &syncpb.ClientFrame_Hello{
		Hello: &syncpb.Hello{NodeId: "A"},
	}}
}

func ackFrame() *syncpb.ClientFrame {
	return &syncpb.ClientFrame{Body: &syncpb.ClientFrame_Ack{
		Ack: &syncpb.Ack{},
	}}
}

func TestParseFaults(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    Faults
		wantErr bool
	}{
		{name: "empty is no faults", in: "", want: Faults{}},
		{name: "single", in: "partition", want: Faults{Partition: true}},
		{
			name: "composed with spaces",
			in:   "partition, skew ,dup",
			want: Faults{Partition: true, ClockSkew: true, Duplicate: true},
		},
		{
			name: "all enables every fault",
			in:   "all",
			want: Faults{
				Partition: true, AsymPartition: true, ClockSkew: true,
				Duplicate: true, Reorder: true, CrashMidAppend: true, SlowPeer: true,
			},
		},
		{name: "unknown name", in: "gremlins", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFaults(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseFaults(%q) error = nil, want error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseFaults(%q) error = %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseFaults(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestFaultsNames(t *testing.T) {
	got := Faults{Partition: true, Reorder: true}.Names()
	want := []string{"partition", "reorder"}
	if len(got) != len(want) {
		t.Fatalf("Names() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Names() = %v, want %v", got, want)
		}
	}
}

func TestInjectorPartitionDropsBothDirections(t *testing.T) {
	in := NewInjector(rand.New(rand.NewSource(1)), Faults{Partition: true})
	in.Partition("A", "B")

	if out := in.Filter("A", "B", helloFrame()); out != nil {
		t.Errorf("Filter(A->B) = %v, want nil (dropped)", out)
	}
	if out := in.Filter("B", "A", helloFrame()); out != nil {
		t.Errorf("Filter(B->A) = %v, want nil (dropped)", out)
	}
	if out := in.Filter("A", "C", helloFrame()); len(out) != 1 {
		t.Errorf("Filter(A->C) len = %d, want 1 (unaffected pair)", len(out))
	}
	if got := in.Stats().Dropped; got != 2 {
		t.Errorf("Stats().Dropped = %d, want 2", got)
	}

	in.Heal()
	if out := in.Filter("A", "B", helloFrame()); len(out) != 1 {
		t.Errorf("after Heal, Filter(A->B) len = %d, want 1", len(out))
	}
}

func TestInjectorAsymmetricPartitionDropsOneDirection(t *testing.T) {
	in := NewInjector(rand.New(rand.NewSource(2)), Faults{AsymPartition: true})
	in.PartitionOneWay("A", "B")

	if out := in.Filter("A", "B", helloFrame()); out != nil {
		t.Errorf("Filter(A->B) = %v, want nil (dropped)", out)
	}
	if out := in.Filter("B", "A", helloFrame()); len(out) != 1 {
		t.Errorf("Filter(B->A) len = %d, want 1 (reverse must survive)", len(out))
	}
}

func TestInjectorDuplicateOnlyDuplicatesEventsFrames(t *testing.T) {
	in := NewInjector(rand.New(rand.NewSource(3)), Faults{Duplicate: true})

	// Prime the prior-frame memory, then drive frames until a duplicate appears.
	dupSeen := false
	for i := 0; i < 200 && !dupSeen; i++ {
		if len(in.Filter("A", "B", eventsFrame())) == 2 {
			dupSeen = true
		}
	}
	if !dupSeen {
		t.Fatalf("no duplicate produced in 200 Events frames; rng wiring is wrong")
	}
	if got := in.Stats().Duplicated; got == 0 {
		t.Errorf("Stats().Duplicated = 0, want > 0")
	}
	for i := 0; i < 200; i++ {
		if got := len(in.Filter("A", "B", helloFrame())); got != 1 {
			t.Fatalf("Hello frame duplicated (len = %d); only Events may duplicate", got)
		}
	}
}

func TestInjectorReorderHoldsOneEventsFrameThenReleasesBoth(t *testing.T) {
	in := NewInjector(rand.New(rand.NewSource(4)), Faults{Reorder: true})

	held := false
	for i := 0; i < 200; i++ {
		out := in.Filter("A", "B", eventsFrame())
		if len(out) == 0 {
			held = true
			// The very next frame must flush the held one plus itself.
			next := in.Filter("A", "B", eventsFrame())
			if len(next) != 2 {
				t.Fatalf("after holding, next Filter len = %d, want 2", len(next))
			}
			break
		}
	}
	if !held {
		t.Fatalf("reorder never held a frame in 200 attempts; rng wiring is wrong")
	}
	if got := in.Stats().Reordered; got == 0 {
		t.Errorf("Stats().Reordered = 0, want > 0")
	}
}

// TestInjectorReorderFlushesHeldFrameBeforeAck is a regression test: a held
// Events frame used to only flush when the NEXT frame was also Events. In
// practice the frame after a session's final Events batch is an Ack, and the
// held batch was silently buffered forever -- an effective, permanent frame
// drop rather than a reorder. The held frame must flush no matter what kind
// of frame follows it.
func TestInjectorReorderFlushesHeldFrameBeforeAck(t *testing.T) {
	in := NewInjector(rand.New(rand.NewSource(4)), Faults{Reorder: true})

	held := false
	for i := 0; i < 200; i++ {
		if len(in.Filter("A", "B", eventsFrame())) == 0 {
			held = true
			break
		}
	}
	if !held {
		t.Fatalf("reorder never held a frame in 200 attempts; rng wiring is wrong")
	}
	next := in.Filter("A", "B", ackFrame())
	if len(next) != 2 {
		t.Fatalf("Filter after Ack following a held Events frame = %d, want 2 (held frame + the Ack) -- held frame was dropped, not reordered", len(next))
	}
}

func TestInjectorReorderNeverHoldsNonEventsFrames(t *testing.T) {
	in := NewInjector(rand.New(rand.NewSource(5)), Faults{Reorder: true})
	for i := 0; i < 200; i++ {
		if got := len(in.Filter("A", "B", helloFrame())); got != 1 {
			t.Fatalf("Hello frame held (len = %d); would deadlock a parked Recv", got)
		}
	}
}

func TestInjectorDisabledFaultsArePassThrough(t *testing.T) {
	in := NewInjector(rand.New(rand.NewSource(6)), Faults{})
	in.Partition("A", "B")
	for i := 0; i < 100; i++ {
		if got := len(in.Filter("A", "B", eventsFrame())); got != 1 {
			t.Fatalf("Filter len = %d with all faults off, want 1", got)
		}
	}
	if in.Stats() != (Stats{}) {
		t.Errorf("Stats() = %+v, want zero", in.Stats())
	}
}

func TestInjectorServerFramesAreClassifiedToo(t *testing.T) {
	in := NewInjector(rand.New(rand.NewSource(7)), Faults{Duplicate: true})
	sf := &syncpb.ServerFrame{Body: &syncpb.ServerFrame_Events{
		Events: &syncpb.Events{Events: []*syncpb.Event{{NodeId: "B", Seq: 1}}},
	}}
	dupSeen := false
	for i := 0; i < 200 && !dupSeen; i++ {
		if len(in.Filter("B", "A", sf)) == 2 {
			dupSeen = true
		}
	}
	if !dupSeen {
		t.Fatalf("ServerFrame Events never duplicated; isEvents does not handle ServerFrame")
	}
}

func TestCrashLogAbortsAppendLeavesNoGapAndReopens(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "crash.db")
	c, err := OpenCrashLog(dsn)
	if err != nil {
		t.Fatalf("OpenCrashLog() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	mint := func(s eventlog.Seq) eventlog.Event {
		return eventlog.Event{
			ID:    eventlog.EventID{NodeID: "A", Seq: s},
			HLC:   clock.HLC{Wall: 1, NodeID: "A"},
			SKU:   "SKU-1",
			Kind:  eventlog.KindQuantityDelta,
			Delta: 1,
		}
	}
	if _, err := c.AppendLocal(ctx, mint); err != nil {
		t.Fatalf("AppendLocal() error = %v", err)
	}

	c.Arm()
	if _, err := c.AppendLocal(ctx, mint); !errors.Is(err, ErrCrash) {
		t.Fatalf("armed AppendLocal() error = %v, want ErrCrash", err)
	}
	if got := c.Crashes(); got != 1 {
		t.Errorf("Crashes() = %d, want 1", got)
	}

	// The reopened database must show the first event and nothing else -- no
	// burned sequence number, no half-written row.
	vv, err := c.VersionVector(ctx)
	if err != nil {
		t.Fatalf("VersionVector() after crash error = %v", err)
	}
	if vv["A"] != 1 {
		t.Errorf("VersionVector()[A] = %d, want 1 (no gap after crash)", vv["A"])
	}

	// The log is usable again and the next Seq is 2, not 3.
	e, err := c.AppendLocal(ctx, mint)
	if err != nil {
		t.Fatalf("AppendLocal() after crash error = %v", err)
	}
	if e.ID.Seq != 2 {
		t.Errorf("Seq after crash = %d, want 2", e.ID.Seq)
	}
}

func TestCrashLogArmIsOneShot(t *testing.T) {
	ctx := context.Background()
	c, err := OpenCrashLog(filepath.Join(t.TempDir(), "oneshot.db"))
	if err != nil {
		t.Fatalf("OpenCrashLog() error = %v", err)
	}
	defer func() { _ = c.Close() }()
	mint := func(s eventlog.Seq) eventlog.Event {
		return eventlog.Event{
			ID:    eventlog.EventID{NodeID: "A", Seq: s},
			HLC:   clock.HLC{Wall: 1, NodeID: "A"},
			SKU:   "SKU-1",
			Kind:  eventlog.KindQuantityDelta,
			Delta: 1,
		}
	}
	c.Arm()
	if _, err := c.AppendLocal(ctx, mint); !errors.Is(err, ErrCrash) {
		t.Fatalf("first armed AppendLocal() error = %v, want ErrCrash", err)
	}
	if _, err := c.AppendLocal(ctx, mint); err != nil {
		t.Fatalf("second AppendLocal() error = %v, want nil (Arm is one-shot)", err)
	}
}

func TestCrashLogDelegatesEverySQLLogMethod(t *testing.T) {
	ctx := context.Background()
	c, err := OpenCrashLog(filepath.Join(t.TempDir(), "delegate.db"))
	if err != nil {
		t.Fatalf("OpenCrashLog() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	e := eventlog.Event{
		ID:    eventlog.EventID{NodeID: "B", Seq: 1},
		HLC:   clock.HLC{Wall: 5, NodeID: "B"},
		SKU:   "SKU-9",
		Kind:  eventlog.KindQuantityDelta,
		Delta: 4,
	}
	if err := c.Append(ctx, e); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	n := 0
	for range c.Since(ctx, eventlog.VersionVector{}) {
		n++
	}
	if n != 1 {
		t.Errorf("Since() yielded %d events, want 1", n)
	}
	n = 0
	for range c.EventsForSKU(ctx, "SKU-9", eventlog.VersionVector{}) {
		n++
	}
	if n != 1 {
		t.Errorf("EventsForSKU() yielded %d events, want 1", n)
	}
	if got, err := c.CountForSKU(ctx, "SKU-9"); err != nil || got != 1 {
		t.Errorf("CountForSKU() = %d, %v, want 1, nil", got, err)
	}
	if err := c.SaveSnapshot(ctx, "SKU-9", []byte(`{}`), eventlog.VersionVector{"B": 1}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	state, covers, err := c.LoadSnapshot(ctx, "SKU-9")
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if string(state) != `{}` || covers["B"] != 1 {
		t.Errorf("LoadSnapshot() = %q, %v, want \"{}\", {B:1}", state, covers)
	}
	if err := c.SetCursor(ctx, "A", 7); err != nil {
		t.Fatalf("SetCursor() error = %v", err)
	}
	if got, err := c.Cursor(ctx, "A"); err != nil || got != 7 {
		t.Errorf("Cursor() = %d, %v, want 7, nil", got, err)
	}
	if err := c.Compact(ctx, eventlog.VersionVector{"B": 1}); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
}

func TestNewWallSkewAndBackwardsJumps(t *testing.T) {
	tests := []struct {
		name      string
		start     int64
		skew      int64
		jumpEvery int
		jumpBy    int64
		calls     int
		want      []int64
	}{
		{
			name:  "no skew advances one milli per call",
			start: 100, calls: 3,
			want: []int64{100, 101, 102},
		},
		{
			name:  "positive skew is a constant offset",
			start: 100, skew: 5000, calls: 2,
			want: []int64{5100, 5101},
		},
		{
			name:  "negative skew is a constant offset",
			start: 100, skew: -50, calls: 2,
			want: []int64{50, 51},
		},
		{
			name:  "backwards jump every third call",
			start: 100, jumpEvery: 3, jumpBy: 30, calls: 6,
			want: []int64{100, 101, 72, 73, 74, 45},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWall(tt.start, tt.skew, tt.jumpEvery, tt.jumpBy)
			for i, want := range tt.want {
				if got := w(); got != want {
					t.Fatalf("call %d = %d, want %d", i+1, got, want)
				}
			}
		})
	}
}
