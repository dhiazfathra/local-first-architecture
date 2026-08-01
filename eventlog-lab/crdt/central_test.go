//go:build integration

// This file is excluded from the default `go test ./...` gate by the
// integration build tag, for the same reason as eventlog/postgres_test.go: it
// needs a live Postgres. Run it with:
//
//	go test -tags=integration ./crdt/ -run TestPostgresAnomalies -v
//
// It lives in package crdt (rather than eventlog) because eventlog.PostgresLog
// deliberately does not import crdt (that would be an import cycle, since
// crdt already imports eventlog) -- this test is the one place that needs
// both crdt.Projector and eventlog.PostgresLog together.
package crdt

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

func anomalyEvent(node clock.NodeID, seq eventlog.Seq, wall int64, sku string, delta int64) eventlog.Event {
	return eventlog.Event{
		ID:    eventlog.EventID{NodeID: node, Seq: seq},
		HLC:   clock.HLC{Wall: wall, NodeID: node},
		SKU:   sku,
		Kind:  eventlog.KindQuantityDelta,
		Delta: delta,
	}
}

func TestPostgresAnomaliesReportNegativeStockWithoutRejectingIt(t *testing.T) {
	dsn := os.Getenv("EVENTLOG_LAB_PG_DSN")
	if dsn == "" {
		t.Fatal("EVENTLOG_LAB_PG_DSN not set; start Postgres and export it (see Task 10)")
	}
	ctx := context.Background()
	l, err := eventlog.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgres() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	// Two nodes each pick 8 of 10 while partitioned. Central merges both --
	// it has no rejection path -- and reports the negative result.
	//
	// Node IDs and SKUs are namespaced to this test (rather than reused from
	// postgres_test.go's "A"/"B"/"C"/"SKU-1") because this test runs against
	// the same live database without truncating: eventlog.PostgresLog does
	// not expose truncateAll outside its package, and Append's idempotency on
	// (node_id, seq) means colliding IDs from another test's leftover rows
	// would silently keep stale data instead of inserting these events.
	events := []eventlog.Event{
		anomalyEvent("ANOM-C", 1, 10, "ANOM-SKU-1", 10),
		anomalyEvent("ANOM-A", 1, 20, "ANOM-SKU-1", -8),
		anomalyEvent("ANOM-B", 1, 20, "ANOM-SKU-1", -8),
		anomalyEvent("ANOM-C", 2, 30, "ANOM-SKU-2", 5),
	}
	for _, e := range events {
		if err := l.Append(ctx, e); err != nil {
			t.Fatalf("Append(%v) error = %v -- central must never reject", e.ID, err)
		}
	}

	p := &Projector{Log: l}
	for _, sku := range []string{"ANOM-SKU-1", "ANOM-SKU-2"} {
		state, err := p.Project(ctx, sku)
		if err != nil {
			t.Fatalf("Project(%q) error = %v", sku, err)
		}
		if err := l.UpsertProjection(ctx, sku, state.Quantity(), state.Name.Value,
			state.ReorderPoint.Value, state.Deleted.Value); err != nil {
			t.Fatalf("UpsertProjection(%q) error = %v", sku, err)
		}
	}

	all, err := l.Anomalies(ctx)
	if err != nil {
		t.Fatalf("Anomalies() error = %v", err)
	}
	// Anomalies() reports every negative projection in the shared table, not
	// just this test's. Running against a live database alongside other
	// packages (e.g. eventlog/postgres_test.go's TRUNCATE) means unrelated
	// rows can appear or this test's rows can be raced out from under it, so
	// scope the assertion to this test's own SKU namespace.
	var got []eventlog.Anomaly
	for _, a := range all {
		if strings.HasPrefix(a.SKU, "ANOM-") {
			got = append(got, a)
		}
	}
	if len(got) != 1 || got[0].SKU != "ANOM-SKU-1" || got[0].Quantity != -6 {
		t.Fatalf("Anomalies() (ANOM- namespace) = %+v, want exactly [{ANOM-SKU-1 -6}]", got)
	}
}
