package central

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// pgDSN is the connection string of the embedded server started for this package's
// tests, and dbSeq names a fresh database per subtest so they cannot interfere.
// testInstant is a fixed wall time so HLC readings are comparable across subtests.
var (
	pgDSN       string
	dbSeq       int
	testInstant = time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
)

// TestMain starts one real Postgres for the whole package. embedded-postgres
// downloads and runs an actual server in a temp directory, so this needs no Docker
// and no developer setup, and no test is ever skipped for want of a database.
func TestMain(m *testing.M) {
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Port(54329).
		Database("warehouse").
		Username("warehouse").
		Password("warehouse").
		RuntimePath(os.TempDir() + "/warehouse-embedded-postgres"))
	if err := pg.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start embedded postgres: %v\n", err)
		os.Exit(1)
	}
	pgDSN = "postgres://warehouse:warehouse@localhost:54329/warehouse?sslmode=disable"
	code := m.Run()
	if err := pg.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "stop embedded postgres: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

// freshPostgres creates an empty database and opens a store on it.
func freshPostgres(t *testing.T) Store {
	t.Helper()
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	dbSeq++
	name := fmt.Sprintf("t%d", dbSeq)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin: %v", err)
	}

	store, err := OpenPostgres(ctx, fmt.Sprintf(
		"postgres://warehouse:warehouse@localhost:54329/%s?sslmode=disable", name))
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
}

func TestPostgresSatisfiesTheStoreContract(t *testing.T) {
	runStoreContract(t, freshPostgres)
}

func TestPostgresRecoversCentralSequenceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	dbSeq++
	name := fmt.Sprintf("t%d", dbSeq)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin: %v", err)
	}
	dsn := fmt.Sprintf("postgres://warehouse:warehouse@localhost:54329/%s?sslmode=disable", name)

	first, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	envs, err := first.EmitCentral(ctx, []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "WIDGET",
		Payload: domain.ItemUpserted{Item: domain.Item{SKU: "WIDGET", BaseUoM: "EA"}}}}, nil, testInstant)
	if err != nil {
		t.Fatalf("EmitCentral: %v", err)
	}
	if envs[0].ID.Seq != 1 {
		t.Fatalf("first seq = %d, want 1", envs[0].ID.Seq)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close() }()
	again, err := second.EmitCentral(ctx, []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "BOLT",
		Payload: domain.ItemUpserted{Item: domain.Item{SKU: "BOLT", BaseUoM: "EA"}}}}, nil, testInstant)
	if err != nil {
		t.Fatalf("EmitCentral after reopen: %v", err)
	}
	if again[0].ID.Seq != 2 {
		t.Errorf("seq after restart = %d, want 2: a restart must never reuse a sequence", again[0].ID.Seq)
	}
	if again[0].HLC.Compare(envs[0].HLC) <= 0 {
		t.Errorf("HLC after restart = %+v, want it above %+v", again[0].HLC, envs[0].HLC)
	}
}

func TestPostgresRejectsABadDSN(t *testing.T) {
	if _, err := OpenPostgres(context.Background(), "postgres://nobody@localhost:1/none"); err == nil {
		t.Fatal("OpenPostgres with an unreachable server: expected an error")
	}
}

func TestPostgresOperationsFailAfterClose(t *testing.T) {
	store := freshPostgres(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()
	if _, err := store.Events(ctx); err == nil {
		t.Error("Events after Close: expected an error")
	}
	if _, err := store.Append(ctx, []domain.Envelope{{ID: domain.EventID{NodeID: "wh-a", Seq: 1},
		Type: domain.TypePutAway, Payload: []byte(`{}`)}}); err == nil {
		t.Error("Append after Close: expected an error")
	}
	if _, _, err := store.Item(ctx, "WIDGET"); err == nil {
		t.Error("Item after Close: expected an error")
	}
	if _, err := store.Outbound(ctx, "wh-a", 0, 10); err == nil {
		t.Error("Outbound after Close: expected an error")
	}
	if _, err := store.OpenTransfers(ctx, testInstant); err == nil {
		t.Error("OpenTransfers after Close: expected an error")
	}
	if _, _, err := store.NodeConfig(ctx, "wh-a"); err == nil {
		t.Error("NodeConfig after Close: expected an error")
	}
	if _, err := store.EmitCentral(ctx, []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "WIDGET",
		Payload: domain.ItemUpserted{Item: domain.Item{SKU: "WIDGET", BaseUoM: "EA"}}}}, nil, testInstant); err == nil {
		t.Error("EmitCentral after Close: expected an error")
	}
	if _, err := store.PushedSeq(ctx, "wh-a"); err == nil {
		t.Error("PushedSeq after Close: expected an error")
	}
	if err := store.SetPushedSeq(ctx, "wh-a", 1); err == nil {
		t.Error("SetPushedSeq after Close: expected an error")
	}
	if _, err := store.DeliveredOrd(ctx, "wh-a"); err == nil {
		t.Error("DeliveredOrd after Close: expected an error")
	}
	if err := store.SetDeliveredOrd(ctx, "wh-a", 1); err == nil {
		t.Error("SetDeliveredOrd after Close: expected an error")
	}
	if err := store.Enqueue(ctx, "wh-a", []domain.Envelope{{ID: domain.EventID{NodeID: "wh-a", Seq: 1}}}); err == nil {
		t.Error("Enqueue after Close: expected an error")
	}
	if err := store.UpsertItem(ctx, domain.Item{SKU: "WIDGET", BaseUoM: "EA"}); err == nil {
		t.Error("UpsertItem after Close: expected an error")
	}
	if err := store.UpsertPurchaseOrder(ctx, "PO1", "WIDGET", 1); err == nil {
		t.Error("UpsertPurchaseOrder after Close: expected an error")
	}
	if _, _, err := store.PurchaseOrder(ctx, "PO1", "WIDGET"); err == nil {
		t.Error("PurchaseOrder after Close: expected an error")
	}
	if err := store.RegisterNode(ctx, "wh-a", []string{"WIDGET"}); err == nil {
		t.Error("RegisterNode after Close: expected an error")
	}
	if err := store.RecordReceipt(ctx, ReceiptFact{EventID: domain.EventID{NodeID: "wh-a", Seq: 1}}); err == nil {
		t.Error("RecordReceipt after Close: expected an error")
	}
	if _, err := store.ReceivedAgainstPO(ctx, "PO1", "WIDGET"); err == nil {
		t.Error("ReceivedAgainstPO after Close: expected an error")
	}
	if _, _, err := store.DeliveryNoteFirstSeen(ctx, "NOTE1", "WIDGET"); err == nil {
		t.Error("DeliveryNoteFirstSeen after Close: expected an error")
	}
	if err := store.RecordDispatch(ctx, InTransitRow{TransferID: "T1", Key: domain.StockKey{SKU: "WIDGET"}}); err == nil {
		t.Error("RecordDispatch after Close: expected an error")
	}
	if err := store.AddReceived(ctx, domain.EventID{NodeID: "wh-a", Seq: 1}, "T1", domain.StockKey{SKU: "WIDGET"}, 1); err == nil {
		t.Error("AddReceived after Close: expected an error")
	}
	if _, _, err := store.InTransit(ctx, "T1", domain.StockKey{SKU: "WIDGET"}); err == nil {
		t.Error("InTransit after Close: expected an error")
	}
	if _, _, err := store.Receipt(ctx, domain.EventID{NodeID: "wh-a", Seq: 1}); err == nil {
		t.Error("Receipt after Close: expected an error")
	}
	if _, err := store.ReceivedFromEvent(ctx, domain.EventID{NodeID: "wh-a", Seq: 1}, "T1",
		domain.StockKey{SKU: "WIDGET"}); err == nil {
		t.Error("ReceivedFromEvent after Close: expected an error")
	}
	if err := store.FailTransfer(ctx, "T1"); err == nil {
		t.Error("FailTransfer after Close: expected an error")
	}
	if err := store.RecordDecision(ctx, Decision{EventID: domain.EventID{NodeID: "wh-a", Seq: 1}, Verdict: VerdictAccepted}); err == nil {
		t.Error("RecordDecision after Close: expected an error")
	}
	if _, _, err := store.Decision(ctx, domain.EventID{NodeID: "wh-a", Seq: 1}); err == nil {
		t.Error("Decision after Close: expected an error")
	}
}

func TestOpenPostgresRejectsAMalformedDSN(t *testing.T) {
	if _, err := OpenPostgres(context.Background(), "://not a url"); err == nil {
		t.Fatal("OpenPostgres with a malformed DSN: expected an error")
	}
}

// TestOpenPostgresPropagatesASchemaApplyFailure renames a column the schema's own
// CREATE INDEX statement depends on, so re-applying the (otherwise idempotent)
// schema on reopen fails outright.
func TestOpenPostgresPropagatesASchemaApplyFailure(t *testing.T) {
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	dbSeq++
	name := fmt.Sprintf("t%d", dbSeq)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin: %v", err)
	}
	dsn := fmt.Sprintf("postgres://warehouse:warehouse@localhost:54329/%s?sslmode=disable", name)

	first, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	if _, err := first.pool.Exec(ctx,
		`ALTER TABLE events RENAME COLUMN hlc_wall TO hlc_wall_renamed`); err != nil {
		t.Fatalf("rename column: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := OpenPostgres(ctx, dsn); err == nil {
		t.Fatal("OpenPostgres over a broken events table: expected a recovery error")
	}
}

func TestPostgresEmitCentralPropagatesAnInvalidPayloadError(t *testing.T) {
	store := freshPostgres(t)
	_, err := store.EmitCentral(context.Background(), []domain.Event{
		{Type: domain.TypePutAway, AggregateID: "WIDGET", Payload: domain.Picked{}},
	}, nil, testInstant)
	if err == nil {
		t.Fatal("EmitCentral with a mismatched payload: expected an error")
	}
}

// TestEventsPropagatesAScanError and its outbound counterpart below insert a row with
// a NULL in a column the schema declares NOT NULL by bypassing insert()/Enqueue()
// directly through the pool, then read it back: Scan into the non-nullable Go field
// then fails exactly like a corrupted row would.
func TestEventsPropagatesAScanError(t *testing.T) {
	store := freshPostgres(t)
	pg := store.(*Postgres)
	ctx := context.Background()
	if _, err := pg.pool.Exec(ctx, `ALTER TABLE events ALTER COLUMN causation_node DROP NOT NULL`); err != nil {
		t.Fatalf("alter column: %v", err)
	}
	if _, err := pg.pool.Exec(ctx, `INSERT INTO events
		(node_id, seq, aggregate_id, type, hlc_wall, hlc_counter, hlc_node, recorded_at,
		 causation_node, causation_seq, payload)
		VALUES ('wh-a', 1, 'WIDGET', 'PutAway', 1, 1, 'wh-a', now(), NULL, 0, '{}')`); err != nil {
		t.Fatalf("insert null-causation row: %v", err)
	}
	if _, err := store.Events(ctx); err == nil {
		t.Fatal("Events over a NULL causation_node: expected a scan error")
	}
}

// TestEventsCarriesCausationThroughRoundTrip exercises the causation-present branch
// of scanEnvelopes, which a plain append never reaches.
func TestEventsCarriesCausationThroughRoundTrip(t *testing.T) {
	store := freshPostgres(t)
	ctx := context.Background()
	cause := domain.EventID{NodeID: "wh-a", Seq: 1}
	if _, err := store.Append(ctx, []domain.Envelope{{ID: cause, Type: domain.TypePutAway,
		RecordedAt: testInstant, Payload: []byte(`{}`)}}); err != nil {
		t.Fatalf("append cause: %v", err)
	}
	if _, err := store.Append(ctx, []domain.Envelope{{ID: domain.EventID{NodeID: "wh-a", Seq: 2},
		Type: domain.TypePutAway, RecordedAt: testInstant, CausationID: &cause,
		Payload: []byte(`{}`)}}); err != nil {
		t.Fatalf("append effect: %v", err)
	}
	envs, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	var found *domain.EventID
	for _, e := range envs {
		if e.ID.Seq == 2 {
			found = e.CausationID
		}
	}
	if found == nil || *found != cause {
		t.Fatalf("causation on seq 2 = %v, want %v", found, cause)
	}
}

func TestOutboundPropagatesAScanError(t *testing.T) {
	store := freshPostgres(t)
	pg := store.(*Postgres)
	ctx := context.Background()
	if _, err := pg.pool.Exec(ctx, `ALTER TABLE events ALTER COLUMN causation_node DROP NOT NULL`); err != nil {
		t.Fatalf("alter column: %v", err)
	}
	if _, err := pg.pool.Exec(ctx, `INSERT INTO events
		(node_id, seq, aggregate_id, type, hlc_wall, hlc_counter, hlc_node, recorded_at,
		 causation_node, causation_seq, payload)
		VALUES ('wh-a', 1, 'WIDGET', 'PutAway', 1, 1, 'wh-a', now(), NULL, 0, '{}')`); err != nil {
		t.Fatalf("insert null-causation row: %v", err)
	}
	if err := store.Enqueue(ctx, "wh-b", []domain.Envelope{{ID: domain.EventID{NodeID: "wh-a", Seq: 1}}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := store.Outbound(ctx, "wh-b", 0, 10); err == nil {
		t.Fatal("Outbound over a NULL causation_node: expected a scan error")
	}
}

// TestOutboundCarriesCausationThroughRoundTrip exercises Outbound's causation-present
// branch the same way TestEventsCarriesCausationThroughRoundTrip does for Events.
func TestOutboundCarriesCausationThroughRoundTrip(t *testing.T) {
	store := freshPostgres(t)
	ctx := context.Background()
	cause := domain.EventID{NodeID: "wh-a", Seq: 1}
	effect := domain.EventID{NodeID: "wh-a", Seq: 2}
	if _, err := store.Append(ctx, []domain.Envelope{
		{ID: cause, Type: domain.TypePutAway, RecordedAt: testInstant, Payload: []byte(`{}`)},
		{ID: effect, Type: domain.TypePutAway, RecordedAt: testInstant, CausationID: &cause, Payload: []byte(`{}`)},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := store.Enqueue(ctx, "wh-b", []domain.Envelope{{ID: effect}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	rows, err := store.Outbound(ctx, "wh-b", 0, 10)
	if err != nil {
		t.Fatalf("Outbound: %v", err)
	}
	if len(rows) != 1 || rows[0].Env.CausationID == nil || *rows[0].Env.CausationID != cause {
		t.Fatalf("Outbound rows = %+v, want one row carrying causation %v", rows, cause)
	}
}

// TestUpsertItemPropagatesAnEncodeError forces json.Marshal to fail: NaN has no JSON
// representation, so it is the one Go value that always breaks encoding.
func TestUpsertItemPropagatesAnEncodeError(t *testing.T) {
	store := freshPostgres(t)
	err := store.UpsertItem(context.Background(), domain.Item{SKU: "WIDGET", BaseUoM: "EA",
		AltUoM: map[domain.UoM]float64{"CASE": math.NaN()}})
	if err == nil {
		t.Fatal("UpsertItem with a NaN alternate-unit factor: expected an encode error")
	}
}

// TestItemPropagatesADecodeError writes alt_uom JSON that is valid (JSONB is
// validated on write, so genuinely malformed JSON never makes it into the column)
// but whose value type does not fit map[UoM]float64, bypassing UpsertItem's own
// encoding so Item's decode step is what has to fail.
func TestItemPropagatesADecodeError(t *testing.T) {
	store := freshPostgres(t)
	pg := store.(*Postgres)
	ctx := context.Background()
	if _, err := pg.pool.Exec(ctx, `INSERT INTO items
		(sku, description, base_uom, alt_uom, lot_tracked, shelf_life_days, deleted)
		VALUES ('WIDGET', '', 'EA', '{"CASE": "not-a-number"}', false, 0, false)`); err != nil {
		t.Fatalf("insert malformed item: %v", err)
	}
	if _, _, err := store.Item(ctx, "WIDGET"); err == nil {
		t.Fatal("Item over malformed alt_uom: expected a decode error")
	}
}

// TestRegisterNodePropagatesAClearRejectsError drops node_rejects out from under
// RegisterNode after the node itself was already inserted, so the DELETE that clears
// its prior rejects is what fails.
func TestRegisterNodePropagatesAClearRejectsError(t *testing.T) {
	store := freshPostgres(t)
	pg := store.(*Postgres)
	ctx := context.Background()
	if _, err := pg.pool.Exec(ctx, `DROP TABLE node_rejects`); err != nil {
		t.Fatalf("drop node_rejects: %v", err)
	}
	if err := store.RegisterNode(ctx, "wh-a", nil); err == nil {
		t.Fatal("RegisterNode without node_rejects: expected an error")
	}
}

// TestRegisterNodePropagatesARejectInsertError uses a NUL byte, which text columns
// reject, to fail the loop's own insert without touching the two statements before it.
func TestRegisterNodePropagatesARejectInsertError(t *testing.T) {
	store := freshPostgres(t)
	if err := store.RegisterNode(context.Background(), "wh-a", []string{"bad\x00sku"}); err == nil {
		t.Fatal("RegisterNode with a NUL byte in a reject SKU: expected an error")
	}
}

// TestNodeConfigPropagatesARejectsQueryError drops node_rejects after the node is
// registered, so NodeConfig's own read of it fails rather than the earlier existence
// check.
func TestNodeConfigPropagatesARejectsQueryError(t *testing.T) {
	store := freshPostgres(t)
	pg := store.(*Postgres)
	ctx := context.Background()
	if err := store.RegisterNode(ctx, "wh-a", nil); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if _, err := pg.pool.Exec(ctx, `DROP TABLE node_rejects`); err != nil {
		t.Fatalf("drop node_rejects: %v", err)
	}
	if _, _, err := store.NodeConfig(ctx, "wh-a"); err == nil {
		t.Fatal("NodeConfig without node_rejects: expected an error")
	}
}

// TestNodeConfigPropagatesARejectsScanError makes node_rejects.sku nullable and
// inserts a NULL directly, so scanning it into a plain string fails.
func TestNodeConfigPropagatesARejectsScanError(t *testing.T) {
	store := freshPostgres(t)
	pg := store.(*Postgres)
	ctx := context.Background()
	if err := store.RegisterNode(ctx, "wh-a", nil); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if _, err := pg.pool.Exec(ctx, `ALTER TABLE node_rejects DROP CONSTRAINT node_rejects_pkey`); err != nil {
		t.Fatalf("drop primary key: %v", err)
	}
	if _, err := pg.pool.Exec(ctx, `ALTER TABLE node_rejects ALTER COLUMN sku DROP NOT NULL`); err != nil {
		t.Fatalf("alter column: %v", err)
	}
	if _, err := pg.pool.Exec(ctx,
		`INSERT INTO node_rejects (node_id, sku) VALUES ('wh-a', NULL)`); err != nil {
		t.Fatalf("insert null reject: %v", err)
	}
	if _, _, err := store.NodeConfig(ctx, "wh-a"); err == nil {
		t.Fatal("NodeConfig over a NULL reject SKU: expected a scan error")
	}
}

// TestOpenTransfersPropagatesAScanError makes in_transit.from_node nullable and
// inserts a NULL directly, so scanning it into a plain string fails.
func TestOpenTransfersPropagatesAScanError(t *testing.T) {
	store := freshPostgres(t)
	pg := store.(*Postgres)
	ctx := context.Background()
	if _, err := pg.pool.Exec(ctx, `ALTER TABLE in_transit ALTER COLUMN from_node DROP NOT NULL`); err != nil {
		t.Fatalf("alter column: %v", err)
	}
	if _, err := pg.pool.Exec(ctx, `INSERT INTO in_transit
		(transfer_id, sku, lot_id, from_node, to_node, dispatched, received, dispatched_at, failed)
		VALUES ('T1', 'WIDGET', 'LOT1', NULL, 'wh-b', 1, 0, $1, false)`, testInstant); err != nil {
		t.Fatalf("insert null from_node: %v", err)
	}
	if _, err := store.OpenTransfers(ctx, testInstant.Add(time.Hour)); err == nil {
		t.Fatal("OpenTransfers over a NULL from_node: expected a scan error")
	}
}
