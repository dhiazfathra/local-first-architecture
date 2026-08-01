package central

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

//go:embed schema.sql
var schemaFS embed.FS

// Postgres is the deployment Store. It passes the same contract test as Memory.
type Postgres struct {
	pool *pgxpool.Pool

	// mu guards central's own sequence counter and clock, which are read-modify-write
	// and must never hand out the same sequence twice.
	mu        sync.Mutex
	centralN  uint64
	centralHL domain.HLC
}

// OpenPostgres connects, applies the schema, and recovers central's sequence number
// and clock reading so a restart never reuses a sequence nor goes backwards in time.
func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	// ponytail: schema.sql is embedded at compile time, so ReadFile cannot fail at
	// runtime short of a corrupted binary; the error is handled anyway because
	// ignoring an *error return is worse than one untestable line.
	schema, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("read schema: %w", err)
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	p := &Postgres{pool: pool}
	if err := p.recoverCentral(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

// ponytail: every column this query reads is also read by schema.sql's own CREATE
// INDEX on events, which OpenPostgres re-applies immediately before calling this —
// so any schema fault big enough to break this SELECT already fails schema
// application first. That coupling was confirmed by attempting the fault injection
// (renamed column, dropped table) rather than assumed; both broke apply-schema
// before reaching here, leaving this query's own error path effectively
// unreachable short of a live connection failure between the two calls.
func (p *Postgres) recoverCentral(ctx context.Context) error {
	var seq, wall, counter int64
	err := p.pool.QueryRow(ctx, `SELECT coalesce(max(seq), 0),
		coalesce(max(hlc_wall), 0),
		coalesce(max(hlc_counter) FILTER (WHERE hlc_wall = (SELECT max(hlc_wall) FROM events WHERE node_id = $1)), 0)
		FROM events WHERE node_id = $1`, string(CentralNode)).Scan(&seq, &wall, &counter)
	if err != nil {
		return fmt.Errorf("recover central sequence: %w", err)
	}
	p.centralN = uint64(seq)
	p.centralHL = domain.HLC{Wall: wall, Counter: uint32(counter), Node: CentralNode}
	return nil
}

// Close releases the connection pool.
func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

// Append stores node events idempotently by (node, seq).
func (p *Postgres) Append(ctx context.Context, envs []domain.Envelope) ([]domain.Envelope, error) {
	fresh := make([]domain.Envelope, 0, len(envs))
	for _, env := range envs {
		inserted, err := p.insert(ctx, env)
		if err != nil {
			return nil, err
		}
		if inserted {
			fresh = append(fresh, env)
		}
	}
	return fresh, nil
}

// insert appends one envelope, reporting whether it was new. Identity is the
// (node_id, seq) pair, so a conflict means the event is already stored and
// re-ingesting it is a no-op — which is what makes sync retries safe.
func (p *Postgres) insert(ctx context.Context, env domain.Envelope) (bool, error) {
	causeNode, causeSeq := "", uint64(0)
	if env.CausationID != nil {
		causeNode, causeSeq = string(env.CausationID.NodeID), env.CausationID.Seq
	}
	tag, err := p.pool.Exec(ctx, `INSERT INTO events
		(node_id, seq, aggregate_id, type, hlc_wall, hlc_counter, hlc_node,
		 recorded_at, causation_node, causation_seq, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (node_id, seq) DO NOTHING`,
		string(env.ID.NodeID), env.ID.Seq, env.AggregateID, env.Type,
		env.HLC.Wall, int64(env.HLC.Counter), string(env.HLC.Node),
		env.RecordedAt, causeNode, causeSeq, []byte(env.Payload))
	if err != nil {
		return false, fmt.Errorf("append event %s: %w", env.ID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Events returns the whole log in total HLC order.
func (p *Postgres) Events(ctx context.Context) ([]domain.Envelope, error) {
	rows, err := p.pool.Query(ctx, `SELECT node_id, seq, aggregate_id, type, hlc_wall, hlc_counter,
		hlc_node, recorded_at, causation_node, causation_seq, payload
		FROM events ORDER BY hlc_wall, hlc_counter, hlc_node, seq`)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	return scanEnvelopes(rows)
}

func scanEnvelopes(rows pgx.Rows) ([]domain.Envelope, error) {
	defer rows.Close()

	out := []domain.Envelope{}
	for rows.Next() {
		var env domain.Envelope
		var node, hlcNode, causeNode string
		var counter, causeSeq int64
		var payload []byte
		if err := rows.Scan(&node, &env.ID.Seq, &env.AggregateID, &env.Type, &env.HLC.Wall,
			&counter, &hlcNode, &env.RecordedAt, &causeNode, &causeSeq, &payload); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		env.ID.NodeID = domain.NodeID(node)
		env.HLC.Counter, env.HLC.Node = uint32(counter), domain.NodeID(hlcNode)
		env.RecordedAt = env.RecordedAt.UTC()
		env.Payload = payload
		if causeNode != "" {
			env.CausationID = &domain.EventID{NodeID: domain.NodeID(causeNode), Seq: uint64(causeSeq)}
		}
		out = append(out, env)
	}
	// ponytail: rows.Err surfaces a mid-stream network/protocol failure once
	// iteration is done; against a local embedded Postgres the whole result set
	// arrives before Next() is first called, so there is nothing left to fail on and
	// no deterministic way to fault-inject it without a flaky race or a fake driver.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return out, nil
}

// EmitCentral seals and appends central's own events.
func (p *Postgres) EmitCentral(ctx context.Context, events []domain.Event, causation *domain.EventID,
	now time.Time) ([]domain.Envelope, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]domain.Envelope, 0, len(events))
	for _, e := range events {
		p.centralN++
		p.centralHL = domain.Tick(p.centralHL, now.UnixMilli(), CentralNode)
		env, err := domain.NewEnvelope(
			domain.EventID{NodeID: CentralNode, Seq: p.centralN}, p.centralHL, now, causation, e)
		if err != nil {
			return nil, err
		}
		if _, err := p.insert(ctx, env); err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, nil
}

// PushedSeq is the highest sequence of a node's own events central has stored.
func (p *Postgres) PushedSeq(ctx context.Context, node domain.NodeID) (uint64, error) {
	return p.cursor(ctx, node, "pushed_seq")
}

// DeliveredOrd is the highest outbound position a node has acknowledged.
func (p *Postgres) DeliveredOrd(ctx context.Context, node domain.NodeID) (uint64, error) {
	return p.cursor(ctx, node, "delivered_ord")
}

func (p *Postgres) cursor(ctx context.Context, node domain.NodeID, column string) (uint64, error) {
	var value int64
	err := p.pool.QueryRow(ctx,
		`SELECT `+column+` FROM cursors WHERE node_id = $1`, string(node)).Scan(&value)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read %s for %s: %w", column, node, err)
	}
	return uint64(value), nil
}

// SetPushedSeq records the highest sequence central has stored for a node.
func (p *Postgres) SetPushedSeq(ctx context.Context, node domain.NodeID, seq uint64) error {
	return p.setCursor(ctx, node, "pushed_seq", seq)
}

// SetDeliveredOrd records the highest outbound position a node has acknowledged.
func (p *Postgres) SetDeliveredOrd(ctx context.Context, node domain.NodeID, ord uint64) error {
	return p.setCursor(ctx, node, "delivered_ord", ord)
}

func (p *Postgres) setCursor(ctx context.Context, node domain.NodeID, column string, value uint64) error {
	if _, err := p.pool.Exec(ctx, `INSERT INTO cursors (node_id, `+column+`) VALUES ($1, $2)
		ON CONFLICT (node_id) DO UPDATE SET `+column+` = excluded.`+column,
		string(node), int64(value)); err != nil {
		return fmt.Errorf("set %s for %s: %w", column, node, err)
	}
	return nil
}

// Enqueue adds events to a node's downstream queue.
func (p *Postgres) Enqueue(ctx context.Context, target domain.NodeID, envs []domain.Envelope) error {
	for _, env := range envs {
		if _, err := p.pool.Exec(ctx,
			`INSERT INTO outbound (target_node, node_id, seq) VALUES ($1, $2, $3)`,
			string(target), string(env.ID.NodeID), env.ID.Seq); err != nil {
			return fmt.Errorf("enqueue %s for %s: %w", env.ID, target, err)
		}
	}
	return nil
}

// Outbound reads up to limit queued events for a node above afterOrd.
func (p *Postgres) Outbound(ctx context.Context, target domain.NodeID, afterOrd uint64, limit int) ([]Outbound, error) {
	rows, err := p.pool.Query(ctx, `SELECT o.ord, e.node_id, e.seq, e.aggregate_id, e.type,
		e.hlc_wall, e.hlc_counter, e.hlc_node, e.recorded_at, e.causation_node, e.causation_seq, e.payload
		FROM outbound o JOIN events e ON e.node_id = o.node_id AND e.seq = o.seq
		WHERE o.target_node = $1 AND o.ord > $2
		ORDER BY o.ord LIMIT $3`, string(target), int64(afterOrd), limit)
	if err != nil {
		return nil, fmt.Errorf("query outbound for %s: %w", target, err)
	}
	defer rows.Close()

	out := []Outbound{}
	for rows.Next() {
		var row Outbound
		var ord int64
		var env domain.Envelope
		var node, hlcNode, causeNode string
		var counter, causeSeq int64
		var payload []byte
		if err := rows.Scan(&ord, &node, &env.ID.Seq, &env.AggregateID, &env.Type, &env.HLC.Wall,
			&counter, &hlcNode, &env.RecordedAt, &causeNode, &causeSeq, &payload); err != nil {
			return nil, fmt.Errorf("scan outbound row: %w", err)
		}
		env.ID.NodeID = domain.NodeID(node)
		env.HLC.Counter, env.HLC.Node = uint32(counter), domain.NodeID(hlcNode)
		env.RecordedAt, env.Payload = env.RecordedAt.UTC(), payload
		if causeNode != "" {
			env.CausationID = &domain.EventID{NodeID: domain.NodeID(causeNode), Seq: uint64(causeSeq)}
		}
		row.Ord, row.Env = uint64(ord), env
		out = append(out, row)
	}
	// ponytail: rows.Err surfaces a mid-stream network/protocol failure once
	// iteration is done; against a local embedded Postgres the whole result set
	// arrives before Next() is first called, so there is nothing left to fail on and
	// no deterministic way to fault-inject it without a flaky race or a fake driver.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbound rows: %w", err)
	}
	return out, nil
}

// UpsertItem inserts or updates one item master record.
func (p *Postgres) UpsertItem(ctx context.Context, item domain.Item) error {
	alt, err := json.Marshal(item.AltUoM)
	if err != nil {
		return fmt.Errorf("encode alternate units for %s: %w", item.SKU, err)
	}
	if _, err := p.pool.Exec(ctx, `INSERT INTO items
		(sku, description, base_uom, alt_uom, lot_tracked, shelf_life_days, deleted)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (sku) DO UPDATE SET description = excluded.description,
			base_uom = excluded.base_uom, alt_uom = excluded.alt_uom,
			lot_tracked = excluded.lot_tracked, shelf_life_days = excluded.shelf_life_days,
			deleted = excluded.deleted`,
		item.SKU, item.Description, string(item.BaseUoM), alt,
		item.LotTracked, item.ShelfLifeDays, item.Deleted); err != nil {
		return fmt.Errorf("upsert item %s: %w", item.SKU, err)
	}
	return nil
}

// Item reads one item master record by SKU.
func (p *Postgres) Item(ctx context.Context, sku string) (domain.Item, bool, error) {
	var item domain.Item
	var base string
	var alt []byte
	err := p.pool.QueryRow(ctx, `SELECT sku, description, base_uom, alt_uom, lot_tracked,
		shelf_life_days, deleted FROM items WHERE sku = $1`, sku).
		Scan(&item.SKU, &item.Description, &base, &alt, &item.LotTracked, &item.ShelfLifeDays, &item.Deleted)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.Item{}, false, nil
	case err != nil:
		return domain.Item{}, false, fmt.Errorf("read item %s: %w", sku, err)
	}
	item.BaseUoM = domain.UoM(base)
	if err := json.Unmarshal(alt, &item.AltUoM); err != nil {
		return domain.Item{}, false, fmt.Errorf("decode alternate units for %s: %w", sku, err)
	}
	return item, true, nil
}

// UpsertPurchaseOrder inserts or updates the ordered quantity for one PO line.
func (p *Postgres) UpsertPurchaseOrder(ctx context.Context, poRef, sku string, ordered float64) error {
	if _, err := p.pool.Exec(ctx, `INSERT INTO purchase_orders (po_ref, sku, qty_ordered)
		VALUES ($1,$2,$3) ON CONFLICT (po_ref, sku) DO UPDATE SET qty_ordered = excluded.qty_ordered`,
		poRef, sku, ordered); err != nil {
		return fmt.Errorf("upsert purchase order %s/%s: %w", poRef, sku, err)
	}
	return nil
}

// PurchaseOrder reads the ordered quantity for one PO line.
func (p *Postgres) PurchaseOrder(ctx context.Context, poRef, sku string) (float64, bool, error) {
	var qty float64
	err := p.pool.QueryRow(ctx,
		`SELECT qty_ordered FROM purchase_orders WHERE po_ref = $1 AND sku = $2`, poRef, sku).Scan(&qty)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("read purchase order %s/%s: %w", poRef, sku, err)
	}
	return qty, true, nil
}

// RegisterNode records that a node exists and which SKUs it refuses to stock.
func (p *Postgres) RegisterNode(ctx context.Context, node domain.NodeID, rejects []string) error {
	if _, err := p.pool.Exec(ctx,
		`INSERT INTO nodes (node_id) VALUES ($1) ON CONFLICT (node_id) DO NOTHING`, string(node)); err != nil {
		return fmt.Errorf("register node %s: %w", node, err)
	}
	if _, err := p.pool.Exec(ctx, `DELETE FROM node_rejects WHERE node_id = $1`, string(node)); err != nil {
		return fmt.Errorf("clear rejects for %s: %w", node, err)
	}
	for _, sku := range rejects {
		if _, err := p.pool.Exec(ctx, `INSERT INTO node_rejects (node_id, sku) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, string(node), sku); err != nil {
			return fmt.Errorf("record reject %s for %s: %w", sku, node, err)
		}
	}
	return nil
}

// NodeConfig reads a node's existence and its refused SKUs.
func (p *Postgres) NodeConfig(ctx context.Context, node domain.NodeID) (map[string]bool, bool, error) {
	var known bool
	err := p.pool.QueryRow(ctx,
		`SELECT true FROM nodes WHERE node_id = $1`, string(node)).Scan(&known)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("read node %s: %w", node, err)
	}
	rows, err := p.pool.Query(ctx, `SELECT sku FROM node_rejects WHERE node_id = $1`, string(node))
	if err != nil {
		return nil, false, fmt.Errorf("read rejects for %s: %w", node, err)
	}
	defer rows.Close()

	rejects := map[string]bool{}
	for rows.Next() {
		var sku string
		if err := rows.Scan(&sku); err != nil {
			return nil, false, fmt.Errorf("scan reject: %w", err)
		}
		rejects[sku] = true
	}
	// ponytail: rows.Err surfaces a mid-stream network/protocol failure once
	// iteration is done; against a local embedded Postgres the whole result set
	// arrives before Next() is first called, so there is nothing left to fail on and
	// no deterministic way to fault-inject it without a flaky race or a fake driver.
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate rejects: %w", err)
	}
	return rejects, true, nil
}

// RecordReceipt is idempotent per event id.
func (p *Postgres) RecordReceipt(ctx context.Context, f ReceiptFact) error {
	if _, err := p.pool.Exec(ctx, `INSERT INTO receipts
		(event_node, event_seq, node_id, po_ref, delivery_note, sku, qty_base)
		VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (event_node, event_seq) DO NOTHING`,
		string(f.EventID.NodeID), f.EventID.Seq, string(f.Node),
		f.PORef, f.DeliveryNote, f.SKU, f.QtyBase); err != nil {
		return fmt.Errorf("record receipt %s: %w", f.EventID, err)
	}
	return nil
}

// ReceivedAgainstPO sums everything received against one PO line.
func (p *Postgres) ReceivedAgainstPO(ctx context.Context, poRef, sku string) (float64, error) {
	var total float64
	if err := p.pool.QueryRow(ctx,
		`SELECT coalesce(sum(qty_base), 0) FROM receipts WHERE po_ref = $1 AND sku = $2`,
		poRef, sku).Scan(&total); err != nil {
		return 0, fmt.Errorf("sum receipts against %s/%s: %w", poRef, sku, err)
	}
	return total, nil
}

// DeliveryNoteFirstSeen returns the event that first keyed this note for this SKU.
func (p *Postgres) DeliveryNoteFirstSeen(ctx context.Context, note, sku string) (domain.EventID, bool, error) {
	var node string
	var seq int64
	err := p.pool.QueryRow(ctx, `SELECT event_node, event_seq FROM receipts
		WHERE delivery_note = $1 AND sku = $2 ORDER BY event_node, event_seq LIMIT 1`, note, sku).
		Scan(&node, &seq)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.EventID{}, false, nil
	case err != nil:
		return domain.EventID{}, false, fmt.Errorf("read first receipt for note %s: %w", note, err)
	}
	return domain.EventID{NodeID: domain.NodeID(node), Seq: uint64(seq)}, true, nil
}

// RecordDispatch records or updates a dispatched in-transit balance.
func (p *Postgres) RecordDispatch(ctx context.Context, row InTransitRow) error {
	if _, err := p.pool.Exec(ctx, `INSERT INTO in_transit
		(transfer_id, sku, lot_id, from_node, to_node, dispatched, received, dispatched_at, failed)
		VALUES ($1,$2,$3,$4,$5,$6,0,$7,false)
		ON CONFLICT (transfer_id, sku, lot_id) DO UPDATE SET dispatched = excluded.dispatched`,
		row.TransferID, row.Key.SKU, row.Key.LotID, string(row.FromNode), string(row.ToNode),
		row.Dispatched, row.DispatchedAt); err != nil {
		return fmt.Errorf("record dispatch %s: %w", row.TransferID, err)
	}
	return nil
}

// AddReceived adds to the received quantity of an existing in-transit balance.
func (p *Postgres) AddReceived(ctx context.Context, transferID string, k domain.StockKey, qty float64) error {
	tag, err := p.pool.Exec(ctx, `UPDATE in_transit SET received = received + $4
		WHERE transfer_id = $1 AND sku = $2 AND lot_id = $3`, transferID, k.SKU, k.LotID, qty)
	if err != nil {
		return fmt.Errorf("add received to %s: %w", transferID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("transfer %s has no dispatched line for %s/%s", transferID, k.SKU, k.LotID)
	}
	return nil
}

// InTransit reads one in-transit balance.
func (p *Postgres) InTransit(ctx context.Context, transferID string, k domain.StockKey) (InTransitRow, bool, error) {
	row := InTransitRow{TransferID: transferID, Key: k}
	var from, to string
	err := p.pool.QueryRow(ctx, `SELECT from_node, to_node, dispatched, received, dispatched_at, failed
		FROM in_transit WHERE transfer_id = $1 AND sku = $2 AND lot_id = $3`,
		transferID, k.SKU, k.LotID).
		Scan(&from, &to, &row.Dispatched, &row.Received, &row.DispatchedAt, &row.Failed)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return InTransitRow{}, false, nil
	case err != nil:
		return InTransitRow{}, false, fmt.Errorf("read in-transit for %s: %w", transferID, err)
	}
	row.FromNode, row.ToNode = domain.NodeID(from), domain.NodeID(to)
	row.DispatchedAt = row.DispatchedAt.UTC()
	return row, true, nil
}

// OpenTransfers returns transfers dispatched before the given instant that are still carrying stock.
func (p *Postgres) OpenTransfers(ctx context.Context, dispatchedBefore time.Time) ([]InTransitRow, error) {
	rows, err := p.pool.Query(ctx, `SELECT transfer_id, sku, lot_id, from_node, to_node,
		dispatched, received, dispatched_at, failed FROM in_transit
		WHERE failed = false AND dispatched - received > 0 AND dispatched_at < $1
		ORDER BY transfer_id, sku, lot_id`, dispatchedBefore)
	if err != nil {
		return nil, fmt.Errorf("query open transfers: %w", err)
	}
	defer rows.Close()

	out := []InTransitRow{}
	for rows.Next() {
		var row InTransitRow
		var from, to string
		if err := rows.Scan(&row.TransferID, &row.Key.SKU, &row.Key.LotID, &from, &to,
			&row.Dispatched, &row.Received, &row.DispatchedAt, &row.Failed); err != nil {
			return nil, fmt.Errorf("scan open transfer: %w", err)
		}
		row.FromNode, row.ToNode = domain.NodeID(from), domain.NodeID(to)
		row.DispatchedAt = row.DispatchedAt.UTC()
		out = append(out, row)
	}
	// ponytail: rows.Err surfaces a mid-stream network/protocol failure once
	// iteration is done; against a local embedded Postgres the whole result set
	// arrives before Next() is first called, so there is nothing left to fail on and
	// no deterministic way to fault-inject it without a flaky race or a fake driver.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate open transfers: %w", err)
	}
	return out, nil
}

// FailTransfer marks a transfer as failed.
func (p *Postgres) FailTransfer(ctx context.Context, transferID string) error {
	if _, err := p.pool.Exec(ctx,
		`UPDATE in_transit SET failed = true WHERE transfer_id = $1`, transferID); err != nil {
		return fmt.Errorf("fail transfer %s: %w", transferID, err)
	}
	return nil
}

// RecordDecision records or updates arbitration's outcome for one event.
func (p *Postgres) RecordDecision(ctx context.Context, d Decision) error {
	compNode, compSeq := "", uint64(0)
	if d.CompensatingID != nil {
		compNode, compSeq = string(d.CompensatingID.NodeID), d.CompensatingID.Seq
	}
	if _, err := p.pool.Exec(ctx, `INSERT INTO decisions
		(event_node, event_seq, verdict, reason, comp_node, comp_seq)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (event_node, event_seq) DO UPDATE SET verdict = excluded.verdict,
			reason = excluded.reason, comp_node = excluded.comp_node, comp_seq = excluded.comp_seq`,
		string(d.EventID.NodeID), d.EventID.Seq, string(d.Verdict), d.Reason,
		compNode, compSeq); err != nil {
		return fmt.Errorf("record decision for %s: %w", d.EventID, err)
	}
	return nil
}

// Decision reads arbitration's outcome for one event.
func (p *Postgres) Decision(ctx context.Context, id domain.EventID) (Decision, bool, error) {
	d := Decision{EventID: id}
	var verdict, compNode string
	var compSeq int64
	err := p.pool.QueryRow(ctx, `SELECT verdict, reason, comp_node, comp_seq FROM decisions
		WHERE event_node = $1 AND event_seq = $2`, string(id.NodeID), id.Seq).
		Scan(&verdict, &d.Reason, &compNode, &compSeq)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Decision{}, false, nil
	case err != nil:
		return Decision{}, false, fmt.Errorf("read decision for %s: %w", id, err)
	}
	d.Verdict = Verdict(verdict)
	if compNode != "" {
		d.CompensatingID = &domain.EventID{NodeID: domain.NodeID(compNode), Seq: uint64(compSeq)}
	}
	return d, true, nil
}
