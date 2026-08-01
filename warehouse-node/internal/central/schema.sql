-- The replicated event log. Partitioned by list on node_id so each warehouse's
-- events live in their own partition; the DEFAULT partition means onboarding a new
-- warehouse needs no migration.
CREATE TABLE IF NOT EXISTS events (
    node_id       TEXT        NOT NULL,
    seq           BIGINT      NOT NULL,
    aggregate_id  TEXT        NOT NULL,
    type          TEXT        NOT NULL,
    hlc_wall      BIGINT      NOT NULL,
    hlc_counter   BIGINT      NOT NULL,
    hlc_node      TEXT        NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL,
    causation_node TEXT       NOT NULL DEFAULT '',
    causation_seq BIGINT      NOT NULL DEFAULT 0,
    payload       JSONB       NOT NULL,
    PRIMARY KEY (node_id, seq)
) PARTITION BY LIST (node_id);

CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

CREATE INDEX IF NOT EXISTS events_order ON events (hlc_wall, hlc_counter, hlc_node, seq);

-- Per-node sync cursors: how far central has received from the node, and how far the
-- node has acknowledged what central sent down. Both are why a week offline resumes.
CREATE TABLE IF NOT EXISTS cursors (
    node_id       TEXT   PRIMARY KEY,
    pushed_seq    BIGINT NOT NULL DEFAULT 0,
    delivered_ord BIGINT NOT NULL DEFAULT 0
);

-- The downstream queue, one row per (target node, event). ord is the monotonic
-- position a node's cursor stores.
CREATE TABLE IF NOT EXISTS outbound (
    ord         BIGSERIAL PRIMARY KEY,
    target_node TEXT   NOT NULL,
    node_id     TEXT   NOT NULL,
    seq         BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS outbound_target ON outbound (target_node, ord);

CREATE TABLE IF NOT EXISTS items (
    sku             TEXT PRIMARY KEY,
    description     TEXT    NOT NULL,
    base_uom        TEXT    NOT NULL,
    alt_uom         JSONB   NOT NULL,
    lot_tracked     BOOLEAN NOT NULL,
    shelf_life_days INTEGER NOT NULL,
    deleted         BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS purchase_orders (
    po_ref      TEXT             NOT NULL,
    sku         TEXT             NOT NULL,
    qty_ordered DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (po_ref, sku)
);

-- One row per received line, keyed by the event that produced it so a resent sync
-- batch cannot double-count a receipt against its purchase order.
CREATE TABLE IF NOT EXISTS receipts (
    event_node    TEXT             NOT NULL,
    event_seq     BIGINT           NOT NULL,
    node_id       TEXT             NOT NULL,
    po_ref        TEXT             NOT NULL,
    delivery_note TEXT             NOT NULL,
    sku           TEXT             NOT NULL,
    qty_base      DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (event_node, event_seq)
);

CREATE INDEX IF NOT EXISTS receipts_po ON receipts (po_ref, sku);
CREATE INDEX IF NOT EXISTS receipts_note ON receipts (delivery_note, sku);

CREATE TABLE IF NOT EXISTS nodes (node_id TEXT PRIMARY KEY);

CREATE TABLE IF NOT EXISTS node_rejects (
    node_id TEXT NOT NULL,
    sku     TEXT NOT NULL,
    PRIMARY KEY (node_id, sku)
);

-- In-transit balances: goods that have left the source and not yet arrived at the
-- destination. They belong to neither node. lot_id is part of the key; there is no
-- location, because in-transit stock is at no location.
CREATE TABLE IF NOT EXISTS in_transit (
    transfer_id   TEXT             NOT NULL,
    sku           TEXT             NOT NULL,
    lot_id        TEXT             NOT NULL,
    from_node     TEXT             NOT NULL,
    to_node       TEXT             NOT NULL,
    dispatched    DOUBLE PRECISION NOT NULL,
    received      DOUBLE PRECISION NOT NULL DEFAULT 0,
    dispatched_at TIMESTAMPTZ      NOT NULL,
    failed        BOOLEAN          NOT NULL DEFAULT FALSE,
    PRIMARY KEY (transfer_id, sku, lot_id)
);

-- The arbitration audit trail: what central decided about each event, why, and which
-- compensating event it emitted.
CREATE TABLE IF NOT EXISTS decisions (
    event_node TEXT   NOT NULL,
    event_seq  BIGINT NOT NULL,
    verdict    TEXT   NOT NULL,
    reason     TEXT   NOT NULL,
    comp_node  TEXT   NOT NULL DEFAULT '',
    comp_seq   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (event_node, event_seq)
);
