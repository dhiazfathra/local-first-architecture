-- Post-deploy migrations: statements that cannot run inside the transaction block
-- schema.sql's multi-statement batch implies, so OpenPostgres applies this file as
-- its own single-statement Exec calls, outside any transaction.

-- Supports OpenTransfers' scan, which otherwise walks every row in a table that
-- grows with every dispatched transfer line. CONCURRENTLY avoids write-blocking
-- locks on in_transit while the index builds.
CREATE INDEX CONCURRENTLY IF NOT EXISTS in_transit_open ON in_transit (dispatched_at)
    WHERE failed = false AND dispatched - received > 0;
