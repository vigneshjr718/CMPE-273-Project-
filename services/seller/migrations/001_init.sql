-- Seller schema v1. Applied by app/migrate.py (once, tracked in schema_migrations).
-- Each seller instance has its own database, so there is no seller_id column.

-- ---------------------------------------------------------------------------
-- inventory: one row per part this seller carries.
-- Invariant (enforced by CHECK): available + reserved + delivered = total.
-- Every stock movement is a single UPDATE on this row, so the row lock is the
-- serialization point for competing reservations of the same part.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory (
    part_id             TEXT PRIMARY KEY,
    description         TEXT        NOT NULL,
    unit_price_cents    INTEGER     NOT NULL CHECK (unit_price_cents > 0),
    total_quantity      INTEGER     NOT NULL CHECK (total_quantity >= 0),
    available_quantity  INTEGER     NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity   INTEGER     NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
    delivered_quantity  INTEGER     NOT NULL DEFAULT 0 CHECK (delivered_quantity >= 0),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT inventory_conservation
        CHECK (available_quantity + reserved_quantity + delivered_quantity = total_quantity)
);

-- ---------------------------------------------------------------------------
-- reservations: one row per successful reserve. Never deleted.
-- operation_key UNIQUE = at most one reservation per key, enforced by the DB.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id          UUID        PRIMARY KEY,
    operation_key           TEXT        NOT NULL UNIQUE,
    order_id                TEXT        NOT NULL,
    line_id                 TEXT        NOT NULL,
    part_id                 TEXT        NOT NULL REFERENCES inventory(part_id),
    quantity                INTEGER     NOT NULL CHECK (quantity > 0),
    unit_price_cents        INTEGER     NOT NULL,
    status                  TEXT        NOT NULL DEFAULT 'RESERVED'
                            CHECK (status IN ('RESERVED', 'CANCELLED', 'DELIVERED')),
    needed_by               TIMESTAMPTZ,
    delivery_status         TEXT        NOT NULL DEFAULT 'PENDING'
                            CHECK (delivery_status IN ('PENDING', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
    estimated_delivery_at   TIMESTAMPTZ NOT NULL,
    cancel_reason           TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_at            TIMESTAMPTZ,
    delivered_at            TIMESTAMPTZ,
    CONSTRAINT status_matches_delivery CHECK (
        (status = 'CANCELLED') = (delivery_status = 'CANCELLED')
        AND (status = 'DELIVERED') = (delivery_status = 'DELIVERED')
    )
);

CREATE INDEX IF NOT EXISTS reservations_order_idx  ON reservations (order_id, line_id);
CREATE INDEX IF NOT EXISTS reservations_active_idx ON reservations (status) WHERE status = 'RESERVED';

-- ---------------------------------------------------------------------------
-- idempotency_records: one row per operation key = the durable truth about
-- that operation, whatever happened to it.
--   outcome RESERVED  -> reservation_id set (live state lives in reservations)
--   outcome REJECTED  -> error_code: definitive failure, no stock was taken
--   outcome FENCED    -> tombstone: cancelled before any reserve arrived
-- The PRIMARY KEY on operation_key serializes every request for one key
-- (reserve, retry, cancel): a second INSERT ... ON CONFLICT waits for the first.
--
-- status_version: monotonic counter PER OPERATION, shared by reservation,
-- delivery and cancellation observations. Bumped in the same transaction as
-- every observable change; identical re-reads return the same version.
-- cancellation_receipt: durable proof issued once when the operation is
-- terminally fenced (cancelled, tombstoned, or a rejected key cancelled).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS idempotency_records (
    operation_key        TEXT        PRIMARY KEY,
    request_hash         TEXT,                     -- NULL for FENCED tombstones
    request_body         JSONB,
    order_id             TEXT,
    line_id              TEXT,
    part_id              TEXT,
    quantity             INTEGER,
    outcome              TEXT        NOT NULL CHECK (outcome IN ('RESERVED', 'REJECTED', 'FENCED')),
    response_code        INTEGER     NOT NULL,
    error_code           TEXT,
    error_message        TEXT,
    -- Deferred: seller_reserve() claims the key (inserting this row with the
    -- reservation_id it is about to use) BEFORE it inserts the reservation.
    reservation_id       UUID        REFERENCES reservations(reservation_id) DEFERRABLE INITIALLY DEFERRED,
    status_version       INTEGER     NOT NULL DEFAULT 1 CHECK (status_version >= 1),
    cancellation_key     TEXT,
    cancellation_receipt UUID        UNIQUE,
    cancelled_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT reserved_has_reservation CHECK ((outcome = 'RESERVED') = (reservation_id IS NOT NULL)),
    CONSTRAINT fenced_has_receipt CHECK (outcome <> 'FENCED' OR cancellation_receipt IS NOT NULL)
);
