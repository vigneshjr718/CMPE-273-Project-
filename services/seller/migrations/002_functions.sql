-- Seller stock operations as single-call, single-transaction DB functions.
--
-- Why in the DB: each operation is one round-trip and one transaction, and the
-- exact logic that runs in production is what the race tests exercise.
-- The API layer only validates input, calls these, and shapes the HTTP reply.
--
-- Concurrency model (READ COMMITTED, the Postgres default):
--   * Same operation key: INSERT ... ON CONFLICT on idempotency_records(PK)
--     blocks until the other transaction commits/rolls back, then sees its row.
--   * Same part: UPDATE inventory ... WHERE available_quantity >= qty takes the
--     row lock and re-checks the condition after waiting, so two requests for
--     the last unit cannot both succeed.
--   * Lock order is ALWAYS idempotency_records -> reservations -> inventory,
--     in every function, so they cannot deadlock each other.

-- ---------------------------------------------------------------------------
-- seller_reserve
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION seller_reserve(
    p_key          TEXT,
    p_hash         TEXT,
    p_body         JSONB,
    p_order_id     TEXT,
    p_line_id      TEXT,
    p_part_id      TEXT,
    p_qty          INTEGER,
    p_needed_by    TIMESTAMPTZ,
    p_eta          TIMESTAMPTZ,
    p_new_id       UUID
) RETURNS TABLE (
    result          TEXT,     -- RESERVED | REPLAYED | REJECTED | KEY_REUSED | OPERATION_CANCELLED
    http_status     INTEGER,
    error_code      TEXT,
    reservation_id  UUID
)
LANGUAGE plpgsql AS $$
DECLARE
    rec      idempotency_records%ROWTYPE;
    v_status TEXT;
    v_price  INTEGER;
    v_code   TEXT;
    v_http   INTEGER;
BEGIN
    IF p_qty IS NULL OR p_qty <= 0 THEN
        RAISE EXCEPTION 'quantity must be positive' USING ERRCODE = '22023';
    END IF;

    -- 1. Claim the key. If another transaction holds it, this waits for it.
    INSERT INTO idempotency_records AS ir
        (operation_key, request_hash, request_body, order_id, line_id, part_id, quantity,
         outcome, response_code, reservation_id, status_version)
    VALUES (p_key, p_hash, p_body, p_order_id, p_line_id, p_part_id, p_qty,
            'RESERVED', 201, p_new_id, 1)
    ON CONFLICT (operation_key) DO NOTHING;

    IF NOT FOUND THEN
        -- 2. Key already has a durable outcome: replay it, never re-execute.
        SELECT * INTO rec FROM idempotency_records ir WHERE ir.operation_key = p_key;

        -- A cancelled/fenced key answers OPERATION_CANCELLED to ANY late reserve,
        -- whatever its body: the fence is about the key, not the payload.
        IF rec.outcome = 'FENCED' THEN
            RETURN QUERY SELECT 'OPERATION_CANCELLED'::TEXT, 409, 'OPERATION_CANCELLED'::TEXT, NULL::UUID;
            RETURN;
        END IF;
        IF rec.outcome = 'RESERVED' THEN
            SELECT r.status INTO v_status FROM reservations r WHERE r.reservation_id = rec.reservation_id;
            IF v_status = 'CANCELLED' THEN
                RETURN QUERY SELECT 'OPERATION_CANCELLED'::TEXT, 409, 'OPERATION_CANCELLED'::TEXT, rec.reservation_id;
                RETURN;
            END IF;
        END IF;

        IF rec.request_hash IS DISTINCT FROM p_hash THEN
            RETURN QUERY SELECT 'KEY_REUSED'::TEXT, 409, 'IDEMPOTENCY_KEY_REUSED'::TEXT, rec.reservation_id;
            RETURN;
        END IF;

        IF rec.outcome = 'REJECTED' THEN
            RETURN QUERY SELECT 'REJECTED'::TEXT, rec.response_code, rec.error_code, NULL::UUID;
            RETURN;
        END IF;

        RETURN QUERY SELECT 'REPLAYED'::TEXT, 200, NULL::TEXT, rec.reservation_id;
        RETURN;
    END IF;

    -- 3. First time for this key: take stock atomically.
    UPDATE inventory i
       SET available_quantity = i.available_quantity - p_qty,
           reserved_quantity  = i.reserved_quantity  + p_qty,
           updated_at         = now()
     WHERE i.part_id = p_part_id
       AND i.available_quantity >= p_qty
    RETURNING i.unit_price_cents INTO v_price;

    IF NOT FOUND THEN
        IF EXISTS (SELECT 1 FROM inventory i WHERE i.part_id = p_part_id) THEN
            v_code := 'INSUFFICIENT_STOCK'; v_http := 409;
        ELSE
            v_code := 'PART_NOT_FOUND';     v_http := 409;
        END IF;
        -- Record the definitive rejection so retries with this key get the same answer.
        UPDATE idempotency_records ir
           SET outcome = 'REJECTED', response_code = v_http, error_code = v_code, reservation_id = NULL
         WHERE ir.operation_key = p_key;
        RETURN QUERY SELECT 'REJECTED'::TEXT, v_http, v_code, NULL::UUID;
        RETURN;
    END IF;

    INSERT INTO reservations
        (reservation_id, operation_key, order_id, line_id, part_id, quantity,
         unit_price_cents, needed_by, estimated_delivery_at)
    VALUES
        (p_new_id, p_key, p_order_id, p_line_id, p_part_id, p_qty,
         v_price, p_needed_by, p_eta);

    RETURN QUERY SELECT 'RESERVED'::TEXT, 201, NULL::TEXT, p_new_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- seller_cancel_operation: terminal fencing of an ORIGINAL operation key.
--
-- Works whether or not the seller has seen the key:
--   unseen key       -> tombstone (FENCED); a late reserve with the key is rejected
--   rejected key     -> receipt issued; the key could never take stock anyway
--   reserved key     -> reservation cancelled + stock restored exactly once
--   already fenced   -> same receipt and version again (idempotent)
--   shipped          -> PENDING (cannot be stopped any more; not fenced)
--   delivered        -> DELIVERED (never fenced)
--
-- http_status: 200 terminal (CANCELLED/DELIVERED), 202 PENDING, 409 mismatch.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION seller_cancel_operation(
    p_key            TEXT,
    p_cancel_key     TEXT,
    p_reason         TEXT,
    p_order_id       TEXT,
    p_line_id        TEXT,
    p_part_id        TEXT,
    p_qty            INTEGER,
    p_reservation_id UUID
) RETURNS TABLE (
    http_status        INTEGER,
    error_code         TEXT,
    status             TEXT,      -- CANCELLED | PENDING | DELIVERED
    reservation_id     UUID,
    part_id            TEXT,
    quantity           INTEGER,
    status_version     INTEGER,
    fenced             BOOLEAN,
    receipt            UUID,
    delivered_at       TIMESTAMPTZ,
    already_cancelled  BOOLEAN,
    stock_restored     INTEGER
)
LANGUAGE plpgsql AS $$
DECLARE
    rec idempotency_records%ROWTYPE;
    r   reservations%ROWTYPE;
    v_receipt UUID;
BEGIN
    -- 1. Unseen key: write the tombstone. If the key exists or is in flight
    --    elsewhere, this waits for that transaction and then does nothing.
    v_receipt := gen_random_uuid();
    INSERT INTO idempotency_records AS ir
        (operation_key, request_hash, order_id, line_id, part_id, quantity,
         outcome, response_code, error_code, error_message, status_version,
         cancellation_key, cancellation_receipt, cancelled_at)
    VALUES (p_key, NULL, p_order_id, p_line_id, p_part_id, p_qty,
            'FENCED', 409, 'OPERATION_CANCELLED', p_reason, 1,
            p_cancel_key, v_receipt, now())
    ON CONFLICT (operation_key) DO NOTHING;

    IF FOUND THEN
        RETURN QUERY SELECT 200, NULL::TEXT, 'CANCELLED'::TEXT, NULL::UUID, p_part_id, p_qty,
                            1, TRUE, v_receipt, NULL::TIMESTAMPTZ, FALSE, 0;
        RETURN;
    END IF;

    -- 2. Known key: lock its record (lock order: record -> reservation -> inventory).
    SELECT * INTO rec FROM idempotency_records ir WHERE ir.operation_key = p_key FOR UPDATE;

    IF (rec.part_id IS NOT NULL AND (rec.part_id <> p_part_id OR rec.quantity <> p_qty))
       OR (p_reservation_id IS NOT NULL AND rec.reservation_id IS DISTINCT FROM p_reservation_id) THEN
        RETURN QUERY SELECT 409, 'OPERATION_MISMATCH'::TEXT, NULL::TEXT, rec.reservation_id,
                            rec.part_id, rec.quantity, rec.status_version, FALSE, NULL::UUID,
                            NULL::TIMESTAMPTZ, FALSE, 0;
        RETURN;
    END IF;

    IF rec.outcome IN ('FENCED', 'REJECTED') THEN
        IF rec.cancellation_receipt IS NULL THEN     -- first cancel of a rejected key
            UPDATE idempotency_records ir
               SET cancellation_key = p_cancel_key, cancellation_receipt = v_receipt,
                   cancelled_at = now(), status_version = ir.status_version + 1, updated_at = now()
             WHERE ir.operation_key = p_key
            RETURNING * INTO rec;
            RETURN QUERY SELECT 200, NULL::TEXT, 'CANCELLED'::TEXT, NULL::UUID, rec.part_id, rec.quantity,
                                rec.status_version, TRUE, rec.cancellation_receipt, NULL::TIMESTAMPTZ, FALSE, 0;
        ELSE
            RETURN QUERY SELECT 200, NULL::TEXT, 'CANCELLED'::TEXT, NULL::UUID, rec.part_id, rec.quantity,
                                rec.status_version, TRUE, rec.cancellation_receipt, NULL::TIMESTAMPTZ, TRUE, 0;
        END IF;
        RETURN;
    END IF;

    -- 3. Key holds (or held) a reservation.
    SELECT * INTO r FROM reservations x WHERE x.reservation_id = rec.reservation_id FOR UPDATE;

    IF r.status = 'CANCELLED' THEN
        RETURN QUERY SELECT 200, NULL::TEXT, 'CANCELLED'::TEXT, r.reservation_id, r.part_id, r.quantity,
                            rec.status_version, TRUE, rec.cancellation_receipt, NULL::TIMESTAMPTZ, TRUE, 0;
        RETURN;
    END IF;

    IF r.status = 'DELIVERED' THEN
        RETURN QUERY SELECT 200, NULL::TEXT, 'DELIVERED'::TEXT, r.reservation_id, r.part_id, r.quantity,
                            rec.status_version, FALSE, NULL::UUID, r.delivered_at, FALSE, 0;
        RETURN;
    END IF;

    IF r.delivery_status = 'SHIPPED' THEN
        -- Physically dispatched: cannot honestly promise "no delivery will happen".
        RETURN QUERY SELECT 202, NULL::TEXT, 'PENDING'::TEXT, r.reservation_id, r.part_id, r.quantity,
                            rec.status_version, FALSE, NULL::UUID, NULL::TIMESTAMPTZ, FALSE, 0;
        RETURN;
    END IF;

    UPDATE reservations x
       SET status = 'CANCELLED', delivery_status = 'CANCELLED',
           cancel_reason = p_reason, cancelled_at = now(), updated_at = now()
     WHERE x.reservation_id = r.reservation_id;

    UPDATE inventory i
       SET available_quantity = i.available_quantity + r.quantity,
           reserved_quantity  = i.reserved_quantity  - r.quantity,
           updated_at         = now()
     WHERE i.part_id = r.part_id;

    UPDATE idempotency_records ir
       SET cancellation_key = p_cancel_key, cancellation_receipt = v_receipt,
           cancelled_at = now(), status_version = ir.status_version + 1, updated_at = now()
     WHERE ir.operation_key = p_key
    RETURNING * INTO rec;

    RETURN QUERY SELECT 200, NULL::TEXT, 'CANCELLED'::TEXT, r.reservation_id, r.part_id, r.quantity,
                        rec.status_version, TRUE, rec.cancellation_receipt, NULL::TIMESTAMPTZ, FALSE, r.quantity;
END;
$$;

-- ---------------------------------------------------------------------------
-- seller_set_delivery: demo/admin control of delivery progress.
-- PENDING -> SHIPPED -> DELIVERED (PENDING -> DELIVERED allowed). ETA can be
-- moved while not delivered. DELIVERED moves units reserved -> delivered.
-- Any real change bumps the operation's status_version; a no-op does not.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION seller_set_delivery(
    p_reservation_id UUID,
    p_status         TEXT,          -- NULL = leave unchanged
    p_eta            TIMESTAMPTZ    -- NULL = leave unchanged
) RETURNS TABLE (http_status INTEGER, error_code TEXT)
LANGUAGE plpgsql AS $$
DECLARE
    v_key TEXT;
    r     reservations%ROWTYPE;
BEGIN
    SELECT x.operation_key INTO v_key FROM reservations x WHERE x.reservation_id = p_reservation_id;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 404, 'RESERVATION_NOT_FOUND'::TEXT; RETURN;
    END IF;
    -- Same lock order as seller_cancel_operation: record first, then reservation.
    PERFORM 1 FROM idempotency_records ir WHERE ir.operation_key = v_key FOR UPDATE;
    SELECT * INTO r FROM reservations x WHERE x.reservation_id = p_reservation_id FOR UPDATE;

    IF r.status = 'CANCELLED' THEN
        RETURN QUERY SELECT 409, 'RESERVATION_CANCELLED'::TEXT; RETURN;
    END IF;
    IF p_status IS NOT NULL AND p_status NOT IN ('PENDING', 'SHIPPED', 'DELIVERED') THEN
        RETURN QUERY SELECT 422, 'VALIDATION_ERROR'::TEXT; RETURN;
    END IF;
    IF r.status = 'DELIVERED' THEN
        IF (p_status IS NULL OR p_status = 'DELIVERED') AND p_eta IS NULL THEN
            RETURN QUERY SELECT 200, NULL::TEXT; RETURN;          -- idempotent repeat
        END IF;
        RETURN QUERY SELECT 409, 'INVALID_TRANSITION'::TEXT; RETURN;
    END IF;
    IF p_status = 'PENDING' AND r.delivery_status = 'SHIPPED' THEN
        RETURN QUERY SELECT 409, 'INVALID_TRANSITION'::TEXT; RETURN;
    END IF;

    IF (p_status IS NULL OR p_status = r.delivery_status)
       AND (p_eta IS NULL OR p_eta = r.estimated_delivery_at) THEN
        RETURN QUERY SELECT 200, NULL::TEXT; RETURN;              -- nothing changed: same version
    END IF;

    IF p_status = 'DELIVERED' THEN
        UPDATE reservations x
           SET status = 'DELIVERED', delivery_status = 'DELIVERED',
               estimated_delivery_at = COALESCE(p_eta, x.estimated_delivery_at),
               delivered_at = now(), updated_at = now()
         WHERE x.reservation_id = p_reservation_id;
        UPDATE inventory i
           SET reserved_quantity  = i.reserved_quantity  - r.quantity,
               delivered_quantity = i.delivered_quantity + r.quantity,
               updated_at = now()
         WHERE i.part_id = r.part_id;
    ELSE
        UPDATE reservations x
           SET delivery_status = COALESCE(p_status, x.delivery_status),
               estimated_delivery_at = COALESCE(p_eta, x.estimated_delivery_at),
               updated_at = now()
         WHERE x.reservation_id = p_reservation_id;
    END IF;

    UPDATE idempotency_records ir
       SET status_version = ir.status_version + 1, updated_at = now()
     WHERE ir.operation_key = v_key;

    RETURN QUERY SELECT 200, NULL::TEXT;
END;
$$;
