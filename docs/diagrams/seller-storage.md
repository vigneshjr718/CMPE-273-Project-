# Seller storage (Package C)

Five seller instances, one image, one database each (`seller1_db` … `seller5_db`,
in the shared Postgres container, isolated by role + CONNECT privilege).

## Schema

```mermaid
erDiagram
    inventory ||--o{ reservations : "part_id"
    reservations ||--o| idempotency_records : "reservation_id"

    inventory {
        text part_id PK
        text description
        int unit_price_cents
        int total_quantity "CHECK available+reserved+delivered = total"
        int available_quantity "CHECK >= 0"
        int reserved_quantity
        int delivered_quantity
    }
    reservations {
        uuid reservation_id PK
        text operation_key UK "at most one reservation per key"
        text order_id
        text line_id
        text part_id FK
        int quantity
        text status "RESERVED | CANCELLED | DELIVERED"
        text delivery_status "PENDING | SHIPPED | DELIVERED | CANCELLED"
        timestamptz estimated_delivery_at
        timestamptz needed_by
    }
    idempotency_records {
        text operation_key PK "serializes same-key requests"
        text request_hash "NULL for tombstones"
        text outcome "RESERVED | REJECTED | FENCED"
        text error_code
        uuid reservation_id FK "deferred"
        int status_version "per operation, shared by delivery + cancel replies"
        text cancellation_key
        uuid cancellation_receipt UK "durable fence proof"
    }
```

## Reserve: one transaction, one round-trip (`seller_reserve`)

```mermaid
sequenceDiagram
    participant B as Coordinator
    participant S as Seller API
    participant DB as seller DB
    B->>B: persist attempt + operation_key
    B->>S: POST /reservations (Idempotency-Key: K)
    S->>DB: SELECT seller_reserve(K, hash, ...)
    Note over DB: BEGIN
    DB->>DB: INSERT idempotency_records(K) ON CONFLICT DO NOTHING<br/>(waits if K is in flight elsewhere)
    alt K already recorded
        DB-->>S: replay 200 RESERVED / stored 409 REJECTED / 409 REJECTED OPERATION_CANCELLED / 409 KEY_REUSED
    else first time
        DB->>DB: UPDATE inventory SET available -= q WHERE available >= q<br/>(row lock = last-unit serialization point)
        alt no row updated
            DB->>DB: record REJECTED (INSUFFICIENT_STOCK / PART_NOT_FOUND)
        else
            DB->>DB: INSERT reservations
        end
    end
    Note over DB: COMMIT
    S-->>B: 201 RESERVED / 200 replay / 409 REJECTED
```

## Timeout → reconcile → retry or fence

```mermaid
sequenceDiagram
    participant B as Coordinator
    participant S as Seller
    B->>S: POST /reservations (K)
    Note over S: committed... reply lost
    B--xB: timeout => UNKNOWN
    B->>S: GET /reservations/by-key/K
    alt 200 status=RESERVED
        S-->>B: reservation (no second reserve needed)
    else 200 status=REJECTED
        S-->>B: definitive, no stock taken
    else 404 KEY_NOT_FOUND (absence of evidence only)
        alt still want stock from this seller
            B->>S: POST /reservations (same K)  -- safe
        else give up on this seller
            B->>S: POST /reservation-operations/K/cancel (cancellation key C)
            S-->>B: 200 CANCELLED, fenced=true, receipt (tombstone)
            Note over S: delayed original reserve(K) arrives -> 409 REJECTED OPERATION_CANCELLED
        end
    end
```

## Late delivery → confirmed cancellation → replacement

```mermaid
sequenceDiagram
    participant B as Coordinator
    participant S1 as Seller (late)
    participant S2 as Seller (replacement)
    B->>S1: GET /reservations/{id}/delivery
    S1-->>B: IN_TRANSIT, estimated_delivery_at > needed_by, status_version=v
    B->>S1: POST /reservation-operations/K/cancel (cancellation key C)
    alt 200 CANCELLED, fenced, receipt, status_version > v
        S1-->>B: terminal proof (stock restored exactly once)
        B->>S2: POST /reservations (new key K2)
    else 200 DELIVERED
        S1-->>B: delivered_at: keep it, no replacement
    else 202 PENDING (shipped)
        S1-->>B: not fenced: retry the same C later
    else timeout / 5xx
        B->>S1: retry with the same C (idempotent) until proof, else RECONCILIATION_REQUIRED
    end
```
