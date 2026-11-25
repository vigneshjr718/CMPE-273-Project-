-- Inventory correctness after a run. Every count must be 0.
SELECT 'bad_inventory_rows',  count(*) FROM inventory i
 WHERE i.reserved_quantity  <> (SELECT coalesce(sum(quantity),0) FROM reservations r WHERE r.part_id=i.part_id AND r.status='RESERVED')
    OR i.delivered_quantity <> (SELECT coalesce(sum(quantity),0) FROM reservations r WHERE r.part_id=i.part_id AND r.status='DELIVERED')
    OR i.available_quantity + i.reserved_quantity + i.delivered_quantity <> i.total_quantity
    OR i.available_quantity < 0
UNION ALL
SELECT 'keys_with_2plus_reservations', count(*) FROM (SELECT operation_key FROM reservations GROUP BY 1 HAVING count(*) > 1) x
UNION ALL
SELECT 'fenced_keys_holding_stock', count(*) FROM idempotency_records ir JOIN reservations r USING (operation_key) WHERE ir.outcome = 'FENCED'
UNION ALL
SELECT 'reserved_records_without_reservation', count(*) FROM idempotency_records ir
  LEFT JOIN reservations r ON r.reservation_id = ir.reservation_id WHERE ir.outcome = 'RESERVED' AND r.reservation_id IS NULL
UNION ALL
SELECT 'cancelled_without_receipt', count(*) FROM reservations r JOIN idempotency_records ir USING (operation_key)
  WHERE r.status = 'CANCELLED' AND ir.cancellation_receipt IS NULL;
