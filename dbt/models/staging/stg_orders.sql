-- Current, non-deleted orders from silver.orders (inlined into the gold models: ephemeral).
select
    order_id,
    user_id,
    status,
    payment_method,
    shipping_city,
    total_amount,
    currency,
    toDateTime64(ordered_at, 6, 'Asia/Ho_Chi_Minh') as ordered_at,
    toDateTime64(created_at, 6, 'Asia/Ho_Chi_Minh') as created_at,
    toDateTime64(updated_at, 6, 'Asia/Ho_Chi_Minh') as updated_at,
    toDateTime64(_silver_updated_at, 6, 'Asia/Ho_Chi_Minh') as _silver_updated_at
from postgresql(silver_pg, table = 'orders')
where _is_current and not _is_deleted
