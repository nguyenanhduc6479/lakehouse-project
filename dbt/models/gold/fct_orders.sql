{{ config(engine='MergeTree()', order_by='order_id') }}
-- One row per current, non-deleted order. order_date is the Vietnam calendar day of ordered_at.
select
    order_id,
    user_id,
    status,
    payment_method,
    shipping_city,
    total_amount,
    currency,
    ordered_at,
    toDate(ordered_at, 'Asia/Ho_Chi_Minh') as order_date,
    created_at,
    updated_at,
    _silver_updated_at
from {{ ref('stg_orders') }}
