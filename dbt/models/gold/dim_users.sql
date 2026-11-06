{{ config(engine='MergeTree()', order_by='user_id') }}
-- Users as they are now (no email / phone: not needed for analytics).
select
    user_id,
    full_name,
    city,
    status,
    created_at,
    updated_at,
    _silver_updated_at
from {{ ref('stg_users') }}
