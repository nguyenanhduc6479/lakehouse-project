-- Current, non-deleted users from silver.users (inlined into the gold models: ephemeral).
select
    user_id,
    full_name,
    city,
    status,
    toDateTime64(created_at, 6, 'Asia/Ho_Chi_Minh') as created_at,
    toDateTime64(updated_at, 6, 'Asia/Ho_Chi_Minh') as updated_at,
    toDateTime64(_silver_updated_at, 6, 'Asia/Ho_Chi_Minh') as _silver_updated_at
from postgresql(silver_pg, table = 'users')
where _is_current and not _is_deleted
