-- Gold must hold exactly the current, non-deleted users of silver at the time of the run.
select gold, silver
from (
    select
        (select count() from {{ ref('dim_users') }}) as gold,
        (select count() from postgresql(silver_pg, table = 'users')
         where _is_current and not _is_deleted) as silver
)
where gold != silver
