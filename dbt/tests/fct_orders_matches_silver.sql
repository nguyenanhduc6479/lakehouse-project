-- Gold must hold exactly the current, non-deleted orders of silver at the time of the run:
-- returns a row (= test failure) when the counts differ.
select gold, silver
from (
    select
        (select count() from {{ ref('fct_orders') }}) as gold,
        (select count() from postgresql(silver_pg, table = 'orders')
         where _is_current and not _is_deleted) as silver
)
where gold != silver
