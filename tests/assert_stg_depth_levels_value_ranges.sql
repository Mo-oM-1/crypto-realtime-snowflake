-- price must be positive, qty non-negative, level non-negative.
select *
from {{ ref('stg_depth_levels') }}
where price <= 0
   or qty < 0
   or level < 0
