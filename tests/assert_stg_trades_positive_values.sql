-- price, quantity and trade_id must be strictly positive.
select *
from {{ ref('stg_trades') }}
where price <= 0
   or quantity <= 0
   or trade_id <= 0
