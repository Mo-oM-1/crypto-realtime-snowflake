-- price, quantity, trade_id and trade_value must be strictly positive.
select *
from {{ ref('fct_trades') }}
where price <= 0
   or quantity <= 0
   or trade_id <= 0
   or trade_value <= 0
