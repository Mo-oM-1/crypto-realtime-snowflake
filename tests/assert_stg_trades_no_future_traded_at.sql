-- traded_at is TIMESTAMP_NTZ in UTC; compare to SYSDATE() (UTC, NTZ)
-- to avoid TIMESTAMP_LTZ/NTZ session-timezone false positives.
select *
from {{ ref('stg_trades') }}
where traded_at > sysdate()
