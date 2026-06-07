-- Not part of $flatten-variant scope (dim_symbols is produced by $realtime-marts).
-- Disabled to avoid a resource-name collision with the seed `dim_symbols`.
{{ config(enabled=false) }}

select 1 as placeholder
