---
name: flatten-variant
description: Scans a Snowflake schema to find VARIANT columns and automatically generates a complete dbt project (staging, intermediate, marts) with LATERAL FLATTEN
tools:
- snowflake_sql_execute
- snowflake_object_search
---

# When to Use

- The user asks to flatten VARIANT columns in a schema
- The user wants to create dbt staging models from tables containing JSON/VARIANT data
- The user wants to explore and flatten Snowflake semi-structured data
- The user wants to create a complete dbt project (staging → intermediate → marts) from VARIANT tables
- Keywords: flatten, variant, json, semi-structured, staging, intermediate, marts, dbt project

# Project Context (crypto real-time)

The RAW tables are fed by **Snowpipe Streaming** with the **raw Binance combined-stream message** stored in a single VARIANT column `RECORD`:

- `RECORD:stream` = the stream name, e.g. `"btcusdt@trade"` or `"btcusdt@depth20@1000ms"`.
  Derive the symbol with `UPPER(SPLIT_PART(RECORD:stream, '@', 1))`.
- **Trades** (`RAW.CRYPTO.RAW_TRADES`): fields live under `RECORD:data`
  (`e` event type, `E` event time ms, `s` symbol, `t` trade id, `p` price,
  `q` quantity, `T` trade time ms, `m` is-buyer-market-maker).
  Convert ms epochs with `TO_TIMESTAMP_NTZ(<ms>, 3)`.
- **Order book** (`RAW.CRYPTO.RAW_DEPTH`): `RECORD:data` has `lastUpdateId`,
  `bids` and `asks` which are **arrays of `[price, qty]` pairs** → use
  `LATERAL FLATTEN` to explode into one row per (symbol, side, level, price, qty).

# What This Skill Provides

Automatically detects VARIANT columns, analyzes their JSON structure, and generates a 3-layer dbt project:

- **Staging**: flatten VARIANT columns with inline `LATERAL FLATTEN` / dot-notation, sourced directly from RAW tables (`{{ source() }}`), materialized as **views**.
- **Intermediate**: enrichment and joins between staging models (`{{ ref('stg_...') }}`), materialized as **views** (zero storage, inspectable).
- **Marts**: dimensions and facts (`{{ ref('int_...') }}`), ready for consumption.

# Instructions

## Step 1: Identify tables with VARIANT columns

```sql
SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
FROM {database}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{schema}'
  AND DATA_TYPE = 'VARIANT'
ORDER BY TABLE_NAME, ORDINAL_POSITION;
```

## Step 2: Analyze the structure of VARIANT columns

```sql
SELECT f.key AS key_name, TYPEOF(f.value) AS value_type, COUNT(*) AS occurrences
FROM {database}.{schema}.{table},
     LATERAL FLATTEN(input => {variant_column}, RECURSIVE => FALSE) f
GROUP BY f.key, TYPEOF(f.value)
ORDER BY f.key;
```

For ARRAY keys, find the max size:

```sql
SELECT MAX(ARRAY_SIZE({variant_column}:{array_key})) AS max_size
FROM {database}.{schema}.{table};
```

For OBJECT keys, explore sub-keys:

```sql
SELECT DISTINCT f2.key AS sub_key, TYPEOF(f2.value) AS sub_type
FROM {database}.{schema}.{table},
     LATERAL FLATTEN(input => {variant_column}:{object_key}) f2
ORDER BY f2.key;
```

## Step 3: Generate the dbt staging models

Write the flatten directly in SQL, choosing the pattern per key type:

- **Scalars** (`p`, `q`, `t`, ...): colon/dot access + cast, e.g. `record:data:p::number(38,8) AS price`.
- **Nested objects**: dot access to sub-keys, e.g. `record:data:location.city::string AS city`.
- **Arrays** (`bids`, `asks`): `LATERAL FLATTEN(input => record:data:bids)` to get one row per element, with `f.index` as the level and `f.value[0]/[1]` as the fields.

### Best Practices

- Always keep the scalar columns in addition to flattened ones.
- Explicitly cast types: `::STRING`, `::NUMBER`, `::DATE`, `::TIMESTAMP_NTZ`.
- Convert Binance epoch-ms to timestamps with `TO_TIMESTAMP_NTZ(<ms>, 3)`.
- **Financial precision**: cast every monetary or quantity field (price, quantity, volume, notional) to `NUMBER(38,8)` — never `FLOAT`.
- **CTE naming**: use descriptive CTE names that do NOT clash with source column names (e.g. `bid_levels` / `ask_levels`, not `bids` / `asks`), and qualify flatten inputs: `LATERAL FLATTEN(input => source.bids)`.
- For arrays, base `max_index` on the actual `MAX(ARRAY_SIZE(...))`.
- Name aliases in UPPER_CASE separated by underscores.
- `stg_trades` → one row per trade. `stg_depth_levels` → one row per (symbol, side, level).

## dbt Layer Architecture Rule

```
sources (RAW tables) → staging → intermediate → marts
```

- **Staging**: `{{ source('raw', '...') }}` only.
- **Intermediate**: `{{ ref('stg_...') }}` only.
- **Marts**: `{{ ref('int_...') }}` only.
- A marts model must NEVER reference a staging model or a source directly; an intermediate model must NEVER reference a source.

## Step 4: Generate intermediate models

Enrich staging via joins/aggregations, sourced exclusively from staging models. Materialized as views.

## Step 5: Generate marts models

Dimensions (`dim_`) and facts (`fct_`), sourced exclusively from intermediate models.

## Step 6: Add _sources.yml and _schema.yml (doc + STRUCTURAL tests only)

This skill owns the **structure + documentation**, NOT the business rules:
- `description:` on every model and key column (documentation, surfaced by `dbt docs`).
- `not_null` / `unique` on **keys** (primary / surrogate), `accepted_values` on obvious enums (e.g. `side`).
- Optionally **model contracts** (`config: contract: {enforced: true}` + column `data_type`) on the marts,
  to fail the build on output-schema drift.

Do NOT add business-rule tests here (ranges, cross-field invariants, RSI, transformation logic) — those
are owned by the `generate-quality-tests` skill. This avoids duplicate tests.

`_sources.yml` declares the RAW source:

```yaml
version: 2
sources:
  - name: raw
    database: RAW
    schema: CRYPTO
    tables:
      - name: raw_trades
      - name: raw_depth
```

## Step 7: Compile, run, test

Run `dbt compile`, then `dbt run`, then `dbt test`. Fix any failure before finishing.

# Examples

## Example 1: Full project from the crypto RAW tables

User: `$flatten-variant` Scan the CRYPTO schema of the RAW database and create a complete dbt project from RAW_TRADES and RAW_DEPTH
Assistant:
1. Finds VARIANT columns (`RECORD`) in both tables.
2. Analyzes `RECORD:data` (trades) and `RECORD:data:bids/asks` (depth arrays).
3. Generates `stg_trades` (flatten scalars + epoch→timestamp) and `stg_depth_levels` (LATERAL FLATTEN of bids/asks).
4. Generates intermediate (e.g. `int_orderbook_top`) and marts.
5. Adds `_sources.yml` + `_schema.yml` (not_null + unique), then runs compile/run/test.
