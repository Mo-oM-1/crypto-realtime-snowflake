---
name: check-schema-drift
description: Detects schema drift in the raw VARIANT tables (new or type-changed JSON keys not yet mapped in the dbt staging models) and additively extends the staging models to absorb it. Self-healing flatten.
tools:
- snowflake_sql_execute
- snowflake_object_search
---

# When to Use

- Check whether the source JSON gained new fields not yet flattened.
- The Binance payload changed and the staging models may be out of sync.
- Keep `stg_*` in sync with the raw VARIANT automatically (self-healing).
- Keywords: schema drift, new field, variant, staging sync, self-healing, payload change.

# Project Context

- Raw tables: `RAW.CRYPTO.RAW_TRADES`, `RAW.CRYPTO.RAW_DEPTH`, each with a VARIANT column `RECORD`
  holding the full Binance combined-stream message (`RECORD:stream`, `RECORD:data`).
- Staging models live in `ANALYTICS.PUBLIC_STAGING` (`STG_TRADES`, `STG_DEPTH_LEVELS`).
- Conventions (must be respected when adding fields): money/quantity → `NUMBER(38,8)`;
  epoch-ms → `TO_TIMESTAMP_NTZ(<ms>, 3)`; booleans → `::BOOLEAN`; aliases UPPER_CASE.

## Known field mapping (the baseline to diff against)

A raw key is **already covered** if it appears below. Anything in `RECORD:data` NOT listed is
**drift** (a candidate new field).

**`RAW_TRADES` — keys under `RECORD:data`:**
| raw key | meaning | staging column | cast |
|---|---|---|---|
| `e` | event type | (ignored) | — |
| `E` | event time (ms) | `event_time` | `TO_TIMESTAMP_NTZ(...,3)` |
| `s` | symbol | `symbol` (also via `RECORD:stream`) | `::STRING` |
| `t` | trade id | `trade_id` | `::NUMBER` |
| `p` | price | `price` | `::NUMBER(38,8)` |
| `q` | quantity | `quantity` | `::NUMBER(38,8)` |
| `T` | trade time (ms) | `traded_at` | `TO_TIMESTAMP_NTZ(...,3)` |
| `m` | is buyer market maker | `is_buyer_market_maker` | `::BOOLEAN` |
| `M` | best-match flag | (ignored) | — |

**`RAW_DEPTH` — keys under `RECORD:data`:**
| raw key | meaning | staging | cast |
|---|---|---|---|
| `lastUpdateId` | snapshot sequence | `last_update_id` | `::NUMBER` |
| `bids` | array of [price, qty] | exploded → `stg_depth_levels` (side='bid') | `::NUMBER(38,8)` |
| `asks` | array of [price, qty] | exploded → `stg_depth_levels` (side='ask') | `::NUMBER(38,8)` |

Top-level `RECORD` keys `stream` and `data` are expected. Anything else at top level is also drift.

# Instructions

## Step 1 — Profile the live VARIANT structure

For each raw table, list the keys actually present and their types:

```sql
-- trades
SELECT f.key AS key_name, TYPEOF(f.value) AS value_type, COUNT(*) AS occurrences
FROM RAW.CRYPTO.RAW_TRADES, LATERAL FLATTEN(input => RECORD:data) f
GROUP BY 1, 2 ORDER BY 1;

-- depth
SELECT f.key AS key_name, TYPEOF(f.value) AS value_type, COUNT(*) AS occurrences
FROM RAW.CRYPTO.RAW_DEPTH, LATERAL FLATTEN(input => RECORD:data) f
GROUP BY 1, 2 ORDER BY 1;
```

Also check the top-level keys of `RECORD` (expect `stream`, `data`):
```sql
SELECT DISTINCT f.key FROM RAW.CRYPTO.RAW_TRADES, LATERAL FLATTEN(input => RECORD) f;
```

## Step 2 — List the columns the staging models currently expose

```sql
SELECT table_name, column_name, data_type
FROM ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'PUBLIC_STAGING'
  AND table_name IN ('STG_TRADES', 'STG_DEPTH_LEVELS')
ORDER BY table_name, ordinal_position;
```

## Step 3 — Diff against the known mapping

For each raw key found in Step 1:
- **Covered** → it is in the *Known field mapping* table above (or already a staging column).
- **NEW** → present in `RECORD:data` but NOT in the known mapping → candidate new field.
- **TYPE CHANGED** → key is known but `TYPEOF` differs from the expected cast family
  (e.g. a field that was numeric now returns OBJECT/ARRAY).
- **MISSING** → a known key no longer appears in recent data → report only (may be transient).

## Step 4 — Remediate (additive only)

If NEW keys or TYPE CHANGES are found, extend the relevant staging model **without breaking
existing columns**:
- Add the new field as a new column, casting per conventions
  (numeric/amount → `NUMBER(38,8)`, epoch-ms → `TO_TIMESTAMP_NTZ(...,3)`, boolean → `::BOOLEAN`,
   nested object → flatten its scalar sub-keys, array → `LATERAL FLATTEN`).
- Alias in UPPER_CASE, descriptive name derived from the field's meaning.
- Keep the dedup / structure intact. **Never drop a column automatically** — report removals instead.

## Step 5 — Validate

Run the build and tests; they must stay green:
```
EXECUTE DBT PROJECT ANALYTICS.PUBLIC.crypto_realtime ARGS='build';
```
(or `dbt compile` + `dbt run` + `dbt test` if a CLI is available).
Optionally add a `not_null` test in `_schema.yml` for each new mandatory column.

## Step 6 — Report

Output a concise drift report per table:
- NEW keys (name, inferred type, proposed column + cast),
- TYPE CHANGES, MISSING keys,
- actions taken (models edited) or "No drift detected — staging in sync".

# Guardrails

- **Additive & non-breaking**: extend, never remove. Report removals for human decision.
- **Idempotent**: with no drift, make zero changes.
- **Respect conventions** (NUMBER(38,8), epoch conversion, UPPER_CASE, naming).
- **Human review** of the diff before commit/merge.

# Examples

## Example 1 — Routine check, no drift
User: `$check-schema-drift`
Assistant: profiles `RECORD:data` of both tables, compares to the known mapping + staging columns,
finds every key covered → "No drift detected on RAW_TRADES / RAW_DEPTH — staging in sync."

## Example 2 — A new field appears
User: `$check-schema-drift`
Assistant: finds key `X` in `RECORD:data` of `RAW_TRADES` not in the mapping → adds
`record:data:X::<cast> AS X` to `stg_trades`, runs the build (green), and reports the new column.
