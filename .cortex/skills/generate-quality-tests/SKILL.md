---
name: generate-quality-tests
description: Owns data-quality assurance. Profiles the dbt models and generates domain-aware business-rule tests (positivity, categoricals, temporal, cross-field invariants) as singular tests, plus unit tests for complex transformation logic, with severity tiering and store_failures, then runs dbt test. Structural key tests + docs belong to the flatten-variant skill.
tools:
- snowflake_sql_execute
- snowflake_object_search
---

# When to Use

- Add or extend data-quality tests beyond not_null / unique.
- Profile the data and derive range / invariant checks.
- Keywords: data quality, tests, profiling, ranges, invariants, expectations.

# Ownership (this skill owns ALL quality assurance)

Single owner of **data-quality / business-rule tests**. The **structural** tests (`not_null` / `unique`
on keys, `accepted_values` on enums) and the column **documentation** are owned by the `flatten-variant`
build skill — **assume they already exist and do NOT re-add them** (no duplicate tests).

This skill produces:
- **business-rule tests** — ranges & cross-field invariants (singular tests in `tests/`) ;
- **unit tests** (dbt 1.8 `unit_tests`) for complex transformation logic — RSI (Wilder), OHLCV open/close,
  microprice — with mocked inputs and an expected output (catch logic bugs without live data). If the
  runtime lacks `unit_tests` support, fall back to a seed-fixture logic test (mini input table + expected) ;
- **severity tiering** — `error` for hard invariants (crossed book), `warn` for "investigate" signals
  (e.g. volume-anomaly counts) ;
- **`store_failures: true`** so failing rows are persisted to a table for triage.

# Project Context

- Models in `ANALYTICS.PUBLIC_STAGING`, `ANALYTICS.PUBLIC_INTERMEDIATE`, `ANALYTICS.PUBLIC_MARTS`.
- **No `dbt_utils` / `dbt_expectations`** packages → use:
  - built-in **column tests** (`not_null`, `unique`, `accepted_values`) in `_schema.yml`;
  - **singular tests** in `tests/` for ranges & cross-field invariants (a `SELECT` returning the
    *violating* rows; the test passes when it returns 0 rows).

# Instructions

## Step 1 — Profile each model

For the key columns, inspect ranges / nulls / domains, e.g.:
```sql
SELECT MIN(price), MAX(price), COUNT_IF(price IS NULL),
       MIN(quantity), MAX(quantity), COUNT(*)
FROM ANALYTICS.PUBLIC_STAGING.STG_TRADES;

SELECT DISTINCT side FROM ANALYTICS.PUBLIC_STAGING.STG_DEPTH_LEVELS;
```

## Step 2 — Derive domain rules (catalogue)

Generate tests for the invariants below (adapt to actual column names found):

**stg_trades / fct_trades** : `price > 0`, `quantity > 0`, `trade_id > 0`, `traded_at <= SYSDATE()` (no future — SYSDATE() is UTC NTZ like traded_at ; `current_timestamp()` is LTZ and mis-compares), `is_buyer_market_maker` boolean.
**stg_depth_levels** : `price > 0`, `qty >= 0`, `side IN ('bid','ask')`, `level >= 0`.
**vw_ohlcv_1min_live / fct_ohlcv_1min** : `high >= low`, `high >= open`, `high >= close`, `low <= open`, `low <= close`, `volume >= 0`, `open/high/low/close > 0`, `vwap BETWEEN low AND high`.
**vw_orderbook_metrics_live / fct_orderbook_snapshots** : `best_bid <= best_ask`, `spread_bps >= 0`, `imbalance BETWEEN 0 AND 1`, `mid BETWEEN best_bid AND best_ask`, `microprice BETWEEN best_bid AND best_ask`.
**vw_market_metrics_live** : `rsi_14 BETWEEN 0 AND 100`.

## Step 3 — Implement

- **Column tests** (`not_null`, `accepted_values`) → add to the layer's `_schema.yml`.
- **Range / invariant tests** → singular `.sql` in `tests/`, named `assert_<model>_<rule>.sql`,
  returning violating rows. Example:

```sql
-- tests/assert_fct_ohlcv_1min_high_ge_low.sql
select * from {{ ref('fct_ohlcv_1min') }} where high < low
```
```sql
-- tests/assert_orderbook_bid_le_ask.sql
select * from {{ ref('fct_orderbook_snapshots') }} where best_bid > best_ask
```

## Step 4 — Run & validate

Run `dbt test` (or `EXECUTE DBT PROJECT ANALYTICS.PUBLIC.crypto_realtime ARGS='test'`).
Tests must pass. **If a test fails on real data, investigate** — it may be a genuine data issue,
not a wrong test. Use sensible tolerances for floating metrics.

# Guardrails

- Encode **true invariants** only; avoid tests that fail on legitimate edge cases.
- Prefer singular tests (no extra packages required).
- **Human review** of generated tests before commit.
- Idempotent: re-running refreshes/extends tests without duplications.

# Examples

User: `$generate-quality-tests`
Assistant: profiles staging + marts, generates `accepted_values` on `side`, positivity tests on
prices/quantities, OHLC invariants (`high >= low` …), order-book invariants (`best_bid <= best_ask`,
`imbalance BETWEEN 0 AND 1`, `microprice BETWEEN best_bid AND best_ask`), `rsi_14 BETWEEN 0 AND 100`,
writes them to `_schema.yml` + `tests/`, runs `dbt test`, and reports coverage.
