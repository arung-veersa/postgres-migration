# Snowflake Warehouse Size Comparison — 2026-02-13

## Context

Two back-to-back pipeline runs on the same ECS container image against the **dev** environment,
differing only in the Snowflake virtual warehouse size used.

- **Snowflake account**: IKB38126.us-east-1 / ANALYTICS.BI
- **PostgreSQL**: pgsbconflictmanagement.hhaexchange.local / conflict_management.conflict_dev
- **Pipeline**: `task00_preflight → task01_copy_to_staging → task02_00_conflict_update → task99_postflight`
- **Data volume**: ~8M conflictlog_staging rows, ~9.2M conflictvisitmaps, ~7.9M conflicts

| Run | Warehouse | Start (UTC) | End (UTC) |
|-----|-----------|-------------|-----------|
| 1 — Bigger WH | *(larger size)* | 02:52:26 | 03:03:58 |
| 2 — Smaller WH | *(smaller size)* | 03:20:56 | 04:03:51 |

---

## Timing Breakdown

| Step | Bigger WH | Smaller WH | Slowdown | Time Added |
|------|-----------|------------|----------|------------|
| **task00_preflight** | 37s | 33s | 0.9x | −4s |
| **task01_copy_to_staging** | **3m 23s** | **3m 58s** | 1.2x | +35s |
| — staging populate | 194.6s | 231.5s | 1.2x | +37s |
| **task02_00_conflict_update** | **6m 37s** | **37m 11s** | **5.6x** | **+30m 34s** |
| — Step 0 (excluded SSNs temp table) | ~6s | ~5s | ~1x | ~0s |
| — Step 1 (delta_keys temp table) | ~7s | ~34s | 4.9x | +27s |
| — Step 2A (base_visits — delta rows CTAS) | 49.5s | 225.9s | **4.6x** | +176s |
| — Step 2B (non-delta JOIN INSERT) | 88.2s | 1695.1s | **19.2x** | **+1607s** |
| — Step 2 total | 137.6s | 1921.1s | 14.0x | +1784s |
| — Step 2d (delta pairs → Postgres) | 57.7s | 55.9s | ~1x | ~0s |
| — Step 3 query startup | ~4s | ~29s | 7.3x | +25s |
| — Step 3 batch processing (19 batches) | ~71s | ~73s | ~1x | ~0s |
| — Step 4 (stale cleanup) | 113.9s | 112.2s | ~1x | ~0s |
| **task99_postflight** | 52s | 1m 12s | 1.4x | +20s |
| — VACUUM (55 tables) | 17s | 36s | 2.1x | +19s |
| — ANALYZE (55 tables) | 18s | 19s | ~1x | ~0s |
| **TOTAL** | **11m 31s** | **42m 54s** | **3.7x** | **+31m 23s** |

---

## Key Insights

### 1. Step 2B is the dominant bottleneck — 19.2x slower on the smaller warehouse

This single step (INSERT of related non-delta visits via `INNER JOIN delta_keys`) accounts
for **26m 47s of the 31m 23s total difference** (85%).

The query scans FACTCONFLICTRESULTS for all rows whose `(VisitDate, SSN)` match the delta
keys but whose timestamps are *older* than the lookback window. On a smaller warehouse,
Snowflake has fewer compute nodes to parallelize this heavy scan + join.

### 2. Step 2A also scales significantly (4.6x), but less dramatically

The delta rows CTAS query goes from 49.5s → 225.9s. Same template as 2B, but filters for
recent timestamps only, producing a smaller intermediate result set.

### 3. PostgreSQL-bound steps are completely unaffected by warehouse size

| PG-bound Step | Bigger WH | Smaller WH |
|---------------|-----------|------------|
| Step 2d (stream 8M pairs to PG) | 57.7s | 55.9s |
| Step 3 batch processing (19 batches) | ~71s | ~73s |
| Step 4 stale cleanup (PG anti-join) | 113.9s | 112.2s |

This confirms the bottleneck is purely Snowflake compute, not network or PostgreSQL.

### 4. Step 3 query startup differs (4s vs 29s)

The final conflict detection self-join on `base_visits` starts streaming in 4s on the
bigger warehouse vs 29s on the smaller one. Once streaming begins, per-batch speed is
identical because processing is PG-bound.

### 5. ~69% of detected conflicts are "New" (not matched to existing PG records)

Across all 19 batches, roughly 28,567 out of 90,440 conflicts matched existing records
(~31%). The other ~69% (~61,873) are new pairs not yet in PostgreSQL. All matched records
show "No updates needed" — existing data is already current.

### 6. Preflight/Postflight row counts are PG-bound

`COUNT(*)` on `conflictlog_staging` (8M+ rows, 249 columns) takes ~22s → ~32s. The
variance comes from PostgreSQL query cache and table bloat state, not Snowflake.

---

## Improvement Recommendations

### High Impact — Target Step 2B

| # | Recommendation | Expected Benefit |
|---|----------------|------------------|
| 1 | **Dynamic warehouse sizing** — Scale up to MEDIUM/LARGE just for Steps 1–2B, scale back down after. Use `ALTER WAREHOUSE ... SET WAREHOUSE_SIZE` before/after. | Recover the 26m 47s gap at minimal cost — big WH runs for only ~2 min. |
| 2 | **Cluster FACTCONFLICTRESULTS on (Visit Date, SSN)** — Step 2B JOINs on these columns; clustering enables better micro-partition pruning. | Reduce scan volume on all warehouse sizes. |
| 3 | **Remove `TRIM(SSN)` from join predicate** — `TRIM(CAR."SSN")` in the JOIN prevents Snowflake from using partition pruning on SSN. Pre-clean SSN data instead. | Better pruning, especially on smaller warehouses. |
| 4 | **Force stats on delta_keys** — After creating the temp table, run `SELECT COUNT(*) FROM delta_keys` so Snowflake computes stats before the Step 2B join. | Better join strategy selection by the optimizer. |

### Medium Impact — General

| # | Recommendation | Expected Benefit |
|---|----------------|------------------|
| 5 | **Use `pg_class.reltuples` for approximate row counts** in preflight/postflight instead of exact `COUNT(*)`. | Save ~30s from preflight, ~15s from postflight. |
| 6 | **Short-circuit Step 4 stale scan** — If total conflicts from Snowflake match expectations, skip the 112s anti-join scan. | Save ~112s when there are no stale records (the common case). |
| 7 | **Scope VACUUM/ANALYZE** to only tables touched by the pipeline (`conflictlog_staging`, `conflicts`, `conflictvisitmaps`, `payer_provider_reminders`). | Save ~20–30s from postflight (currently vacuums all 55 tables). |

### Low Impact — Observability

| # | Recommendation | Expected Benefit |
|---|----------------|------------------|
| 8 | **Log warehouse name and size** — Add `SELECT CURRENT_WAREHOUSE()` and warehouse size to preflight. | Makes future comparisons self-documenting. |
| 9 | **Log `base_visits` row count** after Steps 2A and 2B. | Helps distinguish data volume vs query plan differences. |

---

## Bottom Line

The smaller warehouse is **3.7x slower** end-to-end, but the pain is concentrated almost
entirely in **Snowflake Step 2B** (the non-delta visit expansion JOIN), which alone is
**19.2x slower** and accounts for 85% of the total time difference.

The most cost-effective improvement is **dynamic warehouse sizing**: scale up just for
Steps 1–2B (~2 min on bigger WH), then scale back down. Everything else — PG streaming,
batch processing, stale cleanup — is already warehouse-agnostic and efficient.
