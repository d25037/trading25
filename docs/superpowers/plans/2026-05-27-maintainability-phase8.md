# Maintainability Phase 8 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the `market_db.py` Data Plane hotspot without changing DuckDB schema, adjusted fundamentals semantics, `daily_valuation` ASOF behavior, or the public `MarketDb` API.

**Architecture:** Keep `MarketDb` as the facade for callers. Move adjusted statement metric and daily valuation read/write paths into focused market modules. Schema/bootstrap, metadata, and stock-master writers stay in `market_db.py` for this phase.

**Tech Stack:** Python 3.12, DuckDB, pandas relation registration, pytest, ruff, pyright, maintainability snapshot tooling.

---

## Phase 8 Decision

Phase 8 targets `apps/bt/src/infrastructure/db/market/market_db.py`.

After Phase 7, `market_db.py` is the top repo hotspot. The safest useful slice is not to redesign market storage. It is to extract the adjusted fundamentals valuation cluster:

- `upsert_statement_metrics_adjusted`
- `upsert_daily_valuation`
- `upsert_daily_valuation_from_adjusted_metrics`
- `get_adjusted_statement_metrics`
- `get_daily_valuation`
- `get_daily_valuation_for_codes`
- `get_adjusted_metrics_snapshot`

This cluster owns high-value valuation persistence and reads, and contains the largest local SQL block. It has focused tests under `apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py` and materializer tests under `apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py`.

## Scope

Create:

- `apps/bt/src/infrastructure/db/market/valuation_writers.py`
- `apps/bt/src/infrastructure/db/market/valuation_queries.py`

Modify:

- `apps/bt/src/infrastructure/db/market/market_db.py`
- `docs/maintainability-snapshot-latest.md`
- `docs/maintainability-snapshot-latest.json`
- dated maintainability snapshot outputs

Out of scope:

- Changing `daily_valuation` column definitions or conflict keys.
- Changing forward EPS / forward OP anchor gating.
- Changing ASOF join order or filters.
- Changing schema migration behavior.

## Numeric Targets

Starting point is `docs/maintainability-snapshot-latest.md` after Phase 7.

| metric | phase 7 actual | phase 8 target |
| --- | ---: | ---: |
| `market_db.py` total lines | 2,191 | <= 1,900 |
| `market_db.py` code lines | 1,358 | <= 1,220 |
| `market_db.py` hotspot score | 6,430 | <= 5,900 |
| `market_db.py` max block code lines | 59 | unchanged or lower |
| top extracted writer/query function code lines | n/a | <= 220 |

If total repo LOC increases slightly because responsibilities move into a new module, that is acceptable. Success is concentration reduction in `market_db.py` with identical focused tests.

## Actual Results

Completed on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | phase 7 actual | phase 8 actual | phase 8 target |
| --- | ---: | ---: | ---: |
| `market_db.py` total lines | 2,191 | 1,710 | <= 1,900 |
| `market_db.py` code lines | 1,358 | 1,205 | <= 1,220 |
| `market_db.py` hotspot score | 6,430 | 5,539 | <= 5,900 |
| `market_db.py` max block code lines | 59 | 46 | unchanged or lower |
| top extracted writer/query function code lines | n/a | 60 | <= 220 |

All Phase 8 numeric targets were met. `market_db.py` is no longer the repo top hotspot; the next top hotspot is the research runner `topix100_sma_ratio_rank_future_close_lightgbm.py`.

## Tasks

### Task 1: Extract Valuation Writers

- [x] Create `valuation_writers.py` with module-level helpers for the three writer paths.
- [x] Move relation-based adjusted metrics upsert with the same dedupe and `created_at` behavior.
- [x] Move direct `daily_valuation` upsert with the same conflict update behavior.
- [x] Move bulk `daily_valuation` rebuild SQL unchanged apart from receiving `conn`, `lock`, `table_exists`, and params as arguments.
- [x] Move adjusted metrics and daily valuation read helpers into `valuation_queries.py` after the first snapshot showed the `market_db.py` code-line target was still missed by 34 lines.

### Task 2: Preserve MarketDb Facade

- [x] Replace valuation `MarketDb` method bodies with helper delegation, keeping `_assert_writable()` on write paths.
- [x] Keep method names, signatures, return values, and empty-input behavior unchanged.
- [x] Remove only imports that become unused.

### Task 3: Verify

- [x] Run focused db tests:

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py \
  apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py -q
```

- [x] Run static checks for touched modules:

```bash
uv run --project apps/bt ruff check \
  apps/bt/src/infrastructure/db/market/market_db.py \
  apps/bt/src/infrastructure/db/market/valuation_writers.py \
  apps/bt/src/infrastructure/db/market/valuation_queries.py

uv run --project apps/bt pyright \
  apps/bt/src/infrastructure/db/market/market_db.py \
  apps/bt/src/infrastructure/db/market/valuation_writers.py \
  apps/bt/src/infrastructure/db/market/valuation_queries.py
```

### Task 4: Re-measure

- [x] Re-run maintainability snapshot.
- [x] Confirm Phase 8 numeric targets and record actuals in this plan.
