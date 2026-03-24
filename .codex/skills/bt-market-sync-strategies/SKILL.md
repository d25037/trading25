---
name: bt-market-sync-strategies
description: bt の market 同期（initial、incremental、indices-only、repair）と J-Quants fetch 戦略を扱うスキル。`/api/db/sync`、`sync_service`、`sync_strategies` を変更するときに使用する。
---

# bt-market-sync-strategies

## When to use

- `initial` / `incremental` / `indices-only` / `repair` の sync 戦略や fetch planner を変更するとき。
- `/api/db/sync`、watermark、failed_dates、bulk/rest fallback を見直すとき。

## Source of Truth

- `apps/bt/src/entrypoints/http/routes/db.py`
- `apps/bt/src/application/services/sync_service.py`
- `apps/bt/src/application/services/sync_strategies.py`
- `apps/bt/src/application/services/stock_data_row_builder.py`
- `apps/bt/src/infrastructure/db/market/market_db.py`

## Workflow

1. mode ごとの解決規則（`initial` / `incremental` / `indices-only`）を確認する。
2. `incremental` では anchor、cold-start bootstrap、new date 抽出の順で判断する。
3. fetch planner は date 指定 bulk を基本にし、bulk/rest fallback の理由を残す。
4. stock price は `Adj*` を永続 SoT にせず、raw `O/H/L/C/Vo + AdjFactor` を ingest して local projection で `stock_data` を再生成する。
5. OHLCV 欠損行、placeholder backfill、metadata 更新規約を確認する。

## Guardrails

- 冪等性を壊さない（同日再取得で重複や欠落を作らない）。
- `last_sync_date`、`failed_dates`、fundamentals 系 metadata の更新規約を維持する。
- legacy `market.duckdb` は sync 対象にしない。`stock_data_raw` が無い price snapshot は reset + `initial sync` を要求する。
- `auto` mode の解決規則（`last_sync_date` 有無で `initial|incremental`）を変更しない。
- `repair` は listed-market fundamentals backfill など非 price warning の回復に限定し、adjustment refresh を復活させない。
- `indices_data` は master 補完（placeholder backfill）前提を維持する。

## Verification

- `uv run --project apps/bt pytest tests/unit/server/services/test_sync_strategies.py`
- `uv run --project apps/bt pytest tests/unit/server/test_routes_db_sync.py`
- `uv run --project apps/bt ruff check src/application/services/sync_service.py src/application/services/sync_strategies.py`
- `uv run --project apps/bt pyright src/application/services/sync_service.py src/application/services/sync_strategies.py`
