---
name: bt-market-sync-strategies
description: bt の market 同期（initial、incremental、repair）と intraday minute ingest、J-Quants fetch 戦略を扱うスキル。`/api/db/sync`、`/api/db/intraday/sync`、`sync_service`、`sync_strategies` を変更するときに使用する。
---

# bt-market-sync-strategies

## When to use

- `initial` / `incremental` / `repair` の sync 戦略や fetch planner を変更するとき。
- `/api/db/sync`、watermark、failed_dates、bulk/rest fallback を見直すとき。
- `/equities/bars/minute` からの local ingest、`stock_data_minute_raw`、minute freshness warning を変更するとき。

## Source of Truth

- `apps/bt/src/entrypoints/http/routes/db.py`
- `apps/bt/src/application/services/sync_service.py`
- `apps/bt/src/application/services/sync_strategies.py`
- `apps/bt/src/application/services/intraday_sync_service.py`
- `apps/bt/src/application/services/intraday_schedule.py`
- `apps/bt/src/application/services/stock_data_row_builder.py`
- `apps/bt/src/application/services/stock_minute_data_row_builder.py`
- `apps/bt/src/infrastructure/db/market/market_db.py`
- `apps/bt/src/infrastructure/db/market/time_series_store.py`

## Workflow

1. mode ごとの解決規則（`initial` / `incremental` / `repair`）を確認する。
2. `incremental` では anchor、cold-start bootstrap、new date 抽出、`missing_stock_dates` backfill の順で判断する。
3. fetch planner は date 指定 bulk を基本にし、bulk/rest fallback の理由を残す。
4. stock price は `Adj*` を永続 SoT にせず、raw `O/H/L/C/Vo + AdjFactor` を ingest して local projection で `stock_data` を再生成する。
5. minute bars は `/equities/bars/minute` から raw `O/H/L/C/Vo/Va` を `stock_data_minute_raw` へ保存し、daily `stock_data` / `topix_data` と SoT を混ぜない。
6. OHLCV 欠損行、placeholder backfill、metadata 更新規約を確認する。

## Guardrails

- 冪等性を壊さない（同日再取得で重複や欠落を作らない）。
- `last_sync_date`、`failed_dates`、fundamentals 系 metadata の更新規約を維持する。
- minute ingest は `LAST_INTRADAY_SYNC` と `stock_data_minute_raw` を SoT とし、daily table を minute freshness 判定に流用しない。
- legacy `market.duckdb` は sync 対象にしない。`stock_data_raw` が無い price snapshot は `initial` sync の `resetBeforeSync=true` を要求する。
- `auto` mode の解決規則（`last_sync_date` 有無で `initial|incremental`）を変更しない。
- `repair` は listed-market fundamentals backfill など非 price warning の回復に限定し、adjustment refresh を復活させない。
- `indices_data` は master 補完（placeholder backfill）前提を維持する。
- minute freshness は現状 `16:45 JST` cutoff の wall-clock policy で、exchange holiday 精度が必要なら別途 `markets/calendar` を minute 側の補助ソースとして扱う。

## Verification

- `uv run --project apps/bt pytest tests/unit/server/services/test_sync_strategies.py`
- `uv run --project apps/bt pytest tests/unit/server/test_routes_db_sync.py`
- `uv run --project apps/bt pytest tests/unit/server/services/test_intraday_sync_service.py tests/unit/server/services/test_intraday_schedule.py`
- `uv run --project apps/bt ruff check src/application/services/sync_service.py src/application/services/sync_strategies.py`
- `uv run --project apps/bt ruff check src/application/services/intraday_sync_service.py src/application/services/intraday_schedule.py`
- `uv run --project apps/bt pyright src/application/services/sync_service.py src/application/services/sync_strategies.py src/application/services/intraday_sync_service.py src/application/services/intraday_schedule.py`
