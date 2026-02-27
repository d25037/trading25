---
name: bt-market-sync-strategies
description: bt の market 同期（initial/incremental/indices-only）と J-Quants fetch 戦略を扱うスキル。`/api/db/sync`、`sync_service`、`sync_strategies`、watermark/failed_dates の実装変更・レビュー時に使用する。
---

# bt-market-sync-strategies

## Source of Truth

- API route: `apps/bt/src/entrypoints/http/routes/db.py`
- Job orchestration: `apps/bt/src/application/services/sync_service.py`
- Sync logic: `apps/bt/src/application/services/sync_strategies.py`
- Metadata keys / persistence: `apps/bt/src/infrastructure/db/market/market_db.py`
- OHLCV row normalization: `apps/bt/src/application/services/stock_data_row_builder.py`
- Tests: `apps/bt/tests/unit/server/services/test_sync_strategies.py`

## Mode Semantics

1. `initial`
   - `topix` 全量 -> `equities/master` -> Prime fundamentals（code指定） -> `stock_data`（topix取引日ごと日付指定） -> `indices` -> metadata更新。
2. `incremental`
   - `last_sync_date` が必須。アンカーは `latest_stock_data_date` 優先（なければ `latest_trading_date`）。
   - `topix` を `from=anchor` で取得し、`new_dates` のみ `equities/bars/daily?date=...` を取得。
   - `indices` は code指定増分 + date指定補完で新規コードを回収。
   - fundamentals は `disclosed_date` 増分 + missing prime code backfill。
3. `indices-only`
   - index master catalog seed + 各 index code の時系列同期のみ実施。

## J-Quants Fetch Rules

- OHLCV bulk fetch は `equities/bars/daily` の **date 指定**を基本にする（codeループで置き換えない）。
- 取引日カレンダーは `indices/bars/daily/topix` を SoT として扱う。
- `/fins/summary` は pagination を前提にし、date 指定と code 指定を使い分ける。
- OHLCV 欠損行は `build_stock_data_row` で skip し、warning を集約する。

## Guardrails

- 冪等性を壊さない（同日再取得で重複/欠落を作らない）。
- `last_sync_date` / `failed_dates` / fundamentals 系 metadata の更新規約を維持する。
- `auto` mode の解決規則（`last_sync_date` 有無で `initial|incremental`）を変更しない。
- `indices_data` は master 補完（placeholder backfill）前提を維持する。
- 変更時は `/api/db/sync` 契約と進捗ステージ名の互換性を確認する。

## Verification

- `uv run --project apps/bt pytest tests/unit/server/services/test_sync_strategies.py`
- `uv run --project apps/bt pytest tests/unit/server/test_routes_db_sync.py`
- `uv run --project apps/bt ruff check src/application/services/sync_service.py src/application/services/sync_strategies.py`
- `uv run --project apps/bt pyright src/application/services/sync_service.py src/application/services/sync_strategies.py`
