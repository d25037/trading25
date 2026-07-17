---
name: bt-market-sync-strategies
description: Use when bt の market sync、intraday minute ingest、J-Quants fetch strategy、または Market v4 cutover guidance を変更するとき。
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
- `apps/bt/src/application/services/market_v4_cutover/`
- `apps/bt/src/entrypoints/cli/market_cutover.py`
- `apps/bt/src/infrastructure/db/market/market_db.py`
- `apps/bt/src/infrastructure/db/market/time_series_store.py`

## Workflow

1. active `market.duckdb` が schema v4 / `local_projection_v2_event_time` であることを確認する。J-Quants を使って staging を再構築する explicit full rebuild は `bt market-cutover cutover`、既存 retained rehearsal を再構築せず昇格する canonical path は `bt market-cutover promote-retained REPORT_ID --retained-report-id ... --backup-id ...` と明確に分ける。
2. mode ごとの解決規則（`initial` / `incremental` / `repair`）を確認する。
3. `incremental` では anchor、cold-start bootstrap、new date 抽出、`missing_stock_dates` backfill の順で判断する。
4. fetch planner は date 指定 bulk を基本にし、bulk/rest fallback の理由を残す。
5. 外部 API 側だけでなく、取得後の local DB 処理（DuckDB/metadata DB publish、relation-based upsert、`executemany` fallback、Parquet export、index/rebuild）まで同じ調査単位で確認する。UI 上の「fetch stuck」は DB publish/rebuild 中の progress 表示不足でも起きる。
6. stock price は `Adj*` を永続 SoT にせず、raw `O/H/L/C/Vo + AdjFactor` を ingest して local projection で `stock_data` を再生成する。
7. minute bars は `/equities/bars/minute` から raw `O/H/L/C/Vo/Va` を `stock_data_minute_raw` へ保存し、daily `stock_data` / `topix_data` と SoT を混ぜない。
8. OHLCV 欠損行、placeholder backfill、metadata 更新規約を確認する。

## Guardrails

- 冪等性を壊さない（同日再取得で重複や欠落を作らない）。
- `last_sync_date`、`failed_dates`、fundamentals 系 metadata の更新規約を維持する。
- minute ingest は `LAST_INTRADAY_SYNC` と `stock_data_minute_raw` を SoT とし、daily table を minute freshness 判定に流用しない。
- schema v3以前または adjustment mode が `local_projection_v2_event_time` でない `market.duckdb` は incompatible。自動移行、dual read、compatibility alias、`auto` / `incremental` / `repair` / stocks refresh での救済を追加しない。
- active root の再構築は `bt market-cutover cutover` で rehearsal pass と immutable backup を検証してから行う。単独の destructive reset を運用手順にしない。
- retained promotion は retained report provenance から source root を解決し、`bt market-cutover promote-retained` の command 内で active v3 の create-only immutable backup を作成・検証して atomic exchange する。source path / force / copy fallback / J-Quants option を追加しない。sync / reset / repair / stock refresh / intraday sync / adjusted-metric materialization / rebuild を実行せず、成功 report の `noSync: true` / `noJQuants: true`、exact report/payload/backup/quarantine identity、semantic smoke、server/worker join verdict を検証する。
- journal 継続 authorization は process-local。fresh service は同一 `REPORT_ID` / retained report ID / backup ID に束縛した dedicated same-attempt recovery（same-ID recovery）を先に行う。joined failure は exact rollback、unjoined child は両 lease を保持した deferred fencing とし、operator は lock / journal / staging を手動変更しない。
- immutable backup と quarantined v3 は成功後も保持する。post-commit cleanup staging は journal に束縛された same-ID recovery だけが完了させる。
- `auto` mode の解決規則（`last_sync_date` 有無で `initial|incremental`）を変更しない。
- `repair` は listed-market fundamentals backfill など非 price warning の回復に限定し、adjustment refresh を復活させない。
- `indices_data` は master 補完（placeholder backfill）前提を維持する。
- minute freshness は現状 `16:45 JST` cutoff の wall-clock policy で、exchange holiday 精度が必要なら別途 `markets/calendar` を minute 側の補助ソースとして扱う。
- bulk/rest の外部 request 数だけで「高速」と判断しない。大量行 stage は `time_series_store` の relation-based upsert を使っているか、small-batch `executemany` が許容範囲か、最後の `index_*` / rebuild / export が進捗表示されるかを確認する。
- `market.duckdb` に書く stage で REST fallback を許す場合は、想定 request 数、pagination、DB publish 件数、index/rebuild コストの上限を確認し、長時間直列処理になる場合は fail-fast または bulk 必須化を検討する。

## Verification

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_strategies.py
uv run --directory apps/bt pytest tests/unit/server/test_routes_db_sync.py
uv run --directory apps/bt pytest tests/unit/server/services/test_intraday_sync_service.py tests/unit/server/services/test_intraday_schedule.py
uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover_*.py tests/unit/cli_bt/test_market_cutover_cli.py
uv run --directory apps/bt ruff check src/application/services/sync_service.py src/application/services/sync_strategies.py
uv run --directory apps/bt ruff check src/application/services/intraday_sync_service.py src/application/services/intraday_schedule.py
uv run --directory apps/bt pyright src/application/services/sync_service.py src/application/services/sync_strategies.py src/application/services/intraday_sync_service.py src/application/services/intraday_schedule.py
```
