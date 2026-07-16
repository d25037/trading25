---
name: bt-database-management
description: bt 側の DuckDB + dataset bundle + portfolio SQLite 管理を扱うスキル。market DB、dataset IO、`/api/db*` を変更するときに使用する。
---

# bt-database-management

## When to use

- market.duckdb、dataset bundle、portfolio.db、`/api/db*` の実装や検証を変更するとき。
- DB SoT、path safety、stats/validate/sync 周りの責務を見直すとき。

## Source of Truth

- `apps/bt/src/infrastructure/db/market`
- `apps/bt/src/infrastructure/db/dataset_io`
- `apps/bt/src/application/services`
- `apps/bt/src/application/services/market_v4_cutover/`
- `apps/bt/src/entrypoints/http/routes/db.py`
- `apps/bt/src/entrypoints/http/routes/dataset.py`
- `contracts`

## Workflow

1. 変更対象が market.duckdb、dataset bundle、portfolio.db のどこに属するかを切り分ける。
2. route -> service -> infrastructure の境界と、DuckDB/SQLite の SoT を確認する。
3. stock price を触る場合はMarket schema v4 / `local_projection_v2_event_time`を前提に、`stock_data_raw` が raw `O/H/L/C/Vo + AdjFactor` の SoT、`stock_adjustment_bases` / segmentsがretained regime lineageのSoT、`stock_data` がcurrent convenience projectionであることを確認する。
4. dataset 名や path を触る場合は `DatasetResolver` と XDG 配下制約を確認する。
5. dataset create を触る場合は public API route が `market_reader` と `market.duckdb` source を必須にしていることを確認する。
6. schema や API を変える場合は contracts と db/dataset route の整合を見直す。
7. active XDG data plane を置換する場合は `bt market-cutover` の isolated rehearsal、immutable checksummed backup、staged activation、post-active smoke、explicit restore の証跡を揃える。

## Guardrails

- DB 管理の単一実装は `apps/bt`。`apps/ts` は FastAPI (`:3002`) 経由のみ。
- SQLite アクセスは SQLAlchemy Core を維持し、ORM セッションを導入しない。
- `market-timeseries/market.duckdb`、`portfolio.db`、`datasets/*` の役割分離を崩さない。
- Dataset snapshot は `dataset.duckdb + manifest.v2.json`（物理名はv2、payload `schemaVersion: 3`）のMarket v4 event-time basis bundleのみsupported。schemaVersion 2、旧 `dataset.db`、root-level DB artifactを復活させない。
- Market v3以前や adjustment mode 不一致を自動移行・dual readしない。active root は `bt market-cutover` で v4/event-time staging を検証してから置換し、失敗時は保持した backup から restore する。
- cutover 中の writer/reader quiescence、operation lease、backup checksum、staging confinementを省略しない。運用都合で compatibility fallback を足さない。
- `adjusted_metrics_pit` stageは全regime basisのcatalog/segments/adjusted metrics/valuationをmaterializeし、通常更新でold basisをpruneしない。cutoff-aware readerからcurrent `stock_data`へfallbackしない。
- 市場コードフィルタは legacy (`prime/standard/growth`) と current (`0111/0112/0113`) の同義性を維持する。

## Verification

- `uv run --directory apps/bt pytest tests/unit/server/db tests/unit/server/test_routes_db.py tests/unit/server/test_routes_db_sync.py`
- `uv run --directory apps/bt pytest tests/unit/server/test_dataset_resolver.py tests/unit/server/test_dataset_service.py tests/unit/server/test_routes_dataset_jobs.py`
- `uv run --directory apps/bt pytest tests/unit/server/services/test_db_stats_service.py tests/unit/server/services/test_db_validation_service.py`
- `uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover_*.py tests/unit/cli_bt/test_market_cutover_cli.py`
- `uv run --directory apps/bt ruff check src/infrastructure/db src/application/services/db_stats_service.py src/application/services/db_validation_service.py src/entrypoints/http/routes/db.py`
