---
name: bt-database-management
description: Use when bt の market DuckDB、dataset bundle、portfolio SQLite、dataset IO、または `/api/db*` を変更するとき。
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
7. J-Quants を使って staging を再構築する full rebuild は `bt market-cutover cutover` のまま維持し、既存 retained rehearsal を再構築しない昇格には canonical `bt market-cutover promote-retained REPORT_ID --retained-report-id ... --backup-id ...` だけを使う。
8. retained promotion は retained report provenance から source root を解決し、command 内で create-only immutable backup を作成・検証して atomic exchange する。

## Guardrails

- DB 管理の単一実装は `apps/bt`。`apps/ts` は FastAPI (`:3002`) 経由のみ。
- SQLite アクセスは SQLAlchemy Core を維持し、ORM セッションを導入しない。
- `market-timeseries/market.duckdb`、`portfolio.db`、`datasets/*` の役割分離を崩さない。
- Dataset snapshot は `dataset.duckdb + manifest.v2.json`（物理名はv2、payload `schemaVersion: 3`）のMarket v4 event-time basis bundleのみsupported。schemaVersion 2、旧 `dataset.db`、root-level DB artifactを復活させない。
- Market v3以前や adjustment mode 不一致を自動移行・dual readしない。explicit full rebuild は `bt market-cutover cutover` で v4/event-time staging を構築・検証してから置換する。
- retained promotion は `bt market-cutover promote-retained` を使い、sync / reset / repair / stock refresh / intraday sync / adjusted-metric materialization / rebuild / J-Quants call を禁止する。成功 report の `noSync: true` / `noJQuants: true`、exact report/payload/backup/quarantine identity、semantic smoke、server/worker join verdict を必ず検証する。
- journal 継続 authorization は process-local。fresh service からの継続は同一 `REPORT_ID` / retained report ID / backup ID に束縛した dedicated same-attempt recovery（same-ID recovery）を先に行う。lock / journal / staging を手動変更せず、joined failure は exact rollback、unjoined child は両 lease を保持した deferred fencing とする。
- immutable backup と quarantined v3 は成功後も保持する。post-commit cleanup staging は journal に束縛された same-ID recovery だけが完了させる。
- cutover 中の writer/reader quiescence、operation lease、backup checksum、staging confinementを省略しない。運用都合で compatibility fallback を足さない。
- `adjusted_metrics_pit` stageは全regime basisのcatalog/segments/adjusted metrics/valuationをmaterializeし、通常更新でold basisをpruneしない。cutoff-aware readerからcurrent `stock_data`へfallbackしない。
- 市場コードフィルタは legacy (`prime/standard/growth`) と current (`0111/0112/0113`) の同義性を維持する。

## Verification

```bash
uv run --directory apps/bt pytest tests/unit/server/db tests/unit/server/test_routes_db.py tests/unit/server/test_routes_db_sync.py
uv run --directory apps/bt pytest tests/unit/server/test_dataset_resolver.py tests/unit/server/test_dataset_service.py tests/unit/server/test_routes_dataset_jobs.py
uv run --directory apps/bt pytest tests/unit/server/services/test_db_stats_service.py tests/unit/server/services/test_db_validation_service.py
uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover_*.py tests/unit/cli_bt/test_market_cutover_cli.py
uv run --directory apps/bt ruff check src/infrastructure/db src/application/services/db_stats_service.py src/application/services/db_validation_service.py src/entrypoints/http/routes/db.py
```
