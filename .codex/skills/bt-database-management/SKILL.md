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
3. stock price を触る場合は Market schema v5 / `provider_adjusted_v1` を前提に、`stock_data_raw` が raw + provider `Adj*` provenance、`stock_data` が provider-adjusted consumer SoT、`stock_provider_windows` / `stock_adjustment_events` が coverage / event ledger の SoT であることを確認する。
4. dataset 名や path を触る場合は `DatasetResolver` と XDG 配下制約を確認する。
5. dataset create を触る場合は public API route が `market_reader` と `market.duckdb` source を必須にしていることを確認する。
6. schema や API を変える場合は contracts と db/dataset route の整合を見直す。
7. Market v5 への operator cutover は `bt market-cutover cutover` による full rebuild only（唯一の経路）とする。
8. `providerVintage` と Dataset4（manifest payload `schemaVersion: 4`）の semantic smoke を通してから activation する。

## Guardrails

- DB 管理の単一実装は `apps/bt`。`apps/ts` は FastAPI (`:3002`) 経由のみ。
- SQLite アクセスは SQLAlchemy Core を維持し、ORM セッションを導入しない。
- `market-timeseries/market.duckdb`、`portfolio.db`、`datasets/*` の役割分離を崩さない。
- Dataset snapshot は `dataset.duckdb + manifest.v2.json`（物理名はv2、payload `schemaVersion: 4`）の Market v5 provider-vintage bundle のみ supported。Dataset v3、旧 `dataset.db`、root-level DB artifactを復活させない。
- Market v4以前や adjustment mode 不一致を in-place migration / 自動移行・dual read しない。
- retained Market v4 は v5 candidate として ineligible（不適格）で、昇格しない。CLI / service に retained promotion surface を戻さない。
- cutover は immutable backup を検証し、staged Market v5 を atomic activation し、失敗時は exact rollback する。
- artifact identity は `operations/market-v5-cutover` を使う。operator は operation lock / staging を手動変更しない。
- cutover 中の writer/reader quiescence、operation lease、backup checksum、staging confinementを省略しない。運用都合で compatibility fallback を足さない。
- current-basis `statement_metrics_adjusted` / `daily_valuation` は normal sync が更新し、cutoff-aware readerから raw/current fallbackしない。
- 市場コードフィルタは legacy (`prime/standard/growth`) と current (`0111/0112/0113`) の同義性を維持する。

## Verification

```bash
uv run --directory apps/bt pytest tests/unit/server/db tests/unit/server/test_routes_db.py tests/unit/server/test_routes_db_sync.py
uv run --directory apps/bt pytest tests/unit/server/test_dataset_resolver.py tests/unit/server/test_dataset_service.py tests/unit/server/test_routes_dataset_jobs.py
uv run --directory apps/bt pytest tests/unit/server/services/test_db_stats_service.py tests/unit/server/services/test_db_validation_service.py
uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover_*.py tests/unit/cli_bt/test_market_cutover_cli.py
uv run --directory apps/bt ruff check src/infrastructure/db src/application/services/db_stats_service.py src/application/services/db_validation_service.py src/entrypoints/http/routes/db.py
```
