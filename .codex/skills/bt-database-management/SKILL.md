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
- `apps/bt/src/entrypoints/http/routes/db.py`
- `contracts`

## Workflow

1. 変更対象が market.duckdb、dataset bundle、portfolio.db のどこに属するかを切り分ける。
2. route -> service -> infrastructure の境界と、DuckDB/SQLite の SoT を確認する。
3. stock price を触る場合は `stock_data_raw` が raw `O/H/L/C/Vo + AdjFactor` の SoT、`stock_data` が local projection された adjusted series であることを前提にする。
4. dataset 名や path を触る場合は `DatasetResolver` と XDG 配下制約を確認する。
5. schema や API を変える場合は contracts と db route の整合を見直す。

## Guardrails

- DB 管理の単一実装は `apps/bt`。`apps/ts` は FastAPI (`:3002`) 経由のみ。
- SQLite アクセスは SQLAlchemy Core を維持し、ORM セッションを導入しない。
- `market-timeseries/market.duckdb`、`portfolio.db`、`datasets/*` の役割分離を崩さない。
- legacy `market.duckdb` を自動移行しない。`stock_data_raw` が無い price snapshot は incompatible として `initial` sync の `resetBeforeSync=true` を前提に扱う。
- 市場コードフィルタは legacy (`prime/standard/growth`) と current (`0111/0112/0113`) の同義性を維持する。

## Verification

- `uv run --project apps/bt pytest tests/unit/server/db tests/unit/server/test_routes_db.py tests/unit/server/test_routes_db_sync.py`
- `uv run --project apps/bt pytest tests/unit/server/services/test_db_stats_service.py tests/unit/server/services/test_db_validation_service.py`
- `uv run --project apps/bt ruff check src/infrastructure/db src/application/services/db_stats_service.py src/application/services/db_validation_service.py src/entrypoints/http/routes/db.py`
