---
name: bt-database-management
description: bt 側の SQLite 管理を扱うスキル。`apps/bt/src/lib/market_db` と `dataset_io`、`/api/db*` の実装変更・レビュー時に使用する。
---

# bt-database-management

## Source of Truth

- `apps/bt/src/lib/market_db/base.py`
- `apps/bt/src/lib/market_db/tables.py`
- `apps/bt/src/lib/market_db/query_helpers.py`
- `apps/bt/src/lib/market_db/market_db.py`
- `apps/bt/src/lib/market_db/portfolio_db.py`
- `apps/bt/src/lib/market_db/dataset_db.py`
- `apps/bt/src/lib/market_db/market_reader.py`
- `apps/bt/src/lib/dataset_io/dataset_writer.py`
- `apps/bt/src/server/services/dataset_resolver.py`
- `apps/bt/src/server/services/market_code_alias.py`
- `apps/bt/src/server/routes/db.py`
- `apps/bt/src/server/services/db_stats_service.py`
- `apps/bt/src/server/services/db_validation_service.py`
- Contracts: `contracts/market-db-schema-v1.schema.json`, `contracts/dataset-db-schema-v2.schema.json`, `contracts/portfolio-db-schema-v1.schema.json`

## Operational Rules

- DB 管理の単一実装は `apps/bt`。`apps/ts` は FastAPI (`:3002`) 経由のみ。
- SQLite アクセスは SQLAlchemy Core (`Table`, `select`, `insert`, `update`) を維持し、ORM セッションを導入しない。
- 役割分離を維持する。
  - `market.db`: 市場データ読み書き
  - `portfolio.db`: portfolio / watchlist CRUD
  - `datasets/*/dataset.db`: `DatasetDb` が読み取り、`dataset_writer` が書き込みを担当
- dataset 名・パスは `DatasetResolver` で検証し、絶対パス・`..` を許可しない。
- 市場コードフィルタは legacy (`prime/standard/growth`) と current (`0111/0112/0113`) の同義性を維持する。
- スキーマ変更時は `contracts/` と `apps/bt/tests/unit/server/db/test_tables.py` を必ず同時更新する。

## Change Checklist

1. テーブル定義を変えたか: `tables.py` / DB クラス / 対応 contract / `test_tables.py` を更新。
2. クエリ条件や型を変えたか: `query_helpers.py` と対応ユニットテストを更新。
3. API 影響があるか: `routes/db.py` と OpenAPI 差分を確認し、必要なら `bun run --filter @trading25/shared bt:sync` を実行。
4. ライフサイクル影響があるか: `apps/bt/src/server/app.py` で初期化・`close()`・`close_all()` を確認。

## Verification

- `uv run ruff check apps/bt/src/lib/market_db apps/bt/src/lib/dataset_io apps/bt/src/server/services apps/bt/src/server/routes/db.py`
- `uv run pyright apps/bt/src/lib/market_db apps/bt/src/server`
- `uv run pytest apps/bt/tests/unit/server/db apps/bt/tests/unit/server/test_routes_db.py apps/bt/tests/unit/server/test_dataset_resolver.py`
