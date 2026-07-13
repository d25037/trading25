# bt `src` Layering Guide

`apps/bt/src` は以下の 5 層を固定構成とします。

```text
apps/bt/src/
  entrypoints/
  application/
  domains/
  infrastructure/
  shared/
```

## Where To Write

### `entrypoints`
- HTTP/CLI の入口を配置する層
- 例: FastAPI app, routes, middleware, request/response schema, Typer commands

### `application`
- ユースケース実行を束ねるサービス層
- 例: job orchestration, API response assembly, service-level coordination

### `domains`
- 純粋なビジネスロジック層
- 例: strategy/backtest/optimization/analytics/lab_agent の計算・判定ロジック

### `infrastructure`
- DB・外部API・データアクセスなど I/O 層
- 例: SQLAlchemy access, dataset writer, J-Quants/FastAPI clients, loader

### `shared`
- どの層でも使う共通モジュール
- 例: settings, models, paths, observability, constants, exceptions

## Dependency Guardrails

基本ルール:
- `shared` は `shared` のみ依存
- `infrastructure` は `infrastructure` と `shared` に依存
- `domains` は `domains` / `infrastructure` / `shared` に依存
- `application` は `application` / `domains` / `infrastructure` / `shared` に依存
- `entrypoints` は全層へ依存可能（入口層）

### Transitional application schema debt

`application -> entrypoints.http.schemas` is migration debt, not an allowed design direction.
`tests/unit/architecture/application_http_schema_imports.txt` freezes the exact current set.
New entries are forbidden. A DTO migration must remove its stale baseline entries in the same change.

テストで強制:
- `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- `apps/bt/tests/unit/architecture/test_legacy_imports_removed.py`

## Forbidden Legacy Prefixes

以下の prefix は使用禁止です（互換 shim なし）:

- `src.server`
- `src.cli_bt`
- `src.lib`
- `src.api`
- `src.data`
- `src.backtest`
- `src.strategy_config`

## Validation Commands

```bash
uv run --directory apps/bt pytest tests/unit/architecture
uv run --directory apps/bt ruff check src tests
uv run --directory apps/bt pyright src
```
