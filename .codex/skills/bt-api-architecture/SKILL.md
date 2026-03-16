---
name: bt-api-architecture
description: bt FastAPI サーバーの API アーキテクチャを扱うスキル。ルーティング、ミドルウェア、エラー形式、OpenAPI 契約を伴う実装変更で使用する。
---

# bt-api-architecture

## When to use

- FastAPI route、middleware、OpenAPI、統一エラーフォーマットを変更するとき。
- `entrypoints/http` の wiring や docs UI の挙動を見直すとき。

## Source of Truth

- `apps/bt/src/entrypoints/http/app.py`
- `apps/bt/src/entrypoints/http/openapi_config.py`
- `apps/bt/src/entrypoints/http/routes`
- `apps/bt/src/entrypoints/http/schemas`
- `references/fastapi-routers.md`

## Workflow

1. 変更対象 endpoint の route と schema を確認する。
2. `app.py` の router wiring と middleware 順序への影響を確認する。
3. OpenAPI response と統一エラーフォーマットの整合を保つ。
4. router 一覧に影響がある場合は generated reference を更新する。

## Guardrails

- FastAPI (`apps/bt`) が唯一のバックエンド。
- Docs UI は `/doc` のみ。
- correlation ID (`x-correlation-id`) の伝播を維持する。
- CORS / Correlation / RequestLogger の順序保証を崩さない。

## Verification

- `python3 scripts/skills/refresh_skill_references.py --check`
- `uv run --project apps/bt pytest tests/unit/server/routes tests/unit/server/test_routes_db.py`
- `uv run --project apps/bt ruff check src/entrypoints/http`
