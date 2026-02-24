---
name: bt-api-architecture
description: bt FastAPI サーバーの API アーキテクチャを扱うスキル。ルーティング、ミドルウェア、エラー形式、OpenAPI 契約を伴う実装変更で使用する。
---

# bt-api-architecture

FastAPI (`apps/bt`) が唯一のバックエンド。

## Source of Truth

- Router wiring: `apps/bt/src/entrypoints/http/app.py`
- OpenAPI config: `apps/bt/src/entrypoints/http/openapi_config.py`
- Generated route reference: `references/fastapi-routers.md`

## Architecture Rules

- Docs UI は `/doc` のみ。
- 統一エラーレスポンス形式を維持する。
- correlation ID (`x-correlation-id`) の伝播を維持する。
- CORS / Correlation / RequestLogger の順序保証を崩さない。
