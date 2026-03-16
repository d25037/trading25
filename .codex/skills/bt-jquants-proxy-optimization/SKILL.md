---
name: bt-jquants-proxy-optimization
description: bt FastAPI の J-Quants proxy 最適化を扱うスキル。`/api/jquants/*` のレート制御、キャッシュ、singleflight、再試行、観測性を変更するときに使用する。
---

# bt-jquants-proxy-optimization

## When to use

- `/api/jquants/*` のレート制御、キャッシュ、singleflight、再試行、観測性を変更するとき。
- upstream J-Quants client と FastAPI proxy の責務境界を見直すとき。

## Source of Truth

- `apps/bt/src/entrypoints/http/routes/jquants_proxy.py`
- `apps/bt/src/application/services/jquants_proxy_service.py`
- `apps/bt/src/entrypoints/http/schemas/jquants.py`
- `apps/bt/src/infrastructure/external_api/clients/jquants_client.py`
- `apps/bt/src/infrastructure/external_api/client.py`

## Workflow

1. route -> service -> external client の順で変更影響を確認する。
2. TTL、singleflight、retry、timeout の組み合わせを service 側で揃える。
3. `event="jquants_fetch"` と `event="jquants_proxy_cache"` の観測性を維持する。
4. `x-correlation-id` 伝播と統一エラーフォーマットの整合を確認する。

## Guardrails

- TS/Web/CLI からの direct J-Quants 呼び出しを前提にしない。
- キャッシュキーは API パラメータ差分を確実に反映する。
- 部分失敗は呼び出し単位で扱い、バッチ全体失敗に短絡しない。

## Verification

- `uv run --project apps/bt pytest tests/unit/server/services/test_jquants_proxy_service.py tests/unit/server/routes/test_jquants_proxy.py tests/unit/server/clients/test_jquants_client.py`
- `uv run --project apps/bt ruff check src/application/services/jquants_proxy_service.py src/entrypoints/http/routes/jquants_proxy.py src/infrastructure/external_api/clients/jquants_client.py`
