---
name: bt-jquants-proxy-optimization
description: bt FastAPI の J-Quants proxy 最適化を扱うスキル。`/api/jquants/*` のレート制御、キャッシュ、singleflight、再試行、観測性を変更するときに使用する。
---

# bt-jquants-proxy-optimization

## Source of Truth

- Routes: `apps/bt/src/server/routes/jquants_proxy.py`
- Service: `apps/bt/src/server/services/jquants_proxy_service.py`
- Upstream fetch/retry/logging: `apps/bt/src/server/clients/jquants_client.py`
- Schemas: `apps/bt/src/server/schemas/jquants.py`
- Internal client: `apps/bt/src/api/client.py`

## Focus

1. TTL + singleflight の整合性を維持する。
2. 429/5xx に対する再試行・待機戦略を明示する。
3. `event="jquants_fetch"` と `event="jquants_proxy_cache"` の構造化ログを維持する。
4. `x-correlation-id` 伝播と統一エラーフォーマットを維持する。

## Guardrails

- TS/Web/CLI からの direct J-Quants 呼び出しを前提にしない。
- キャッシュキーは API パラメータ差分を確実に反映する。
- 部分失敗は呼び出し単位で扱い、バッチ全体失敗に短絡しない。
