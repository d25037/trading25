---
name: ts-jquants-api-optimization
description: J-Quants 連携の取得効率と安定性を最適化するスキル。レート制限、並列度、バッチ処理、再試行、API利用戦略の実装・改善時に使用する。
---

# ts-jquants-api-optimization

## Source of Truth

- Plan limits and limiter: `apps/ts/packages/clients-ts/src/base/BaseJQuantsClient.ts`
- Dataset bulk fetch logic: `apps/ts/packages/shared/src/dataset/fetchers.ts`
- API consumers: `apps/ts/packages/web/src/hooks/useTopixData.ts`, `apps/ts/packages/cli/src/utils/api-client.ts`

## Current Architecture

- バックエンドは FastAPI (`apps/bt`, `:3002`)。
- 通常利用は `/api/jquants/*` を優先し、ts から直接外部 API を叩く範囲を限定する。
- Plan (`free`, `light`, `standard`, `premium`) に応じた呼び出し間隔と並列度を守る。

## Guardrails

- API 呼び出し戦略を変える場合、結果整合とレート制限超過回避を同時に確認する。
- `JQUANTS_PLAN` 未設定時の挙動を明示する。
- バッチ処理は部分失敗を想定し、全失敗扱いにしない。
- 監視用ログと correlation ID 伝播を壊さない。
