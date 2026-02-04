---
id: ts-010
title: "OpenAPI 契約テスト/型生成"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-01-30
depends_on: []
blocks: []
parent: null
---

# ts-010 OpenAPI 契約テスト/型生成

## 目的
API 仕様と TS 型のズレを自動検知する。

## 受け入れ条件
- OpenAPI から型生成または契約テストを導入
- 仕様変更がビルドで検出される

## 実施内容
- `index.ts` と `generate-openapi.ts` のルートマウントを `app-routes.ts` に共通化（9 → 25+ モジュール）
- `openapi-typescript` で OpenAPI spec から TypeScript 型を自動生成
- shared 型と生成型のコンパイル時互換性チェック（22型）
- Spec 鮮度・スキーマ完全性・パス完全性のランタイム契約テスト（66テスト）
- CI で `generate:types` 実行 + `git diff --exit-code` による差分チェック

## 結果
- ルート共通化: `packages/api/src/app-routes.ts`
- 型生成: `packages/api/src/generated/api-types.ts` (自動生成)
- 型互換性チェック: `packages/api/src/generated/type-compatibility-check.ts`
- 契約テスト: `packages/api/src/scripts/__tests__/openapi-contract.test.ts`
- CI更新: `.github/workflows/ci.yml`
- スクリプト: `packages/api/package.json` に `generate:types` 追加

## 補足
- `@hono/zod-openapi` が Zod v4 の `.nullable()` を OpenAPI spec に正しく反映しないため、`FutureReturns` / `ScreeningResultItem` / `MarketScreeningResponse` の型互換性チェックは除外
