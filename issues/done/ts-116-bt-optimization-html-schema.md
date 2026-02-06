---
id: ts-116
title: "bt契約: OptimizationHtmlFile* 型を bt スキーマに追加して互換チェック対象にする"
status: closed
priority: low
labels: [backtest, types]
project: ts
created: 2026-02-01
updated: 2026-02-06
closed: 2026-02-06
depends_on: [ts-109]
blocks: []
---

# ts-116 bt契約: OptimizationHtmlFile* 型を bt スキーマに追加して互換チェック対象にする

## 現状
- 手動型に `OptimizationHtmlFileInfo`, `OptimizationHtmlFileListResponse`, `OptimizationHtmlFileContentResponse` が定義されている
- bt (FastAPI) の OpenAPI スキーマにはこれらのスキーマが存在しない
- `type-compatibility-check.ts` でチェック対象外となっている

## 対応方針
- bt 側で対応するエンドポイント/スキーマを公開する
- スキーマ更新後、`type-compatibility-check.ts` にチェックを追加する

## 受け入れ条件
- `OptimizationHtmlFile*` 3型が `type-compatibility-check.ts` で互換チェック対象になる
- `bun run typecheck:all` が通る
