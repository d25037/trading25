---
id: ts-123
title: Remove deprecated fundamentals-data.ts service
status: closed
priority: low
labels: [cleanup, tech-debt]
project: ts
created: 2026-02-03
updated: 2026-02-03
depends_on: []
blocks: []
parent: null
---

# ts-123 Remove deprecated fundamentals-data.ts service

## 目的
Single Source of Truth原則に基づき、fundamentals計算がapps/bt/ APIに移行完了したため、
apps/ts/api側の旧実装を削除してコードベースを簡潔に保つ。

## 受け入れ条件
- `packages/api/src/services/fundamentals-data.ts` が削除されている
- 関連するimport/exportが削除されている
- テストが全てパスする
- apps/bt/ API経由のfundamentals機能が正常動作する

## 実施内容
（着手後に記載）

## 結果
fundamentals-data.ts (705行) と fundamentals-data.test.ts (847行) を削除。
shared/AGENTS.md の deprecated 参照も削除。(2026-02-06)

## 補足
- 移行コミット: d9a5181 (Phase 4.3完了)
- 現在の状態: `@deprecated` アノテーション付きでフォールバック用に保持
- 関連ファイル:
  - `packages/api/src/services/fundamentals-data.ts` (削除対象)
  - `packages/api/src/routes/analytics/fundamentals.ts` (BacktestClient使用に移行済み)
