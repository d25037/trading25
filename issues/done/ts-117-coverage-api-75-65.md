---
id: ts-117
title: "Coverage Gate: api 75/65"
status: done
priority: medium
labels: [test]
project: ts
created: 2026-02-02
updated: 2026-02-09
closed: 2026-02-09
depends_on: [ts-112]
blocks: []
---

# ts-117 Coverage Gate: api 75/65

## 現状
- 閾値: lines 65% / functions 45%
- 実績: lines 65.33% / functions 53.56% (ts-112 で達成)

## 目標
- 閾値を **lines 75% / functions 65%** に引き上げ、テストを追加して通す

## 補足
- ルートハンドラのテストは `mock.module` + dynamic import パターンを使用すること（`roe.test.ts` が模範）
- サービス層（`services/`）のユニットテスト追加が効果的
- `utils/` 配下のヘルパー関数もカバレッジ向上の余地あり

## 変更対象
- `scripts/check-coverage.ts` — `api: { lines: 0.75, functions: 0.65 }`
- `packages/api/src/` 配下のテスト追加

## 受け入れ条件
- `bun run test` 全パス
- `bun run check:coverage` が新閾値で通る

## 結果
- 2026-02-09 時点で `apps/ts/packages/api` は Phase 3F により archived・read-only（FastAPI へ完全移行済み）。
- 本 Issue の対象（API package coverage gate 引き上げ）は現行アーキテクチャの実行対象外となったためクローズ。
- Phase 4 の追跡は `ts-124`（TS 側責務分離）へ引き継ぐ。
