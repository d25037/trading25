---
id: ts-113
title: "Coverage Gate: cli 70/70"
status: done
priority: medium
labels: [test, coverage]
project: ts
created: 2026-02-01
updated: 2026-02-19
depends_on: []
blocks: [ts-014c]
parent: ts-129
---

# ts-113 Coverage Gate: cli 70/70

## 現状
- 閾値: lines 60% / functions 60%

## 目標
- 閾値を **lines 70% / functions 70%** に引き上げ、テストを追加して通す

## 変更対象
- `scripts/check-coverage.ts` — `cli: { lines: 0.7, functions: 0.7 }`
- `packages/cli/src/` 配下のテスト追加

## 受け入れ条件
- `bun run test` 全パス
- `bun run check:coverage` が新閾値で通る

## 実施内容
- `apps/ts/scripts/check-coverage.ts` の `cli` 閾値を `lines: 0.7 / functions: 0.7` に更新
- `apps/ts/packages/cli/bunfig.toml` の `coveragePathIgnorePatterns` に `../clients-ts/**` を追加し、CLI gate を CLI 実装に限定
- 低カバレッジ領域に対して以下のテストを追加/拡充
  - `src/utils/api-clients/base-client.test.ts`
  - `src/utils/api-clients/domain-clients.test.ts`
  - `src/utils/error-handling.test.ts`
  - `src/commands/backtest/error-handler.test.ts`
  - `src/commands/analysis/screening.test.ts`
  - `src/commands/screening/output-formatter.test.ts`
- `bun run --filter @trading25/cli test:coverage` と `bun run check:coverage` を実行して gate 通過を確認

## 結果
- `cli` coverage は 70/70 gate を上回る状態で通過
- `bun run --cwd apps/ts test:coverage` と `bun run --cwd apps/ts check:coverage` で全 package の gate 通過を確認

## 整理メモ
- `ts-014c`（CLI 90/90）は本Issue（70/70）達成後のストレッチ目標として扱う。
