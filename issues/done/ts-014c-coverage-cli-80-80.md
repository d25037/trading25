---
id: ts-014c
title: "Coverage Gate: cli 80/80"
status: done
priority: medium
labels: [test, coverage]
project: ts
created: 2026-01-31
updated: 2026-02-19
depends_on: [ts-113]
blocks: []
parent: ts-129
---

# ts-014c Coverage Gate: cli 80/80

## 現状
- 閾値: lines 70% / functions 70%
- 実績: lines 82.4% / functions 94.4%

## 目標
- 閾値を **lines 80% / functions 80%** に引き上げ、テストを追加して通す

## 備考
実績は lines 82.4% / functions 94.4%。80/80 到達は見込めるが、再計測で確認して gate 固定する。

## 変更対象
- `scripts/check-coverage.ts` — `cli: { lines: 0.8, functions: 0.8 }`
- `packages/cli/src/` 配下のテスト追加

## 受け入れ条件
- `bun run check:coverage` が新閾値で通る

## 実施内容
- `apps/ts/scripts/check-coverage.ts` の `cli` 閾値を `lines: 0.8 / functions: 0.8` に更新
- `bun run --cwd apps/ts --filter @trading25/cli test:coverage` を実行し、CLI の実測を再取得
- `bun run --cwd apps/ts test:coverage` と `bun run --cwd apps/ts check:coverage` を実行して全 package の gate 通過を確認

## 結果
- CLI coverage（lcov集計）は `lines 91.09% / functions 91.88%` で 80/80 gate を上回って通過
- `bun run --cwd apps/ts check:coverage` が新閾値で成功

## 整理メモ
- `ts-113`（CLI 70/70）を先行タスクとして依存関係を設定。
- 本Issueは CLI coverage の次段階目標（80/80）として管理する。
