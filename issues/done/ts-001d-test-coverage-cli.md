---
id: ts-001d
title: "Test Coverage: cli パッケージ"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-01-30
depends_on: []
blocks: []
parent: "ts-001"
---

# ts-001d Test Coverage: cli パッケージ

## 目的
CLI コマンドの回帰を防ぎ、最低カバレッジ 60% を達成する。

## 受け入れ条件
- `bun run --filter @trading25/cli test` が安定して通る
- cli のカバレッジが 60% 以上
- 主要コマンドの正常系/異常系がテストされている

## 実施内容
- `packages/cli/bunfig.toml` で coverage 対象を cli 配下に限定

## 結果
- `bun run --filter @trading25/cli test` ✅
- `bun run --filter @trading25/cli test:coverage` ✅（All files: funcs 97.22%, lines 89.13%）

## 補足
