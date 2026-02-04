---
id: ts-001b
title: "Test Coverage: api パッケージ"
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

# ts-001b Test Coverage: api パッケージ

## 目的
API の重要エンドポイントをカバーし、最低カバレッジ 60% を達成する。

## 受け入れ条件
- `bun run --filter @trading25/api test` が安定して通る
- api のカバレッジが 60% 以上
- 主要ルートの正常系/異常系がテストされている

## 実施内容
- analytics / dataset / market のルートテスト拡充
- dataset / market サービスのユニットテスト追加
- ルートテストのモック方式をサービスの関数差し替えに変更
- `packages/api/bunfig.toml` で coverage 対象を api 配下に限定

## 結果
- `bun run --filter @trading25/api test` ✅
- `bun run --filter @trading25/api test:coverage` ✅（All files: funcs 77.52%, lines 78.93%）

## 補足
