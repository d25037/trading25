---
id: ts-001e
title: "Coverage Gate (CI)"
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

# ts-001e Coverage Gate (CI)

## 目的
lint/typecheck/test の必須化とカバレッジ閾値の自動検証を行う。

## 受け入れ条件
- CI で `lint`, `typecheck:all`, `test` が走る
- 失敗時に PR をブロックできる
- 各パッケージのカバレッジ閾値が設定されている

## 実施内容
- GitHub Actions で `lint`, `typecheck:all`, `test:coverage` を実行する CI を追加
- bun の coverage を lcov 出力し、`scripts/check-coverage.ts` で shared/api/cli の閾値を検証
- vitest の coverage 閾値を設定

## 結果
- CI: `.github/workflows/ci.yml`
- coverage gate:
  - `scripts/check-coverage.ts`
  - `packages/shared/bunfig.toml`
  - `packages/api/bunfig.toml`
  - `packages/cli/bunfig.toml`
  - `packages/web/vitest.config.ts`

## 補足
