---
id: ts-139
title: "web 未使用依存の削除と pin 整合"
status: done
priority: medium
labels: [refactor, dependencies, cleanup]
project: ts
created: 2026-03-16
updated: 2026-03-23
depends_on: []
blocks: []
parent: ts-138
---

# ts-139 web 未使用依存の削除と pin 整合

## 目的
- `packages/web` の未使用依存を削除し、root override と package 宣言の version drift をなくす

## 受け入れ条件
- `@radix-ui/react-label` が削除されている
- `tsx` の要否が再評価され、不要なら root から削除されている
- `monaco-editor` の package version が root override と一致している
- typecheck, test, build が通る

## 実施内容
- [x] web manifest の未使用依存を削除する
- [x] root tooling dependency の必要性を再確認する
- [x] root override と package dependency の pin を揃える
- [x] README の dependency policy に反映する

## 結果
- `@radix-ui/react-label` と `tsx` を削除し、追加の棚卸しで `react-json-view-lite` と `jsdom` も除去した。
- `monaco-editor` は `@monaco-editor/react` の peer/runtime として維持し、`apps/ts/package.json` の root override と `packages/web/package.json` の宣言を `0.53.0` に揃えた。
- `apps/ts/README.md` の dependency policy に keep/remove/pin の理由を追記した。
- 実行結果:
  - `bun run quality:typecheck` ✅
  - `bun run workspace:test` ✅
  - `bun run workspace:build` ✅
  - `bun run quality:deps:audit` ✅

## 補足
- `monaco-editor` は `@monaco-editor/react` の peer/runtime として維持する
- root override の transitive pin は削除対象ではない
