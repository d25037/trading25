---
id: ts-139
title: "web 未使用依存の削除と pin 整合"
status: open
priority: medium
labels: [refactor, dependencies, cleanup]
project: ts
created: 2026-03-16
updated: 2026-03-16
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
- [ ] web manifest の未使用依存を削除する
- [ ] root tooling dependency の必要性を再確認する
- [ ] root override と package dependency の pin を揃える
- [ ] README の dependency policy に反映する

## 結果
- 未着手

## 補足
- `monaco-editor` は `@monaco-editor/react` の peer/runtime として維持する
- root override の transitive pin は削除対象ではない
