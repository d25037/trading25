---
id: ts-137
title: "core を責務別パッケージへ分割（superseded）"
status: wontfix
priority: medium
labels: [refactor, architecture, packages, breaking-change, superseded]
project: ts
created: 2026-03-04
updated: 2026-03-04
depends_on: [ts-136]
blocks: []
parent: null
---

# ts-137 core を責務別パッケージへ分割（superseded）

## 目的
- 本来は `@trading25/core` を起点に責務別パッケージへ分割する想定だった。
- 方針変更により `core` を経由せず、`shared` から最終3分割へ直接移行する。

## 受け入れ条件
- `ts-135` の一括移行完了をもって、本Issueの目的が包含されること。

## 実施内容
- [ ] 追加作業なし（`ts-135` に統合）

## 結果
- `2026-03-04`: 方針変更により superseded。`ts-135`（shared から最終3分割への一括移行）へ統合。

## 補足
- `ts-136` 依存の段階実施は採用しない。
