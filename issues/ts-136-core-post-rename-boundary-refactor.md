---
id: ts-136
title: "core リネーム後の軽量境界リファクタ（superseded）"
status: wontfix
priority: medium
labels: [refactor, architecture, packages, superseded]
project: ts
created: 2026-03-04
updated: 2026-03-04
depends_on: [ts-135]
blocks: []
parent: null
---

# ts-136 core リネーム後の軽量境界リファクタ（superseded）

## 目的
- 本来は `shared -> core` 後の軽量再編を行う想定だった。
- 方針変更により、`core` 中間段階を作らず `shared` から最終3分割へ一括移行する。

## 受け入れ条件
- `ts-135` の一括移行完了をもって、本Issueの目的が包含されること。

## 実施内容
- [ ] 追加作業なし（`ts-135` に統合）

## 結果
- `2026-03-04`: 方針変更により superseded。`ts-135`（shared から最終3分割への一括移行）へ統合。

## 補足
- `core` を中間段階として維持しないため、本Issueは独立実施しない。
