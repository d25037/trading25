---
id: ts-140
title: "route search state への移行で zustand 責務を縮小"
status: open
priority: high
labels: [refactor, state-management, router]
project: ts
created: 2026-03-16
updated: 2026-03-16
depends_on: []
blocks: []
parent: ts-138
---

# ts-140 route search state への移行で zustand 責務を縮小

## 目的
- URL と相性の良い UI state を TanStack Router search params に寄せ、zustand を chart preset / panel visibility / active job tracking 中心へ縮小する

## 受け入れ条件
- `/charts`, `/portfolio`, `/indices`, `/analysis`, `/backtest` の選択 state が URL で復元できる
- 旧 persisted state からの one-time migration が動作する
- `uiStore` が撤去され、analysis/backtest/chart store から route-managed field が除去されている

## 実施内容
- [ ] 各 page の route search schema と setter を統一する
- [ ] zustand store から URL 管理対象 field を削除する
- [ ] 旧 persisted key から search param への migration を入れる
- [ ] route search parse / serialize / migration test を整備する

## 結果
- 未着手

## 補足
- session/job tracking の zustand 利用は継続してよい
- state の SoT は URL 優先、local/session storage は補助扱いにする
