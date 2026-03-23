---
id: ts-140
title: "route search state への移行で zustand 責務を縮小"
status: done
priority: high
labels: [refactor, state-management, router]
project: ts
created: 2026-03-16
updated: 2026-03-23
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
- [x] 各 page の route search schema と setter を統一する
- [x] zustand store から URL 管理対象 field を削除する
- [x] 旧 persisted key から search param への migration を入れる
- [x] route search parse / serialize / migration test を整備する

## 結果
- `/charts`, `/portfolio`, `/indices`, `/analysis`, `/backtest` の選択 state を Router search params へ寄せ、再訪・共有時に URL から復元できるようにした。
- `uiStore` は撤去済みで、analysis/backtest/chart store から route-managed field を除去し、active job tracking や panel preset のような session-local state のみに寄せた。
- 旧 persisted key から search params への one-time migration を `usePageRouteState` に実装し、legacy storage key の pruning も入れた。
- `routeSearch` / `usePageRouteState` の parse・serialize・migration test を整備した。
- 実行結果:
  - `bun run quality:typecheck` ✅
  - `bun run workspace:test` ✅

## 補足
- session/job tracking の zustand 利用は継続してよい
- state の SoT は URL 優先、local/session storage は補助扱いにする
