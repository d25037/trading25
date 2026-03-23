---
id: ts-138
title: "TS 依存簡素化と state boundary 整理"
status: done
priority: medium
labels: [refactor, dependencies, architecture]
project: ts
created: 2026-03-16
updated: 2026-03-23
depends_on: []
blocks: []
parent: null
---

# ts-138 TS 依存簡素化と state boundary 整理

## 目的
- `apps/ts` の依存宣言を責務ベースで整理し、zustand と route search state の境界を明確にする
- 依存 drift を再発させない guardrail を整える

## 受け入れ条件
- dependency policy が `apps/ts/README.md` に明文化されている
- route search state と zustand の責務分離方針が実装と issue に反映されている
- dependency audit guardrail が root command から実行できる
- `ts-139`, `ts-140`, `ts-141` が完了している

## 実施内容
- [x] 依存棚卸しと policy の保守ルールを一本化する
- [x] route-managed state と session-local state の境界を確定する
- [x] dependency audit を継続運用できる品質フローへ載せる

## 結果
- `apps/ts/README.md` に dependency policy を明文化し、keep/remove/pin 方針と `quality:deps:audit` の運用を集約した。
- Router search params を SoT にする画面選択 state と、zustand に残す session-local state の境界を `routeSearch` / `usePageRouteState` / 各 store 実装へ反映した。
- dependency audit guardrail を root command から実行できるようにし、`quality:typecheck` と `workspace:test` フローへ組み込んだ。
- 子 issue `ts-139` / `ts-140` / `ts-141` を完了扱いに更新した。
- 実行結果:
  - `bun run quality:typecheck` ✅
  - `bun run workspace:test` ✅
  - `bun run workspace:build` ✅

## 補足
- `zustand` の完全撤去はこの親 issue の完了条件ではない
- Monaco 置換や Radix 全面見直しは別 issue に切り出す
