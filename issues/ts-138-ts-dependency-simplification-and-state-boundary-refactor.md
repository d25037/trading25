---
id: ts-138
title: "TS 依存簡素化と state boundary 整理"
status: open
priority: medium
labels: [refactor, dependencies, architecture]
project: ts
created: 2026-03-16
updated: 2026-03-16
depends_on: []
blocks: [ts-139, ts-140, ts-141]
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
- [ ] 依存棚卸しと policy の保守ルールを一本化する
- [ ] route-managed state と session-local state の境界を確定する
- [ ] dependency audit を継続運用できる品質フローへ載せる

## 結果
- 未着手

## 補足
- `zustand` の完全撤去はこの親 issue の完了条件ではない
- Monaco 置換や Radix 全面見直しは別 issue に切り出す
