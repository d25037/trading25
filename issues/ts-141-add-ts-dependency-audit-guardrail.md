---
id: ts-141
title: "TS 依存宣言 drift を防ぐ audit guardrail 追加"
status: open
priority: medium
labels: [tooling, ci, dependencies]
project: ts
created: 2026-03-16
updated: 2026-03-16
depends_on: [ts-139]
blocks: []
parent: ts-138
---

# ts-141 TS 依存宣言 drift を防ぐ audit guardrail 追加

## 目的
- `apps/ts` の dependency declaration drift を検出する軽量 audit を導入し、未使用依存や pin ずれを早期に止める

## 受け入れ条件
- root command から dependency audit を実行できる
- allowlist 付きで unused dependency と missing declaration と override drift を検出できる
- script の unit test があり、local quality flow か CI のいずれかに組み込まれている

## 実施内容
- [ ] audit script と pure helper を `apps/ts/scripts/` に追加する
- [ ] allowlist と override drift ルールを明文化する
- [ ] root task / script から呼び出せるようにする
- [ ] script test を追加し、workspace test で実行する

## 結果
- 未着手

## 補足
- security audit とは別物で、目的は manifest と実 usage の整合確認
- root override-only pin は runtime dependency と区別して扱う
