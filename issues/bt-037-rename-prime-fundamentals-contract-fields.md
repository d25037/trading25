---
id: bt-037
title: "listed-market fundamentals scope に合わせて Prime 命名の外部契約を整理"
status: open
priority: medium
labels: [api, contracts, schema, breaking-change, bt, ts]
project: bt
created: 2026-03-06
updated: 2026-03-06
depends_on: []
blocks: []
parent: null
---

# bt-037 listed-market fundamentals scope に合わせて Prime 命名の外部契約を整理

## 目的
- fundamentals sync / validation / stats の対象が listed-market に拡張された現状に合わせて、外部契約の `primeCoverage` / `missingPrimeStocks*` などの命名を整理する。

## 背景
- 現在の実装挙動は `prime/standard/growth` を対象にしているが、API schema と生成型は Prime 命名を維持して後方互換を優先している。
- このままだと consumer 側で意味と名前がずれ、将来の運用や契約管理で誤解を生みやすい。

## 受け入れ条件
- [ ] backend schema / response field の Prime 命名を listed-market に整合する名前へ変更する移行方針が定義される。
- [ ] breaking change として contracts / generated types / web consumer の更新手順が明確化される。
- [ ] 既存 consumer への移行期間または互換レイヤーの扱いが決まる。

## 実施内容
- [ ] OpenAPI / schema / contracts の rename 対象を棚卸しする。
- [ ] bt backend と ts consumer の影響範囲を整理する。
- [ ] 移行方式（breaking rename または alias 併存期間）を決める。

## 結果
- 未着手

## 補足
- 現在の実装は挙動優先で後方互換を維持しており、本 issue は命名整合の追補タスク。
