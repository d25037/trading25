---
id: bt-037
title: "listed-market fundamentals scope に合わせて Prime 命名の外部契約を整理"
status: done
priority: medium
labels: [api, contracts, schema, breaking-change, bt, ts]
project: bt
created: 2026-03-06
updated: 2026-03-06
closed: 2026-03-06
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
- [x] backend schema / response field の Prime 命名を listed-market に整合する名前へ変更する移行方針が定義される。
- [x] breaking change として contracts / generated types / web consumer の更新手順が明確化される。
- [x] 既存 consumer への移行期間または互換レイヤーの扱いが決まる。

## 実施内容
- [x] OpenAPI / schema / contracts の rename 対象を棚卸しする。
- [x] bt backend と ts consumer の影響範囲を整理する。
- [x] 移行方式を breaking rename に決定し、互換 alias は撤去する。

## 結果
- `GET /api/db/stats` の `fundamentals` から `primeCoverage` を削除し、`listedMarketCoverage` のみを外部契約として残した。
- `GET /api/db/validate` の `fundamentals` から `missingPrimeStocks*` を削除し、`missingListedMarketStocks*` のみを返すようにした。
- `apps/ts/packages/contracts` の手書き型・OpenAPI snapshot・generated types と `apps/ts/packages/web` の consumer/tests を breaking rename に合わせて更新した。

## 補足
- 互換レイヤーは持たず、bt/ts を同時更新する breaking change として扱う。
