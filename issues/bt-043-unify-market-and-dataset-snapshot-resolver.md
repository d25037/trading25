---
id: bt-043
title: "market / dataset の snapshot resolver を共通化"
status: open
priority: high
labels: [snapshot, market, dataset, resolver, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
depends_on: [bt-038, bt-039]
blocks: [bt-044, bt-045]
parent: bt-037
---

# bt-043 market / dataset の snapshot resolver を共通化

## 目的
- market plane と dataset plane の入力解決を 1 つの snapshot resolver へ寄せる。
- screening / backtest / optimize / lab が同じ input snapshot contract を共有できるようにする。

## 受け入れ条件
- [ ] market latest / market snapshot / dataset snapshot を同一 API で解決できる。
- [ ] `dataset.db` 直参照や loader 個別分岐が snapshot resolver 経由へ置き換わる。
- [ ] run metadata に解決済み snapshot ID を保存できる。
- [ ] 既存 direct mode loader の主要経路に回帰テストがある。

## 実施内容
- [ ] snapshot resolver domain/service を追加する。
- [ ] `apps/bt/src/infrastructure/data_access/clients.py` と loaders を順次置き換える。
- [ ] market / dataset の resolver policy と fallback ルールを定義する。
- [ ] docs と settings 説明を更新する。

## 結果
- 未着手

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 4.2, 4.3, 10

