---
id: bt-041
title: "VectorbtAdapter を抽出し domain から vbt.Portfolio を除去"
status: open
priority: high
labels: [vectorbt, adapter, refactor, domain, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
depends_on: [bt-039, bt-040]
blocks: [bt-045, bt-046]
parent: bt-037
---

# bt-041 VectorbtAdapter を抽出し domain から vbt.Portfolio を除去

## 目的
- `vectorbt` を SoT ではなく backend adapter の 1 つへ格下げする。
- domain / protocol / strategy state に漏れている `vbt.Portfolio` 依存を引き剥がす。

## 受け入れ条件
- [ ] strategy protocol / runtime state / analytics の公開境界から `vbt.Portfolio` が消える。
- [ ] 現行 backtest path は `VectorbtAdapter` 経由で動作する。
- [ ] canonical result writer により `vectorbt` 結果を正規化できる。
- [ ] 回帰テストで既存 backtest/optimization の主要指標が維持される。

## 実施内容
- [ ] `apps/bt/src/domains/strategy/core/mixins/protocols.py` の portfolio 型境界を置き換える。
- [ ] `backtest_executor_mixin.py` / Kelly analyzer / attribution から engine-specific surface を分離する。
- [ ] indicator/signal 計算の `vectorbt` 依存を棚卸しし、必要な純粋計算を抽出する。
- [ ] `VectorbtAdapter` と正規化レイヤーを追加する。

## 結果
- 未着手

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 5.3, 5.5

