---
id: bt-051
title: "regime conditioning research の shared core を抽出"
status: open
priority: high
labels: [bt, analytics, refactor, regime]
project: bt
created: 2026-03-27
updated: 2026-03-27
depends_on: [bt-050]
blocks: []
parent: bt-049
---

# bt-051 regime conditioning research の shared core を抽出

## 目的
- TOPIX / NT倍率 regime の query、sigma bucket 化、collapsed regime 化、regime 別 summary / pairwise / hypothesis 集計を shared core にまとめる。
- `price_sma_20_80 x volume_sma_20_80` 版と `price vs 20SMA gap x volume_sma_20_80` 版を、split panel 入力だけ差し替えて使える構造にする。

## 背景
- [topix100_sma_ratio_regime_conditioning.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix100_sma_ratio_regime_conditioning.py) は bucket spec と regime engine が密結合している。
- [topix100_price_vs_sma20_regime_conditioning.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix100_price_vs_sma20_regime_conditioning.py) は現在 thin wrapper だが、shared API が曖昧なまま base module の private helper に依存している。

## 受け入れ条件
- [ ] regime market query / stats / assignment / summary / pairwise / hypothesis を shared module に抽出できる。
- [ ] SMA ratio 版と price-vs-SMA20 版は split panel provider の違いだけで動く。
- [ ] collapsed regime と original 5-bucket の双方が後方互換を保つ。

## 実施内容
- [ ] market regime history query と stats builder を shared core 化する。
- [ ] `split_panel_df -> horizon_panel_df -> regime summary` の pipeline を共通化する。
- [ ] hypothesis spec と bucket label map の注入ポイントを整理する。
- [ ] price-vs-SMA20 wrapper の private helper 依存をなくす。

## 結果
（完了後に記載）

## 補足
- `REGIME_TYPE_ORDER`、sigma threshold、bucket collapse ルールは shared SoT に寄せる。
