---
id: bt-051
title: "regime conditioning research の shared core を抽出"
status: done
priority: high
labels: [bt, analytics, refactor, regime]
project: bt
created: 2026-03-27
updated: 2026-03-30
depends_on: [bt-050]
blocks: []
parent: bt-049
---

# bt-051 regime conditioning research の shared core を抽出

## 目的
- TOPIX / NT倍率 regime の query、sigma bucket 化、collapsed regime 化、regime 別 summary / pairwise / hypothesis 集計を shared core にまとめる。
- `price_sma_20_80 x volume_sma_20_80` 版と `price vs 20SMA gap x volume_sma_20_80` 版を、split panel 入力だけ差し替えて使える構造にする。

## 背景
- `apps/bt/src/domains/analytics/topix100_sma_ratio_regime_conditioning.py` は bucket spec と regime engine が密結合している。
- `apps/bt/src/domains/analytics/topix100_price_vs_sma20_regime_conditioning.py` は現在 thin wrapper だが、shared API が曖昧なまま base module の private helper に依存している。

## 受け入れ条件
- [x] regime market query / stats / assignment / summary / pairwise / hypothesis を shared module に抽出できる。
- [x] SMA ratio 版と price-vs-SMA20 版は split panel provider の違いだけで動く。
- [x] collapsed regime と original 5-bucket の双方が後方互換を保つ。

## 実施内容
- [x] market regime history query と stats builder を shared core 化する。
- [x] `split_panel_df -> horizon_panel_df -> regime summary` の pipeline を共通化する。
- [x] hypothesis spec と bucket label map の注入ポイントを整理する。
- [x] price-vs-SMA20 wrapper の private helper 依存をなくす。

## 結果
- `apps/bt/src/domains/analytics/topix_regime_conditioning_core.py` を追加し、market regime query、sigma bucket assignment、collapsed regime、summary / pairwise / hypothesis 集計を shared 化した。
- `apps/bt/src/domains/analytics/topix100_sma_ratio_regime_conditioning.py` と `apps/bt/src/domains/analytics/topix100_price_vs_sma20_regime_conditioning.py` を split panel provider 差分だけに寄せた。
- 実装は commit `e35ac22` で反映した。

## 補足
- `REGIME_TYPE_ORDER`、sigma threshold、bucket collapse ルールは shared SoT に寄せる。
