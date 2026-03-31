---
id: bt-050
title: "TOPIX rank research の shared core を抽出"
status: done
priority: high
labels: [bt, analytics, refactor]
project: bt
created: 2026-03-27
updated: 2026-03-30
depends_on: []
blocks: []
parent: bt-049
---

# bt-050 TOPIX rank research の shared core を抽出

## 目的
- universe query、rolling feature enrichment、decile 付与、horizon 展開、daily mean / significance 集計を shared core として切り出す。
- `price_sma_*` 版と `price vs 20SMA gap` 版を同じ engine に載せ、差分を feature spec と bucket spec に閉じ込める。

## 背景
- `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py` と `apps/bt/src/domains/analytics/topix100_price_vs_sma20_rank_future_close.py` は、query / warmup / horizon / pairwise / Holm 補正の大半を共有している。
- `prime_ex_topix500` はほぼ preset wrapper であり、universe spec 化の余地が大きい。

## 受け入れ条件
- [x] shared core module が作成され、query / enrichment / decile / significance の責務が移される。
- [x] SMA ratio 版と price-vs-SMA20 版が shared core を使う。
- [x] `prime_ex_topix500` wrapper は preset 指定のみの薄い module に留まる。
- [x] 既存の notebook / test / public result shape を壊さずに移行できる。

## 実施内容
- [x] `universe spec`、`feature spec`、`bucket spec` の dataclass / protocol を設計する。
- [x] `_query_universe_stock_history`、`_default_start_date`、`_build_horizon_panel`、Holm/pairwise 系 helper を shared module へ移す。
- [x] SMA ratio 版と price-vs-SMA20 版の bucket-specific 処理だけを leaf module に残す。
- [x] wrapper / notebook / test の import 経路を更新する。

## 結果
- `apps/bt/src/domains/analytics/topix_rank_future_close_core.py` を追加し、universe history query、warmup/default dates、horizon panel、Holm/pairwise などの shared helper を集約した。
- `apps/bt/src/domains/analytics/topix100_price_vs_sma20_rank_future_close.py` を最初の consumer として shared core へ移し、その後 `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py` も delegate 化した。
- `apps/bt/src/domains/analytics/prime_ex_topix500_sma_ratio_rank_future_close.py` は preset wrapper として維持した。
- 実装は commit `2c8211c` と `4598aa1` で段階反映した。

## 補足
- 第一対象: `topix100_sma_ratio_rank_future_close.py`
- 第二対象: `topix100_price_vs_sma20_rank_future_close.py`
