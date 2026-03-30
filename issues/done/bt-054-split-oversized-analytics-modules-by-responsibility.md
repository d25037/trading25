---
id: bt-054
title: "oversized analytics module を責務別に分割"
status: done
priority: medium
labels: [bt, analytics, refactor, maintainability]
project: bt
created: 2026-03-27
updated: 2026-03-30
depends_on: []
blocks: []
parent: bt-049
---

# bt-054 oversized analytics module を責務別に分割

## 目的
- 1000 行超の analytics module を、market query / signal construction / metrics summary / report shaping などの責務単位へ分ける。
- review と局所修正のコストを下げ、将来の research 派生を追加しやすくする。

## 背景
- [hedge_1357_nt_ratio_topix.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/hedge_1357_nt_ratio_topix.py) は 1400 行超で、market query、rule signal、beta-neutral 重み、ETF split 比較、annual summary が同居している。
- [topix100_sma_ratio_rank_future_close.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py) も巨大だが、`bt-050` で core 抽出後も leaf 側に残る責務の整理が必要になる。

## 受け入れ条件
- [x] `hedge_1357_nt_ratio_topix.py` の責務分割方針が定まり、少なくとも内部 helper 群が論理単位へ分かれる。
- [x] rank research 系も core 抽出後に leaf module が読み切れるサイズまで縮む。
- [x] result dataclass と public entrypoint は 1 箇所に保ち、内部 module だけを分割する。

## 実施内容
- [x] `hedge_1357` 系を `market_frame` / `signal_rules` / `hedge_metrics` / `summary_tables` 相当へ分割する。
- [x] rank research leaf module の bucket-specific helper を近接 module へ分ける。
- [x] import graph と circular dependency を点検する。
- [x] notebook / test の import path を最小変更で追従させる。

## 結果
- [topix100_sma_ratio_rank_future_close.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py) を leaf module 化し、bucket / selection / support helper を [topix_sma_ratio_rank_future_close_buckets.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix_sma_ratio_rank_future_close_buckets.py)、[topix_sma_ratio_rank_future_close_selection.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix_sma_ratio_rank_future_close_selection.py)、[topix_sma_ratio_rank_future_close_support.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix_sma_ratio_rank_future_close_support.py) へ分離した。
- [hedge_1357_nt_ratio_topix.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/hedge_1357_nt_ratio_topix.py) を thin entrypoint にし、market frame / strategy metrics / summary table / support を各 helper module に切り出した。
- notebook / test の import 経路は public entrypoint を維持したまま追従し、circular dependency なしで `ruff` / `pytest` / `marimo check` を通過させた。
- 実装は commit `c4b1f24` と `c07f2bb` で反映した。

## 補足
- まずは内部 module 分割を優先し、public entrypoint 名は極力維持する。
