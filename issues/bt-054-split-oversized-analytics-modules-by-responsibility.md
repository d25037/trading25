---
id: bt-054
title: "oversized analytics module を責務別に分割"
status: open
priority: medium
labels: [bt, analytics, refactor, maintainability]
project: bt
created: 2026-03-27
updated: 2026-03-27
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
- [ ] `hedge_1357_nt_ratio_topix.py` の責務分割方針が定まり、少なくとも内部 helper 群が論理単位へ分かれる。
- [ ] rank research 系も core 抽出後に leaf module が読み切れるサイズまで縮む。
- [ ] result dataclass と public entrypoint は 1 箇所に保ち、内部 module だけを分割する。

## 実施内容
- [ ] `hedge_1357` 系を `market_frame` / `signal_rules` / `hedge_metrics` / `summary_tables` 相当へ分割する。
- [ ] rank research leaf module の bucket-specific helper を近接 module へ分ける。
- [ ] import graph と circular dependency を点検する。
- [ ] notebook / test の import path を最小変更で追従させる。

## 結果
（完了後に記載）

## 補足
- まずは内部 module 分割を優先し、public entrypoint 名は極力維持する。
