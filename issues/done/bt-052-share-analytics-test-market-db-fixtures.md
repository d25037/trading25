---
id: bt-052
title: "analytics unit test の market DB fixture を共通化"
status: done
priority: medium
labels: [bt, analytics, testing, refactor]
project: bt
created: 2026-03-27
updated: 2026-03-30
depends_on: []
blocks: []
parent: bt-049
---

# bt-052 analytics unit test の market DB fixture を共通化

## 目的
- analytics unit test 間で重複している DuckDB schema / stock pattern / regime pattern builder を shared fixture helper にまとめる。
- schema 変更や price pattern 変更の追従を 1 箇所で完結させる。

## 背景
- `apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close.py`、`apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_regime_conditioning.py`、`apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma20_rank_future_close.py`、`apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma20_regime_conditioning.py` がほぼ同じ builder を持つ。
- `prime_ex_topix500` test も schema 定義を複製している。

## 受け入れ条件
- [x] shared fixture helper module が追加される。
- [x] 上記 5 系統の analytics test が shared helper を使う。
- [x] universe 差分、regime table 有無、duplicate 4/5 桁 code の有無を引数で切り替えられる。

## 実施内容
- [x] test 用 market DB builder を helper module に切り出す。
- [x] stocks / stock_data / topix_data / indices_data の schema 定義を 1 箇所へ集約する。
- [x] fixture data pattern を universe / regime 有無ごとに parameter 化する。
- [x] 各 test から重複 builder を削除する。

## 結果
- `apps/bt/tests/unit/analytics_market_research_db.py` を追加し、TOPIX100 / PRIME ex TOPIX500 向け market DB builder を共通化した。
- rank future close / regime conditioning / prime wrapper の unit test が共通 fixture helper を使うように移行した。
- 実装は commit `ea69b7c` で反映した。

## 補足
- helper の置き場所候補: `apps/bt/tests/unit/domains/analytics/_fixtures/`
