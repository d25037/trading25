---
id: bt-058
title: "price_vs_sma bounce / regime research に LightGBM を exploratory 導入する"
status: migrated
original_status: open
github_issue: https://github.com/d25037/trading25/issues/351
migrated_at: 2026-04-22
priority: low
labels: [bt, analytics, lightgbm, research, price-vs-sma, regime]
project: bt
created: 2026-03-31
updated: 2026-03-31
depends_on: [bt-056]
blocks: []
parent: bt-055
---

# bt-058 price_vs_sma bounce / regime research に LightGBM を exploratory 導入する

## 目的
- `price_vs_sma_50_gap + volume_sma_5_20` を主軸に進めている bounce 研究に、LightGBM を exploratory bench として入れる。
- bucket/regime 分析で見えている signal が、連続特徴 + 非線形相互作用でも再現されるかを確認する。

## 背景
- `price_vs_sma` 系は `SMA50` と `volume_sma_5_20 low` が最も強い、という記述的事実までは出ている。
- regime conditioning でも `TOPIX close neutral/strong` や `NT ratio neutral` で強いことが見えている。
- ただし feature 数はまだ少なく、LightGBM を入れても `SMA ratio` 研究ほどの飛躍があるかは不明で、優先順位は lower でよい。

## 受け入れ条件
- [ ] `price_vs_sma` panel を leakage-safe な training frame として再利用できる。
- [ ] `price_vs_sma_20/50/100`、`volume_sma_5_20/20_80/50_150`、regime 連続値を入力にした LightGBM ranking helper を試せる。
- [ ] baseline の bucket/regime summary と同じ評価表で比較できる。
- [ ] OOS で bucket rule を明確に上回るか、上回らないかを判断できる。

## 実施内容
- [ ] `topix100_price_vs_sma_rank_future_close.py` の panel を LightGBM 用に再利用する。
- [ ] regime bucket ではなく regime continuous feature を入れる設計を作る。
- [ ] `q10 bounce` 仮説と `full cross-sectional ranking` の両方で比較する。
- [ ] 効果が弱ければ deterministic bucket/regime rule を主導線に戻す。

## 結果
- 未着手。

## 補足
- 研究 panel: `apps/bt/src/domains/analytics/topix100_price_vs_sma_rank_future_close.py#L1`
- bounce/regime: `apps/bt/src/domains/analytics/topix100_price_vs_sma_q10_bounce_regime_conditioning.py#L1`
- これは runtime 展開前提ではなく、research-only の exploratory issue。
