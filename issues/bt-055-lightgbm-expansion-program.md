---
id: bt-055
title: "LightGBM の project-wide 展開候補を段階管理する"
status: open
priority: high
labels: [bt, analytics, lightgbm, research, ranking, signals]
project: bt
created: 2026-03-31
updated: 2026-03-31
depends_on: []
blocks: [bt-056, bt-057, bt-058]
parent: null
---

# bt-055 LightGBM の project-wide 展開候補を段階管理する

## 目的
- notebook playground 内で有効性が見えた `LightGBM lambdarank` を、project 全体でどこまで展開するかを優先順位つきで管理する。
- `ranking` / `signal` / `fundamental ranking` / 研究 analytics のうち、研究効率または運用価値が大きい候補だけを順に進める。
- 研究用 helper と API/runtime への導入を混同せず、段階的に issue を切って進める。

## 背景
- `TOPIX100 SMA ratio` 研究では `LightGBM` が walk-forward OOS で baseline composite より優位だった。
- 一方で `price_vs_sma` や `q10 bounce` は記述的研究までは進んでいるが、まだ LightGBM を接続していない。
- runtime 側には `TOPIX100 ranking` と `universe_rank_bucket` があり、既存 research の成果を product surface に接続しやすい。
- fundamentals 側は feature 群が厚いが、現状の ranking surface はほぼ単一 ratio に留まっている。

## 受け入れ条件
- [ ] LightGBM 展開候補の優先順位が issue 単位で整理されている。
- [ ] `runtime/signal 直結候補` と `research-only 候補` が分離されている。
- [ ] 各 child issue に、対象 domain、前提データ、期待効果、主要リスクが明記されている。
- [ ] 実施順が `bt-056 -> bt-057 -> bt-058` として明示されている。

## 実施内容
- [ ] `TOPIX100 ranking + signal` への LightGBM scorer 導入 issue を管理する。
- [ ] `fundamental ranking / screening` の historical panel 化 issue を管理する。
- [ ] `price_vs_sma / q10 bounce` の LightGBM exploratory issue を管理する。
- [ ] child issue の結果に応じて優先順位を更新する。

## 結果
- 2026-03-31: tracking issue として作成。現時点の優先順位は `TOPIX100 ranking/signal` を最優先、`fundamental ranking` を次点、`price_vs_sma` 系 LightGBM は exploratory 扱い。

## 補足
- 参照: `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py`
- 参照: `apps/bt/src/application/services/ranking_service.py`
- 参照: `apps/bt/src/domains/strategy/signals/universe_rank_bucket.py`
