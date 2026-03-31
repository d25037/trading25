---
id: bt-057
title: "fundamental ranking を historical panel 化して LightGBM 候補を検証する"
status: open
priority: medium
labels: [bt, analytics, lightgbm, fundamentals, ranking, screening]
project: bt
created: 2026-03-31
updated: 2026-03-31
depends_on: [bt-056]
blocks: []
parent: bt-055
---

# bt-057 fundamental ranking を historical panel 化して LightGBM 候補を検証する

## 目的
- 現在の単一 ratio 中心の `fundamental ranking` を、複数 feature を持つ historical disclosure panel に拡張し、LightGBM ranking の有効性を検証する。
- `ranking` と `screening` の fundamentals surface を、手組み threshold 群から cross-sectional score へ広げられるか判断する。

## 背景
- 現行の `fundamental ranking` は実質 `eps_forecast_to_actual` を high/low で並べる構造で、feature 合成がない。
- fundamentals engine には `roe`, `per`, `pbr`, `operatingMargin`, `netMargin`, `fcfYield`, `cfoMargin`, `forecastEpsChangeRate`, `dividend/payout` など十分な候補がある。
- ただし fundamentals は開示タイミング、share adjustment、forecast revision の時点整列が必要で、market-price panel よりリーク管理が難しい。

## 受け入れ条件
- [ ] disclosure-date 基準の historical fundamental panel を構築できる。
- [ ] 当時点で利用可能な fundamental feature だけを使う leakage-safe な training frame を定義できる。
- [ ] `future_return` を目的変数とした cross-sectional ranking 実験を notebook or internal helper として実行できる。
- [ ] baseline として現行 `eps_forecast_to_actual` ranking と比較できる。
- [ ] 有望なら API/runtime 展開の判断材料を残す。

## 実施内容
- [ ] fundamentals engine から ranking 用に使える feature 群を棚卸しする。
- [ ] 開示時点・有効期限・price join の SoT を決める。
- [ ] historical panel builder を `domains/analytics` に追加する。
- [ ] LightGBM ranking helper を research-only で追加する。
- [ ] baseline comparison と feature importance を記録する。

## 結果
- 未着手。

## 補足
- 現行 ranking surface: `apps/bt/src/application/services/ranking_service.py#L239`
- 現行 ratio calculator: `apps/bt/src/domains/analytics/fundamental_ranking.py#L123`
- candidate features: `apps/bt/src/domains/fundamentals/calculator.py#L272`
- 主リスクは disclosure alignment と look-ahead bias。
