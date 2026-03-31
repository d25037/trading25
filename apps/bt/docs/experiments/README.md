# Experiments

`apps/bt` の notebook / domain 実験の索引です。

## Conventions

- 実験コードの SoT は `apps/bt/notebooks/playground/` と `apps/bt/src/domains/analytics/` に置く。
- 長く残す知見は `apps/bt/docs/experiments/` に集約する。
- 各実験は `README.md` を canonical note、`baseline-YYYY-MM-DD.md` を時点固定の結果メモとして残す。
- 画像を固定資産として残す場合のみ `figures/` に保存する。

## Index

- [market-behavior/topix-gap-intraday-distribution/](./market-behavior/topix-gap-intraday-distribution/README.md)
  - TOPIX の寄り付き gap を条件に、個別銘柄群の当日 intraday と簡易 rotation ルールを観察する実験。
- [market-behavior/topix-close-stock-overnight/](./market-behavior/topix-close-stock-overnight/README.md)
  - TOPIX の当日引け変動を条件に、個別銘柄群の `close -> next open` を観察する実験。
- [market-behavior/nt-ratio-change-stock-overnight/](./market-behavior/nt-ratio-change-stock-overnight/README.md)
  - NT 倍率の前日比を条件に、個別銘柄群の `close -> next open` を観察する実験。
- [market-behavior/nt-ratio-change-topix-close-stock-overnight/](./market-behavior/nt-ratio-change-topix-close-stock-overnight/README.md)
  - NT 倍率前日比と TOPIX 引け変動の joint regime ごとに、個別銘柄群の `close -> next open` を観察する実験。
- [market-behavior/topix100-vi-change-regime-conditioning/](./market-behavior/topix100-vi-change-regime-conditioning/README.md)
  - 日経VI 前日比 regime ごとに、TOPIX100 の price/volume split がその後どう振る舞うかを観察する実験。
- [market-behavior/topix100-price-vs-sma-rank-future-close/](./market-behavior/topix100-price-vs-sma-rank-future-close/README.md)
  - TOPIX100 の `price / SMA20|50|100` 単独特徴を decile と price/volume split で比較し、continuation か mean-reversion かを観察する実験。
- [market-behavior/topix100-price-vs-sma-q10-bounce/](./market-behavior/topix100-price-vs-sma-q10-bounce/README.md)
  - `price / SMA` family の `Q10` 側だけを切り出し、`Q10 Low vs ...` の bounce 仮説を feature / horizon ごとに比較する実験。
- [market-behavior/topix100-price-vs-sma-q10-bounce-regime-conditioning/](./market-behavior/topix100-price-vs-sma-q10-bounce-regime-conditioning/README.md)
  - `SMA50 Q10 Low` bounce を same-day `TOPIX close` / `NT ratio` regime で条件付けし、どの market state で反発が強いかを見る実験。
- [market-behavior/topix100-sma-ratio-lightgbm/](./market-behavior/topix100-sma-ratio-lightgbm/README.md)
  - TOPIX100 の 6 本の SMA ratio 特徴に対して、hand-crafted composite baseline と LightGBM ranker を walk-forward OOS で比較する実験。
- [market-behavior/stock-intraday-overnight-share/](./market-behavior/stock-intraday-overnight-share/README.md)
  - 個別銘柄の値幅を `open -> close` と `close -> next open` に分解し、銘柄群ごとの intraday / overnight 構成比を観察する実験。
