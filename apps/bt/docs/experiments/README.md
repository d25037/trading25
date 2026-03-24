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
