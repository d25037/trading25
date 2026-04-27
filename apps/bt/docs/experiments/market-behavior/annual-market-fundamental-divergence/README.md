# Annual Market Fundamental Divergence

`annual-first-open-last-close-fundamental-panel` の realized event ledger を入力に、
Prime / Standard / Growth の市場区分ごとに、どのファンダメンタル指標の水準差が
大きいかを比較する研究。

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_market_fundamental_divergence.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_market_fundamental_divergence.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`
  - `summary.json`

## Design

- Input: annual first-open/last-close fundamental panel bundle.
- Universe: input bundle の `status = realized` rows。
- Market split: upstream annual panel と同じ current `stocks.market_code` snapshot
  による retrospective market split。
- Fundamental timing: upstream annual panel で entry date 以前の FY row に切った
  as-of 済み指標だけを使う。この研究では新しい fundamental join は行わない。
- Cross-market scale:
  - 各 feature を `year` 内の全市場共通 z-score と percentile rank に変換する。
  - `year x market` 内で再スケールしないため、市場区分間の水準差が残る。
- Diagnostics:
  - `eps_non_positive_flag`
  - `forward_per_non_positive_flag`
  - `forecast_missing_flag`
  - `cfo_non_positive_flag`
- Return decomposition:
  - `event_return_pct` を winsorize した `event_return_winsor_pct` を目的変数にする。
  - `market_only` から `sector_adjusted`、`value_size_adjusted`、
    `full_fundamental_adjusted` へ段階的に説明変数を足し、market dummy の残差を比較する。
  - market dummy の基準は `prime`。

## Outputs

- `prepared_panel_df`: realized event panel with raw features, year z-scores,
  year percentile ranks, and diagnostic flags.
- `market_feature_profile_df`: market x feature の coverage、median、IQR、
  mean z-score、median percentile。
- `market_pair_divergence_df`: pairwise market comparison
  (`prime_vs_standard`, `prime_vs_growth`, `standard_vs_growth`) with
  standardized mean difference, quantile distance, and KS statistic.
- `feature_divergence_rank_df`: market ごとに、他市場との乖離が大きい feature を
  rank した table。
- `market_return_decomposition_df`: market dummy と追加 factor controls による
  annual return gap decomposition。

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_market_fundamental_divergence.py \
  --output-root /tmp/trading25-research
```

出力先:

`/tmp/trading25-research/market-behavior/annual-market-fundamental-divergence/<run_id>/`

## Interpretation Guide

- まず `feature_divergence_rank_df` で市場ごとの上位 feature を見る。
- 次に `market_pair_divergence_df` で、特に `prime_vs_growth` と
  `standard_vs_growth` の signed difference を確認する。
- `market_feature_profile_df` では raw median と diagnostic flag の比率を見る。
  `forward_per_non_positive_flag` などの flag は percent 単位。
- `market_return_decomposition_df` では、`growth` dummy の係数が
  `market_only` から `full_fundamental_adjusted` でどれだけ縮むかを見る。

## Caveats

- Market split は current snapshot proxy であり、historical market migration は
  厳密には復元していない。
- 乖離が大きい feature は、市場構成を説明する feature であって、単独 alpha とは限らない。
- Diagnostic flags は分布差を見やすくするため percent scale で持つ。
- ADV60 は capacity / execution diagnostic として扱い、alpha feature として過信しない。
