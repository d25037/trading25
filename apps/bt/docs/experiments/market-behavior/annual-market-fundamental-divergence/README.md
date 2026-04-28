# Annual Market Fundamental Divergence

`annual-first-open-last-close-fundamental-panel` の realized event ledger を入力に、
Prime / Standard / Growth の市場区分ごとに、どのファンダメンタル指標の水準差が
大きいかを比較する研究。

## Published Readout

### Decision

Growth は現行の年間ファンダメンタル・バリュー系ユニバースに広く混ぜず、
市場区分そのものを強い事前フィルタとして扱う。Growth を使う場合は、
別枠の quality-screened Growth 研究として切り出し、赤字・予想欠損・低CFO・
無配/低配当の混在を先に除く設計を前提にする。

### Why This Research Was Run

年間保有パネルでは Standard と Growth の挙動が同じ「小型株」では説明しにくく、
Growth の弱さが高PBRだけなのか、利益・予想・キャッシュフロー・配当を含む
複合的なファンダメンタル差なのかを切り分ける必要があった。特に、
市場区分別のリターン差が既存の sector / value / size / liquidity /
fundamental controls でどこまで縮むかを確認するために実行した。

### Data Scope / PIT Assumptions

入力は `annual-first-open-last-close-fundamental-panel` の
`20260423_forward_eps_ratio` bundle で、分析期間は `2017-01-04` から
`2025-12-30`、realized events は `29,294` 件。対象は upstream panel の
`status = realized` のみで、リターンは `1% / 99%` winsorize 後の
`event_return_winsor_pct` を分解に使った。

ファンダメンタル指標は upstream annual panel 側で entry date 以前の FY row に
as-of 済みの値だけを利用し、この研究では新しい fundamental join は行っていない。
市場区分は current `stocks.market_code` snapshot による retrospective proxy で、
厳密な historical market migration は復元していない。特徴量は各年内の全市場共通
z-score / percentile に変換し、市場内で再スケールしないため、市場区分間の水準差を
残して比較している。

### Main Findings

#### Growth は年間保有の broad universe に混ぜるには弱く、market-level prior として別扱いにする。

| Market | Events | Mean | Median |
| --- | ---: | ---: | ---: |
| Prime | `13,632` | `12.34%` | `6.31%` |
| Standard | `12,899` | `8.93%` | `1.82%` |
| Growth | `2,763` | `3.02%` | `-8.72%` |

#### Growth の弱さは高PBR単独ではなく、利益・予想・CFO・配当の悪化が重なった profile と読む。

| Feature | Growth divergence score |
| --- | ---: |
| 予想配当性向 | `1.66` |
| 予想配当利回り | `1.66` |
| 配当利回り | `1.64` |
| EPS <= 0 flag | `1.58` |
| forward PER <= 0 flag | `1.56` |
| CFO yield <= 0 flag | `1.46` |
| forecast missing flag | `1.45` |
| ROA | `1.43` |
| PBR | `1.02` |

#### Growth は非正EPS・非正forward PER・予想欠損・CFO悪化が Prime / Standard よりかなり多い。

| Metric | Prime | Standard | Growth |
| --- | ---: | ---: | ---: |
| EPS <= 0 share | `5.52%` | `16.14%` | `34.59%` |
| forward PER <= 0 share | `3.01%` | `7.35%` | `21.22%` |
| forecast missing share | `7.56%` | `9.25%` | `25.55%` |
| CFO yield <= 0 share | `9.54%` | `18.00%` | `35.24%` |

#### Standard は小さく流動性も低いが、Growth と同じ悪い小型株 bucket ではない。

| Metric | Prime | Standard | Growth |
| --- | ---: | ---: | ---: |
| median market cap | `66.82bn JPY` | `6.82bn JPY` | `5.68bn JPY` |
| ADV60 | `285.70mn JPY` | `12.87mn JPY` | `49.07mn JPY` |
| median PBR | n/a | `0.83` | `2.93` |

#### 観測した fundamentals は Growth penalty の一部しか説明しない。

| Model | Growth dummy | R2 |
| --- | ---: | ---: |
| `market_only` | `-10.14pp` | `0.156` |
| `full_fundamental_adjusted` | `-8.92pp` | `0.169` |

### Interpretation

Growth の弱さは「高PBRだから避ける」という単純な話ではない。高PBRは確かに見えるが、
より大きな差は、赤字または非正の予想PER分母、予想欠損、低いCFO yield、
無配/低配当、弱いROAが同時に集まることにある。つまり Growth は value 指標の
逆張り対象というより、利益・予想・キャッシュフローの品質が崩れた銘柄群を多く含む
市場区分として読んだ方がよい。

Standard は Prime より小さく、流動性も低い。中央値の時価総額は Prime
`66.82bn JPY` に対して Standard `6.82bn JPY`、ADV60 は Prime `285.70mn JPY` に
対して Standard `12.87mn JPY`。ただし Standard は PBR 中央値 `0.83` と安く、
Growth ほど非正EPS・非正forward PER・予想欠損・CFO悪化が重くないため、
Growth と同じ悪い小型株バケットとして扱うべきではない。

### Production Implication

production 側では、Growth を広い年間 value / fundamental universe に自動投入する
よりも、まず除外または強い減点として扱うのが妥当。Standard は capacity /
execution diagnostic を厳しく見る必要があるが、Growth のような残差ペナルティとは
別問題として扱う。Growth を採用するなら、EPS > 0、forward PER > 0、forecast
available、CFO yield > 0、ROA、配当/予想配当の存在などを組み合わせた専用の
quality gate を作り、Prime / Standard と同じ閾値で混ぜない。

### Caveats

リターン分解では Growth dummy が `market_only` の `-10.14pp` から
`full_fundamental_adjusted` の `-8.92pp` までしか縮まらず、R2 も `0.156` から
`0.169` 程度に留まった。これは観測した feature が差の一部を説明する一方で、
まだ大きな市場レベルの残差ペナルティがあることを示す。ただし、これは因果推定ではなく、
current market snapshot proxy と annual realized panel に基づく横断比較である。

ADV60 は alpha feature ではなく capacity / execution diagnostic として読む。
また、diagnostic flag は percent scale の分布差を見るための補助指標であり、
単体の売買ルールとして扱わない。

### Source Artifacts

- Canonical note: `apps/bt/docs/experiments/market-behavior/annual-market-fundamental-divergence/README.md`
- Baseline readout: `apps/bt/docs/experiments/market-behavior/annual-market-fundamental-divergence/baseline-2026-04-27.md`
- Bundle summary: `/tmp/trading25-research/market-behavior/annual-market-fundamental-divergence/20260427_market_fundamental_divergence_v2/summary.md`
- Bundle JSON: `/tmp/trading25-research/market-behavior/annual-market-fundamental-divergence/20260427_market_fundamental_divergence_v2/summary.json`
- Bundle database: `/tmp/trading25-research/market-behavior/annual-market-fundamental-divergence/20260427_market_fundamental_divergence_v2/results.duckdb`

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
