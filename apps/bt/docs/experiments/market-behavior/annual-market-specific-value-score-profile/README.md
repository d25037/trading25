# Annual Market-Specific Value Score Profile

`annual-fundamental-confounder-analysis` と `annual-value-composite-selection` の v3 rerun をつなぎ、Ranking page で Prime / Standard の value score profile を分ける判断を記録する decision research。

## Published Readout

### Decision

Prime と Standard は、小型・低 `PBR`・低 `forward PER` の効き方が違うため、Ranking page では同一 score profile に混ぜず、`standard_pbr_tilt` と `prime_size_tilt` を別 profile として扱う。Standard は低 `PBR` を主役にし、Prime は小型 + 低 `forward PER` を厚く見る。`ADV60` は alpha score に混ぜず、capacity / execution diagnostic として別管理する。

### Why This Research Was Run

v3 PIT stock-master で `standard` の小型・低 `PBR`・低 `forward PER` edge を確認した後、同じ composite を Prime に流用してよいかが問題になった。Prime は Standard よりリターン水準は落ちるが、流動性と運用容量が大きい可能性があるため、独立効果と portfolio lens を市場別に読み直し、Ranking page の score method 実装へ落とした。

### Data Scope / PIT Assumptions

入力 panel は `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_share_basis_rerun/`。`PBR > 0` と `forward PER > 0` を要求した positive-ratio run を使い、`32,264` realized events から `24,660` analysis / scored events を作った。独立効果は `annual-fundamental-confounder-analysis/20260502_statement_doc_semantics_positive` の `panel_regression_df`、portfolio lens は `annual-value-composite-selection/20260502_share_basis_positive` の `portfolio_summary_df` と追加 top `5%` profile check を参照する。

### Main Findings

#### 独立効果

`panel_regression_df` の `core_value_size_liquidity` model。係数は winsorized annual return に対する `1sd` あたりの pp。

| Market | Small market cap | Low PBR | Low forward PER | Read |
| --- | ---: | ---: | ---: | --- |
| `prime` | `+3.83pp / t=6.52` | `+2.98pp / t=7.70` | `+3.72pp / t=10.53` | 小型と低 `forward PER` が主役。低 `PBR` も正だが相対的に薄い。 |
| `standard` | `+2.51pp / t=5.42` | `+5.41pp / t=9.92` | `+3.07pp / t=6.31` | 低 `PBR` が最も強い。低 `forward PER` と小型も残る。 |

#### Portfolio Lens

`portfolio_summary_df` の no-liquidity-floor / top `10%`。`55/25/20` は小型 `55%`、低 `PBR` `25%`、低 `forward PER` `20%`。

| Market | Score | Top | Events | CAGR | Sharpe | MaxDD |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `prime` | `equal_weight` | `10%` | `1,437` | `21.59%` | `1.21` | `-33.49%` |
| `prime` | `fixed_55_25_20` | `10%` | `1,437` | `22.52%` | `1.26` | `-33.07%` |
| `prime` | `walkforward_regression_weight` | `10%` | `1,437` | `23.08%` | `1.27` | `-33.11%` |
| `standard` | `equal_weight` | `10%` | `829` | `34.68%` | `2.12` | `-29.79%` |
| `standard` | `fixed_55_25_20` | `10%` | `829` | `34.58%` | `2.11` | `-30.44%` |
| `standard` | `walkforward_regression_weight` | `10%` | `829` | `34.71%` | `2.13` | `-30.14%` |

#### top `5%` profile check

| Market | Score profile | Top | Events | CAGR | Sharpe | MaxDD | Read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `prime` | `equal_weight` | `5%` | `721` | `24.97%` | `1.35` | `-31.17%` | baseline |
| `prime` | old `prime_size_tilt` small `45%` / PBR `20%` / fPER `35%` | `5%` | `721` | `25.54%` | `1.38` | `-31.67%` | previous implementation |
| `prime` | new `prime_size_tilt` small `46.5%` / PBR `5%` / fPER `48.5%` | `5%` | `721` | `26.98%` | `1.44` | `-31.84%` | best Prime top `5%` row |
| `standard` | `equal_weight` | `5%` | `418` | `44.61%` | `2.11` | `-30.12%` | competitive |
| `standard` | current `standard_pbr_tilt` small `35%` / PBR `40%` / fPER `25%` | `5%` | `418` | `44.08%` | `2.10` | `-29.28%` | best drawdown |
| `standard` | walk-forward approx small `40.6%` / PBR `34.3%` / fPER `25.1%` | `5%` | `418` | `44.87%` | `2.12` | `-29.50%` | marginally best return |

#### Ranking Surface

| Surface | Score method | Weight read | Implementation |
| --- | --- | --- | --- |
| Standard Ranking | `standard_pbr_tilt` | small `35%`, low `PBR` `40%`, low `forward PER` `25%` | `1a301d04` |
| Prime Ranking | `prime_size_tilt` | small `46.5%`, low `PBR` `5%`, low `forward PER` `48.5%` | share-basis top `5%` update |

### Interpretation

Standard は top-decile return / Sharpe が高く、低 `PBR` の独立効果も最も強いため、PBR tilt を primary profile にする。Prime は Standard より return 水準は低いが、独立効果では小型と低 `forward PER` が強く、portfolio lens でも top-decile は成立している。statement-document semantics fix 後は positive-ratio universe が増えたが、この market-specific read は変わらない。Prime / Standard を単一ランキングに混ぜると、return profile と capacity profile が混在して解釈しにくいため、market-specific sleeve として分ける方が自然。

`walkforward_regression_weight` は説明上は動的推定に見えるが、Ranking page 実装としては固定 profile の方が透明で、旧名は UI / API surface から外した。share-basis rerun 後の top `5%` check では、Prime は低 `PBR` を `20%` 残すより、低 `forward PER` と小型へ寄せた profile の方が良かったため、`prime_size_tilt` は低 `PBR` を `5%` まで下げる。

### Production Implication

Ranking page では、Standard は `standard_pbr_tilt` をデフォルト候補、Prime は `prime_size_tilt` を別 profile として使う。Prime は「低リターン・高キャパシティ候補」として読み、Standard と同じ alpha sleeve として混ぜない。`ADV60 >= 10mn` は alpha factor ではなく、position sizing、capacity、execution risk の診断軸に留める。

### Caveats

この note は新しい runner ではなく、既存 v3 bundle をつなぐ decision record。portfolio lens は年次 open-to-close equal-weight で、コスト、スリッページ、capacity、turnover、borrowability は含まない。Prime / Standard の profile 差は十分に大きいが、weight の細部は誤差を含むため、`standard_pbr_tilt` と `prime_size_tilt` は実装上の説明可能な初期 profile として扱う。

### Source Artifacts

- Input panel: `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_share_basis_rerun/`
- Independent factor bundle: `/tmp/trading25-research/market-behavior/annual-fundamental-confounder-analysis/20260502_statement_doc_semantics_positive/`
- Portfolio lens bundle: `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive/`
- Ranking implementation:
  - `1a301d04 feat(ranking): add standard pbr tilt value score`
  - `prime_size_tilt` share-basis top `5%` update: small `46.5%`, low `PBR` `5%`, low `forward PER` `48.5%`
- Related follow-up: [`annual-sector-relative-value-composite`](../annual-sector-relative-value-composite/README.md)

## Current Surface

- Docs-only decision record:
  - `apps/bt/docs/experiments/market-behavior/annual-market-specific-value-score-profile/README.md`
- Source bundles:
  - `annual-fundamental-confounder-analysis/20260502_statement_doc_semantics_positive`
  - `annual-value-composite-selection/20260502_share_basis_positive`
- Product surface:
  - Ranking page score method: `standard_pbr_tilt` / `prime_size_tilt`

## Search Keywords

`standard_pbr_tilt`, `prime_size_tilt`, `Prime Value`, `Standard Value`, `Ranking page`, `score profile`, `小型`, `低PBR`, `低forwardPER`, `55/25/20`, `market-specific value score`
