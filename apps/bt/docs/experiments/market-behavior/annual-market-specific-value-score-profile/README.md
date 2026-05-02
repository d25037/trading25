# Annual Market-Specific Value Score Profile

`annual-fundamental-confounder-analysis` と `annual-value-composite-selection` の v3 rerun をつなぎ、Ranking page で Prime / Standard の value score profile を分ける判断を記録する decision research。

## Published Readout

### Decision

Prime と Standard は、小型・低 `PBR`・低 `forward PER` の効き方が違うため、Ranking page では同一 score profile に混ぜず、`standard_pbr_tilt` と `prime_size_tilt` を別 profile として扱う。Standard は低 `PBR` を主役にし、Prime は小型 + 低 `forward PER` を厚く見る。`ADV60` は alpha score に混ぜず、capacity / execution diagnostic として別管理する。

### Why This Research Was Run

v3 PIT stock-master で `standard` の小型・低 `PBR`・低 `forward PER` edge を確認した後、同じ composite を Prime に流用してよいかが問題になった。Prime は Standard よりリターン水準は落ちるが、流動性と運用容量が大きい可能性があるため、独立効果と portfolio lens を市場別に読み直し、Ranking page の score method 実装へ落とした。

### Data Scope / PIT Assumptions

入力 panel は `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_statement_doc_semantics/`。`PBR > 0` と `forward PER > 0` を要求した positive-ratio run を使い、`32,264` realized events から `24,660` analysis / scored events を作った。独立効果は `annual-fundamental-confounder-analysis/20260502_statement_doc_semantics_positive` の `panel_regression_df`、portfolio lens は `annual-value-composite-selection/20260502_statement_doc_semantics_positive` の `portfolio_summary_df` を参照する。

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
| `prime` | `equal_weight` | `10%` | `1,437` | `21.89%` | `1.22` | `-33.41%` |
| `prime` | `fixed_55_25_20` | `10%` | `1,437` | `22.67%` | `1.26` | `-32.93%` |
| `prime` | `walkforward_regression_weight` | `10%` | `1,437` | `23.03%` | `1.27` | `-33.35%` |
| `standard` | `equal_weight` | `10%` | `829` | `34.19%` | `2.10` | `-29.79%` |
| `standard` | `fixed_55_25_20` | `10%` | `829` | `34.33%` | `2.09` | `-30.44%` |
| `standard` | `walkforward_regression_weight` | `10%` | `829` | `34.51%` | `2.12` | `-30.14%` |

#### Ranking Surface

| Surface | Score method | Weight read | Implementation |
| --- | --- | --- | --- |
| Standard Ranking | `standard_pbr_tilt` | small `35%`, low `PBR` `40%`, low `forward PER` `25%` | `1a301d04` |
| Prime Ranking | `prime_size_tilt` | small `45%`, low `PBR` `20%`, low `forward PER` `35%` | `65f34a08` |

### Interpretation

Standard は top-decile return / Sharpe が高く、低 `PBR` の独立効果も最も強いため、PBR tilt を primary profile にする。Prime は Standard より return 水準は低いが、独立効果では小型と低 `forward PER` が強く、portfolio lens でも top-decile は成立している。statement-document semantics fix 後は positive-ratio universe が増えたが、この market-specific read は変わらない。Prime / Standard を単一ランキングに混ぜると、return profile と capacity profile が混在して解釈しにくいため、market-specific sleeve として分ける方が自然。

`walkforward_regression_weight` は説明上は動的推定に見えるが、Ranking page 実装としては固定 profile の方が透明で、旧名は UI / API surface から外した。Prime で `PBR` weight をほぼゼロにするほどの根拠はまだ弱いため、`prime_size_tilt` では低 `PBR` を `20%` 残す。

### Production Implication

Ranking page では、Standard は `standard_pbr_tilt` をデフォルト候補、Prime は `prime_size_tilt` を別 profile として使う。Prime は「低リターン・高キャパシティ候補」として読み、Standard と同じ alpha sleeve として混ぜない。`ADV60 >= 10mn` は alpha factor ではなく、position sizing、capacity、execution risk の診断軸に留める。

### Caveats

この note は新しい runner ではなく、既存 v3 bundle をつなぐ decision record。portfolio lens は年次 open-to-close equal-weight で、コスト、スリッページ、capacity、turnover、borrowability は含まない。Prime / Standard の profile 差は十分に大きいが、weight の細部は誤差を含むため、`standard_pbr_tilt` と `prime_size_tilt` は実装上の説明可能な初期 profile として扱う。

### Source Artifacts

- Input panel: `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_statement_doc_semantics/`
- Independent factor bundle: `/tmp/trading25-research/market-behavior/annual-fundamental-confounder-analysis/20260502_statement_doc_semantics_positive/`
- Portfolio lens bundle: `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_statement_doc_semantics_positive/`
- Ranking implementation:
  - `1a301d04 feat(ranking): add standard pbr tilt value score`
  - `65f34a08 feat(ranking): add prime size tilt value score`
- Related follow-up: [`annual-sector-relative-value-composite`](../annual-sector-relative-value-composite/README.md)

## Current Surface

- Docs-only decision record:
  - `apps/bt/docs/experiments/market-behavior/annual-market-specific-value-score-profile/README.md`
- Source bundles:
  - `annual-fundamental-confounder-analysis/20260502_statement_doc_semantics_positive`
  - `annual-value-composite-selection/20260502_statement_doc_semantics_positive`
- Product surface:
  - Ranking page score method: `standard_pbr_tilt` / `prime_size_tilt`

## Search Keywords

`standard_pbr_tilt`, `prime_size_tilt`, `Prime Value`, `Standard Value`, `Ranking page`, `score profile`, `小型`, `低PBR`, `低forwardPER`, `55/25/20`, `market-specific value score`
