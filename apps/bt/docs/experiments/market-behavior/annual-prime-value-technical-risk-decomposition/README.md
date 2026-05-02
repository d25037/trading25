# Annual Prime Value Technical Risk Decomposition

[`annual-value-technical-feature-importance`](../annual-value-technical-feature-importance/README.md)
の Prime rerun で、Standard の `rebound_from_252d_low_pct` 仮説が弱く、代わりに volatility family だけ平均 return が強く見えたため、Prime value technical effect が alpha なのか risk exposure なのかを切り分ける研究。

## Published Readout

### Decision

Prime value では Standard のような `rebound_from_252d_low_pct` / `return_252d_pct` overlay をそのまま使わない。Prime で強く見える technical は stock volatility family で、event lens では p10 を悪化させ、idiosyncratic volatility と beta を同時に増やす。一方、日次 portfolio lens では high-vol bucket が CAGR / Sharpe を明確に押し上げるが、MaxDD は悪化する。したがって alpha selector ではなく、right-tail capture を伴う return engine + drawdown budget exposure として扱う。

Prime profile の次の研究は「高ボラを買うか」ではなく、「高ボラ右尾を残しながら p10 / worst をどこまで制御できるか」に置く。Standard の rebound overlay とは別設計にする。

### Why This Research Was Run

Standard value では、top `5%` / top `10%` とも `rebound_from_252d_low_pct` と `topix_volatility_60d_pct` が強かった。一方、Prime rerun では `rebound_from_252d_low_pct`、`return_252d_pct`、`return_20d_pct`、`volume_ratio_20_60` がほぼ効かず、stock volatility 系だけが平均 return を押し上げた。この違いが「本当の alpha」なのか「リスクを増やしただけ」なのかを確認した。

### Data Scope / PIT Assumptions

入力は positive-ratio value bundle `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive/`。分析対象は `prime` / `none` / top `5%` and top `10%` / `equal_weight`, `walkforward_regression_weight` の `4,316` selected events。`fixed_55_25_20` は除外した。

technical と market-risk feature は `entry_date` より前の日次 return だけで計算する。`beta_adjusted_event_return_pct` は、entry 前 `252d` beta に同じ holding period の TOPIX return を掛けて event return から控除した近似値。

### Main Findings

#### 結論

| Fraction | Feature | Mean spread | P10 spread | Beta-adjusted spread | Beta spread | Idio vol spread | 読み |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `top5` | `volatility_60d_pct` | `+17.56pp` | `-6.50pp` | `+12.49pp` | `+0.44` | `+21.28pp` | beta だけではなく idio vol exposure |
| `top5` | `volatility_20d_pct` | `+16.26pp` | `-5.20pp` | `+12.03pp` | `+0.38` | `+17.27pp` | 右尾は強いが左尾悪化 |
| `top5` | `beta_252d` | `+15.27pp` | `-10.73pp` | `+7.52pp` | `+0.79` | `+12.55pp` | market beta と high idio risk の混合 |
| `top5` | `downside_volatility_60d_pct` | `+14.82pp` | `-9.56pp` | `+11.06pp` | `+0.36` | `+17.65pp` | downside vol high は特に左尾が悪い |
| `top10` | `volatility_20d_pct` | `+14.69pp` | `-5.53pp` | `+10.86pp` | `+0.34` | `+16.65pp` | top10 でも同じ構図 |
| `top10` | `volatility_60d_pct` | `+14.28pp` | `-8.73pp` | `+9.64pp` | `+0.40` | `+20.40pp` | 平均 return と引き換えに左尾を悪化 |

`high - low` spread は、同一年の cross-sectional bucket で high bucket から low bucket を引いた値。平均 return は強いが、p10 は全て悪化した。beta-adjusted return でも平均 spread は残るため、単純に TOPIX beta を買っているだけではない。一方で idiosyncratic volatility spread が大きく、left-tail cost が明確。

#### Not Market-Regime Alpha

| Fraction | Feature | Mean spread | P10 spread | Beta-adjusted spread | 読み |
| --- | --- | ---: | ---: | ---: | --- |
| `top5` | `topix_volatility_60d_pct` | `-6.97pp` | `-8.28pp` | `-7.27pp` | TOPIX volatility regime は Prime value に有効ではない |
| `top10` | `topix_volatility_60d_pct` | `-11.06pp` | `-8.34pp` | `-11.37pp` | Standard で強かった低TOPIXボラ仮説は Prime では崩れる |
| `top5` | `correlation_topix_252d` | `-0.36pp` | `-6.94pp` | `-3.87pp` | TOPIX correlation high は alpha ではない |
| `top10` | `correlation_topix_252d` | `-2.62pp` | `-4.23pp` | `-5.80pp` | market sensitivity だけでは説明できない |

Prime の high-vol effect は market regime や TOPIX correlation ではなく、stock-specific volatility / idiosyncratic volatility 側に寄っている。

#### Daily Portfolio Lens

| Fraction | Score method | Feature high bucket | CAGR baseline → high | Sharpe baseline → high | MaxDD baseline → high | Worst year baseline → high | Worst trade baseline → high |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `top5` | `equal_weight` | `volatility_20d_pct` | `24.91% → 36.29%` | `1.35 → 1.49` | `-31.17% → -38.57%` | `2018 -11.72% → 2018 -3.49%` | `-51.79% → -47.23%` |
| `top5` | `walkforward_regression_weight` | `volatility_60d_pct` | `26.31% → 39.33%` | `1.41 → 1.51` | `-32.00% → -40.82%` | `2018 -9.81% → 2018 -2.70%` | `-55.84% → -54.83%` |
| `top10` | `equal_weight` | `volatility_20d_pct` | `21.59% → 31.98%` | `1.21 → 1.43` | `-33.49% → -39.75%` | `2018 -15.41% → 2018 -15.14%` | `-57.21% → -57.21%` |
| `top10` | `walkforward_regression_weight` | `volatility_20d_pct` | `23.08% → 34.15%` | `1.27 → 1.46` | `-33.11% → -37.92%` | `2018 -14.29% → 2018 -7.78%` | `-57.21% → -57.21%` |

日次 path では、high-vol bucket は top `5%` / top `10%` とも CAGR と Sharpe を改善した。代表的には top `5%` / `walkforward_regression_weight` / `volatility_60d_pct` high bucket で CAGR は `26.31%` から `39.33%`、Sharpe は `1.41` から `1.51` に上がる。ただし MaxDD は `-32.00%` から `-40.82%` へ悪化した。worst trade は大きく改善しないため、event-level p10 悪化と整合的に、右尾で勝つが深い drawdown を許容する profile。

### Interpretation

Prime value の technical は、Standard の「反転開始を拾う」構図ではない。Prime で平均 return が伸びるのは、volatility high bucket が right-tail を持っているから。ただしその bucket は p10 を悪化させ、idiosyncratic volatility を大きく増やす。日次 portfolio lens では CAGR / Sharpe は改善するので単純に避ける対象ではないが、MaxDD が `5pp` から `9pp` 程度悪化するため、position sizing と drawdown budget の問題として扱うべき。

beta-adjusted mean spread が残るため、「TOPIX beta を買っているだけ」とは言い切れない。しかし `beta_252d` 自体も mean spread が強く p10 が悪いので、market beta と stock-specific risk が混ざった高リスク bucket と見るべき。

### Production Implication

Prime value profile に `rebound_from_252d_low_pct` を Standard と同じ重みで入れない。Prime で technical を使うなら、volatility high を単純な除外条件にも単純加点にもせず、risk budget / position sizing / cap として扱う。CAGR / Sharpe を取りにいくなら high-vol bucket は候補に残るが、MaxDD と worst trade を別制約で見る。

次の Prime 研究は、以下を同時に満たす high-vol subset があるかを見る。

| Candidate split | 目的 |
| --- | --- |
| high vol + low beta | market beta ではない右尾だけ残せるか |
| high vol + positive value residual | value score で説明できない右尾か |
| high vol + liquidity/capacity floor | 低流動性 lottery を避けられるか |
| high vol + low downside vol / low max drawdown | p10 悪化を抑えられるか |

### Caveats

`beta_adjusted_event_return_pct` は entry 前 beta を年次 holding period の TOPIX return に掛けた近似で、日次 path の実ポートフォリオ beta-adjusted PnL ではない。日次 portfolio lens は `entry_open` から日次 `close` への equal-weight path で、年次 rebalance 中の signal 更新、約定コスト、position sizing、capacity は未反映。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_prime_value_technical_risk_decomposition.py`
- Runner: `apps/bt/scripts/research/run_annual_prime_value_technical_risk_decomposition.py`
- Bundle: `/tmp/trading25-research/market-behavior/annual-prime-value-technical-risk-decomposition/20260502_prime_value_technical_risk_decomposition_v3/`
- Results DB: `/tmp/trading25-research/market-behavior/annual-prime-value-technical-risk-decomposition/20260502_prime_value_technical_risk_decomposition_v3/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/annual-prime-value-technical-risk-decomposition/20260502_prime_value_technical_risk_decomposition_v3/summary.md`

## Current Surface

- Input bundle: `annual-value-composite-selection`
- Output tables:
  - `enriched_event_df`
  - `risk_bucket_summary_df`
  - `risk_spread_df`
  - `portfolio_daily_df`
  - `portfolio_summary_df`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_prime_value_technical_risk_decomposition.py \
  --input-bundle /tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive \
  --output-root /tmp/trading25-research \
  --run-id 20260502_prime_value_technical_risk_decomposition_v3
```
