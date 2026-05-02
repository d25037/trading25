# Annual Prime Value Volume Volatility Participation

[`annual-prime-value-technical-risk-decomposition`](../annual-prime-value-technical-risk-decomposition/README.md)
で Prime value の high-volatility bucket が CAGR / Sharpe を押し上げる一方で MaxDD を悪化させることが分かったため、その high-vol effect が「参加者増加・出来高増加」の proxy なのかを切り分ける研究。

## Published Readout

### Decision

Prime value の `volatility_20d_pct` high bucket は `volume_ratio_20_60` / `trading_value_ratio_20_60` と中程度に連動するが、`volatility_60d_pct` high bucket は participation ratio とほぼ連動しない。さらに high-vol bucket 内で participation ratio が高い銘柄は、低い銘柄より平均 return がむしろ弱い。したがって、Prime high-vol effect を単純に「参加者増加 proxy」と解釈するのは弱い。

実運用上は、Prime の raw `Vol 20d` / `Vol 60d` は引き続き Ranking の technical metrics として表示するが、`volume_ratio_20_60` / `trading_value_ratio_20_60` は加点条件ではなく、過熱・crowding diagnostic として扱う。high participation をさらに買うより、high vol の中で参加急増が過剰でない subset を次に見る。

### Why This Research Was Run

Standard value では `rebound_from_252d_low_pct` / `return_252d_pct` が効き、Prime value では `volatility_20d_pct` / `volatility_60d_pct` が効いた。仮説として、Standard は低流動性で市場折り込みが遅く、1年スケールの見直しを拾う。一方 Prime は流動性が高く、短期 volatility が参加者増加のサインではないか、と考えた。この研究はそのうち Prime 側の仮説を検証する。

### Data Scope / PIT Assumptions

入力は positive-ratio value bundle `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive/`。分析対象は `prime` / `none` / top `5%` and top `10%` / `equal_weight`, `walkforward_regression_weight` の `4,316` selected events。`fixed_55_25_20` は除外した。

technical と participation feature は `entry_date` より前の日次 session だけで計算する。`volume_ratio_20_60` は直近20日出来高平均 / 直近60日出来高平均、`trading_value_ratio_20_60` は同じく売買代金平均の比率。日次 portfolio lens は `entry_open` から日次 `close` への equal-weight path。

### Main Findings

#### 結論

| Vol feature | Corr with volume ratio | Corr with trading value ratio | High-vol volume ratio delta | High-vol TV ratio delta | High-vol mean spread | High-vol p10 spread | 読み |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `volatility_20d_pct` | `+0.35` | `+0.31` | `+0.18` | `+0.18` | `+15.47pp` | `-5.36pp` | 20d high vol は参加増加を多少含む |
| `volatility_60d_pct` | `+0.03` | `+0.00` | `-0.08` | `-0.07` | `+15.92pp` | `-7.62pp` | 60d high vol は参加増加 proxy ではない |

`volatility_20d_pct` は volume / trading value ratio と中程度に連動する。一方で、`volatility_60d_pct` は participation ratio とほぼ無相関。前回研究で強かった 60d volatility は、短期の出来高急増というより、過去60日の価格変動状態そのものを見ている可能性が高い。

#### High Vol 内の Participation Split

| Vol feature | Participation feature | Avg delta mean high-low participation | Avg delta p10 high-low participation | Avg delta downside vol high-low participation | 読み |
| --- | --- | ---: | ---: | ---: | --- |
| `volatility_20d_pct` | `trading_value_ratio_20_60` | `-9.69pp` | `+0.93pp` | `-3.29pp` | high participation は平均 return を押し下げる |
| `volatility_20d_pct` | `volume_ratio_20_60` | `-5.81pp` | `+1.51pp` | `-2.42pp` | p10 は少し改善するが右尾を削る |
| `volatility_60d_pct` | `trading_value_ratio_20_60` | `-5.13pp` | `-0.75pp` | `-2.33pp` | high participation 加点は弱い |
| `volatility_60d_pct` | `volume_ratio_20_60` | `-4.70pp` | `+0.32pp` | `-1.90pp` | 参加増加は return engine ではない |

high-vol bucket の中で participation ratio が高い subset は、低い subset より平均 return が下がった。downside vol はむしろ下がる傾向があるため、防御的には少し意味があるが、Prime high-vol の right-tail capture を強める条件ではない。

#### Daily Portfolio Lens

代表例では、top `5%` / `walkforward_regression_weight` / `volatility_60d_pct x volume_ratio_20_60` の high participation は low participation に対して CAGR `-7.82pp`、Sharpe `-0.26`、MaxDD `-2.16pp`。top `5%` / `equal_weight` / `volatility_60d_pct x volume_ratio_20_60` だけは CAGR `+6.33pp` / Sharpe `+0.11` だったが、全体としては high participation の優位性は安定しない。

### Interpretation

Prime high-vol effect は「参加者が増えたから上がる」という単純な話ではない。20d volatility には出来高・売買代金の短期増加が混ざっているが、60d volatility ではそれが消える。むしろ high participation を追加条件にすると平均 return が落ちるため、急な参加増加はすでに価格に織り込まれた後、または短期過熱・crowding を示している可能性がある。

Prime value で見るべきなのは、出来高急増そのものではなく、価格変動が出始めているが参加急増で過熱しきっていない状態かもしれない。

### Production Implication

Ranking page の raw technical metrics では、Prime に `Vol 20d` / `Vol 60d` / `Down Vol 60d` を表示する方針を維持する。`volume_ratio_20_60` / `trading_value_ratio_20_60` は現時点では score column に昇格しない。追加するなら、加点ではなく `Participation Spike` / `Crowding` diagnostic とする。

次の Prime 実運用向け検証は以下。

| Candidate split | 目的 |
| --- | --- |
| high vol + not-high participation | 過熱前の volatility state を拾えるか |
| high vol + low downside vol | drawdown cost を下げられるか |
| high vol + participation spike exclusion | 急騰後追い・crowding を避けられるか |

### Caveats

`volume_ratio_20_60` / `trading_value_ratio_20_60` は20日平均と60日平均の単純比率で、板厚、約定分布、ニュース起点の出来高、指数イベントは見ていない。日次 portfolio lens はコスト、capacity、position sizing を未反映。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_prime_value_volume_volatility_participation.py`
- Runner: `apps/bt/scripts/research/run_annual_prime_value_volume_volatility_participation.py`
- Bundle: `/tmp/trading25-research/market-behavior/annual-prime-value-volume-volatility-participation/20260502_prime_value_volume_volatility_participation/`
- Results DB: `/tmp/trading25-research/market-behavior/annual-prime-value-volume-volatility-participation/20260502_prime_value_volume_volatility_participation/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/annual-prime-value-volume-volatility-participation/20260502_prime_value_volume_volatility_participation/summary.md`

## Current Surface

- Input bundle: `annual-value-composite-selection`
- Output tables:
  - `enriched_event_df`
  - `volatility_participation_summary_df`
  - `participation_split_df`
  - `portfolio_daily_df`
  - `portfolio_summary_df`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_prime_value_volume_volatility_participation.py \
  --input-bundle /tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive \
  --output-root /tmp/trading25-research \
  --run-id 20260502_prime_value_volume_volatility_participation
```
