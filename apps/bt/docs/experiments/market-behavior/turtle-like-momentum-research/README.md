# Turtle-like Momentum Research

Donchian channel breakout を使う Turtle-like trend following を、日本株の日足 `market.duckdb` で再現できる範囲に落とした runner-first research。

## Published Readout

### Decision

Turtle-like breakout は、今回の日足 long-only 日本株 portfolio lens では core alpha として弱い。`TOPIX500` / `Prime ex TOPIX500` の best Sharpe は `0.64` 程度で、value periodic の水準には届かない。Standard / Growth は一部 CAGR が出るが、MaxDD が深く、production の主導線にはしない。

ただし trade return の skew は明確に正で、Turtle 的な「低勝率・右尾獲得」の形は出ている。したがって、N日 breakout は value filter ではなく、trend-following / right-tail diagnostic として別枠で扱う。次に進めるなら、銘柄選択 alpha ではなく、risk cap / market regime / sector exposure / crisis behavior の研究にする。

### Data Scope / PIT Assumptions

入力は active `market.duckdb`。universe は `stock_master_daily` の同日 membership で `TOPIX500` / `Prime ex TOPIX500` / `Standard` / `Growth` に分ける。`ADV60 >= 10mn JPY` を entry signal 日の trailing average trading value で判定する。

日足だけで Turtle を完全再現することはできないため、execution は以下の近似にした。

- `close_confirmed`: signal 日の `close > prior N-day high` で翌営業日 `open` entry。
- `high_touch_next_open`: signal 日の `high > prior N-day high` で翌営業日 `open` entry。
- exit は `close_confirmed` では `close < prior M-day low`、`high_touch_next_open` では `low < prior M-day low` を signal とし、翌営業日 `open` で exit。
- 期末まで exit signal が無い trade は `end_of_sample_close` で閉じる。
- pyramiding、short side、intraday stop order 約定、gap slippage は未実装。
- sizing は `equal_weight` と `inverse_atr`。`inverse_atr` は entry signal 日の `ATR20 / close` の逆数を active portfolio 内で相対 weight として使う。

### Main Findings

#### 結論

| Universe | Best spec | Entry | Sizing | Trades | CAGR | Sharpe | MaxDD | Win | P90 | Skew | Read |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `topix500` | `20d_entry_10d_exit` | `high_touch_next_open` | `equal_weight` | `28,684` | `8.81%` | `0.64` | `-36.04%` | `38.8%` | `9.48%` | `5.10` | positive だが core には弱い |
| `prime_ex_topix500` | `20d_entry_10d_exit` | `high_touch_next_open` | `equal_weight` | `74,882` | `9.01%` | `0.64` | `-46.77%` | `37.0%` | `10.48%` | `6.57` | TOPIX500 と同程度 |
| `standard` | `55d_entry_20d_exit` | `high_touch_next_open` | `inverse_atr` | `14,775` | `8.06%` | `0.61` | `-47.83%` | `36.6%` | `21.00%` | `11.39` | 右尾は大きいが drawdown も深い |
| `growth` | `55d_entry_20d_exit` | `high_touch_next_open` | `inverse_atr` | `7,452` | `9.25%` | `0.48` | `-58.52%` | `30.8%` | `24.30%` | `12.82` | lottery-like |

#### TOPIX500 / Prime ex TOPIX500

| Universe | Spec | Entry | Sizing | CAGR | Sharpe | MaxDD | Win | Skew |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `topix500` | `20d_entry_10d_exit` | `high_touch_next_open` | `equal_weight` | `8.81%` | `0.64` | `-36.04%` | `38.8%` | `5.10` |
| `topix500` | `20d_entry_10d_exit` | `high_touch_next_open` | `inverse_atr` | `8.25%` | `0.63` | `-34.64%` | `38.8%` | `5.10` |
| `prime_ex_topix500` | `20d_entry_10d_exit` | `high_touch_next_open` | `equal_weight` | `9.01%` | `0.64` | `-46.77%` | `37.0%` | `6.57` |
| `prime_ex_topix500` | `20d_entry_10d_exit` | `high_touch_next_open` | `inverse_atr` | `7.64%` | `0.63` | `-42.63%` | `37.0%` | `6.57` |

#### Standard / Growth

| Universe | Spec | Entry | Sizing | CAGR | Sharpe | MaxDD | Win | P90 | Skew |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `standard` | `55d_entry_20d_exit` | `high_touch_next_open` | `inverse_atr` | `8.06%` | `0.61` | `-47.83%` | `36.6%` | `21.00%` | `11.39` |
| `standard` | `20d_entry_10d_exit` | `close_confirmed` | `inverse_atr` | `6.53%` | `0.56` | `-48.08%` | `35.6%` | `17.46%` | `7.64` |
| `growth` | `55d_entry_20d_exit` | `high_touch_next_open` | `inverse_atr` | `9.25%` | `0.48` | `-58.52%` | `30.8%` | `24.30%` | `12.82` |
| `growth` | `20d_entry_10d_exit` | `high_touch_next_open` | `inverse_atr` | `5.18%` | `0.35` | `-54.05%` | `32.1%` | `17.19%` | `17.12` |

### Interpretation

Turtle-like breakout は、単純な cross-sectional momentum より「右尾を取る」性質がはっきり出る。win rate は `30-39%` 程度と低く、P90 と skew は高い。これは Turtle の思想と整合的。

一方、long-only 日本株で全 breakout trade を持つだけでは、risk-adjusted return は弱い。active positions は多く、best row でも平均 200-550 銘柄程度が同時保有されるため、実質的には広く薄い trend exposure になっている。right-tail はあるが、drawdown と停滞を十分に抑えられていない。

`inverse_atr` は Standard / Growth では Sharpe と MaxDD を改善しやすいが、TOPIX500 / Prime ex TOPIX500 では equal weight が CAGR/Sharpe で上に来ることもある。大型 universe は低ボラへ寄せるより、素直に breakout basket を持つほうが右尾を残しやすい可能性がある。

### Production Implication

production ranking の主導線は引き続き value。Turtle-like breakout は以下の用途に限定する。

- `TOPIX500` / `Prime ex TOPIX500` の trend-following diagnostic
- Standard / Growth の lottery-like right-tail bucket の観察
- value portfolio の hard filter ではなく、別 sleeve の risk budget candidate

次にやるなら、pyramiding より先に `market regime` と `risk cap` を確認する。今回の best Sharpe が `0.64` 程度なので、pyramiding で劇的に改善する前提は置かない。改善余地は、market filter、sector-neutral exposure、TOPIX trend filter、position count cap、ATR risk target の順に確認する。

### Caveats

日足近似なので、本来の stop order 約定、intraday breakout price、gap slippage、同日 entry/exit、pyramiding は再現していない。short side も未実装。cost、slippage、税コスト、borrow は未控除。全 breakout trade を同時保有するため、資金制約や最大保有銘柄数を考慮した Turtle portfolio とは異なる。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/turtle_like_momentum_research.py`
- Runner: `apps/bt/scripts/research/run_turtle_like_momentum_research.py`
- Bundle: `/tmp/trading25-research/market-behavior/turtle-like-momentum-research/20260509_turtle_like_momentum_v1/`
- Results DB: `/tmp/trading25-research/market-behavior/turtle-like-momentum-research/20260509_turtle_like_momentum_v1/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/turtle-like-momentum-research/20260509_turtle_like_momentum_v1/summary.md`

## Current Surface

- `universe_summary_df`: market-split universe coverage.
- `trade_ledger_df`: non-overlapping trade ledger by code x channel spec x entry mode.
- `portfolio_daily_df`: dense daily portfolio curve with cash return as zero on inactive days.
- `portfolio_summary_df`: CAGR / Sharpe / Sortino / Calmar / MaxDD plus trade return skew and P90.

## Performance Note

Initial implementation scanned every code-day in Python and was too slow for the full 10-year run. The current runner vectorizes entry/exit masks per code and uses `searchsorted` to connect entry candidates to the next exit candidate. Portfolio daily aggregation is a DuckDB range join over `trade_ledger_df` and the price panel.

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_turtle_like_momentum_research.py \
  --output-root /tmp/trading25-research \
  --run-id 20260509_turtle_like_momentum_v1 \
  --channel-specs 20:10,55:20 \
  --entry-modes close_confirmed,high_touch_next_open \
  --sizing-methods equal_weight,inverse_atr \
  --min-avg-trading-value-mil-jpy 10
```
