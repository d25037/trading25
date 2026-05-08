# Standard Value Pump/Fade Portfolio Filter

Standard value+breakout portfolio に、pump/fade hard filter を同じ daily portfolio
metric 定義で重ねる研究。`standard / prime_size_tilt / adv10m /
breakout_additive / 120d / 20s / 3m / top10` を主対象にする。

## Published Readout

### Decision

前回の base は再現できた。`standard / prime_size_tilt / adv10m /
breakout_additive / 120d / 20s / 3m / top10` は CAGR `64.22%`、
Sharpe `2.23`、maxDD `-35.80%`。

月足高値基準と非排他的 flag に直しても、pump/fade hard filter は production
rule として採用しない。`risk_score>=3` を除外して top100 から補充すると
Sharpe は `2.24` とわずかに上がるが、CAGR は `63.38%` へ落ち、
maxDD は `-38.23%` へ悪化する。月足高値 fade や大型月足後の deep drawdown
を除外すると CAGR / Sharpe は明確に落ちる。

したがって、pump/fade は hard exclude ではなく、Ranking diagnostic と
sizing / risk budget の候補として扱う。次にやるなら 0/1 filter ではなく、
`risk_score>=3` を 0.5x、`deep_high_fade_after_large_month` を 0.25x のような
haircut を同じ portfolio lens で検証する。

これは quality filter で portfolio を綺麗にする研究ではない。base が強い理由の一部は、
仕手的に見える right-tail を含む `speculative value sleeve` を持っていることにある。
除外して心理的には綺麗になっても、daily portfolio metric は改善しなかった。

### Main Findings

#### 結論: base は前回の Sharpe 2.2 台を再現した。

| Policy | Refill | Events | Avg pos | CAGR | Sharpe | Sortino | MaxDD | Ann vol |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `base` | `base` | 380 | 9.94 | `64.22%` | `2.23` | `2.86` | `-35.80%` | `24.34%` |

#### 結論: 月足高値基準の hard filter は base を上回らない。

| Policy | Refill | Events | Avg pos | CAGR | Sharpe | Sortino | MaxDD | Ann vol |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `exclude_large_month_high_fade` | `drop_only` | 359 | 9.39 | `61.26%` | `2.15` | `2.75` | `-35.85%` | `24.31%` |
| `exclude_large_month_high_fade` | `refill_to_top_n` | 380 | 9.94 | `61.18%` | `2.17` | `2.76` | `-35.79%` | `24.01%` |
| `exclude_deep_drawdown_after_large_month` | `drop_only` | 341 | 8.92 | `61.50%` | `2.15` | `2.79` | `-35.87%` | `24.37%` |
| `exclude_deep_drawdown_after_large_month` | `refill_to_top_n` | 380 | 9.95 | `59.92%` | `2.14` | `2.69` | `-35.79%` | `23.97%` |

`exclude_large_month_high_fade_or_deep_drawdown` はこの run では
`exclude_deep_drawdown_after_large_month` と同じ選別になった。

#### 結論: `risk_score>=3` の hard filter も採用しにくい。

| Policy | Refill | Events | Avg pos | CAGR | Sharpe | Sortino | MaxDD | Ann vol |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `exclude_risk_ge3` | `drop_only` | 251 | 6.77 | `62.45%` | `2.06` | `2.79` | `-38.97%` | `26.76%` |
| `exclude_risk_ge3` | `refill_to_top_n` | 380 | 9.96 | `63.38%` | `2.24` | `2.82` | `-38.23%` | `23.96%` |

補充ありの Sharpe は `+0.01` 程度改善するが、CAGR は `-0.84pt`、
maxDD は `-2.43pt` 悪化する。これは明確な改善とは言いにくい。

#### 結論: base top10 には pump/fade risk がかなり入っている。

| Diagnostic | Count / 380 |
| --- | ---: |
| `risk_score>=3` | 129 |
| `large_month_high_fade` | 21 |
| `deep_drawdown_after_large_month` | 39 |
| `large_month_close_fade` | 24 |

混ざっていること自体は事実。ただし、それを hard filter で取り除くと portfolio
metric は改善しない。

### Interpretation

Standard value+breakout の強さは、普通の value だけではなく、microcap / high-vol /
過去急騰後 fade の右尾も含んでいる。月足高値基準に直すと、人間が見ている
「吹いた高値からの崩落」はより素直に拾えるが、hard filter の結論は変わらない。

`2673` / `6276` / `7603` のような銘柄は、新定義では
`deep_high_fade_after_large_month` として捕捉できる。一方で、過去 portfolio では
この family を機械的に消すと CAGR / Sharpe が落ちる。
つまり「仕手っぽいから見えていない」のではなく、「仕手っぽいと分かったうえで、
その optionality をどう小さく持つか」が次の論点になる。

### Production Implication

Ranking には pump/fade diagnostic を出す価値がある。ただし score から除外するのではなく、
portfolio construction 側で position size、最大保有数、損切り、再エントリー制御に使う。

次の実験は hard filter ではなく haircut:

- `ordinary_value_score_0_1`: 1.0x
- `watch_score_2`: 0.75x
- `risk_score>=3`: 0.5x
- `deep_high_fade_after_large_month`: 0.25x

同じ daily portfolio path で CAGR / Sharpe / Sortino / maxDD / turnover を比較する。

### Caveats

対象は `standard / prime_size_tilt / adv10m / breakout_additive / 120d / 20s / 3m /
top10` に絞った。別 window、top5、2m rebalance では感度が変わる可能性がある。

`refill_to_top_n` は top100 pool から filter 後に各 rebalance period の top10 を取り直す。
`drop_only` は元 top10 から除外し、残った銘柄へ等ウェイト再配分する。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/standard_value_pump_fade_portfolio_filter.py`
- Runner: `apps/bt/scripts/research/run_standard_value_pump_fade_portfolio_filter.py`
- Bundle: `/tmp/trading25-research/market-behavior/standard-value-pump-fade-portfolio-filter/20260508_standard_value_pump_fade_portfolio_filter_v3_high_flags/`
- Results DB: `/tmp/trading25-research/market-behavior/standard-value-pump-fade-portfolio-filter/20260508_standard_value_pump_fade_portfolio_filter_v3_high_flags/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/standard-value-pump-fade-portfolio-filter/20260508_standard_value_pump_fade_portfolio_filter_v3_high_flags/summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_standard_value_pump_fade_portfolio_filter.py \
  --output-root /tmp/trading25-research
```
