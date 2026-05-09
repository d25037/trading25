# Value Breakout Overheat Filter

`forward-eps-technical-horizon-decomposition` で見えた短期過熱 filter / sizing haircut を、`annual-value-breakout-periodic-rebalance` の selected portfolio に重ねた portfolio-lens research。

## Published Readout

### Decision

`value + breakout` に過熱銘柄の hard exclude を足すのは、現時点では採用しない。Standard は平均的に右尾も削りやすく、Prime でも rule によっては改善するが value-only best を大きく超えるほどではない。

一方で、`short_climax_10d_q80_overlap_ge2` の `0.5x haircut` は Standard / Prime の value-only と Standard breakout_additive で Sharpe を小幅に押し上げた。実装に進めるなら hard filter ではなく、Ranking diagnostic または position sizing haircut 候補として扱う。

### Data Scope / PIT Assumptions

入力は既存の `annual-value-breakout-periodic-rebalance` bundle の `selected_event_df`。technical features は `signal_date` 時点の price history で算出し、`signal_date` が無い場合だけ `entry_date` の直前営業日にフォールバックする。閾値は `market_scope` ごとに pre-holdout sample の Q80 で calibration し、holdout が空になる場合は全サンプルにフォールバックする。

使った過熱 rule は forward EPS 側と同じ family:

- `short_climax_10d_q80_overlap_ge2`: `RSI10` / `runup_10d_pct` / `risk_adjusted_return_10d` のうち2つ以上が Q80 超え。
- `trend_maturity_60d_q80_overlap_ge2`: `RSI60` / `runup_60d_pct` / `risk_adjusted_return_60d` のうち2つ以上が Q80 超え。
- `legacy_20_60_runup_rar60_q80_overlap_ge2`: `runup_20d_pct` / `runup_60d_pct` / `risk_adjusted_return_60d` のうち2つ以上が Q80 超え。
- `overheat_runup_rar_cross_horizon_q80_overlap_ge3`: 10/20/60d の run-up と RAR のうち3つ以上が Q80 超え。

### Main Findings

#### 結論

#### Best Overlay By Lens

| Market | Base policy | Rebalance | Top | Base CAGR | Base Sharpe | Best overlay | Variant | CAGR | Sharpe | MaxDD |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: |
| `standard` | `value_only` | `3m` | `10` | `59.16%` | `2.16` | `short_climax_10d_q80_overlap_ge2` | `0.5x haircut` | `61.06%` | `2.21` | `-35.67%` |
| `standard` | `breakout_additive 20d/5s` | `3m` | `10` | `63.77%` | `2.23` | `short_climax_10d_q80_overlap_ge2` | `0.5x haircut` | `64.12%` | `2.27` | `-37.15%` |
| `standard` | `breakout_additive 120d/20s` | `3m` | `10` | `64.22%` | `2.23` | `legacy_20_60_runup_rar60_q80_overlap_ge2` | `0.5x haircut` | `63.75%` | `2.26` | `-36.27%` |
| `prime` | `value_only` | `2m` | `5` | `50.33%` | `1.85` | `short_climax_10d_q80_overlap_ge2` | `0.5x haircut` | `53.04%` | `1.90` | `-32.56%` |
| `prime` | `breakout_additive 120d/20s` | `2m` | `10` | `38.96%` | `1.67` | `overheat_runup_rar_cross_horizon_q80_overlap_ge3` | `exclude_no_refill` | `39.93%` | `1.74` | `-30.15%` |

#### Hard Exclude Check

Standard の hard exclude は安定しない。`short_climax_10d` は value-only で CAGR `63.77%` まで上がるが、breakout_additive では Sharpe が下がり、他の rule は大きく劣化しやすい。特に 60d maturity 系は Standard の winner も多く削る。

Prime の breakout_additive では cross-horizon hard exclude が CAGR `38.96%` / Sharpe `1.67` から `39.93%` / `1.74` に改善した。ただし Prime value-only best は `short_climax_10d` haircut で `53.04%` / `1.90` まで上がるため、Prime で breakout_additive を主軸に戻すほどの evidence ではない。

### Interpretation

短期過熱は「value 候補のうち近すぎる climax を少し軽くする」には使えるが、「買ってよい/買ってはいけない」を切る hard gate としては雑すぎる。これは前回の breakout hard filter と同じ構図で、technical condition は alpha の有無よりも position sizing / risk budget の問題として扱うほうが自然。

Standard の `short_climax_10d` は、value-only でも breakout_additive でも一貫して Sharpe を押し上げる。一方、MaxDD は改善せず、むしろ悪化するケースがある。したがって production 候補にするなら、まず `0.5x haircut` を tail-budget overlay として検討し、hard exclude にはしない。

Prime は `short_climax_10d` haircut が value-only best を少し改善したが、breakout_additive では cross-horizon exclude が最良だった。Prime は rule dependency が強いので、Standard の rule をそのまま移植せず、Prime 専用の sizing / risk cap として別途確認する。

### Production Implication

Ranking / strategy surface へ進めるなら、まず `short_climax_10d_q80_overlap_ge2` を diagnostic として表示し、portfolio sizing では `0.5x haircut` の candidate として扱う。entry eligibility を切る hard filter にはしない。

Standard の production 候補は `breakout_additive 20d/5s` または `120d/20s` に `short_climax_10d` haircut を重ねる方向。ただし MaxDD 改善ではなく Sharpe 改善の overlay なので、cost / turnover / drawdown attribution を確認してからにする。

Prime は value-only best の `2m / Top5 / ADV60 >= 10mn` に短期climax haircut を重ねる候補が先で、breakout_additive を主導線に戻す evidence はない。

### Caveats

cost、slippage、税コスト、turnover は未控除。`exclude_no_refill` は補充なしのため、実運用で full invested を維持する with-refill policy とは資金配分が異なる。Q80 threshold は今回の selected-event universe 内で calibration しており、全候補 universe に広げた場合の閾値とは異なり得る。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/value_breakout_overheat_filter.py`
- Runner: `apps/bt/scripts/research/run_value_breakout_overheat_filter.py`
- Standard value-only bundle: `/tmp/trading25-research/market-behavior/value-breakout-overheat-filter/20260509_standard_value_only_overheat/`
- Standard breakout 20d/5s bundle: `/tmp/trading25-research/market-behavior/value-breakout-overheat-filter/20260509_standard_add20_5_overheat/`
- Standard breakout 120d/20s bundle: `/tmp/trading25-research/market-behavior/value-breakout-overheat-filter/20260509_standard_add120_20_overheat/`
- Prime value-only bundle: `/tmp/trading25-research/market-behavior/value-breakout-overheat-filter/20260509_prime_value_only_overheat/`
- Prime breakout 120d/20s bundle: `/tmp/trading25-research/market-behavior/value-breakout-overheat-filter/20260509_prime_add120_20_overheat/`

## Current Surface

- `enriched_selected_event_df`: selected events plus RSI / run-up / risk-adjusted-return features.
- `threshold_summary_df`: market-scope Q80 calibration thresholds.
- `overheat_rule_event_df`: per event x rule overheat flag and overlap count.
- `overheat_rule_summary_df`: event-level kept vs overheat return profile.
- `portfolio_event_df`: baseline / exclude_no_refill / haircut_0_5 event ledger.
- `portfolio_daily_df`: weighted daily portfolio curve.
- `portfolio_summary_df`: CAGR / Sharpe / Sortino / Calmar / MaxDD.

## Run

Standard 20d/5s breakout_additive:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_value_breakout_overheat_filter.py \
  --input-bundle /tmp/trading25-research/market-behavior/annual-value-breakout-periodic-rebalance/20260508_standard_value_breakout_2m3m_focused_v2 \
  --output-root /tmp/trading25-research \
  --run-id 20260509_standard_add20_5_overheat \
  --market-scope standard \
  --score-method prime_size_tilt \
  --liquidity-scenario adv10m \
  --breakout-policy breakout_additive \
  --breakout-window 20 \
  --breakout-lookback-sessions 5 \
  --rebalance-months 3 \
  --selection-count 10
```

Prime value-only:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_value_breakout_overheat_filter.py \
  --input-bundle /tmp/trading25-research/market-behavior/annual-value-breakout-periodic-rebalance/20260508_prime_value_breakout_2m3m_focused_v2 \
  --output-root /tmp/trading25-research \
  --run-id 20260509_prime_value_only_overheat \
  --market-scope prime \
  --score-method prime_size_tilt \
  --liquidity-scenario adv10m \
  --breakout-policy value_only \
  --breakout-window 0 \
  --breakout-lookback-sessions -1 \
  --rebalance-months 2 \
  --selection-count 5
```
