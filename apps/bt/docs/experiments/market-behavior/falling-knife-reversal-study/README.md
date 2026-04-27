# Falling Knife Reversal Study

## Purpose

投資格言「落ちるナイフを掴むな」を、急落中の即時買いと安定化確認後の買いの比較として検証する。

この研究では、`stock_data` の日足 OHLC だけを使い、signal date close までに観測できる条件で falling knife event を定義する。entry は常に翌営業日 open とし、future leak を避ける。

## Definition

Falling knife は以下の条件の overlap 数で判定する。

- `5d return <= -10%`
- `20d return <= -20%`
- `close / rolling_60d_high - 1 <= -25%`
- `close < SMA20 < SMA60` かつ `SMA20 slope 5d < 0`
- Daily Risk Adjusted Return が閾値以下

Daily Risk Adjusted Return は既存の `compute_risk_adjusted_return()` を使い、Sharpe / Sortino の両方を出力する。event 判定に使う ratio は runner option `--condition-ratio-type` で選ぶ。default は `sortino`。

同じ下落 episode を毎日重複して数えすぎないよう、default では同一銘柄に `20` sessions の signal cooldown を置く。必要に応じて `--signal-cooldown-days` で調整する。

## Strategy Comparison

- `catch_next_open`: falling knife signal の翌営業日 open で買う。
- `wait_for_stabilization`: signal 後、`close > SMA5` かつ `low >= prior 3-session low` を初めて満たした日の翌営業日 open で買う。

Forward horizon は default で `5 / 20 / 60` sessions。trade-level summary は mean / median / hit rate / p10 / severe loss rate を出す。`paired_delta_df` は同じ event で `wait - catch` を比較する。

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_reversal_study.py
```

Useful options:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_reversal_study.py \
  --condition-ratio-type sortino \
  --risk-adjusted-lookback 60 \
  --min-condition-count 2 \
  --max-wait-days 10 \
  --signal-cooldown-days 20
```

The runner writes `manifest.json`, `results.duckdb`, `summary.md`, and `summary.json` under the research bundle root.

## Tables

- `event_df`: signal-level features, Daily Risk Adjusted Return values, condition flags, catch returns, and wait returns.
- `trade_summary_df`: grouped trade-level return distribution for catch and wait entries.
- `paired_delta_df`: event-matched wait-minus-catch comparison.
- `condition_profile_df`: which falling-knife conditions dominate the event set.

## Interpretation

The target conclusion is not simply "all sharp selloffs are bad." The practical question is whether immediate entry has a worse left tail than waiting for stabilization, and whether Daily Risk Adjusted Return helps identify the subset where catching the knife is especially costly.
