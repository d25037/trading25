# Falling Knife Reversal Study

## Published Readout

### Decision

この研究結果だけでは、落ちるナイフを機械的に翌営業日寄りで拾う production rule は採用しない。`wait_for_stabilization` も全体で安定して優位ではなく、tested rule では「待てば改善する」とは言えない。production では、急落イベントを単純な買いシグナルではなく、左尾リスクを診断して除外・縮小するための stress regime signal として扱う。

### Why This Research Was Run

急落後に即時反発を狙うべきか、安定化確認まで待つべきかを、日足 OHLC だけで PIT-safe に検証するために実行した。特に、平均リターンではなく、10%超の severe loss、p10、market bucket、Daily Risk Adjusted Return bucket の差を見て、production strategy の悪いサブセットを見つける材料にすることを目的にした。

### Data Scope / PIT Assumptions

入力は market.duckdb v3 の `stock_data` と PIT-safe stock master を使った live DuckDB read で、利用可能範囲は `2016-05-02 -> 2026-04-28`、分析範囲は `2016-06-01 -> 2026-04-27`。対象市場は `0111` プライム、`0112` スタンダード、`0113` グロースで、source rows は `7,954,155`、falling-knife events は `154,812`、対象 symbol は `3,734`、stabilization entry を持つ event は `147,699`。signal 条件は signal date close までに観測できる `5d <= -10%`、`20d <= -20%`、`60d high から -25%以下`、SMA downtrend、`60d sortino <= 0.00` の overlap で、最小 overlap は `2`、同一銘柄 signal cooldown は `20` sessions。entry は常に翌営業日 open で、wait rule は最大 `10` sessions 内の安定化確認後の翌営業日 open。

### Main Findings

#### `catch_next_open` は平均ではプラスだが、horizon が長いほど左尾が重くなる。

| Horizon | Mean | Median | P10 | Severe loss |
| --- | ---: | ---: | ---: | ---: |
| 5d | `0.35%` | `0.07%` | `-5.42%` | `3.23%` |
| 20d | `1.29%` | `0.49%` | `-10.45%` | `10.66%` |
| 60d | `3.33%` | `0.86%` | `-16.85%` | `19.44%` |

#### グロースは反発の右尾が残る一方で、20d の左尾が明確に悪い。

| Market | Horizon | Mean | Median | Severe loss |
| --- | --- | ---: | ---: | ---: |
| Prime | 20d | `1.62%` | `1.15%` | `8.53%` |
| Standard | 20d | `1.12%` | `0.22%` | `9.71%` |
| Growth | 20d | `0.88%` | `-0.77%` | `20.29%` |

#### tested rule では `wait_for_stabilization` は `catch_next_open` を上回らない。

| Horizon | `wait - catch` mean | `wait - catch` median | Wait better |
| --- | ---: | ---: | ---: |
| 5d | `-0.30%` | `-0.49%` | `42.80%` |
| 20d | `-0.39%` | `-0.69%` | `40.39%` |
| 60d | `-0.57%` | `-0.66%` | `41.11%` |

#### event の多くは risk-adjusted return 悪化と downtrend で拾われるが、深い drawdown 条件は頻度が低くても左尾が重い。

| Condition | Event rate | Severe loss |
| --- | ---: | ---: |
| `poor_risk_adjusted_return` | `92.65%` | `2.80%` |
| `downtrend_sma` | `81.61%` | `2.57%` |
| `deep_60d_drawdown` | `22.80%` | `7.20%` |
| `deep_20d_drop` | `5.91%` | `8.62%` |

### Interpretation

急落イベントは平均ではプラスを残すが、production 上の問題は期待値そのものより左尾の集中にある。特にグロースと深い drawdown 条件では、20d/60d horizon の p10 と severe loss が大きく悪化する。安定化待ちは entry を遅らせることで一部の短期 left-tail を抑える局面はあるが、tested rule では機会損失が大きく、event-matched で wait better rate が 40%台前半に留まった。

### Production Implication

この結果は「急落を買う」rule の採用根拠ではなく、急落局面で size を落とす、対象市場を分ける、または bad-tail pruning を追加する根拠として使う。特に 20d severe loss がプライム `8.53%` に対してグロース `20.29%` まで上がるため、市場をまとめた single threshold は避ける。次段では `catch_next_open` の rebound exposure を固定したまま、グロース、Daily Risk Adjusted Return bucket、deep drawdown 条件による除外 rule を検証する。

### Caveats

この readout は `20260429_204107_e60eacef` bundle の単一 run に基づく。source は local market.duckdb v3 の live DuckDB read で、manifest は `git_dirty: true` を示している。約定は翌営業日 open / horizon close の research approximation で、手数料、スリッページ、板流動性、実運用の同時保有制約は評価していない。Daily Risk Adjusted Return は `60d sortino` の bucket と閾値 `0.00` に依存するため、別 lookback や market-specific threshold では結果が変わりうる。

### Source Artifacts

- Bundle: `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260429_204107_e60eacef`
- Summary: `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260429_204107_e60eacef/summary.md`
- Published numbers: `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260429_204107_e60eacef/summary.json`
- Tables: `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260429_204107_e60eacef/results.duckdb` (`event_df`, `trade_summary_df`, `paired_delta_df`, `condition_profile_df`)
- Manifest: `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260429_204107_e60eacef/manifest.json`

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
