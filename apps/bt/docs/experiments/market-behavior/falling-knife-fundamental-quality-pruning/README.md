# Falling Knife Fundamental Quality Pruning

## Purpose

`falling-knife-bad-tail-pruning` で見えた Growth market の悪い左尾が、市場区分そのものなのか、Growth に多い fundamental quality の低さなのかを分解する。

この研究は `falling-knife-reversal-study` の `event_df` を入力にし、各 `signal_date` 時点で開示済みの最新 `statements` 行だけを join する。`latest per event` は `disclosed_date <= signal_date` の後に選ぶ。

## Quality Definition

初期の `quality_score` は以下5条件の合計。

- forecast EPS > 0
- Profit > 0
- OperatingCashFlow > 0
- simple FCF margin > 0
- equity ratio >= 30%

Default では `quality_score >= 3` を `high_quality`、それ未満を `low_quality` とする。開示済み statement が無い event は `missing_statement`。

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_fundamental_quality_pruning.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle
```

Useful options:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_fundamental_quality_pruning.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle \
  --horizon-days 20 \
  --severe-loss-threshold -0.10 \
  --min-quality-score 3
```

## Tables

- `enriched_event_df`: falling-knife event plus PIT-safe statement features and quality labels.
- `quality_segment_summary_df`: market / quality / forecast / profit / CFO / FCF / equity bucket summaries.
- `quality_rule_summary_df`: exclusion-rule kept / removed comparison.

## Baselines

- [baseline-2026-04-27.md](./baseline-2026-04-27.md)

## Interpretation

The key question is whether Growth can be kept when quality is high, and whether non-Growth low-quality names still carry bad-tail risk. A useful rule should reduce severe-loss rate without removing the rebound right tail mechanically.
