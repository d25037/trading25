# Falling Knife Bad-Tail Pruning

## Purpose

`falling-knife-reversal-study` の event bundle を入力に、リバウンドの平均・右尾を残しながら severe loss を減らせる除外ルールを比較する。

この研究は OHLC を再計算しない。前段 research の `event_df` を固定し、`catch_next_open` の trade-level return 分布に対して rule ごとの kept / removed を集計する。

## Baseline

Default は `catch_return_20d` を使い、`-10%` 以下を severe loss とする。

比較する指標:

- kept fraction / removed fraction
- mean / median
- p10 / p90
- severe loss rate
- severe loss rate reduction
- mean return cost
- removed severe loss share

## Rule Candidates

初期候補:

- Growth market exclusion
- Daily Risk Adjusted Return `Q5_highest` exclusion
- `Q5_highest or unbucketed`
- `deep_60d_drawdown`
- `deep_20d_drop`
- `sharp_5d_drop`
- `condition_count >= 3`
- `condition_count >= 4`
- Growth x Daily Risk Adjusted Return `Q5_highest`
- Growth x `deep_60d_drawdown`
- Daily Risk Adjusted Return `Q5_highest` x `deep_60d_drawdown`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_bad_tail_pruning.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle
```

If `--input-bundle` is omitted, the latest `falling-knife-reversal-study` bundle under the same output root is used.

## Tables

- `rule_summary_df`: rule-level kept / removed comparison.
- `segment_summary_df`: market, Daily Risk Adjusted Return bucket, and condition-count segment distributions.

## Interpretation

The preferred rule is not necessarily the one with the largest severe-loss reduction. It should reduce severe-loss rate without destroying the median return or removing too much of the right-tail rebound.
