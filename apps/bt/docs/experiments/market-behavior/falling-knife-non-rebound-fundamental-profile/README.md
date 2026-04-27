# Falling Knife Non-Rebound Fundamental Profile

## Purpose

`falling-knife-reversal-study` の次の問いを、除外ルール最適化ではなく特徴分析として扱う。

> 落ちるナイフの中で、リバウンドしない銘柄にはどんなファンダ的特徴があるか？

Primary label は `catch_return_{horizon}d <= rebound_threshold`。Default では 20営業日後 return が 0% 以下なら `non_rebound` とする。`severe_loss` は副次的な tail diagnostic として残す。

## Feature Construction

入力は `falling-knife-reversal-study` の `event_df`。各 `signal_date` 時点で開示済みの最新 `statements` 行だけを join する。

- `disclosed_date <= signal_date` で切ってから latest row を選ぶ
- forecast EPS / Profit / OperatingCashFlow / simple FCF margin / equity ratio から `quality_score` を作る
- signal date の `close` と PIT-safe valuation inputs から PER / forward PER / PBR を作る
  - PBR: latest FY BPS disclosed on or before `signal_date`
  - PER: latest FY actual EPS disclosed on or before `signal_date`
  - forward PER: latest non-null forecast EPS disclosed on or before `signal_date`
- `quality_score >= 3` を default の `high_quality` とする

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_non_rebound_fundamental_profile.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle
```

Useful options:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_non_rebound_fundamental_profile.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle \
  --horizon-days 20 \
  --rebound-threshold 0.0 \
  --severe-loss-threshold -0.10 \
  --min-quality-score 3
```

## Tables

- `enriched_event_df`: falling-knife event plus PIT-safe statement features and `non_rebound` / `rebound` labels.
- `fundamental_profile_summary_df`: segment-level non-rebound rate by market, quality, forecast/profit/CFO/FCF/equity/valuation buckets.
- `feature_lift_summary_df`: binary feature prevalence in non-rebound vs rebound groups, relative risk, odds ratio, and return diagnostics.

## Baselines

- [baseline-2026-04-27.md](./baseline-2026-04-27.md)

## Interpretation

This experiment is designed to answer the descriptive question first. A good next strategy rule should come after identifying which fundamental features are over-represented among non-rebound events, not before.
