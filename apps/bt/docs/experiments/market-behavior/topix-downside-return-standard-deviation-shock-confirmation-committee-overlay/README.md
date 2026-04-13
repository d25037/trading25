# TOPIX Downside Standard Deviation Shock Confirmation Committee Overlay

TOPIX を long-only で持つ前提で、`downside return standard deviation` を shock 検知に使い、TOPIX trend と TOPIX100 breadth で confirmation をかける overlay 実験です。

## Purpose

- `return standard deviation` 単独では parameter transfer が弱かったため、`shock / trend / breadth` の 3 family を組み合わせて「落とすべき下落ストレス」と「ただのノイズ」を分ける。
- single-point parameter の overfit を避けるため、固定 family 上で `mean x high` を committee 化し、walk-forward で単点より安定するかを確認する。
- 実務上の使い方として、TOPIX を握りつつ shock 時だけ機械的に beta を落とす overlay に落とし込めるかを判定する。

## Scope

- Asset:
  - `TOPIX`
- Stress family:
  - downside return standard deviation window: `5`
  - mean windows: `1, 2`
  - high thresholds: `0.24, 0.25`
  - low threshold: `0.22`
- Confirmation family:
  - mode: `stress_and_trend_and_breadth`
  - trend family: `close_below_sma20`, `sma20_below_sma60`, `drawdown_63d_le_neg0p05`, `return_10d_le_neg0p03`
  - trend votes: `>= 1`
  - breadth family: `topix100_above_sma20_le_0p40`, `topix100_positive_5d_le_0p40`, `topix100_at_20d_low_ge_0p20`
  - breadth votes: `3/3`
- Exposure rule:
  - reduced exposure ratio: `0.00`
  - committee members:
    - `(mean=1, high=0.24)`
    - `(mean=1, high=0.25)`
    - `(mean=2, high=0.24)`
    - `(mean=2, high=0.25)`
  - final exposure is the equal-weight average of member exposures

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_trend_breadth_overlay.py`
  - `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py`
  - `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_trend_breadth_overlay.py`
  - `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py`
  - `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_trend_breadth_overlay.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`
  - `apps/bt/tests/unit/scripts/test_run_topix_downside_return_standard_deviation_trend_breadth_overlay.py`
  - `apps/bt/tests/unit/scripts/test_run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py`
  - `apps/bt/tests/unit/scripts/test_run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`

## Latest Baseline

- [baseline-2026-04-13.md](./baseline-2026-04-13.md)

## Current Read

- fixed family と committee 化まで進めると、validation では `TOPIX hold` より明確に良いです。one-shot pure OOS holdout (`2023-05-09 -> 2026-04-10`) で、fixed committee 100% は `CAGR 25.63% / Sharpe 1.50 / MaxDD -13.86%`、TOPIX hold は `CAGR 23.08% / Sharpe 1.08 / MaxDD -23.97%` でした。
- `mean x high` single-point の discovery ranking はまだ揺れますが、committee 空間に落とすと walk-forward stability はかなり改善しました。single-point space の `avg fold Spearman Sharpe = 0.31` に対して、committee space は `0.64`、`avg top5 overlap ratio` も `0.31 -> 0.95` まで上がっています。
- fixed committee を full sample (`2016-03-25 -> 2026-04-10`) に記述統計として当てると、`TOPIX hold` の `CAGR 12.48% / Sharpe 0.74 / MaxDD -35.31%` に対して、committee は `CAGR 13.17% / Sharpe 0.87 / MaxDD -25.91%` でした。headline としては OOS を優先し、full sample は descriptive read に留めます。
- 130% の信用二階建て仮説は、validation で `CAGR 33.96% / Sharpe 1.51 / MaxDD -17.74%` でした。金利・手数料・スリッページはまだ未考慮なので exploratory read に留めます。
- `high=0.24` と `0.25` は同じ `mean` の中ではほぼ同じ動きで、validation 一致率は `99.58%` (`mean1`) と `99.30%` (`mean2`) でした。したがって 4-member committee は実質的には 2 block に近いです。
- ただし、4 通りの 2-member simplification を OOS で比べると最良案は `mean1_high0.25 + mean2_high0.24` ですが、4-member fixed committee との差は validation 717 日中 8 日しか現れず、`best2 - ref4` の Newey-West 近似 `t=1.25, p≈0.21` でした。現時点では「2-member の方が有意に良い」とは言えないので、research lead は 4-member fixed committee に置きます。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt \
  python apps/bt/scripts/research/run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py \
  --run-id 20260413_topix_shock_confirmation_committee_overlay
```

bundle は
`~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-shock-confirmation-committee-overlay/<run_id>/`
に保存されます。

## Next Questions

- fixed 4-member committee を TOPIX100 5D swing lead candidate とどう組み合わせると、beta overlay と stock alpha を同時に扱えるか。
- `TOPIX overlay x TOPIX100 top-N sleeve` の重ね合わせで、return / Sharpe / MaxDD がどこまで改善するか。
- leverage を含む実務案では、金利・売買手数料・寄り付きスリッページを入れても edge が残るか。
