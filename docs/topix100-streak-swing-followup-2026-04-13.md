# TOPIX100 Streak Swing Follow-Up (2026-04-13)

## Scope

This note records the follow-up judgment after the leak-free TOPIX100 streak 3 / 53 swing walk-forward runs below:

- `5D raw target`: `20260411_swing5d_wf126`
- `10D raw target`: `20260411_swing10d_wf126`
- `5D excess-vs-TOPIX target`: `20260411_swing5d_excess_topix_wf126`

All three use the same point-in-time snapshot discipline:

- signal built with information available up to day `X`
- entry at `X+1 open`
- exit at `X+5 close` or `X+10 close`
- walk-forward re-estimation inside each train block only

## Main Read

At this stage, the primary practical candidate is:

- `5D raw target`
- `Top1`
- `5-sleeve`
- `long-only`

This configuration is currently the most convincing research result in the swing study and looks materially better than simply holding the benchmark on both absolute and risk-adjusted return.

The exact `5-sleeve` reconstruction was recomputed from `walkforward_topk_pick_df` plus daily `stock_data` / `topix_data`, marking positions to market each day from entry through exit and trimming the final `H-1` unwind days for like-for-like comparison.

## Exact 5-Sleeve Metrics

| Book | CAGR | Sharpe | Sortino | MaxDD |
|---|---:|---:|---:|---:|
| Top1 | `51.27%` | `1.55` | `2.42` | `-27.35%` |
| Top3 | `35.65%` | `1.33` | `1.93` | `-28.80%` |
| Top5 | `29.70%` | `1.20` | `1.70` | `-27.45%` |
| TOPIX 5-sleeve benchmark | `11.72%` | `0.72` | `1.01` | `-26.74%` |

## Interpretation

- `Top1` is the strongest option on `CAGR`, `Sharpe`, and `Sortino`.
- `Top1` is not paying for that edge with a meaningfully worse exact `5-sleeve` drawdown than `Top5`.
- `Top3` and `Top5` are more diversified in construction, but in this run they are dominated by `Top1` on risk-adjusted return.
- `5D excess-vs-TOPIX` did not improve the ranking slope in raw-return space enough to replace the raw-target line.
- `10D raw` improved neither downside profile nor risk-adjusted return enough to displace the `5D raw Top1` candidate.

## Cautions

- This is still pre-cost and pre-slippage research.
- `Top1` remains a concentrated book and should be stress-tested against open-auction slippage and single-name gap risk.
- The exact `5-sleeve` drawdown numbers above replace earlier rough approximations that overstated drawdown.

## Current Decision

Unless later cost / execution analysis rejects it, the next research line should treat `5D raw / Top1 / 5-sleeve / long-only` as the lead candidate rather than `Top3`, `Top5`, `10D raw`, or `5D excess-vs-TOPIX`.
