# Stop-Limit Buy-Only Next-Close Followthrough

`stop_low × intraday_range` の翌日引けが発生日終値を上回ったか下回ったかで分け、その翌日引けで買う buy-only 枝を読む実験です。

## Purpose

- `standard / growth では空売り困難な銘柄が多い` という前提で、buy-only に落とし直す。
- `next_open` ではなく `next_close` まで confirmation を待った場合に edge が残るかを確認する。
- trade-level の見え方と、同日等ウェイト portfolio lens の見え方を分ける。

## Scope

- Input events:
  - `market in {プライム, スタンダード, グロース}`
  - `limit_side = stop_low`
  - `intraday_state = intraday_range`
- Signal split:
  - `next_close > event_close` -> `plus`
  - `next_close < event_close` -> `minus`
  - exact flat は参考列に残すが主読解からは外す
- Trade rule:
  - buy at `next_close`
  - exit at `close +3 sessions` / `close +5 sessions`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_stop_limit_buy_only_next_close_followthrough.py`
- Parent classification:
  - `apps/bt/scripts/research/run_stop_limit_daily_classification.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/stop_limit_buy_only_next_close_followthrough.py`
  - `apps/bt/src/domains/analytics/stop_limit_daily_classification.py`
  - `apps/bt/src/domains/analytics/jpx_daily_price_limits.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_stop_limit_buy_only_next_close_followthrough.py`
  - `apps/bt/tests/unit/domains/analytics/test_stop_limit_daily_classification.py`
  - `apps/bt/tests/unit/domains/analytics/test_jpx_daily_price_limits.py`

## Latest Baseline

- [baseline-2026-04-20.md](./baseline-2026-04-20.md)

## Current Read

- Filtered events は `3,263` 件で、`plus = 1,600`, `minus = 1,632`, `flat = 31` でした。
- trade-level では `next_close = plus` が明確に強く、5 日後成績は
  - スタンダード `stop_low`: mean `+4.99%`, median `+5.88%`, win `69.2%`
  - グロース `stop_low`: mean `+6.48%`, median `+6.95%`, win `69.6%`
  - プライム `stop_low`: mean `+5.19%`, median `+4.79%`, win `72.2%`
  - プライム `off_limit_close`: mean `+3.14%`, median `+3.78%`, win `65.9%`
- ただし同日等ウェイト portfolio lens ではかなり削られます。
  - スタンダード `stop_low + plus`: mean cohort `-2.06%`, win `38.1%`
  - グロース `stop_low + plus`: mean cohort `+0.06%`, win `37.8%`
  - プライム `stop_low + plus`: mean cohort `+1.01%`, win `50.7%`
  - プライム `off_limit_close + plus`: mean cohort `+1.04%`, win `54.8%`
- つまり `銘柄を 1 件ずつ数えると強いが、毎日その条件を全部買うと弱い` という構造です。スタンダード / グロースでは、少数の crash-rebound 日が trade average を押し上げていました。
- 代表的な多銘柄日には `2024-08-06` と `2025-04-08` が含まれ、前営業日の TOPIX が大きく崩れ、entry 日に反発していました。この枝は個別 stop-low edge だけではなく、`市場ショック翌日の反発確認` をかなり含んでいます。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_stop_limit_buy_only_next_close_followthrough.py \
  --output-root /tmp/trading25-research
```

bundle は
`/tmp/trading25-research/market-behavior/stop-limit-buy-only-next-close-followthrough/<run_id>/`
に保存されます。

## Next Questions

- `TOPIX event-day return <= -3% / -5% / -8%` のような market shock 条件を明示的に入れると、plus branch は portfolio lens でも残るか。
- `same-day signal count >= 4` のような breadth proxy を戦略条件にすると、trade-level と date-level のギャップは縮まるか。
- liquidity / size / volume shock を追加すると、スタンダード / グロースの single-name day を改善できるか。
