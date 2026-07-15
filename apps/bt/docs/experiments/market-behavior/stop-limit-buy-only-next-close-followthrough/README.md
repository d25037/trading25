# Stop-Limit Buy-Only Next-Close Followthrough

## Published Readout

### Decision
- PIT-safe parent rerun completed. 旧 baseline の market grouping headline は撤回し、`20260608_pit_safe_parent_stock_master_daily_v2` の結果で置き換える。

### Why This Research Was Run
- stop-limit classification を signal-date `stock_master_daily` grouping に直した後、`stop_low × intraday_range` の buy-only branch が残るかを再検証する。
- trade-level の強さと、同一 entry date で全銘柄を等ウェイトに買う portfolio lens の差を分けて読む。

### Data Scope / PIT Assumptions
- Run ID: `20260608_pit_safe_parent_stock_master_daily_v2`
- Parent classification run: `20260608_pit_safe_stock_master_daily`
- Analysis range: `2022-04-05 -> 2026-06-05`
- Historical parent-run market schema: `3`（retired; 数値の provenance としてのみ保持）
- Current rerun requirement: parent/child とも Market schema v4 / `local_projection_v2_event_time`
- Parent universe source: `stock_master_daily`
- As-of policy: 親 stop-limit event は event date の `stock_master_daily` market grouping。latest `stocks` fallback は使わない。

### Main Findings
#### 結論: trade-level では plus branch が強い

| Market | Close state | Sign | Events | Mean 5d | Win 5d |
| --- | --- | --- | ---:| ---:| ---:|
| グロース | stop_low | plus | `266` | `+7.65%` | `76.32%` |
| スタンダード | stop_low | plus | `223` | `+7.30%` | `79.64%` |
| プライム | stop_low | plus | `113` | `+6.65%` | `82.30%` |
| スタンダード | off_limit_close | plus | `196` | `+4.80%` | `65.13%` |
| グロース | off_limit_close | plus | `165` | `+4.15%` | `64.42%` |
| プライム | off_limit_close | plus | `137` | `+3.79%` | `70.59%` |

#### 結論: portfolio lens ではプライム以外がかなり削られる

| Market | Close state | Sign | Dates | Avg names | Mean cohort | Win |
| --- | --- | --- | ---:| ---:| ---:| ---:|
| プライム | stop_low | plus | `36` | `3.14` | `+2.10%` | `55.56%` |
| プライム | off_limit_close | plus | `50` | `2.74` | `+1.47%` | `55.10%` |
| グロース | off_limit_close | plus | `67` | `2.46` | `-1.76%` | `36.36%` |
| スタンダード | off_limit_close | plus | `72` | `2.72` | `-1.80%` | `33.80%` |
| グロース | stop_low | plus | `78` | `3.41` | `-1.81%` | `34.62%` |
| スタンダード | stop_low | plus | `59` | `3.78` | `-2.38%` | `36.21%` |

### Interpretation
- `next_close` が event close を上回った銘柄だけを見る trade-level では、全市場で 5d mean が強い。
- しかし同日等ウェイトで買うと、スタンダード / グロースは negative になる。個別銘柄平均の edge は、少数の大きな rebound day に依存している。
- プライムだけは cohort lens でも `stop_low + plus` と `off_limit_close + plus` が小幅プラスで、最も実運用に近い候補として残る。

### Production Implication
- production 候補としては、プライムの `stop_low × intraday_range × next_close plus` を先に検討する。
- スタンダード / グロースは single-name selection や liquidity filter なしに「全部買う」形へ移すべきではない。market shock / breadth 条件を明示して再評価する。

### Caveats
- entry assumption は `next_close` で signal confirm かつ close auction 近似で約定する前提。実際の引け約定、流動性、成行 impact は未評価。
- 旧 baseline の数値は下の既存セクションに残るが、この `Published Readout` より優先しない。

### Source Artifacts
- Experiment: `market-behavior/stop-limit-buy-only-next-close-followthrough`
- Runner: `apps/bt/scripts/research/run_stop_limit_buy_only_next_close_followthrough.py`
- Parent runner: `apps/bt/scripts/research/run_stop_limit_daily_classification.py`
- Domain logic: `apps/bt/src/domains/analytics/stop_limit_buy_only_next_close_followthrough.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/stop-limit-buy-only-next-close-followthrough/20260608_pit_safe_parent_stock_master_daily_v2/`
- Bundle tables: `signal_event_df`, `signal_summary_df`, `entry_cohort_df`, `cohort_portfolio_summary_df`, `yearly_summary_df`
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

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
