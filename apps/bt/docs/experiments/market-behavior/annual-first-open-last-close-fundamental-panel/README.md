# Annual First-Open Last-Close Fundamental Panel

各カレンダー年について、各銘柄を大発会（その年の最初の取引日）の `Open`
で買い、大納会（その年の最後の取引日）の `Close` で売った場合の年次保有
リターンを、買付時点で利用可能な FY ファンダメンタル指標と結合して見る研究。

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_first_open_last_close_fundamental_panel.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`
  - `summary.json`

## Design

- Entry: complete calendar year の最初の取引日の `Open`。
- Exit: 同じ年の最後の取引日の `Close`。
- Market scope: current `stocks.market_code` snapshot による retrospective market split。
- Fundamental as-of: entry date 以前に開示済みの最新 FY。
- Per-share adjustment:
  - EPS / BPS / forward EPS / dividend-per-share は entry date 以前の最新株式数
    baseline に補正する。
  - baseline は最新四半期の `shares_outstanding` を優先し、なければ最新任意
    開示の `shares_outstanding` に fallback する。
  - これにより、FY 発表後に株式分割や株式併合が発生した場合でも、FY 時点
    の EPS/BPS を買付時点の株式数基準へ寄せる。
- Price-dependent valuation:
  - PER / PBR / market cap は約定価格として entry `Open` を使う。
  - 寄前判定用の確認列として previous close 基準の PER/PBR も event ledger に残す。
- Liquidity:
  - ADV60 は entry 前の 60 セッションだけを使う。
  - 60 セッション未満の場合は `avg_trading_value_60d` を欠損にし、
    `avg_trading_value_60d_source_sessions` に利用可能セッション数を残す。

## Outputs

- `calendar_df`: 年ごとの entry / exit trading date。
- `event_ledger_df`: 銘柄年次の特徴量、補正診断、return/path metrics。
- `feature_coverage_df`: factor coverage。
- `feature_bucket_summary_df`: 年 x market scope 内 quantile bucket の return/path metrics。
- `factor_spread_summary_df`: high bucket と low bucket の spread。
- `annual_portfolio_daily_df`: 年次リバランス等ウェイト portfolio の daily curve。
- `annual_portfolio_summary_df`: return / CAGR / Sharpe / Sortino / Calmar / maxDD。

## Current Findings

Baseline result: [`baseline-2026-04-23.md`](./baseline-2026-04-23.md)

- `2017-2025` complete years, `29,294` realized stock-year events.
- Full-market annual equal-weight baseline: CAGR `11.4%`, Sharpe `0.76`,
  Sortino `0.86`, Calmar `0.32`, maxDD `-36.2%`.
- Market split: `standard` was the best broad market lens in this run
  (CAGR `12.6%`, Sharpe `0.87`), while `growth` had weaker risk-adjusted
  performance and deeper drawdown.
- The strongest reusable single-factor families were low `PBR`, low
  `forward PER`, low `PER`, and high dividend yield / forecast dividend yield.
- Low `PBR + small cap` was the strongest cross condition. `standard`
  `PBR Q1 + market-cap Q1` produced CAGR `37.7%`, Sharpe `2.16`, Sortino
  `2.40`, Calmar `1.18`, maxDD `-31.9%`.
- The small-cap / low-ADV effect is large but should be treated as an
  implementation-risk axis, not a free live signal. Slippage and capacity
  controls are mandatory before considering production use.
- Per-share adjustment mattered: `5,336` realized events had
  `share_adjustment_applied = true`, so ignoring post-FY share-count changes
  would materially distort EPS/BPS/forward EPS valuation buckets.

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_first_open_last_close_fundamental_panel.py \
  --output-root /tmp/trading25-research
```

出力先:

`/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/<run_id>/`

## Caveats

- Historical market migration は `market.duckdb` に保持されていないため、market split
  は current snapshot retrospective proxy。
- 現在年の大納会が未到来の場合、既定では incomplete last year を除外する。
- Factor bucket は最初の観察用であり、そのまま live threshold として使わない。
- Low-ADV / small-cap branches must be rechecked with liquidity floors,
  turnover/capacity assumptions, and realistic execution costs.
