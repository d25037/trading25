# Annual First-Open Last-Close Fundamental Panel

各カレンダー年について、各銘柄を大発会（その年の最初の取引日）の `Open`
で買い、大納会（その年の最後の取引日）の `Close` で売った場合の年次保有
リターンを、買付時点で利用可能な FY ファンダメンタル指標と結合して見る研究。

## Published Readout

### Decision

- Low `PBR + small cap` is the strongest reusable annual cross-section readout from this run, but it stays a research/ranking diagnostic until capacity, liquidity floors, turnover, and execution cost checks are added.

### Why This Research Was Run

- Annual first-open to last-close holding returns provide a clean calendar-year lens for testing whether PIT-safe FY fundamentals explain broad cross-sectional return differences.
- The study also checks whether per-share adjustment and entry-price-dependent valuation materially change factor buckets.

### Data Scope / PIT Assumptions

- Complete years `2017-2025`; `29,294` realized stock-year events.
- Entry is the first trading day `Open`; exit is the same calendar year last trading day `Close`.
- FY fundamentals are selected only when disclosed on or before the entry date.
- EPS, BPS, forward EPS, and dividend per share are adjusted to the latest entry-date share-count baseline.
- Market split uses the current `stocks.market_code` snapshot as a retrospective proxy because historical market migration is not stored.

### Main Findings

#### Full-market baseline は年次保有の比較基準として有効だが、単体で十分な選別力はない。

| Scope | CAGR | Sharpe | Sortino | Calmar | MaxDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full market baseline | `11.4%` | `0.76` | `0.86` | `0.32` | `-36.2%` |

#### broad market lens では `standard` が最も良い。

| Market | CAGR | Sharpe |
| --- | ---: | ---: |
| `standard` | `12.6%` | `0.87` |

#### 再利用しやすい単独 factor は低 `PBR`、低 `forward PER`、低 `PER`、高配当/高予想配当利回り。

| Factor family | Readout |
| --- | --- |
| low `PBR` | factor bucket summary の上位に残った |
| low `forward PER` | factor bucket summary の上位に残った |
| low `PER` | factor bucket summary の上位に残った |
| high dividend / forecast dividend yield | factor bucket summary の上位に残った |

#### `forward_eps_to_actual_eps` は広い selector としては弱い。

| Market lens | Q5 high ratio vs Q1 low ratio |
| --- | --- |
| `all` | Q5 が Q1 を上回らない |
| `standard` | Q5 が Q1 を上回らない |
| `growth` | Q5 が Q1 を上回らない |
| `prime` | Q5 に小さな positive tilt |

#### 最強の cross condition は低 `PBR + small cap` だが、capacity risk が大きい。

| Condition | CAGR | Sharpe | Sortino | Calmar | MaxDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| `standard` `PBR Q1 + market-cap Q1` | `37.7%` | `2.16` | `2.40` | `1.18` | `-31.9%` |

#### per-share adjustment は省略できない。

| Metric | Value |
| --- | ---: |
| realized events with `share_adjustment_applied = true` | `5,336` |

### Interpretation

- The strongest signal is not simply "cheap" or "small"; it is the interaction where very low valuation and very small market cap concentrate the annual return edge.
- `standard` looks better than `growth` on risk-adjusted annual holding metrics in this run, but the market split is retrospective and should not be treated as a historical membership truth.
- `forward EPS / actual EPS` is weaker than expected as a broad selector. The ratio is not useless, but it does not dominate the simpler low valuation families.
- The share-count adjustment is not cosmetic: without it, EPS/BPS/forward EPS valuation buckets would be materially distorted for thousands of events.

### Production Implication

- Use this as a ranking research input for value/composite ranking, especially low `PBR`, low `forward PER`, and market-cap interaction diagnostics.
- Do not promote the strongest `small-cap + low-PBR` branch directly into production before liquidity floors, turnover/capacity modeling, and realistic execution cost checks.
- Treat ADV as a capacity/execution diagnostic, not as the source of alpha.

### Caveats

- Factor buckets are observational and must not be copied directly into live thresholds.
- Historical market migration is unavailable, so market split is a current-snapshot proxy.
- The strongest branch is exposed to small-cap / low-ADV implementation risk.
- Current-year incomplete annual returns are excluded by default.

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py`
- Runner: `apps/bt/scripts/research/run_annual_first_open_last_close_fundamental_panel.py`
- Baseline: [`baseline-2026-04-23.md`](./baseline-2026-04-23.md)
- Bundle artifacts: `manifest.json`, `results.duckdb`, `summary.md`, `summary.json`

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
- `forward EPS / actual EPS` was added as `forward_eps_to_actual_eps`; it was
  weak as a broad selector in this run. Q5 high ratio did not beat Q1 low ratio
  for `all`, `standard`, or `growth`, though `prime` had a small positive Q5
  tilt.
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
