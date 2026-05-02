# Annual First-Open Last-Close Fundamental Panel

各カレンダー年について、各銘柄を大発会（その年の最初の取引日）の `Open`
で買い、大納会（その年の最後の取引日）の `Close` で売った場合の年次保有
リターンを、買付時点で利用可能な FY ファンダメンタル指標と結合して見る研究。

## Published Readout

### Decision

- v3 `market.duckdb` では `stock_master_daily` の entry-date 構成と `TypeOfDocument` semantics による FY financial-statement 行を使う。直近 FY financial-statement 行を actual metrics の SoT にした後も、低 `PBR`、小型、低 `forward PER` は引き続き再利用価値があるが、production へ直結させず ranking / research diagnostic として扱う。

### Why This Research Was Run

- Annual first-open to last-close holding returns provide a clean calendar-year lens for testing whether PIT-safe FY fundamentals explain broad cross-sectional return differences.
- The study also checks whether per-share adjustment and entry-price-dependent valuation materially change factor buckets.

### Data Scope / PIT Assumptions

- Complete years `2017-2025`; `32,264` realized stock-year events.
- Entry is the first trading day `Open`; exit is the same calendar year last trading day `Close`.
- FY actual fundamentals are selected from financial-statement documents only when disclosed on or before the entry date. FY forecast / revision documents remain forecast sources and are not allowed to shadow actual BPS / EPS rows.
- EPS, BPS, forward EPS, and dividend per share are adjusted to the latest entry-date share-count baseline.
- Market split uses `stock_master_daily` on each entry date. Historical JPX segment codes are normalized to the current research labels: `0101/0111 -> prime`, `0102/0106/0112 -> standard`, `0104/0107/0113 -> growth`.

### Main Findings

#### Full-market baseline は年次保有の比較基準として有効だが、単体で十分な選別力はない。

| Scope | CAGR | Sharpe | Sortino | Calmar | MaxDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full market baseline | `11.5%` | `0.77` | `0.86` | `0.32` | `-36.0%` |

#### broad market lens では `standard` が最も良い。

| Market | CAGR | Sharpe |
| --- | ---: | ---: |
| `prime` | `9.4%` | `0.62` |
| `standard` | `15.9%` | `1.09` |
| `growth` | `8.9%` | `0.48` |

#### 再利用しやすい単独 factor は低 `PBR`、低 `forward PER`、低 `PER`、高配当/高予想配当利回り。

| Factor family | Readout |
| --- | --- |
| low `PBR` | factor bucket summary の上位に残った |
| low `forward PER` | factor bucket summary の上位に残った |
| low `PER` | factor bucket summary の上位に残った |
| high dividend / forecast dividend yield | factor bucket summary の上位に残った |

#### v3では低 `PBR` と低 `forward PER` が最上位の単独 spread として残る。

| Market / factor | Preferred spread |
| --- | ---: |
| `growth` / `pbr` | `42.6pp` |
| `standard` / `pbr` | `21.3pp` |
| `all` / `pbr` | `20.0pp` |
| `standard` / `forward_per` | `19.4pp` |
| `all` / `forward_per` | `18.2pp` |

#### per-share adjustment は省略できない。

| Metric | Value |
| --- | ---: |
| realized events with `share_adjustment_applied = true` | `6,389` |

### Interpretation

- The strongest signal is not simply "cheap" or "small"; it is the interaction where very low valuation and very small market cap concentrate the annual return edge.
- `standard` looks better than `growth` on risk-adjusted annual holding metrics in this run, and the market split is now PIT-safe at each annual entry date.
- `forward EPS / actual EPS` is weaker than expected as a broad selector. The ratio is not useless, but it does not dominate the simpler low valuation families.
- The share-count adjustment and document semantics are not cosmetic: without them, EPS/BPS/forward EPS valuation buckets would be materially distorted for thousands of events, and forecast-only FY rows can incorrectly shadow actual BPS.

### Production Implication

- Use this as a ranking research input for value/composite ranking, especially low `PBR`, low `forward PER`, and market-cap interaction diagnostics.
- Do not promote the strongest `small-cap + low-PBR` branch directly into production before liquidity floors, turnover/capacity modeling, and realistic execution cost checks.
- Treat ADV as a capacity/execution diagnostic, not as the source of alpha.

### Caveats

- Factor buckets are observational and must not be copied directly into live thresholds.
- Legacy JPX segment labels are collapsed into current research labels for comparability, so `prime/standard/growth` before 2022 are proxy buckets rather than literal then-current market names.
- The strongest branch is exposed to small-cap / low-ADV implementation risk.
- Current-year incomplete annual returns are excluded by default.

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py`
- Runner: `apps/bt/scripts/research/run_annual_first_open_last_close_fundamental_panel.py`
- Baseline: [`baseline-2026-04-23.md`](./baseline-2026-04-23.md)
- v3 statement-document semantics bundle: `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_statement_doc_semantics/`
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
- Market scope: `stock_master_daily` の entry date snapshot による PIT market split。
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

v3 PIT stock-master + statement-document semantics rerun:
`/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_statement_doc_semantics/`

- `2017-2025` complete years, `32,264` realized stock-year events.
- Full-market annual equal-weight baseline: CAGR `11.5%`, Sharpe `0.77`,
  Sortino `0.86`, Calmar `0.32`, maxDD `-36.0%`.
- Market split: `standard` was the best broad market lens in this run
  (CAGR `15.9%`, Sharpe `1.09`), while `growth` had weaker risk-adjusted
  performance and deeper drawdown.
- The strongest reusable single-factor families were low `PBR`, low
  `forward PER`, low `PER`, and high dividend yield / forecast dividend yield.
- `forward EPS / actual EPS` was added as `forward_eps_to_actual_eps`; it was
  weak as a broad selector in this run. Q5 high ratio did not beat Q1 low ratio
  for `all`, `standard`, or `growth`, though `prime` had a small positive Q5
  tilt.
- Low `PBR` and low `forward PER` remained the strongest simple spread families.
  The next-stage composite studies still need to handle capacity and liquidity
  outside the alpha score.
- The small-cap / low-ADV effect is large but should be treated as an
  implementation-risk axis, not a free live signal. Slippage and capacity
  controls are mandatory before considering production use.
- Per-share adjustment mattered: `6,389` realized events had
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

- v3 rerun は `stock_master_daily` を使うが、2022年以前の legacy JPX segment は
  current research label へ proxy collapse している。
- 現在年の大納会が未到来の場合、既定では incomplete last year を除外する。
- Factor bucket は最初の観察用であり、そのまま live threshold として使わない。
- Low-ADV / small-cap branches must be rechecked with liquidity floors,
  turnover/capacity assumptions, and realistic execution costs.
