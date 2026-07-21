# Annual First-Open Last-Close Fundamental Panel

各カレンダー年について、各銘柄を大発会（その年の最初の取引日）の `Open`
で買い、大納会（その年の最後の取引日）の `Close` で売った場合の年次保有
リターンを、買付時点で利用可能な FY ファンダメンタル指標と結合して見る研究。

## Published Readout

> [!WARNING]
> **Status: `historical_archive`; `rerun_required`.** This Market v3 evidence
> is retained only as a historical candidate. It must not drive production,
> thresholds, or Ranking decisions before a physical Market v5
> `market.duckdb` rerun with
> `stock_price_adjustment_mode=provider_adjusted_v1`, signal-date PIT
> membership, and provider-vintage/current-basis provenance.

### Decision

- この v3 `market.duckdb` readout は `stock_master_daily` の entry-date 構成と `TypeOfDocument` semantics による FY financial-statement 行を使った historical measurement である。低 `PBR`、小型、低 `forward PER` は Market v5 rerun の候補として保存するが、production、threshold、Ranking diagnostic の根拠には使わない。

### Why This Historical Research Was Run

- Annual first-open to last-close holding returns provided a clean calendar-year lens for testing whether historically available FY fundamentals explained broad cross-sectional return differences.
- The study also checks whether per-share adjustment and entry-price-dependent valuation materially change factor buckets.

### Historical Data Scope / PIT Assumptions

- Complete years `2017-2025`; `32,264` realized stock-year events.
- Entry is the first trading day `Open`; exit is the same calendar year last trading day `Close`.
- FY actual fundamentals are selected from financial-statement documents only when disclosed on or before the entry date. FY forecast / revision documents remain forecast sources and are not allowed to shadow actual BPS / EPS rows.
- EPS, BPS, forward EPS, and dividend per share are adjusted to the latest entry-date share-count baseline.
- Market split uses `stock_master_daily` on each entry date. Historical JPX segment codes are normalized to the current research labels: `0101/0111 -> prime`, `0102/0106/0112 -> standard`, `0104/0107/0113 -> growth`.

### Main Findings

**Historical measurement only:** the following Market v3 measurements preserve
their prior dates and provenance, but must not drive production, thresholds, or
Ranking decisions before the required Market v5 `provider_adjusted_v1` rerun.

#### Historical full-market baseline は年次保有の比較基準だったが、単体で十分な選別力はない。

| Scope | CAGR | Sharpe | Sortino | Calmar | MaxDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full market baseline | `11.5%` | `0.77` | `0.86` | `0.32` | `-36.0%` |

#### Historical broad market lens では `standard` が最も良かった。

| Market | CAGR | Sharpe |
| --- | ---: | ---: |
| `prime` | `9.4%` | `0.62` |
| `standard` | `15.9%` | `1.09` |
| `growth` | `8.9%` | `0.48` |

#### Market v5 で再検証する単独 factor 候補は低 `PBR`、低 `forward PER`、低 `PER`、高配当/高予想配当利回り。

| Factor family | Readout |
| --- | --- |
| low `PBR` | factor bucket summary の上位に残った |
| low `forward PER` | factor bucket summary の上位に残った |
| low `PER` | factor bucket summary の上位に残った |
| high dividend / forecast dividend yield | factor bucket summary の上位に残った |

#### v3では低 `PBR` と低 `forward PER` が最上位の単独 spread として記録された。

| Market / factor | Preferred spread |
| --- | ---: |
| `growth` / `pbr` | `43.8pp` |
| `standard` / `pbr` | `21.7pp` |
| `all` / `pbr` | `19.9pp` |
| `standard` / `forward_per` | `20.0pp` |
| `all` / `forward_per` | `18.4pp` |

#### per-share adjustment は省略できない。

| Metric | Value |
| --- | ---: |
| realized events with `share_adjustment_applied = true` | `6,534` |

### Interpretation

- The historical v3 run recorded its strongest relationship in the interaction of very low valuation and very small market cap; it is a rerun hypothesis, not a current signal.
- `standard` looked better than `growth` on risk-adjusted annual holding metrics in this historical run. Its entry-date split does not establish the required Market v5 provider-adjusted evidence.
- `forward EPS / actual EPS` was weaker than expected as a broad selector in the historical measurement. It does not dominate the simpler low valuation families in that run.
- The historical share-count adjustment and document semantics showed material sensitivity for thousands of events. Market v5 must instead use the current-basis `statement_metrics_adjusted` and PIT `daily_valuation` contract where applicable.

### Production Implication

- Re-evaluate low `PBR`, low `forward PER`, and market-cap interactions as historical candidates after a Market v5 rerun.
- Do not promote any `small-cap + low-PBR` branch into production; a valid rerun still requires liquidity floors, turnover/capacity modeling, and realistic execution cost checks.
- Treat ADV as a capacity/execution diagnostic, not as the source of alpha.

### Caveats

- Factor buckets are historical observations and must not be copied into live thresholds or Ranking decisions before the Market v5 rerun.
- Legacy JPX segment labels are collapsed into current research labels for comparability, so `prime/standard/growth` before 2022 are proxy buckets rather than literal then-current market names.
- The strongest branch is exposed to small-cap / low-ADV implementation risk.
- Current-year incomplete annual returns are excluded by default.

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py`
- Runner: `apps/bt/scripts/research/run_annual_first_open_last_close_fundamental_panel.py`
- Baseline: [`baseline-2026-04-23.md`](./baseline-2026-04-23.md)
- historical v3 share-basis bundle: `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_share_basis_rerun/`
- Bundle artifacts: `manifest.json`, `results.duckdb`, `summary.md`

## Historical Source Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_first_open_last_close_fundamental_panel.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

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

## Historical Findings

Baseline result: [`baseline-2026-04-23.md`](./baseline-2026-04-23.md)

Historical v3 stock-master + statement-document semantics run:
`/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_statement_doc_semantics/`

- `2017-2025` complete years, `32,264` realized stock-year events.
- Full-market annual equal-weight baseline: CAGR `11.5%`, Sharpe `0.77`,
  Sortino `0.86`, Calmar `0.32`, maxDD `-36.0%`.
- Market split: `standard` was the best broad market lens in this run
  (CAGR `15.9%`, Sharpe `1.09`), while `growth` had weaker risk-adjusted
  performance and deeper drawdown.
- The single-factor candidates for a Market v5 rerun were low `PBR`, low
  `forward PER`, low `PER`, and high dividend yield / forecast dividend yield.
- `forward EPS / actual EPS` was added as `forward_eps_to_actual_eps`; it was
  weak as a broad selector in this run. Q5 high ratio did not beat Q1 low ratio
  for `all`, `standard`, or `growth`, though `prime` had a small positive Q5
  tilt.
- Low `PBR` and low `forward PER` were the strongest simple spread families in
  this historical run. Any composite study must rerun on Market v5 and handle
  capacity and liquidity outside the alpha score.
- The small-cap / low-ADV effect is a historical implementation-risk axis, not
  a live signal. Slippage and capacity controls remain mandatory after a valid
  rerun.
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

- historical v3 run は `stock_master_daily` を使うが、2022年以前の legacy JPX segment は
  current research label へ proxy collapse している。
- 現在年の大納会が未到来の場合、既定では incomplete last year を除外する。
- Factor bucket は最初の観察用であり、そのまま live threshold として使わない。
- Low-ADV / small-cap branches must be rechecked with liquidity floors,
  turnover/capacity assumptions, and realistic execution costs.
