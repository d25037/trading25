# Pre-Earnings EPS 1.2x Proxy

## Published Readout

### Decision

`pre_earnings_eps120_proxy` は、発表前までに観測できる Daily Ranking 系 valuation proxy が「FY 発表で `positive event` かつ `next_year_forecast_eps >= actual_eps * 1.2`」を予測できるかを見る research として追加する。

今回の初回 run では、Prime FY において `EPS 1.2x positive` は全体 `9.83%`、対象 EPS が揃う eligible event 内では `16.68%`。低PER・低forward PER・低PBR・小型は発生率を上げず、むしろ下げる。最も強い単独 proxy は `forward PER >= 30` で、target rate `20.03%`、base 比 `2.04x`。

これは「割安だから EPS 1.2x positive が出やすい」ではなく、「発表前から高い forward PER / PER を許容されている銘柄ほど、FY で強い次期EPS予想が出やすい」という結果。Daily Ranking の value 系条件をそのまま proxy にするより、期待成長・高評価を許容されている状態を別 regime として扱う方が自然。

この readout は bundle `/private/tmp/trading25-research/market-behavior/pre-earnings-eps120-proxy/20260516_pre_earnings_eps120_proxy_v2` に基づく。入力 DB は `/Users/shinjiroaso/.local/share/trading25/market-timeseries/market.duckdb`、対象は `2016-04-01` から `2026-05-14`。

### Main Findings

#### 結論

Prime FY event は `26,895` 件。発表前 valuation coverage は forward PER が最も高く `94.27%`、PER / PBR / market cap は `60-65%` 程度。

| scope | events | eligible | target | target rate | eligible target rate | forward PER coverage | PER coverage | PBR coverage |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Prime FY | `26,895` | `15,853` | `2,644` | `9.83%` | `16.68%` | `94.27%` | `60.47%` | `65.18%` |

#### Forward PER

forward PER は明確に monotonic。低forward PERは target 発生率が低く、高forward PERほど `EPS 1.2x positive` が出やすい。

| forward PER bucket | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `<=10` | `8,157` | `400` | `4.90%` | `7.86%` | `0.50x` |
| `10-15` | `5,748` | `413` | `7.19%` | `11.29%` | `0.73x` |
| `15-20` | `3,531` | `405` | `11.47%` | `17.82%` | `1.17x` |
| `20-30` | `3,246` | `486` | `14.97%` | `25.37%` | `1.52x` |
| `>30` | `4,672` | `936` | `20.03%` | `43.58%` | `2.04x` |

#### PER / PBR / Market Cap

PER も高い側で lift が出る。PBR は `>2.0` で小幅 positive、低PBRは negative。market cap は単独では弱く、小型も大型も target 発生率を上げない。

| condition | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `forward_per >= 30` | `4,672` | `936` | `20.03%` | `43.58%` | `2.04x` |
| `forward_per >= 20` | `7,920` | `1,422` | `17.95%` | `34.99%` | `1.83x` |
| `per >= 30` | `2,697` | `371` | `13.76%` | `25.98%` | `1.40x` |
| `per >= 20` | `4,763` | `577` | `12.11%` | `21.40%` | `1.23x` |
| `pbr >= 2.0` | `3,936` | `440` | `11.18%` | `19.58%` | `1.14x` |
| `market_cap >= 1000bn` | `1,196` | `102` | `8.53%` | `13.75%` | `0.87x` |
| `market_cap <= 300bn` | `14,213` | `1,156` | `8.13%` | `14.93%` | `0.83x` |

#### Value Combos

Daily Ranking / value-composite 的な低forward PER・低PBR・小型 combo は、`EPS 1.2x positive` の予測には効かない。むしろ base を下回る。

| condition | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `low_pbr1.0_and_mcap_le300` | `7,643` | `553` | `7.24%` | `13.73%` | `0.74x` |
| `low_forward_per15_and_mcap_le300` | `8,181` | `423` | `5.17%` | `8.83%` | `0.53x` |
| `low_forward_per15_and_low_pbr1.5` | `8,208` | `413` | `5.03%` | `8.32%` | `0.51x` |
| `low_forward_per15_low_pbr1.5_mcap_le300` | `7,020` | `350` | `4.99%` | `8.48%` | `0.51x` |

### Interpretation

`EPS 1.2x positive` は、value factor ではなく growth-expectation factor に近い。低forward PER は「割安」ではあるが、強い次期EPS予想が新たに出る状態を予測する proxy にはなっていない。

一方、発表前 forward PER が高い銘柄は、すでに市場が高い成長期待を払っている状態で、その期待をFY予想が裏付けるケースが多い。これは「高forward PERを買えば良い」ではなく、「EPS 1.2x positive の事前発生確率を上げる proxy は value ではなく期待成長側」という意味。

market cap は単独では弱い。小型は target rate を上げず、大型も上げない。サイズより、発表前にどれだけ高い forward PER / PER を許容されているかの方が説明力がある。

### Production Implication

この Phase 1 だけで production strategy に entry rule を追加しない。

次に見る価値がある候補は以下。

| 用途 | 候補 |
|---|---|
| EPS 1.2x positive 事前 proxy | `forward_per >= 20` / `forward_per >= 30` |
| Avoid proxy | `forward_per <= 15`、低PBR・小型 value combo |
| Ranking integration | value ranking とは別に growth-expectation regime として扱う |
| Next validation | `forward_per >= 20/30` を hold-through / post-entry return と no-fill rate に接続し、年度 split と runup / ADV/FF を重ねる |

### Caveats

- target は発表後 outcome: `is_fy=true`, `event_strength=positive`, `actual_eps > 0`, `next_year_forecast_eps >= actual_eps * 1.2`。
- proxy は発表前営業日の close と、発表前までに開示済みの latest FY / latest forecast EPS だけで計算する。発表当日の FY row は valuation proxy に使わない。
- valuation proxy は Daily Ranking の daily valuation semantics に寄せ、`PER = close / latest FY EPS`, `forward PER = close / latest forecast EPS`, `PBR = close / latest FY BPS`, `market cap = close * latest shares_outstanding` とした。
- share adjustment は Phase 1 では簡易化している。Daily Ranking calculator は share adjustment events を使うため、株式分割を跨ぐ古い event の market cap には残差がありうる。
- PER / PBR / market cap coverage は statements の EPS/BPS/share coverage に依存する。missing bucket は単純に投資可能 signal として読まない。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/pre_earnings_eps120_proxy.py`
- runner: `apps/bt/scripts/research/run_pre_earnings_eps120_proxy.py`
- bundle experiment id: `market-behavior/pre-earnings-eps120-proxy`
- latest result bundle: `/private/tmp/trading25-research/market-behavior/pre-earnings-eps120-proxy/20260516_pre_earnings_eps120_proxy_v2`
- result tables: `event_feature_df`, `coverage_diagnostics_df`, `feature_bucket_df`, `threshold_grid_df`, `combo_grid_df`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_pre_earnings_eps120_proxy.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --min-events 100 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260516_pre_earnings_eps120_proxy_v2
```

## Artifact Tables

- `event_feature_df`: event-level post-disclosure target plus pre-disclosure valuation proxies.
- `coverage_diagnostics_df`: event count, eligible target count, target rate, and proxy coverage.
- `feature_bucket_df`: PER / forward PER / PBR / market cap bucket target rates.
- `threshold_grid_df`: single-condition target-rate grid.
- `combo_grid_df`: value-style combo target-rate grid.
