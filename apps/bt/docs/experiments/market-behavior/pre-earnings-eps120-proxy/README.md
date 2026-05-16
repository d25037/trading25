# Pre-Earnings EPS 1.2x Proxy

## Published Readout

### Decision

`pre_earnings_eps120_proxy` は、発表前までに観測できる Daily Ranking 系 valuation proxy が「FY 発表で `positive event` かつ `next_year_forecast_eps >= actual_eps * 1.2`」を予測できるかを見る research として追加する。

今回の adjusted valuation run では、Prime FY において `EPS 1.2x positive` は全体 `9.88%`、対象 EPS が揃う eligible event 内では `16.71%`。低PER・低forward PER・低PBR・小型は発生率を上げず、むしろ下げる。最も強い単独 proxy は `forward PER >= 30` で、target rate `21.56%`、base 比 `2.18x`。

これは「割安だから EPS 1.2x positive が出やすい」ではなく、「発表前から高い forward PER / PER を許容されている銘柄ほど、FY で強い次期EPS予想が出やすい」という結果。Daily Ranking の value 系条件をそのまま proxy にするより、期待成長・高評価を許容されている状態を別 regime として扱う方が自然。

この readout は bundle `/private/tmp/trading25-research/market-behavior/pre-earnings-eps120-proxy/20260516_pre_earnings_eps120_proxy_adjusted_valuation_v1` に基づく。入力 DB は `/Users/shinjiroaso/.local/share/trading25/market-timeseries/market.duckdb`、対象は `2016-04-01` から `2026-05-14`。

### Main Findings

#### 結論

Prime FY event は `27,065` 件。発表前 valuation coverage は PBR / market cap が `92-95%`、PER / forward PER が `85-86%` 程度。

| scope | events | eligible | target | target rate | eligible target rate | forward PER coverage | PER coverage | PBR coverage |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Prime FY | `27,065` | `16,007` | `2,675` | `9.88%` | `16.71%` | `86.14%` | `85.26%` | `92.71%` |

#### Forward PER

forward PER は明確に monotonic。低forward PERは target 発生率が低く、高forward PERほど `EPS 1.2x positive` が出やすい。

| forward PER bucket | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `<=10` | `7,642` | `366` | `4.79%` | `7.45%` | `0.48x` |
| `10-15` | `5,397` | `396` | `7.34%` | `11.16%` | `0.74x` |
| `15-20` | `3,298` | `376` | `11.40%` | `17.20%` | `1.15x` |
| `20-30` | `2,980` | `470` | `15.77%` | `25.42%` | `1.60x` |
| `>30` | `3,998` | `862` | `21.56%` | `43.25%` | `2.18x` |

#### PER / PBR / Market Cap

PER も高い側で lift が出る。PBR は `>2.0` で小幅 positive、低PBRは negative。market cap は単独では弱く、小型も大型も target 発生率を上げない。

| condition | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `forward_per >= 30` | `3,998` | `862` | `21.56%` | `43.25%` | `2.18x` |
| `forward_per >= 20` | `6,979` | `1,332` | `19.09%` | `34.67%` | `1.93x` |
| `per >= 30` | `3,852` | `577` | `14.98%` | `27.98%` | `1.52x` |
| `per >= 20` | `6,815` | `919` | `13.48%` | `23.37%` | `1.36x` |
| `pbr >= 2.0` | `5,371` | `647` | `12.05%` | `20.53%` | `1.22x` |
| `market_cap >= 1000bn` | `1,524` | `142` | `9.32%` | `14.82%` | `0.94x` |
| `market_cap <= 300bn` | `21,425` | `2,119` | `9.89%` | `17.45%` | `1.00x` |

#### Value Combos

Daily Ranking / value-composite 的な低forward PER・低PBR・小型 combo は、`EPS 1.2x positive` の予測には効かない。むしろ base を下回る。

| condition | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `low_pbr1.0_and_mcap_le300` | `7,643` | `553` | `7.24%` | `13.73%` | `0.74x` |
| `low_forward_per15_and_mcap_le300` | `8,181` | `423` | `5.17%` | `8.83%` | `0.53x` |
| `low_forward_per15_and_low_pbr1.5` | `8,208` | `413` | `5.03%` | `8.32%` | `0.51x` |
| `low_forward_per15_low_pbr1.5_mcap_le300` | `7,020` | `350` | `4.99%` | `8.48%` | `0.51x` |

#### Liquidity Residual Z

2026-05-16 follow-up で、Daily Ranking と同じ考え方の Prime `liquidity_residual_z` を追加した。EPS 1.2x positive target に対しては、high residual の単独 lift は小さい。つまり、流動性 Z は「強い次期EPS予想が出るか」よりも、その後の price return / participation state の説明に向いている。

| liquidity Z bucket | events | target rate | eligible target rate | lift | median Z |
|---|---:|---:|---:|---:|---:|
| `low` | `2,914` | `9.27%` | `14.86%` | `0.94x` | `-1.33` |
| `neutral` | `15,220` | `10.16%` | `16.65%` | `1.03x` | `-0.07` |
| `high` | `3,101` | `10.93%` | `19.65%` | `1.11x` | `+1.46` |
| `missing` | `5,830` | `8.90%` | `16.34%` | `0.90x` | `NA` |

低 forward PER と high liquidity Z の合わせ技は、EPS 1.2x positive 予測としては弱い。Prime FY の `low_forward_per15_and_high_liquidity_z` は target rate `8.06%`、lift `0.82x` で、price return target での有効性とは別物として扱う。

#### Valuation Adjustment Audit

2026-05-16 の疑義確認で、`liquidity_z_v2` までは forward PER が raw statement の per-share 値を使い、Daily Ranking と同じ share-basis adjustment / FY cycle guard になっていないことを確認した。`adjusted_valuation_v1` では発表前 as-of の最新四半期株数を baseline に EPS / BPS / forecast EPS を揃え、forecast EPS は最新FY以後の四半期・予想修正、なければそのFY forecast だけを使う。

Prime `rerating_participation` かつ `20d strong_runup` 除外では、adjusted 後も forward PER `<=10` が最多。したがって「rerating 群なのに低 forward PER が多い」現象は、株数調整ミスだけでは説明できない。

| scope | bucket | old events | adjusted events | adjusted median fPER |
|---|---:|---:|---:|---:|
| Prime non-FY | `<=10` | `1,364` | `1,288` | `3.65` |
| Prime non-FY | `missing` | `84` | `276` | `NA` |
| Prime FY | `<=10` | `554` | `520` | `3.81` |
| Prime FY | `missing` | `24` | `105` | `NA` |

adjustment 後の return でも低 forward PER bucket は残る。ただしこれは `EPS 1.2x positive` 予測の話とは逆で、price return target では「低 valuation + rerating participation」が残っている。

| entry | scope | bucket | events | mean ex20 | median ex20 | win20 | severe20 |
|---|---|---:|---:|---:|---:|---:|---:|
| post-entry | non-FY | `<=10` | `1,288` | `+1.53%` | `+1.13%` | `55.43%` | `7.76%` |
| post-entry | non-FY | `>30` | `152` | `-0.55%` | `-2.69%` | `40.13%` | `28.95%` |
| hold-through | non-FY | `<=10` | `1,288` | `+2.49%` | `+1.46%` | `56.75%` | `10.95%` |
| hold-through | non-FY | `>30` | `152` | `-0.28%` | `-2.50%` | `42.11%` | `34.21%` |

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
- valuation proxy は Daily Ranking の daily valuation semantics に寄せ、発表前 as-of の latest quarterly share baseline へ EPS / BPS / forecast EPS を調整したうえで、`PER = close / adjusted latest FY EPS`, `forward PER = close / adjusted latest forecast EPS`, `PBR = close / adjusted latest FY BPS`, `market cap = close * baseline shares_outstanding` とした。
- `stock_data.close` は local projection の adjusted close を使う。今回の修正は price adjustment そのものではなく、価格と分母EPS・BPS・株数の基準不整合を潰すもの。Daily Ranking calculator が持つ share adjustment events ベースの厳密な price-basis projection とは、古い split event でまだ残差がありうる。
- PER / PBR / market cap coverage は statements の EPS/BPS/share coverage に依存する。missing bucket は単純に投資可能 signal として読まない。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/pre_earnings_eps120_proxy.py`
- runner: `apps/bt/scripts/research/run_pre_earnings_eps120_proxy.py`
- bundle experiment id: `market-behavior/pre-earnings-eps120-proxy`
- latest result bundle: `/private/tmp/trading25-research/market-behavior/pre-earnings-eps120-proxy/20260516_pre_earnings_eps120_proxy_adjusted_valuation_v1`
- result tables: `event_feature_df`, `coverage_diagnostics_df`, `feature_bucket_df`, `threshold_grid_df`, `combo_grid_df`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_pre_earnings_eps120_proxy.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --min-events 100 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260516_pre_earnings_eps120_proxy_adjusted_valuation_v1
```

## Artifact Tables

- `event_feature_df`: event-level post-disclosure target plus pre-disclosure valuation proxies.
- `coverage_diagnostics_df`: event count, eligible target count, target rate, and proxy coverage.
- `feature_bucket_df`: PER / forward PER / PBR / market cap / liquidity residual Z bucket target rates.
- `threshold_grid_df`: single-condition target-rate grid.
- `combo_grid_df`: value-style combo target-rate grid.
