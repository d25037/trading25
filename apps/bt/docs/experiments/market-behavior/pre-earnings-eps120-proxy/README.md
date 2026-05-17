# Pre-Earnings EPS 1.2x Proxy

## Published Readout

### Decision

`pre_earnings_eps120_proxy` は、発表前までに観測できる Daily Ranking 系 valuation proxy が「FY 発表で `positive event` かつ `next_year_forecast_eps >= actual_eps * 1.2`」を予測できるかを見る research として追加する。

今回の `daily_valuation` SoT rerun では、Prime FY において `EPS 1.2x positive` は全体 `9.88%`、対象 EPS が揃う eligible event 内では `16.71%`。低PER・低forward PER・低PBR・小型は発生率を上げず、むしろ下げる。最も強い単独 proxy は `forward PER >= 30` で、target rate `25.68%`、base 比 `2.60x`。

これは「割安だから EPS 1.2x positive が出やすい」ではなく、「発表前から高い forward PER / PER を許容されている銘柄ほど、FY で強い次期EPS予想が出やすい」という結果。Daily Ranking の value 系条件をそのまま proxy にするより、期待成長・高評価を許容されている状態を別 regime として扱う方が自然。

この readout は bundle `/private/tmp/trading25-research/market-behavior/pre-earnings-eps120-proxy/20260516_pre_earnings_eps120_proxy_temporal_cross_section_v1` に基づく。入力 DB は `~/.local/share/trading25/market-timeseries/market.duckdb`、対象 event は `2016-04-01` から `2026-05-14`。current cross-section は Daily Ranking 相当の latest trading date `2026-05-15`。

### Main Findings

#### 結論

Prime FY event は `27,065` 件。`daily_valuation` SoT は `96.06%` の event に入った。発表前 valuation coverage は forward PER `80.38%`、PER `68.08%`、market cap `75.01%`。PBR は新SoTの BPS coverage に従うため `24.18%` まで下がり、PBR 単独の解釈は旧readoutより弱める。

| scope | events | eligible | target | target rate | eligible target rate | daily valuation source | forward PER coverage | PER coverage | PBR coverage |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Prime FY | `27,065` | `16,007` | `2,675` | `9.88%` | `16.71%` | `96.06%` | `80.38%` | `68.08%` | `24.18%` |

#### Forward PER

forward PER は明確に monotonic。低forward PERは target 発生率が低く、高forward PERほど `EPS 1.2x positive` が出やすい。

| forward PER bucket | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `<=10` | `4,641` | `162` | `3.49%` | `5.33%` | `0.35x` |
| `10-15` | `5,987` | `436` | `7.28%` | `10.86%` | `0.74x` |
| `15-20` | `3,860` | `449` | `11.63%` | `17.16%` | `1.18x` |
| `20-30` | `3,533` | `581` | `16.44%` | `24.98%` | `1.66x` |
| `>30` | `3,734` | `959` | `25.68%` | `44.32%` | `2.60x` |

#### PER / PBR / Market Cap

PER も高い側で lift が出る。PBR は `>2.0` で小幅 positive、低PBRは negative。market cap は単独では弱く、小型も大型も target 発生率を上げない。

| condition | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `forward_per >= 30` | `3,734` | `959` | `25.68%` | `44.32%` | `2.60x` |
| `forward_per >= 20` | `7,268` | `1,540` | `21.19%` | `34.30%` | `2.14x` |
| `per >= 30` | `5,586` | `843` | `15.09%` | `30.83%` | `1.53x` |
| `per >= 20` | `9,529` | `1,238` | `12.99%` | `23.46%` | `1.31x` |
| `pbr >= 2.0` | `1,572` | `118` | `7.51%` | `13.44%` | `0.76x` |
| `market_cap >= 1000bn` | `1,621` | `127` | `7.83%` | `12.25%` | `0.79x` |
| `market_cap <= 300bn` | `16,282` | `1,406` | `8.64%` | `16.17%` | `0.87x` |

#### Value Combos

Daily Ranking / value-composite 的な低forward PER・低PBR・小型 combo は、`EPS 1.2x positive` の予測には効かない。むしろ base を下回る。

| condition | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `low_pbr1.0_and_mcap_le300` | `2,755` | `205` | `7.44%` | `14.77%` | `0.75x` |
| `low_forward_per15_and_mcap_le300` | `7,480` | `369` | `4.93%` | `8.03%` | `0.50x` |
| `low_forward_per15_and_low_pbr1.5` | `2,333` | `105` | `4.50%` | `7.09%` | `0.46x` |
| `low_forward_per15_low_pbr1.5_mcap_le300` | `2,093` | `98` | `4.68%` | `7.52%` | `0.47x` |

`20d return >= +30%` の `overheat` は、EPS 1.2x target の単独 proxy ではない。Prime FY `overheat` は target rate `9.76%`、base 比 `0.99x`。eligible 内 rate は `21.24%` と高めだが、event rate では base を上回らないため、「強いEPS予想が出やすい状態」ではなく、price return 側の risk state として扱う。

| condition | events | target | target rate | eligible target rate | lift |
|---|---:|---:|---:|---:|---:|
| `overheat_20d_ge30` | `246` | `24` | `9.76%` | `21.24%` | `0.99x` |
| `low_forward_per15_and_not_overheat` | `10,572` | `597` | `5.65%` | `8.50%` | `0.57x` |

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

#### Historical vs Current Cross-Section

2026-05-16 follow-up で、pooled historical result と current Daily Ranking cross-section を混ぜて読まないため、bundle に `annual_valuation_regime_df` と `current_cross_section_df` を追加した。

Prime non-FY の `rerating_participation` かつ `20d strong_runup` 除外では、historical pooled では `forward PER <=10` が多い。ただし年別に見ると構成は大きく変わっており、2026年は `<=10` が `22.99%` まで低下し、`>30` が `21.84%` まで上がっている。

| year | total | `<=10` events | `<=10` share | `>30` events | `>30` share | missing |
|---:|---:|---:|---:|---:|---:|---:|
| `2017` | `304` | `189` | `62.17%` | `11` | `3.62%` | `25` |
| `2022` | `206` | `148` | `71.84%` | `11` | `5.34%` | `25` |
| `2023` | `255` | `182` | `71.37%` | `12` | `4.71%` | `28` |
| `2025` | `280` | `145` | `51.79%` | `33` | `11.79%` | `36` |
| `2026` | `87` | `20` | `22.99%` | `19` | `21.84%` | `6` |

current Daily Ranking cross-section では、Prime `rerating_participation` は `71` 銘柄、そのうち `forward PER <=10` は `2` 銘柄だけ。したがって「今日の rerating 群に低 forward PER がほぼない」という観察と、historical event pooled で低 forward PER が多いという観察は矛盾しない。

今回の overheat 導入後の current cross-section では、Prime `rerating_participation` `71` 銘柄のうち `overheat` は `18` 銘柄。その `18` 銘柄中 `forward PER >30` が `12` 銘柄で、current の rerating + overheat は高 forward PER 側にかなり偏る。一方で historical pooled event は古い valuation regime を含むため、この current 構成を過去全期間へ外挿しない。

| snapshot | regime | bucket | stocks | share within regime | median fPER |
|---|---|---:|---:|---:|---:|
| `2026-05-15` | `rerating_participation` | `<=10` | `2` | `2.82%` | `9.34` |
| `2026-05-15` | `rerating_participation` | `10-15` | `9` | `12.68%` | `13.80` |
| `2026-05-15` | `rerating_participation` | `15-20` | `14` | `19.72%` | `16.57` |
| `2026-05-15` | `rerating_participation` | `20-30` | `11` | `15.49%` | `23.10` |
| `2026-05-15` | `rerating_participation` | `>30` | `24` | `33.80%` | `48.12` |
| `2026-05-15` | `rerating_participation` | `missing` | `11` | `15.49%` | `NA` |

### Interpretation

`EPS 1.2x positive` は、value factor ではなく growth-expectation factor に近い。低forward PER は「割安」ではあるが、強い次期EPS予想が新たに出る状態を予測する proxy にはなっていない。

一方、発表前 forward PER が高い銘柄は、すでに市場が高い成長期待を払っている状態で、その期待をFY予想が裏付けるケースが多い。これは「高forward PERを買えば良い」ではなく、「EPS 1.2x positive の事前発生確率を上げる proxy は value ではなく期待成長側」という意味。

market cap は単独では弱い。小型は target rate を上げず、大型も上げない。サイズより、発表前にどれだけ高い forward PER / PER を許容されているかの方が説明力がある。

pooled historical result は、時代ごとの valuation 水準や流動性 regime の構成変化を平均してしまう。特に `rerating_participation` と forward PER の関係は、current cross-section と必ずしも同じではない。今後この系統の readout では、pooled table だけでなく `annual_valuation_regime_df` と `current_cross_section_df` を併記する。

### Production Implication

この Phase 1 だけで production strategy に entry rule を追加しない。

次に見る価値がある候補は以下。

| 用途 | 候補 |
|---|---|
| EPS 1.2x positive 事前 proxy | `forward_per >= 20` / `forward_per >= 30` |
| Avoid proxy | `forward_per <= 15`、低PBR・小型 value combo |
| Ranking integration | value ranking とは別に growth-expectation regime として扱う |
| Next validation | `forward_per >= 20/30` を hold-through / post-entry return と no-fill rate に接続し、年度 split / current cross-section / runup / ADV/FF を重ねる |

#### P/OP Follow-Up

2026-05-17 follow-up で、DB SoT化前の研究内派生指標として `P/OP = market cap / operating_profit` と `forward P/OP = market cap / forecast_operating_profit` を追加した。これは `PER = market cap / profit`、`forward PER = market cap / forecast profit` と同じ構造で、EPSやforward EPSのper-share表現を時価総額/利益の形へ戻したもの。

単独の低 `forward P/OP` は、低 `forward PER` の完全な上位互換ではない。FY eventのEPS 1.2x proxyでは、むしろ高 `forward PER` の方が強いtarget proxyとして残る。一方で、`forward PER` が低いのに `forward P/OP` が相対的に高い銘柄は、「EPSは安く見えるが営業利益ベースでは安くない」状態を拾いやすく、特益・営業外・税要因を含む見かけの低PERを疑う品質フィルタとして価値がある。

FY event全体では、`forward_per <= 15` かつ `forward_p_op >= 20` は `141` events / `103` codesで、EPS120 eligible target rate `3.57%`。`forward_per <= 15` 全体の `8.32%` より大きく悪化した。post-entry / hold-throughでも同条件は20d平均・中央値が悪化し、低forward PERの質を落とす警戒bucketとして読める。

rerating participationかつoverheat除外に絞ると母数はさらに小さいが、方向は維持された。`forward_per <= 20` では `171` events、`forward_p_op >= 20` まで重ねると `6` eventsに留まる。したがって決算系では、現時点では強いhard excludeではなく、`low forward PER` を読む際の注意・減点signalとして扱う。

### Caveats

- target は発表後 outcome: `is_fy=true`, `event_strength=positive`, `actual_eps > 0`, `next_year_forecast_eps >= actual_eps * 1.2`。
- proxy は発表前営業日の close と、発表前までに開示済みの latest FY / latest forecast EPS だけで計算する。発表当日の FY row は valuation proxy に使わない。
- valuation proxy は Daily Ranking の daily valuation semantics に寄せ、発表前 as-of の latest quarterly share baseline へ EPS / BPS / forecast EPS を調整したうえで、`PER = close / adjusted latest FY EPS`, `forward PER = close / adjusted latest forecast EPS`, `PBR = close / adjusted latest FY BPS`, `market cap = close * baseline shares_outstanding` とした。
- `P/OP` / `forward P/OP` はこのfollow-up時点ではDB SoTではなく、研究内でstatementsの営業利益・予想営業利益とpre-event時価総額から派生した。
- `stock_data.close` は local projection の adjusted close を使う。今回の修正は price adjustment そのものではなく、価格と分母EPS・BPS・株数の基準不整合を潰すもの。Daily Ranking calculator が持つ share adjustment events ベースの厳密な price-basis projection とは、古い split event でまだ残差がありうる。
- PER / PBR / market cap coverage は statements の EPS/BPS/share coverage に依存する。missing bucket は単純に投資可能 signal として読まない。
- `annual_valuation_regime_df` は historical event panel、`current_cross_section_df` は latest Daily Ranking cross-section で、母集団が異なる。pooled historical result をそのまま今日の銘柄断面の説明に使わない。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/pre_earnings_eps120_proxy.py`
- runner: `apps/bt/scripts/research/run_pre_earnings_eps120_proxy.py`
- bundle experiment id: `market-behavior/pre-earnings-eps120-proxy`
- latest result bundle: `~/.local/share/trading25/research/market-behavior/pre-earnings-eps120-proxy/20260516_overheat_adjusted_sot`
- result tables: `event_feature_df`, `coverage_diagnostics_df`, `feature_bucket_df`, `threshold_grid_df`, `combo_grid_df`, `annual_valuation_regime_df`, `current_cross_section_df`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_pre_earnings_eps120_proxy.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --min-events 100 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260516_pre_earnings_eps120_proxy_temporal_cross_section_v1
```

## Artifact Tables

- `event_feature_df`: event-level post-disclosure target plus pre-disclosure valuation proxies.
- `coverage_diagnostics_df`: event count, eligible target count, target rate, and proxy coverage.
- `feature_bucket_df`: PER / forward PER / P/OP / forward P/OP / PBR / market cap / liquidity residual Z bucket target rates.
- `threshold_grid_df`: single-condition target-rate grid.
- `combo_grid_df`: value-style combo target-rate grid.
- `annual_valuation_regime_df`: event-year x liquidity regime x forward PER bucket trend table, with `all_events` and `ex_20d_strong_runup` scopes.
- `current_cross_section_df`: latest Daily Ranking Prime cross-section by collection, liquidity regime, and forward PER bucket.
