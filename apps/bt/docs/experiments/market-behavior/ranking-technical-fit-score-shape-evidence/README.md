# Ranking Technical Fit Score Shape Evidence

## Published Readout

### Decision

**最終判断: `neither` — fixed 20D/60D endpoint return と OLS fitted move のどちらも勝者ではなく、Technical Fit Score を Ranking に導入しない。**

- Published run: `20260718_prime_pit_technical_fit_shape_v1`

`decision_gate` 最終状態: `neither`。fixed と OLS はともに十分な sample を持つが `fails_adoption_gate` だった。20D primary の `shape_classification` は、全期間では `core_high_high` / `near_high_high_1` / `near_high_high_2` の両 family ですべて `unstable_shape` であり、interior sweet spot はどの ring でも確認できなかった。2017–2021 学習時の union mapping も fixed は `q1`、OLS は `q5` が最大で、事前に想定しなかった境界 bin だった。

Ranking の Value Score と Long Hybrid Score がともに高い候補に第三の high-is-good score を加える根拠はない。`20D<0` は非負群より概して弱い caution 診断、fixed `20D>=30%` overheat は薄い tail 診断として残すが、candidate ring、Fit mapping、primary gate、hard exclude の条件には使わない。

本研究は signal close 確定後にだけ利用できる observation-level forward return 研究であり、portfolio backtest、売買コスト、capacity、execution を示さない。2024+ は hypothesis-origin の walk-forward evidence で、clean holdout ではない。

### Main Findings

#### 結論: sample と PIT contract は満たしたが、両 family とも adoption gate を通らなかった

対象は exact signal-date Prime-equivalent membership のみで、市場再編前 `0101` と再編後 `0111` を使った。Standard / Growth は sample、集計、gate に含めていない。Value Score と Long Hybrid Score だけで三つの mutually exclusive ring を先に materialize し、その後に Prime-wide fixed / OLS percentile と forward outcome を結合した。

| Ring | Predicate | Observations | Dates | Median candidates/date | Completed 20D coverage |
| --- | --- | ---: | ---: | ---: | ---: |
| `core_high_high` | Value `>=0.8` and Long Hybrid `>=0.8` | 43,554 | 1,898 | 13 | 98.868% |
| `near_high_high_1` | both `>=0.7`, core を除外 | 129,358 | 1,937 | 61 | 99.018% |
| `near_high_high_2` | both `>=0.6`,上位2 ring を除外 | 260,970 | 2,108 | 122 | 99.195% |

全 433,882 observations の coverage aggregate は `0101,0111` のみ。20D OOS は十分に成立したが、fixed / OLS とも必要な ring replication、positive CI、IC、period stability を同時に満たさなかった。

| Family | Gate decision | Sufficient sample | Passed |
| --- | --- | --- | --- |
| fixed equal | `fails_adoption_gate` | yes | no |
| OLS equal | `fails_adoption_gate` | yes | no |
| fixed vs OLS | `neither` | yes | no |

#### Published Artifact Contract

この table は publication test が durable `results.duckdb` を直接開いて照合する machine-readable contract である。

| key | published_value |
| --- | ---: |
| `observation_count` | `433882` |
| `fixed_core_oos_mean_lift_pct` | `0.2279` |
| `ols_core_oos_mean_lift_pct` | `0.3741` |
| `near1_fixed_minus_ols_mean_lift_pct` | `-0.2566` |
| `fixed_top5_mean_lift_pct` | `0.0148` |
| `ols_top5_mean_lift_pct` | `0.4707` |

#### 結論: 2017–2021 の raw five-bin shape は非線形だが不安定で、interior sweet spot ではない

raw level は Prime 全銘柄で percentile 化し、`q1=[0,.2)`、`q2=[.2,.4)`、`q3=[.4,.6)`、`q4=[.6,.8)`、`q5=[.8,1]` に固定した。下表は 2017–2021 の20D date-equal mean TOPIX-excess returnで、括弧内は severe-loss rate。すべて `%`。

| Family | Ring | q1 | q2 | q3 | q4 | q5 | Shape |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| fixed equal | `core_high_high` | +0.700 (5.805) | +0.906 (4.287) | +0.479 (6.060) | +0.731 (7.569) | +0.727 (8.818) | `unstable_shape` |
| fixed equal | `near_high_high_1` | +0.176 (6.282) | -0.191 (4.773) | -0.133 (5.235) | -0.335 (6.639) | +0.798 (10.657) | `unstable_shape` |
| fixed equal | `near_high_high_2` | +0.208 (6.474) | +0.251 (4.831) | +0.002 (5.387) | -0.643 (7.774) | -0.331 (10.894) | `unstable_shape` |
| OLS equal | `core_high_high` | +0.905 (6.347) | +0.727 (5.310) | +0.801 (5.486) | +0.737 (6.088) | +0.066 (10.808) | `unstable_shape` |
| OLS equal | `near_high_high_1` | +0.161 (6.177) | -0.087 (5.005) | -0.161 (5.342) | -0.290 (6.709) | +1.002 (10.086) | `unstable_shape` |
| OLS equal | `near_high_high_2` | +0.248 (6.105) | +0.223 (4.912) | -0.073 (5.788) | -0.522 (7.442) | -0.181 (10.730) | `unstable_shape` |

最初の 2022 evaluation mapping は三つの ring の union で学習した。全 bin は pre-registered minimum の200 observations / 50 datesを大幅に超え、training signal end は 2021-12-02、20D outcome completion end は 2021-12-30 だった。

| Family | q1 expectancy / Fit | q2 | q3 | q4 | q5 | Learned best |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| fixed equal | +0.135 / 1.000 | +0.068 / 0.903 | +0.022 / 0.838 | -0.564 / 0.000 | +0.003 / 0.811 | boundary `q1` |
| OLS equal | +0.138 / 0.955 | +0.082 / 0.859 | -0.081 / 0.583 | -0.425 / 0.000 | +0.165 / 1.000 | boundary `q5` |

raw shape の period classification は次のとおり。OLS `near_high_high_1` の 2024+ だけは `monotonic` になったが、2017–2021 / 2022–2023 と一致せず、`core_high_high` と別 near ring に再現しないため sweet spot を救済しない。

| Family | Ring | 2017–2021 training | 2022–2023 walk-forward | 2024+ hypothesis-origin |
| --- | --- | --- | --- | --- |
| fixed equal | `core_high_high` | `unstable_shape` | `unstable_shape` | `unstable_shape` |
| fixed equal | `near_high_high_1` | `unstable_shape` | `unstable_shape` | `unstable_shape` |
| fixed equal | `near_high_high_2` | `unstable_shape` | `unstable_shape` | `unstable_shape` |
| OLS equal | `core_high_high` | `unstable_shape` | `unstable_shape` | `unstable_shape` |
| OLS equal | `near_high_high_1` | `unstable_shape` | `unstable_shape` | `monotonic` |
| OLS equal | `near_high_high_2` | `unstable_shape` | `unstable_shape` | `unstable_shape` |

#### 結論: 2022–2023 と 2024+ の OOS Fit lift / IC は ring 間で再現しない

20D primary では毎 signal date に候補10銘柄以上、top / bottom 30% を各3銘柄以上要求した。下表の lift は date-equal top-minus-bottom mean、IC は daily Spearman の median、positive は IC-positive dates、severe は top-minus-bottom severe-loss rate difference。

| Family | Ring | Period | Dates | Mean lift | Median IC | IC positive | Severe diff |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| fixed | `core_high_high` | 2022–2023 | 348 | -0.058 pp | +0.0536 | 59.20% | -1.314 pp |
| fixed | `core_high_high` | 2024+ | 470 | +0.440 pp | +0.0089 | 52.77% | +1.135 pp |
| fixed | `near_high_high_1` | 2022–2023 | 489 | -0.149 pp | +0.0232 | 56.65% | -0.278 pp |
| fixed | `near_high_high_1` | 2024+ | 597 | +0.160 pp | -0.0172 | 45.23% | -0.136 pp |
| fixed | `near_high_high_2` | 2022–2023 | 490 | -0.168 pp | +0.0019 | 51.22% | +0.263 pp |
| fixed | `near_high_high_2` | 2024+ | 598 | -0.013 pp | -0.0132 | 45.48% | -0.430 pp |
| OLS | `core_high_high` | 2022–2023 | 348 | +0.217 pp | +0.0590 | 62.36% | +1.325 pp |
| OLS | `core_high_high` | 2024+ | 470 | +0.491 pp | +0.0388 | 58.09% | +1.460 pp |
| OLS | `near_high_high_1` | 2022–2023 | 489 | +0.003 pp | -0.0042 | 48.67% | +0.966 pp |
| OLS | `near_high_high_1` | 2024+ | 597 | +0.503 pp | +0.0072 | 51.59% | +0.064 pp |
| OLS | `near_high_high_2` | 2022–2023 | 490 | -0.262 pp | -0.0135 | 44.08% | +1.453 pp |
| OLS | `near_high_high_2` | 2024+ | 598 | -0.134 pp | -0.0188 | 42.31% | +0.066 pp |

全 OOS の 2,000-resample moving-block bootstrap でも CI lower bound は全 ring で0以下だった。fixed は core の point estimate が adoption threshold `+0.25 pp` に届かず、near rings も非正またはほぼゼロ。OLS は core / near1 の point estimateが threshold を超えたが CI が0を跨ぎ、near1 の IC は弱く、near2 は負、core severe deterioration は `+1.402 pp` で許容上限 `+1.0 pp` を超えた。

| Family | Ring | OOS dates | Mean lift | 95% CI | Median IC | IC positive | Severe diff |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: |
| fixed | `core_high_high` | 818 | +0.228 pp | [-0.539, +1.044] | +0.0274 | 55.50% | +0.093 pp |
| fixed | `near_high_high_1` | 1,086 | +0.021 pp | [-0.455, +0.479] | +0.0014 | 50.37% | -0.200 pp |
| fixed | `near_high_high_2` | 1,088 | -0.082 pp | [-0.390, +0.238] | -0.0070 | 48.07% | -0.118 pp |
| OLS | `core_high_high` | 818 | +0.374 pp | [-0.437, +1.211] | +0.0493 | 59.90% | +1.402 pp |
| OLS | `near_high_high_1` | 1,086 | +0.278 pp | [-0.161, +0.729] | +0.0030 | 50.28% | +0.470 pp |
| OLS | `near_high_high_2` | 1,088 | -0.191 pp | [-0.524, +0.142] | -0.0176 | 43.11% | +0.691 pp |

#### 結論: paired fixed-minus-OLS は near1 だけ OLS 優位だが、family-level winner にはできない

同一 eligible date で `fixed Fit lift - OLS Fit lift` を比較した。`near_high_high_1` は CI upper も負で OLS 優位だが、core は同方向でもCIが0を跨ぎ、`near_high_high_2` は fixed方向でCIが0を跨いだ。さらに両 family とも adoption gate を失敗しているため、局所的な OLS 優位を `ols_wins` に昇格させない。

| Ring | All-OOS fixed-minus-OLS | 95% CI | 2022–2023 | 2024+ | Reading |
| --- | ---: | --- | ---: | ---: | --- |
| `core_high_high` | -0.146 pp | [-0.635, +0.377] | -0.275 pp | -0.051 pp | OLS方向、not significant |
| `near_high_high_1` | -0.257 pp | [-0.534, -0.002] | -0.152 pp | -0.343 pp | OLS優位 |
| `near_high_high_2` | +0.109 pp | [-0.077, +0.310] | +0.094 pp | +0.121 pp | fixed方向、not significant |

#### 結論: combined Top 5 / Top 10 の point estimate は正でも downside と concentration が悪化した

三つの ring の union を Fit Score 順に Top 5 / Top 10 とし、eligible basket と比較した。fixed は全 OOS でほぼゼロ、OLS は正だが 2022–2023 の Top 10 は負で、全 CI が0を跨いだ。すべて severe-loss rate と sector HHI が悪化し、Top-K point estimate 単独で failed ring gate を救済できない。

| Family | K | Dates | All-OOS lift | 95% CI | 2022–2023 | 2024+ | Severe diff | Sector HHI diff | Turnover |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| fixed | 5 | 1,088 | +0.015 pp | [-0.665, +0.677] | -0.171 pp | +0.167 pp | +2.238 pp | +0.199 | 31.26% |
| fixed | 10 | 1,088 | +0.009 pp | [-0.555, +0.599] | -0.065 pp | +0.070 pp | +1.264 pp | +0.110 | 27.97% |
| OLS | 5 | 1,088 | +0.471 pp | [-0.272, +1.239] | +0.065 pp | +0.803 pp | +3.323 pp | +0.182 | 21.73% |
| OLS | 10 | 1,088 | +0.437 pp | [-0.160, +1.050] | -0.010 pp | +0.803 pp | +2.643 pp | +0.126 | 18.37% |

#### 結論: `20D<0` は caution、overheat は sparse diagnostic のまま扱う

下表は family 共通の fixed-return diagnostic で、20D forward TOPIX-excess mean。`20D<0` は三 ring とも nonnegative より弱いが、deep pullback は ring 間で符号が揃わない。overheat は208–505 observationsと薄く、core / near2 では mean が高い一方 near1 では低い。どちらも pre-registered primary mapping を変更する根拠にはしない。

| Ring | `20D>=0` | `-10%<20D<0` | `20D<=-10%` | overheat `20D>=30%` |
| --- | ---: | ---: | ---: | ---: |
| `core_high_high` | +1.602% (23,710) | +0.070% (10,472) | +0.131% (2,216) | +1.919% (208) |
| `near_high_high_1` | +0.955% (43,745) | +0.243% (25,126) | -0.380% (3,355) | +0.168% (362) |
| `near_high_high_2` | +0.634% (74,902) | -0.027% (46,142) | +0.099% (6,130) | +1.536% (505) |

### Interpretation

nonlinear mapping を prior-only で学習しても、第三の high-is-good Technical Fit Score は ring を跨いで安定しなかった。fixed は2017–2021に低 raw levelを高く評価し、OLSは最上位 raw binを高く評価したが、いずれも raw response は mountain shape ではなく `unstable_shape`。2022–2023 と 2024+ では fixed の符号が多くの ring で反転し、OLSも core と near1 の point estimateに対して near2 が一貫して負だった。

OLSは `near_high_high_1` の paired comparison ではfixedより良い。しかしその優位は全 ring に再現せず、OLS自身も CI、near-ring IC、severe-loss gateを満たさない。したがって「OLSがfixedより統計的に優れている」でも「simplerなfixedを同等時に運用優先する」でもなく、正式状態は `neither` である。

`20D<0` は優先度を下げる review lens としては整合的だが、deep pullback の符号が揃わず hard exclude にはできない。overheat は sample が薄く右尾も残るため、急騰後riskの注意表示を超える意味を与えない。

### Data Plane / PIT Lineage

v1 は physical `market.duckdb` schema v4 と `stock_price_adjustment_mode=local_projection_v2_event_time` を要求し、universe は `stock_master_daily` exact signal-date `0101/0111`、no latest membership fallback で構築した。一方、v1 artifact は basis-dependent source `daily_valuation` の cutoff-valid `basis_id` 一覧と、`stock_adjustment_bases` / `stock_adjustment_basis_segments` に対する全 consumed Prime rows の照合結果を永続化していない。

したがって v1 の invalidation disposition は `historical_archive` とし、production / Ranking / Screening evidence には使用しない。v1 headline は provenance のため保持するが、event-time basis audit を fail-closed で通した v2 により provenance-only で supersede する。v2 は missing materialization、basis mismatch、segment missing/overlap を拒否し、`daily_valuation` を service-local 再計算または latest/current basis fallback で補完しない。

### Production Implication

- Ranking に `Technical Fit Score` column、badge、sort key、API field、materialization、UI を追加しない。
- fixed 20D/60D と OLS fitted move のどちらも第三 score として採用しない。component-only や single-ring の好結果で equal-weight primary failure を置換しない。
- 既存 fixed 20D/60D は informational / diagnostic field として扱えるが、本研究を priority score の有効性根拠にしない。
- `20D<0` は caution / review lens、overheat は sparse risk diagnostic のままにし、candidate population や primary gate を変更しない。
- 将来別定義を検証する場合は、この v1 artifact や gate を上書きせず、別の承認済み research design と versioned run id を使う。production 導入には別途承認済み implementation design が必要。

### Caveats

- signal date `X` の close を含む after-close feature であり、pre-open decidability は未検証。
- outcome は 5D / 20D / 60D close-to-close excess return。primary は20D TOPIX-excessで、5Dはentry timing、60Dはhold diagnostic。
- observation-level studyであり、portfolio construction、position sizing、turnover cost、slippage、capacity、executionを含まない。
- 2024+ は walk-forward だが hypothesis-origin period で、clean holdout / full OOS proof ではない。2026年は完了 horizon までのpartial year。
- Top-K は同日候補の相対比較で、severe-loss と sector concentration が悪化した。portfolio採用を示さない。
- exact-date Prime `0101/0111` だけの結果で、Standard / Growth や他市場へ外挿しない。
- manifest の `git_dirty=true` は Task 5 のpublication RED testと既存scratch reportがrun時に未commitだったため。runner/module の provenance commit は `7951e658723a0f9bb8f206187b87d597e20a31f4` で、artifact はrun後に変更していない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_technical_fit_score_shape_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`
- Durable bundle: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260718_prime_pit_technical_fit_shape_v1/`
- Manifest: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260718_prime_pit_technical_fit_shape_v1/manifest.json`
- Results: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260718_prime_pit_technical_fit_shape_v1/results.duckdb`
- Summary: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260718_prime_pit_technical_fit_shape_v1/summary.md`
- Bundle tables: `ring_registry`, `raw_score_registry`, `coverage_attrition`, `raw_shape_daily`, `raw_shape_summary`, `walkforward_mapping`, `oos_fit_score_lift`, `fixed_vs_ols_paired`, `topk_operational_lift`, `overheat_negative_diagnostics`, `segment_stability`, `annual_stability`, `bootstrap_effect_ci`, `decision_gate`, `observation_sample`

Validation: exact 15 non-empty tables、Prime-only sample、2022–2026 mapping の prior-year signal/outcome completion cutoff、same-date fixed/OLS pairing、5D `2026-07-08` / 20D `2026-06-17` / 60D `2026-04-16` completion cutoffs、`summary.md` と `decision_gate` の `neither` 一致を確認した。

Reproduce:

```bash
uv run --directory apps/bt python \
  scripts/research/run_ranking_technical_fit_score_shape_evidence.py \
  --db-path ~/.local/share/trading25/market-timeseries/market.duckdb \
  --start-date 2017-01-01 \
  --horizons 5,20,60 \
  --min-training-observations 200 \
  --min-training-dates 50 \
  --bootstrap-resamples 2000 \
  --run-id 20260718_prime_pit_technical_fit_shape_v1
```
