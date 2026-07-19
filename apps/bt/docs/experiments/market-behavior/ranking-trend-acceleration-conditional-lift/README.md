# Ranking Trend Acceleration Conditional Lift

## Published Readout

### Decision

- Ranking に continuous 列を追加しない。
- 3条件 badge も追加しない。
- 2つの独立した long 候補群で結果が再現せず、既存の fixed `20D/60D` とすべての production Ranking surface は変更しない。

`priceLrSlope20Pct`、`priceLrSlope60Pct`、`trendAccelerationMarginPct` と `trend_acceleration_triple` badge は不採用とする。正式な判定は `reject_introduction` である。候補抽出、API、materialization、UI を含む production Ranking は変更しない。

PR 480 publication review後のv7 rerunでは、銘柄固有の実completion dateでshared panelが計算したN225 excessを、5D / 20D / 60Dすべてで最終observation bundleにも透過した。v6の計算値自体はcompletion-alignedだったが、sample artifactからN225列が欠落していた。overlap membership、event-time adjustment frontier、Top-K outcome coverageとfrozen gateは維持している。continuous `1 / 7`、binary `0 / 7`、`reject_introduction`という最終判断は変わらない。

### Why This Research Was Run

この研究は、既存情報で選ばれた Daily Ranking long candidate の内側で、短期・長期 trend acceleration が forward TOPIX-excess return の追加的な優先順位情報になるかを検証した。rolling log-price OLS feature は母集団を選ぶためには使わず、既存の valuation、liquidity、ATR、fixed `20D/60D` momentum で固定した候補群を順位付けするためだけに使った。

### Data Scope / PIT Assumptions

対象は signal date `X` の `stock_master_daily` exact-date membership で解決した Prime 相当 universe のみである。市場再編前は `0101`（東証一部）、再編後は `0111`（Prime）を使い、Standard / Growth は含めない。candidate membership、market membership、valuation、liquidity、sector、ATR、endpoint return、OLS feature はすべて `X` close 時点までの情報で解決した。forward outcome は `X` 後から始まるため、これは after-close の研究であり pre-open 利用は対象外である。

価格の物理 SoT は `stock_data_raw` であり、`stock_data` fallback は行わない。signal feature は exact signal-date `basis_id` を complete lookback 全体へ適用し、forward outcome は exact completion-date `basis_id` を signal/completion 両 endpoint へ適用した。旧 v2 bundle は convenience price lineage、v3 bundle は generated summary の outcome-request count 欠落、v4 bundle は review前の overlap/outcome-frontier contract、v5 bundle は consumerがauthoritative completion date / TOPIX excessを捨ててnominal leadを再計算していたため、それぞれ immutable archive として保持しつつ canonical publication から supersede する。v6はshared panel内のN225計算を最終observation artifactへ保存しなかったためsupersedeし、以下のv7をpublication SoTとする。

primary outcome は `20D` close-to-close TOPIX-excess return、補助 horizon は `5D` と `60D` である。未完了の forward window は効果集計から除外する一方、Top-Kでは`outcome_status=incomplete_outcomes`の監査行として保持する。最新 complete signal date `2026-07-08` は signal availability の上限であり、各 comparison の最新 paired date ではない。最新 paired date は horizon、comparison、family ごとに異なる。

### What Was Compared

| Reader-facing label | Definition |
| --- | --- |
| Continuous ordering | `slope20 - slope60` で候補を順位付けする。 |
| Three-condition badge | `slope20 > 0`、`slope60 > 0`、かつ `slope20 > slope60`。 |
| Independent long groups | `core_long_only` と `momentum_value_only`。 |
| Primary outcome | `20D` close-to-close TOPIX-excess return。 |

`core_long_only` は既存の core long 条件を満たし、他の primary group との重複を除いた独立 slice である。`momentum_value_only` は既存の value と fixed `20D/60D` momentum 条件を満たし、同じく重複を除いた独立 slice である。独立 family の再現性だけを production 導入の根拠に数え、overlap、broad sensitivity、nested `earnings_priority` は独立成功として数えない。

### Main Findings

#### 連続順位は2つの long 候補群で改善しなかった

2017-2023 の historical replication では、continuous margin の 20D top-minus-bottom lift は両方の独立 family で負だった。`core_long_only` の median IC は `+0.0460`、IC 正の日の割合は `58.33%` だったが、ordering の実際の lift は改善を示さなかった。

| Independent family | Eligible dates | Observations | Mean 20D lift (pp) | 95% moving-block bootstrap CI (pp) | Result |
| --- | ---: | ---: | ---: | --- | --- |
| `core_long_only` | 24 | 516 | `-1.1282` | `[-2.5242, +0.2158]` | 改善せず |
| `momentum_value_only` | 71 | 1,791 | `-1.9916` | `[-4.4568, -0.1151]` | 改善せず |

`momentum_value_only` の median IC は `-0.0417`、IC 正の日の割合は `42.25%` だった。

#### 3条件 badge は片方だけ良かったが、採用条件を満たさなかった

`momentum_value_only` は median lift と positive-date rate が正だった。しかし 95% CI はゼロを跨ぎ、median badge candidates は `3`（必要な `5` 未満）で、もう一方の独立 family には再現しなかった。

| Independent family | Paired dates | Observations | Median 20D lift (pp) | Mean 20D lift (pp) | Positive-date rate | 95% moving-block bootstrap CI (pp) | Median badge candidates | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |
| `core_long_only` | 250 | 2,485 | `-0.9082` | `-0.5492` | `42.00%` | `[-1.4953, +0.4093]` | 3 | 改善せず |
| `momentum_value_only` | 352 | 4,054 | `+0.7512` | `+0.6839` | `55.68%` | `[-0.2600, +1.6086]` | 3 | 一部改善だが不十分 |

#### Top-K に絞っても改善は確認できなかった

Operational Top-K lens の all-available primary 20D 結果でも、4行すべての 95% CI はゼロを跨いだ。

| Independent family | K | Paired dates | Mean priority lift (pp) | 95% moving-block bootstrap CI (pp) |
| --- | ---: | ---: | ---: | --- |
| `core_long_only` | 5 | 240 | `+0.2537` | `[-0.1821, +0.6798]` |
| `core_long_only` | 10 | 47 | `+0.3192` | `[-0.2316, +0.8272]` |
| `momentum_value_only` | 5 | 433 | `+0.0130` | `[-0.6395, +0.6565]` |
| `momentum_value_only` | 10 | 72 | `-0.4741` | `[-1.2636, +0.2486]` |

#### 採用条件を総合すると導入できない

採用には、個別に良く見える結果ではなく、独立 family 間で同じ条件を満たすことを求めた。

| Requirement | Continuous ordering | Three-condition badge |
| --- | --- | --- |
| 2つの独立 family で正方向に再現 | 両 family の historical lift が負で不通過 | `momentum_value_only` だけが正で不通過 |
| 95% CI の下限が正 | `core_long_only` はゼロを跨ぎ、`momentum_value_only` は負で不通過 | 両 family ともゼロを跨ぎ不通過 |
| candidate count が十分 | — | median `3`、必要な `5` に届かず不通過 |
| 3 time segment で再現 | 各 segment で `min_observations=300` を満たす独立 family がなく不通過 | `momentum_value_only` のみ3期間正、`core_long_only` は pre-reorg が負で不通過 |
| severe-loss が `1` pp 超悪化しない | `core_long_only` が `+2.2917` pp、`momentum_value_only` が `+1.2911` pp で不通過 | `core_long_only` が `+1.5315` pp で不通過（`momentum_value_only` は `-1.1358` pp） |
| feature coverage が十分 | 両 primary family で `100%` | 両 primary family で `100%`（binary gate 数には含めない） |

監査用の正式 gate count は continuous が `1 / 7`（coverage のみ）、binary が `0 / 7`、最終 verdict は `reject_introduction = true` である。

### Interpretation

孤立した正の signal は、事前に定めた独立 family 間の replication gate を上書きしない。`core_long_only` の continuous IC や `momentum_value_only` の binary lift を単独で採用根拠にはできず、family ごとに異なる成功条件を組み合わせて2 family replication と数えることもない。

これは trend acceleration が常に無情報だという一般命題ではない。この Prime 相当 PIT universe、既存 candidate、固定 feature、指定 gate における production 導入を支持しない、after-close の observation-level evidence という結論である。portfolio performance、取引可能性、capacity を示すものではない。

v7の`topk_priority_lift_df`は23,653 complete rowsと221 `incomplete_outcomes` rowsを区別する。未完了outcomeを暗黙に成績0へ変換しないaligned-outcome集計でも、published lift、CI、gate verdictは上記のままである。

### Production Implication

- continuous 列を追加しない。
- badge を追加しない。
- fixed `20D/60D`、`momentum_20_60_top20`、liquidity regime、`Overheat`、candidate selection を変更しない。
- この bundle を根拠に production API、materialization、UI の follow-on を開始しない。

v7はcompletion-aligned outcomeを最終artifactまで保存したcanonical evidenceであり、旧v5/v6だけを根拠にfollow-onを再開しない。

別の feature 定義や portfolio lens を再検証する場合は、今回の結果を上書きせず、別の承認済み research design として扱う。

### Validation Details

2017-2023 の historical replication は `results.duckdb` の raw daily rows を再集計し、`segment_stability_df` と `bootstrap_effect_ci_df` に一致することを確認した。lift と CI は percentage point、severe loss は triple/top 側と control/bottom 側の rate 差である。

binary 20D mean daily lift は `momentum_value_only` だけが3期間すべて正だった。2024年以降は仮説の起点であり、holdout としては扱わない。

| Independent family | 2017-2021 | 2022-2023 | 2024+ binary 20D paired-date coverage | 2024+ mean daily lift | 3期間の正方向 |
| --- | ---: | ---: | --- | ---: | --- |
| `core_long_only` | `-1.1905` | `+0.5335` | `2024-01-19`–`2026-06-08` | `+0.2071` | No |
| `momentum_value_only` | `+0.4122` | `+0.9495` | `2024-01-15`–`2026-04-13` | `+0.4392` | Yes |

continuous lens では、`core_long_only` の 2022-2023 が213 observations、`momentum_value_only` の 2017-2021 が87 observations、2024年以降が21 observations に留まり、各 segment の `min_observations=300` を満たして3期間を再現した独立 family はなかった。

`aggressive_rerating` は continuous の candidate/date 最低20銘柄を満たす row がなく、binary の historical replication も1 paired date・4 observations だけで `min_observations=300` を満たさなかった。独立 family の成功数には数えていない。

`topk_priority_lift_df` の `(candidate_group, horizon, K)` 42組すべてに fixed-seed moving-block bootstrap の `all_available` row がある。expected/actual は42/42、missing・extra・invalid row はすべて0である。Top-K daily rowsは23,653 complete / 221 incompleteで、後者はcoverage countと`outcome_status`を保持し、効果metricは集計に入れない。trend feature coverage は `core_long_only` の7,971 observations、`momentum_value_only` の13,005 observations でいずれも `100%` だった。run は signal date exact-match の `stock_master_daily` から `0101` と `0111` だけを解決し、2017-01-01 以降、最新 complete signal date `2026-07-08` までを対象にした。全 candidate/group observation は185,617 rows である。

Price-PIT audit は canonical raw 9,748,001 rows、signal features 4,511,414 rows、outcome requests 13,534,242 rows、completed outcomes 13,375,258 rowsを検証した。signal basis / segment は3,582 / 5,542、completion basis / segment は3,583 / 4,742で、全 hash と count は `manifest.json.result_metadata.price_projection` と `summary.md` に保存した。projection SHA-256 は `7bd911d7964d924cd21b46cdbf13b349b41b7230ede70304bcc442df80b4235f`、verification status は `verified`、`no_stock_data_fallback=true` である。

Endpoint alignment auditではPrime equivalent全complete keyのstock completion / nominal TOPIX completion不一致が5D `1,044 / 4,502,049`、20D `2,509 / 4,473,964`、60D `4,431 / 4,399,245`あった。v5の10,000-row sampleはcode `7916`の60Dで4 signal datesが不一致となり、TOPIX-excessの絶対差は最大`1.102532 pp`だった。v7の`observation_sample_df`は5D/20D/60Dそれぞれのcompletion date、stock return、TOPIX excess、completion-aligned N225 excessを保持する。10,000-row sampleのN225列は全3 horizonで10,000件すべて非NULLで、Market v4のsignal/completion N225 endpointからの再計算との差は最大`0.0 pp`だった。

### Caveats

- OLS feature は signal date の close までを含むため、pre-open use は未検証である。
- candidate group の overlap は replication count を水増ししない。nested `earnings_priority` は独立 family として数えない。
- incomplete forward window は効果集計から除外する。Top-Kの221行は`outcome_status=incomplete_outcomes`、効果metric `NULL`の監査行である。latest complete signal date は結果 table の共通終点ではなく、forward-complete paired-date coverage は horizon / comparison / family に依存する。結果は Prime 相当 universe と設定した horizons に限定される。
- 取引費用、capacity、portfolio construction、execution timing は含まれない。
- continuous comparison は candidate/date ごとに20銘柄を要求するため、独立 slice の eligible dates が少ない。これは gate 不通過を覆さないが、推定精度の制約として読む。
- durable run の `observation_sample_df` は先頭10,000 rows の診断用 sample であり、全 observation の代替ではない。判断は aggregate table と bootstrap table に基づく。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_trend_acceleration_conditional_lift.py`
- Module: `apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py`
- Durable bundle: `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260719_prime_price_pit_conditional_lift_v7/`
- Manifest: `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260719_prime_price_pit_conditional_lift_v7/manifest.json`
- Results: `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260719_prime_price_pit_conditional_lift_v7/results.duckdb`
- Summary: `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260719_prime_price_pit_conditional_lift_v7/summary.md`
- Bundle provenance: manifest `git_commit` は `e55781ef9e4b2fef74fb47d145f272cef97071ae`、`git_dirty` は `false`。artifact SHA-256はmanifest `36f9191bf4e8d4c491efad792d4aedb581856e041754cf99b3489bf51e51defb`、results `8553d38ba3eb3fbead33b268f909e88adb5c20e99a8103a0355e9e8708a7e47f`、summary `11b4b0a25ea669410ee561eea67cae9747a99f837de5cee306ac9a272998ea84`。
- Superseded immutable archives:
  - `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260719_prime_price_pit_conditional_lift_v3/`（price-PIT results は v4 と一致するが、generated summary の outcome-request count が不完全）
  - `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260719_prime_price_pit_conditional_lift_v4/`（review前のoverlap/outcome-frontier contract）
  - `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260719_prime_price_pit_conditional_lift_v5/`（nominal benchmark endpoint再計算のpublication。manifest/results/summary SHA-256 `a7c8eafc…` / `09469082…` / `83ea4d86…`でimmutable保持）
  - `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260719_prime_price_pit_conditional_lift_v6/`（completion-aligned N225をshared panelで計算したが最終observation sampleに保存しなかったpublication。manifest/results/summary SHA-256 `967925a5…` / `b98ec3e3…` / `8e214ad3…`でimmutable保持）
  - `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260718_prime_pit_conditional_lift_v2/`（convenience price lineage）
- Result tables: `coverage_diagnostics_df`, `candidate_registry_df`, `conditional_binary_lift_df`, `fixed_incremental_2x2_df`, `continuous_rank_lift_df`, `topk_priority_lift_df`, `segment_stability_df`, `bootstrap_effect_ci_df`, `decision_gate_df`, `observation_sample_df`
- Runner command:

```bash
uv run --project apps/bt python \
  apps/bt/scripts/research/run_ranking_trend_acceleration_conditional_lift.py \
  --run-id 20260719_prime_price_pit_conditional_lift_v7
```
