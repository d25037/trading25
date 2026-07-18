# Ranking Trend Acceleration Conditional Lift

## Published Readout

### Decision

- Ranking に continuous 列を追加しない。
- 3条件 badge も追加しない。
- 2つの独立した long 候補群で結果が再現せず、既存の fixed `20D/60D` とすべての production Ranking surface は変更しない。

`priceLrSlope20Pct`、`priceLrSlope60Pct`、`trendAccelerationMarginPct` と `trend_acceleration_triple` badge は不採用とする。正式な判定は `reject_introduction` である。候補抽出、API、materialization、UI を含む production Ranking は変更しない。

### Why This Research Was Run

この研究は、既存情報で選ばれた Daily Ranking long candidate の内側で、短期・長期 trend acceleration が forward TOPIX-excess return の追加的な優先順位情報になるかを検証した。rolling log-price OLS feature は母集団を選ぶためには使わず、既存の valuation、liquidity、ATR、fixed `20D/60D` momentum で固定した候補群を順位付けするためだけに使った。

### Data Scope / PIT Assumptions

対象は signal date `X` の `stock_master_daily` exact-date membership で解決した Prime 相当 universe のみである。市場再編前は `0101`（東証一部）、再編後は `0111`（Prime）を使い、Standard / Growth は含めない。candidate membership、market membership、valuation、liquidity、sector、ATR、endpoint return、OLS feature はすべて `X` close 時点までの情報で解決した。forward outcome は `X` 後から始まるため、これは after-close の研究であり pre-open 利用は対象外である。

primary outcome は `20D` close-to-close TOPIX-excess return、補助 horizon は `5D` と `60D` である。未完了の forward window は除外する。最新 complete signal date `2026-07-08` は signal availability の上限であり、各 comparison の最新 paired date ではない。最新 paired date は horizon、comparison、family ごとに異なる。

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
| `core_long_only` | 24 | 518 | `-1.1818` | `[-2.5810, +0.2442]` | 改善せず |
| `momentum_value_only` | 69 | 1,739 | `-1.9560` | `[-4.6738, -0.0927]` | 改善せず |

`momentum_value_only` の median IC は `-0.0431`、IC 正の日の割合は `39.13%` だった。

#### 3条件 badge は片方だけ良かったが、採用条件を満たさなかった

`momentum_value_only` は median lift と positive-date rate が正だった。しかし 95% CI はゼロを跨ぎ、median badge candidates は `3`（必要な `5` 未満）で、もう一方の独立 family には再現しなかった。

| Independent family | Paired dates | Observations | Median 20D lift (pp) | Mean 20D lift (pp) | Positive-date rate | 95% moving-block bootstrap CI (pp) | Median badge candidates | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |
| `core_long_only` | 252 | 2,513 | `-0.8827` | `-0.6071` | `41.67%` | `[-1.6817, +0.3644]` | 3 | 改善せず |
| `momentum_value_only` | 343 | 3,956 | `+0.7431` | `+0.6850` | `56.27%` | `[-0.2915, +1.6498]` | 3 | 一部改善だが不十分 |

#### Top-K に絞っても改善は確認できなかった

Operational Top-K lens の all-available primary 20D 結果でも、4行すべての 95% CI はゼロを跨いだ。

| Independent family | K | Paired dates | Mean priority lift (pp) | 95% moving-block bootstrap CI (pp) |
| --- | ---: | ---: | ---: | --- |
| `core_long_only` | 5 | 241 | `+0.2474` | `[-0.1743, +0.6716]` |
| `core_long_only` | 10 | 47 | `+0.3222` | `[-0.2090, +0.8283]` |
| `momentum_value_only` | 5 | 425 | `+0.0098` | `[-0.6588, +0.6555]` |
| `momentum_value_only` | 10 | 70 | `-0.4457` | `[-1.3034, +0.3109]` |

#### 採用条件を総合すると導入できない

採用には、個別に良く見える結果ではなく、独立 family 間で同じ条件を満たすことを求めた。

| Requirement | Continuous ordering | Three-condition badge |
| --- | --- | --- |
| 2つの独立 family で正方向に再現 | 両 family の historical lift が負で不通過 | `momentum_value_only` だけが正で不通過 |
| 95% CI の下限が正 | `core_long_only` はゼロを跨ぎ、`momentum_value_only` は負で不通過 | 両 family ともゼロを跨ぎ不通過 |
| candidate count が十分 | — | median `3`、必要な `5` に届かず不通過 |
| 3 time segment で再現 | 各 segment で `min_observations=300` を満たす独立 family がなく不通過 | `momentum_value_only` のみ3期間正、`core_long_only` は pre-reorg が負で不通過 |
| severe-loss が `1` pp 超悪化しない | `core_long_only` が `+2.2917` pp で不通過（`momentum_value_only` は `+0.7488` pp） | `core_long_only` が `+1.3370` pp で不通過（`momentum_value_only` は `-1.1905` pp） |
| feature coverage が十分 | 両 primary family で `100%` | 両 primary family で `100%`（binary gate 数には含めない） |

監査用の正式 gate count は continuous が `1 / 7`（coverage のみ）、binary が `0 / 7`、最終 verdict は `reject_introduction = true` である。

### Interpretation

孤立した正の signal は、事前に定めた独立 family 間の replication gate を上書きしない。`core_long_only` の continuous IC や `momentum_value_only` の binary lift を単独で採用根拠にはできず、family ごとに異なる成功条件を組み合わせて2 family replication と数えることもない。

これは trend acceleration が常に無情報だという一般命題ではない。この Prime 相当 PIT universe、既存 candidate、固定 feature、指定 gate における production 導入を支持しない、after-close の observation-level evidence という結論である。portfolio performance、取引可能性、capacity を示すものではない。

### Production Implication

- continuous 列を追加しない。
- badge を追加しない。
- fixed `20D/60D`、`momentum_20_60_top20`、liquidity regime、`Overheat`、candidate selection を変更しない。
- この bundle を根拠に production API、materialization、UI の follow-on を開始しない。

別の feature 定義や portfolio lens を再検証する場合は、今回の結果を上書きせず、別の承認済み research design として扱う。

### Validation Details

2017-2023 の historical replication は `results.duckdb` の raw daily rows を再集計し、`segment_stability_df` と `bootstrap_effect_ci_df` に一致することを確認した。lift と CI は percentage point、severe loss は triple/top 側と control/bottom 側の rate 差である。

binary 20D mean daily lift は `momentum_value_only` だけが3期間すべて正だった。2024年以降は仮説の起点であり、holdout としては扱わない。

| Independent family | 2017-2021 | 2022-2023 | 2024+ binary 20D paired-date coverage | 2024+ mean daily lift | 3期間の正方向 |
| --- | ---: | ---: | --- | ---: | --- |
| `core_long_only` | `-1.2829` | `+0.5289` | `2024-01-19`–`2026-06-08` | `+0.1860` | No |
| `momentum_value_only` | `+0.3978` | `+0.9544` | `2024-01-15`–`2026-04-13` | `+0.3659` | Yes |

continuous lens では、`core_long_only` の 2022-2023 が214 observations、`momentum_value_only` の 2017-2021 が87 observations、2024年以降が21 observations に留まり、各 segment の `min_observations=300` を満たして3期間を再現した独立 family はなかった。

`aggressive_rerating` は continuous の candidate/date 最低20銘柄を満たす row がなく、binary の historical replication も1 paired date・4 observations だけで `min_observations=300` を満たさなかった。独立 family の成功数には数えていない。

`topk_priority_lift_df` の `(candidate_group, horizon, K)` 42組すべてに fixed-seed moving-block bootstrap の `all_available` row がある。expected/actual は42/42、missing・extra・invalid row はすべて0である。trend feature coverage は `core_long_only` の7,990 observations、`momentum_value_only` の12,837 observations でいずれも `100%` だった。run は signal date exact-match の `stock_master_daily` から `0101` と `0111` だけを解決し、2017-01-01 以降、最新 complete signal date `2026-07-08` までを対象にした。

### Caveats

- OLS feature は signal date の close までを含むため、pre-open use は未検証である。
- candidate group の overlap は replication count を水増ししない。nested `earnings_priority` は独立 family として数えない。
- incomplete forward window は除外する。latest complete signal date は結果 table の共通終点ではなく、forward-complete paired-date coverage は horizon / comparison / family に依存する。結果は Prime 相当 universe と設定した horizons に限定される。
- 取引費用、capacity、portfolio construction、execution timing は含まれない。
- continuous comparison は candidate/date ごとに20銘柄を要求するため、独立 slice の eligible dates が少ない。これは gate 不通過を覆さないが、推定精度の制約として読む。
- durable run の `observation_sample_df` は先頭10,000 rows の診断用 sample であり、全 observation の代替ではない。判断は aggregate table と bootstrap table に基づく。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_trend_acceleration_conditional_lift.py`
- Module: `apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py`
- Durable bundle: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260718_prime_pit_conditional_lift_v2/`
- Manifest: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260718_prime_pit_conditional_lift_v2/manifest.json`
- Results: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260718_prime_pit_conditional_lift_v2/results.duckdb`
- Summary: `/Users/mirage/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/20260718_prime_pit_conditional_lift_v2/summary.md`
- Bundle provenance: manifest `git_commit` は `a97eb5ded29add0f836ddf191e9fcc16f476b678`、`git_dirty` は `true`。実行時の worktree にはユーザー所有の `.gitignore` と既存 Task report の未コミット変更が残っていたが、研究ロジック・テスト・Daily Ranking base 修正は同 commit に含まれる。
- Result tables: `coverage_diagnostics_df`, `candidate_registry_df`, `conditional_binary_lift_df`, `fixed_incremental_2x2_df`, `continuous_rank_lift_df`, `topk_priority_lift_df`, `segment_stability_df`, `bootstrap_effect_ci_df`, `decision_gate_df`, `observation_sample_df`
- Run telemetry: wall `202.70s`; maximum RSS `8,186,183,680 bytes`; swap `0`
- Runner command:

```bash
uv run --directory apps/bt python \
  scripts/research/run_ranking_trend_acceleration_conditional_lift.py \
  --start-date 2017-01-01 \
  --bootstrap-resamples 2000 \
  --bootstrap-seed 20260718 \
  --run-id 20260718_prime_pit_conditional_lift_v2 \
  --notes 'Final whole-slice review correction: same-family binary gates and Top-K moving-block bootstrap.'
```
