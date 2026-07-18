# Ranking Trend Acceleration Conditional Lift

既存の Daily Ranking long candidate の中で、rolling log-price OLS による短期・長期 trend acceleration が、forward TOPIX-excess return の追加的な優先順位情報になるかを検証する。候補選定は既存の valuation、liquidity、ATR、momentum のみで固定し、OLS slope は候補抽出には使わない。

対象は signal date 時点の Prime 相当 universe のみである。`0101`（市場再編前の東証一部）と `0111`（Prime）を `stock_master_daily` の exact-date membership で解決し、Standard / Growth は対象外とする。

## Published Readout

### Decision

Decision: continuous columns / binary badge ともに不採用

`priceLrSlope20Pct`、`priceLrSlope60Pct`、`trendAccelerationMarginPct` の Ranking 列追加と、`trend_acceleration_triple` badge の導入をともに棄却する。`decision_gate_df` では continuous 側は coverage だけが通過し、binary 側は全 gate が不通過、最終判断は `reject_introduction` だった。production Ranking の fixed `20D/60D`、候補抽出、API、materialization、UI は変更しない。

### Why This Research Was Run

既存の Daily Ranking long candidate の内側で、短期・長期の rolling log-price OLS slope から得る trend acceleration が、すでに使っている valuation、liquidity、ATR、固定 `20D/60D` momentum を置き換えずに追加の優先順位情報になるかを検証するために実行した。OLS feature は研究対象の候補抽出には使わず、既存条件で固定した候補群の forward TOPIX-excess return を比較した。目的は新しい列や badge を正当化できるかの判定であり、既存 `20D/60D` semantics の置換ではない。

### Data Scope / PIT Assumptions

対象 universe は signal date `X` の `stock_master_daily` exact-date membership で解決した Prime 相当だけであり、市場再編前は `0101`、再編後は `0111` を用いる。Standard / Growth は含めない。candidate membership、market membership、valuation、liquidity、sector、ATR、endpoint return、OLS feature はすべて `X` close 時点までの情報で解決し、forward outcome は `X` 後から始まるため、この feature の pre-open 利用はこの研究の対象外である。

primary outcome は `20D` close-to-close TOPIX-excess return、補助 horizon は `5D` と `60D` である。未完了の forward window は除外する。したがって最新 complete signal date `2026-07-08` は signal availability の上限であって、各 comparison の最新 paired date ではない。最新 paired date は horizon、comparison、family ごとに異なる。

### Main Findings

#### 結論

2017-2023 の historical replication では、独立 primary slice のどちらにも、continuous margin の正の 20D top-minus-bottom lift はなかった。binary triple は `momentum_value_only` だけが median lift と paired-date win rate の閾値を超えたが、bootstrap CI 下限が負、median triple candidates が3、もう一方の独立 primary slice が再現しなかったため badge gate を満たさない。

| 推奨候補 | gate 通過数 | 通過した gate | 判断 |
| --- | ---: | --- | --- |
| `add_continuous_columns` | 1 / 7 | `coverage_ge_95_every_primary_family` | 不採用 |
| `add_binary_badge_only` | 0 / 7 | なし | 不採用 |
| 最終 | — | `reject_introduction = true` | fixed `20D/60D` を維持 |

#### Historical replication の主要根拠

`results.duckdb` の raw daily rows を 2017-2023 に限定して再集計し、`segment_stability_df` と `bootstrap_effect_ci_df` に一致することを確認した。lift と CI は percentage point、severe loss は triple/top 側と control/bottom 側の rate 差である。

| Lens / exclusive family | Eligible dates / observations | IC または paired-date positive rate | 20D daily lift | 95% moving-block bootstrap CI | Severe-loss rate 差 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Continuous / `core_long_only` | 24 / 518 | median IC `+0.0460`; IC+ `58.33%` | mean `-1.1818` | `[-2.5810, +0.2442]` | `+2.2917` |
| Continuous / `momentum_value_only` | 69 / 1,739 | median IC `-0.0431`; IC+ `39.13%` | mean `-1.9560` | `[-4.6738, -0.0927]` | `+0.7488` |
| Binary / `core_long_only` | 252 / 2,513 | positive date `41.67%` | median `-0.8827`; mean `-0.6071` | `[-1.6817, +0.3644]` | `+1.3370` |
| Binary / `momentum_value_only` | 343 / 3,956 | positive date `56.27%` | median `+0.7431`; mean `+0.6850` | `[-0.2915, +1.6498]` | `-1.1905` |

`aggressive_rerating` は continuous の candidate/date 最低20銘柄を満たす row がなく、binary の historical replication も1 paired date・4 observations だけで `min_observations=300` を満たさなかった。独立 family の成功数には数えていない。

#### Operational Top-K bootstrap

`topk_priority_lift_df` に存在する `(candidate_group, horizon, K)` 42組のすべてについて、fixed-seed moving-block bootstrap の `all_available` row を `bootstrap_effect_ci_df` に保存した。expected/actual は42/42、missing・extra・invalid row はいずれも0である。primary exclusive family の20D結果では、いずれの95% CI下限もゼロを上回らず、operational Top-K lens も導入棄却を覆さない。

| Exclusive family | K | Paired dates | Mean priority lift | 95% moving-block bootstrap CI |
| --- | ---: | ---: | ---: | ---: |
| `core_long_only` | 5 | 241 | `+0.2474` | `[-0.1743, +0.6716]` |
| `core_long_only` | 10 | 47 | `+0.3222` | `[-0.2090, +0.8283]` |
| `momentum_value_only` | 5 | 425 | `+0.0098` | `[-0.6588, +0.6555]` |
| `momentum_value_only` | 10 | 70 | `-0.4457` | `[-1.3034, +0.3109]` |

#### Segment replication

binary 20D mean daily lift は `momentum_value_only` だけが3期間すべて正だった。`core_long_only` は pre-reorg 期間が負であり、2つの独立 family を要求する gate は不通過となった。2024年以降は仮説の起点であり、holdout としては扱わない。

| Exclusive family | 2017-2021 | 2022-2023 | 2024+ binary 20D paired-date coverage | 2024+ mean daily lift | 3期間の正方向 |
| --- | ---: | ---: | --- | ---: | --- |
| `core_long_only` | `-1.2829` | `+0.5289` | `2024-01-19`–`2026-06-08` | `+0.1860` | No |
| `momentum_value_only` | `+0.3978` | `+0.9544` | `2024-01-15`–`2026-04-13` | `+0.3659` | Yes |

continuous lens は `core_long_only` の 2022-2023 が214 observations、`momentum_value_only` の 2017-2021 が87 observations、2024年以降が21 observations に留まり、各 segment の `min_observations=300` を満たして3期間を再現した独立 family はなかった。

#### Coverage と universe

`coverage_diagnostics_df` では `core_long_only` 7,990 observations、`momentum_value_only` 12,837 observations の trend feature coverage はともに `100%` だった。run は signal date exact-match の `stock_master_daily` から `0101` と `0111` だけを解決し、2017-01-01 以降、最新 complete signal date `2026-07-08` までを対象にした。ただし forward-complete row の終点は horizon / comparison / family に依存し、binary 20D recent paired-date coverage は上表のとおりである。この Prime 相当 PIT universe の外には結論を外挿しない。

### Interpretation

`core_long_only` の continuous IC が閾値を超えたことや、`momentum_value_only` の binary lift が正だったことは、単独では導入根拠にならない。前者は historical top-minus-bottom lift と左尾が悪化し、後者は historical CI がゼロを跨ぎ、median triple candidates も gate の5に対して3だった。binary の全条件を同時に満たした独立 family は0であり、条件ごとに異なる family 集合を組み合わせて2 family replication と数えていない。重複 scaffold や nested `earnings_priority` を独立成功として数えずに読むと、固定された replication gate は明確に不通過である。

この研究は after-close の observation-level evidence であり、portfolio performance、取引可能性、capacity を示すものではない。今回の棄却は「trend acceleration が常に無情報」という一般命題ではなく、この Prime 相当 PIT universe、既存 candidate、固定 feature、指定 gate における production 導入を支持しないという判断である。

### Production Implication

連続列と badge は追加しない。既存 fixed `20D/60D`、`momentum_20_60_top20`、liquidity regime、`Overheat`、candidate selection を維持する。今回の bundle を根拠に production API/materialization/UI の follow-on を開始しない。別 feature 定義や portfolio lens を再検証する場合は、今回の結果を上書きせず、別の承認済み research design として扱う。

### Caveats

- OLS feature は signal date の close までを含むため、pre-open use は未検証である。
- candidate group の overlap は replication count を水増ししない。nested `earnings_priority` は独立 family として数えない。
- incomplete forward window は除外する。したがって latest complete signal date は結果 table の共通終点ではなく、forward-complete paired-date coverage は horizon / comparison / family に依存する。結果は Prime 相当 universe と設定した horizons に限定される。
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
