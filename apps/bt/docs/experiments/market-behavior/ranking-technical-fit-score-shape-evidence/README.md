# Ranking Technical Fit Score Shape Evidence

## Published Readout

### Decision

**最終判断: `neither` — fixed 20D/60D endpoint return と OLS fitted move のどちらも勝者ではなく、Technical Fit Score を Ranking に導入しない。**

- Published run: `20260719_prime_pit_technical_fit_shape_v8`
- Immutable bundle: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v8/`
- Provenance commit: `dadad21c8d6e11fc566c73207c6b53f84b6bc7f7` (`git_dirty=false`)

`decision_gate` は fixed / OLS の両方を `fails_adoption_gate`、最終比較を `neither` とした。両 family は sample contract を満たすが、20D OOS CI、ring replication、IC、period stability、および同一 near ring の期間別 raw-shape gate を同時に通らない。primary raw mapping の winner は各評価年で境界 `q1` / `q5` にあり、interior winner が存在しないため、`segment_stability` の primary shape-pair slice は `date_count=0`、effect metrics `NULL`、pass `false` として明示的に残した。

Ranking の Value Score と Long Hybrid Score がともに高い候補へ第三の high-is-good score を追加する根拠はない。`20D<0` と fixed `20D>=30%` overheat は診断に留め、candidate ring、mapping、primary gate、hard exclude を変更しない。

PR 480 review後のv8 lineage-repair rerunでは、v7で導入したevent-time adjustment frontier、Top-K outcome coverage、raw expectancy spread `<=0.01 pp` のnear-flat mapping contractを維持し、manifest/summaryの`invalidation_disposition`へv6→v7とv7→v8を明記した。150 walk-forward mappingsはすべて`ready`で、near-flat判定に該当する実データrowは0件だった。published metricsと`neither`判断はv7から変わらない。

### Main Findings

#### Coverage and decision

対象は `stock_master_daily` の exact signal-date Prime-equivalent membership (`0101`, `0111`) のみ。Value Score と Long Hybrid Score だけで mutually exclusive な三 ring を先に凍結し、その後に Prime-wide fixed / OLS level と completion-basis outcome を結合した。

| Ring | Observations | Dates | Median candidates/date | Completed 20D coverage |
| --- | ---: | ---: | ---: | ---: |
| `core_high_high` | 43,554 | 1,898 | 13 | 98.868% |
| `near_high_high_1` | 129,358 | 1,937 | 61 | 99.017% |
| `near_high_high_2` | 260,970 | 2,108 | 122 | 99.195% |

| Gate | Decision | Sufficient sample | Passed |
| --- | --- | --- | --- |
| fixed equal | `fails_adoption_gate` | yes | no |
| OLS equal | `fails_adoption_gate` | yes | no |
| fixed vs OLS | `neither` | yes | no |

#### Published artifact contract

通常の unit publication test はこの値を committed digest と README に対して hermetic に検証する。実 artifact 照合は `TRADING25_VERIFY_PUBLISHED_RESEARCH_ROOT` を設定した opt-in integration test で行う。

| key | published_value |
| --- | ---: |
| `observation_count` | `433882` |
| `fixed_core_oos_mean_lift_pct` | `0.2561` |
| `ols_core_oos_mean_lift_pct` | `0.3765` |
| `near1_fixed_minus_ols_mean_lift_pct` | `-0.2125` |
| `fixed_top5_mean_lift_pct` | `0.0155` |
| `ols_top5_mean_lift_pct` | `0.4670` |

v8の`topk_operational_lift`は12,956 complete rowsと340 `incomplete_outcomes` audit rowsを区別する。未完了outcomeのmetricは`NULL`で、上表とadoption gateにはcomplete rowsだけを使う。

#### 20D OOS adoption evidence

lift は日付等ウェイトの top 30% minus bottom 30% mean TOPIX-excess return。CI は frozen 2,000-resample moving-block bootstrap。

| Family | Ring | Dates | Mean lift | 95% CI | Median IC | IC positive | Severe diff |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: |
| fixed | `core_high_high` | 818 | +0.256 pp | [-0.501, +1.078] | +0.0318 | 55.01% | +0.202 pp |
| fixed | `near_high_high_1` | 1,086 | +0.065 pp | [-0.404, +0.536] | +0.0024 | 50.37% | -0.075 pp |
| fixed | `near_high_high_2` | 1,088 | -0.079 pp | [-0.387, +0.239] | -0.0067 | 48.16% | +0.073 pp |
| OLS | `core_high_high` | 818 | +0.376 pp | [-0.435, +1.211] | +0.0493 | 59.90% | +1.402 pp |
| OLS | `near_high_high_1` | 1,086 | +0.278 pp | [-0.161, +0.730] | +0.0026 | 50.28% | +0.470 pp |
| OLS | `near_high_high_2` | 1,088 | -0.191 pp | [-0.524, +0.142] | -0.0176 | 43.20% | +0.691 pp |

fixed は core point estimate が threshold を僅かに超えるが CI が0を跨ぎ、near replication がない。OLS は core / near1 の point estimate が正でも CI と IC を満たさず、core severe deterioration は許容上限 `+1.0 pp` を超える。2022–2023 / 2024+ の mean lift も fixed core `-0.043 / +0.478 pp`、fixed near1 `-0.098 / +0.199 pp` と符号が反転し、OLS near2 は `-0.262 / -0.133 pp` で一貫して負だった。

#### Same-near raw-shape gate

`segment_stability.analysis=raw_shape_pair_gate` は全6 raw score × 2 near ring × 2 required period の24行を持つ。各行は core と指定 near ring の selected-bin lift の小さい方、severe-loss deterioration の大きい方を同一 period slice で記録し、lift `>0` かつ severe deterioration `<=1 pp` のときだけ passする。同一 near ring が 2022–2023 と 2024+ の両方で passしなければ score-level gate は failする。

primary `fixed_equal_level` / `ols_equal_level` は評価年 mapping winner が境界 binで interior comparisonを定義できない。各 primary は2 near ×2 periodの4行を欠落させず、`date_count=0` / metrics `NULL` / pass `false` として保持する。旧 `raw_shape_summary.oos_*` 集約 flags は削除した。これにより期間ごとに異なる near が勝つ Simpson 型の混同や、period別 `+2 pp/-2 pp` severe deterioration の pooled maskingを許さない。

primary の全 ring における `shape_classification` は `unstable_shape` で、`interior_sweet_spot_confirmed` は0件だった。

#### Fixed versus OLS and Top-K

| Ring | Fixed minus OLS | 95% CI |
| --- | ---: | --- |
| `core_high_high` | -0.120 pp | [-0.576, +0.374] |
| `near_high_high_1` | -0.213 pp | [-0.474, +0.027] |
| `near_high_high_2` | +0.112 pp | [-0.070, +0.310] |

| Family | K | Mean lift | 95% CI | Severe diff | Sector HHI diff |
| --- | ---: | ---: | --- | ---: | ---: |
| fixed | 5 | +0.015 pp | [-0.666, +0.683] | +2.220 pp | +0.200 |
| fixed | 10 | +0.011 pp | [-0.551, +0.601] | +1.255 pp | +0.112 |
| OLS | 5 | +0.467 pp | [-0.279, +1.235] | +3.341 pp | +0.184 |
| OLS | 10 | +0.437 pp | [-0.160, +1.050] | +2.643 pp | +0.128 |

paired effect は全 ring で CI が0を跨ぐ。Top-K point estimate は正でも全 CI が0を跨ぎ、severe loss と sector concentration が悪化するため、failed family gate を救済しない。

### Data Plane / PIT Lineage

published v8 は physical `market.duckdb` schema v4 と `stock_price_adjustment_mode=local_projection_v2_event_time` を要求する。Technical Fit の株価 source は `stock_data_raw` のみで、`stock_data` fallback はない。

- signal feature: exact signal-date `basis_id` を full lookback の全 raw OHLCV rowへ適用。
- outcome: exact completion-date `basis_id` を signal / completion の両 endpointへ適用。
- adjusted OHLC: `raw OHLC * cumulative_factor`。
- adjusted volume: `ROUND(raw volume / cumulative_factor)`。
- segment coverage: factor validityで絞る前に exactly one を要求し、唯一の factor が finite / positive であることを別段階で検証。
- valuation: exact signal-date `daily_valuation.basis_version` と signal price basis の一致を要求。

| Audit item | v8 value |
| --- | ---: |
| canonical raw rows | 9,748,001 |
| signal feature rows | 4,511,414 |
| signal basis / segment rows | 3,582 / 5,542 |
| outcome requests / completed | 13,534,242 / 13,375,258 |
| completion basis / segment rows | 3,583 / 4,742 |

- signal basis SHA-256: `cf3375da87a858d1e033c327040a11bf7775dfef9c7b21e3c6c03dbd49eedc76`
- signal segment SHA-256: `ccd1c4df2ecd330b52a79a02d8bd82a7fd76b4031752d2056c8085bff75c6ab5`
- completion basis SHA-256: `fa3cc08e7348137c743436fa855fa5345b5bba5aac38742a73b904d3ece8bd24`
- completion segment SHA-256: `b68490f299c75207bb4764b1a23b671584d7cfb5f4e597c541f38e472bcad7c1`
- forward outcome SHA-256: `e157b83f88ff75bff5ee86a3a9b61259ae853c5a6c0f8e7cb240c7283e0c25c5`
- combined price projection SHA-256: `7bd911d7964d924cd21b46cdbf13b349b41b7230ede70304bcc442df80b4235f`
- invalidation disposition: `v1_v2_historical_archive_v3_superseded_by_v4_for_price_basis_gate_ci_hardening_v4_superseded_by_v5_for_explicit_failed_shape_slices_v5_superseded_by_v6_for_lineage_disposition_hardening_v6_superseded_by_v7_for_review_fixed_frontier_and_flat_mapping_v7_superseded_by_v8_for_lineage_disposition_hardening`

### Superseded immutable history

- v1: exact basis ID list / catalog evidence不足の provenance-only archive。
- v2: headlineはv3と一致するが、segment cardinalityを factor validityより先に監査しない gap がある。
- v3: valuation lineageはhardening済みだが、technical/ATR/liquidity/leadership/OLS価格を convenience `stock_data` から読んでおり、shape gateがperiod内の同一near pairを要求せず、通常CI testがdeveloper XDG artifactを要求したため superseded。
- v4: raw-price signal/completion basis、same-near gate、hermetic CI を導入したが、interior winner不存在時の primary `raw_shape_pair_gate` rowsを欠落させ `insufficient_evidence` としたため canonical publishせず superseded。
- v5: 全期待shape sliceを明示 fail rowとして保持したが、artifact内の `invalidation_disposition` がv3→v4までで止まり、v4→v5のcanonical lineageを記録しなかったため superseded。
- v6: v1–v5の履歴とv6 canonical理由をmanifest/summaryへ一貫記録した旧canonical artifact。review前のTop-K outcome frontierとnear-flat mapping contractのためv7がsupersedeする。
- v7: event-time adjustment frontier、明示的なTop-K outcome coverage、`0.01 pp` near-flat thresholdを同一clean commitで再実行したが、artifact内の`invalidation_disposition`がv6で止まったためv8がsupersedeする。
- v8: v6→v7 review fixとv7→v8 lineage repairをmanifest/summaryへ明示した現行canonical artifact。v1–v8 はすべて immutable で、旧bundleを変更・削除しない。

### Interpretation

PIT-safe な価格へ切り替えても、fixed / OLS の順位づけは adoption に必要な再現性を得なかった。fixed core と OLS core / near1 の point estimate は一部正だが、CI、near-ring replication、IC、downsideを同時に満たさない。raw mappingは境界 binを選び、仮説である interior sweet spotを支持しない。したがって局所的なpoint estimateやTop-K liftを第三scoreの導入根拠へ昇格させず、正式状態を `neither` とする。

v8でもnear-flat guardrailの境界を維持し、実データの150 mappingはいずれもthreshold外だった。判断不変はguardrailを使わなかったことによる偶然の上書きではなく、完全なlineage dispositionを持つreview-fixed contractで同じevidenceを再確認した結果である。

### Caveats

- signal closeを含む after-close featureで、pre-open decidabilityはない。
- 5D / 20D / 60D close-to-close observation-level outcomeであり、portfolio P&Lではない。
- transaction cost、slippage、capacity、position sizing、executionを含まない。
- 2024+ は hypothesis-origin walk-forward periodで、独立したclean holdoutではない。
- exact-date Prime `0101/0111` のみで、Standard / Growthや他市場へ外挿しない。
- incomplete forward windowは効果集計から除外する。Top-Kの340行は`outcome_status=incomplete_outcomes`、効果metric `NULL`の監査行である。
- near-flat thresholdは将来のexpectancy spread `<=0.01 pp` mappingをneutral `0.5`へ束ねるcontractだが、v8 artifactでは該当rowがないため、flat regimeの実証結果ではない。

### Production Implication

- Ranking に `Technical Fit Score` column、badge、sort key、API field、materialization、UI を追加しない。
- fixed / OLS のどちらも第三 score として採用しない。
- `20D<0` は caution、overheat は sparse risk diagnostic のままにする。
- 本研究は after-close observation-level forward-return studyであり、portfolio construction、cost、slippage、capacity、executionを示さない。
- v8のlineage-complete rerunでもgateは変わらず、API、materialization、UIのfollow-onを開始しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_technical_fit_score_shape_evidence.py`
- Modules: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`, `apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`
- Durable bundle: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v8/`
- Manifest: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v8/manifest.json`
- Results: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v8/results.duckdb`
- Summary: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v8/summary.md`
- Provenance: manifest git commit `dadad21c8d6e11fc566c73207c6b53f84b6bc7f7`; `git_dirty=false`。
- Bundle tables: `ring_registry`, `raw_score_registry`, `coverage_attrition`, `raw_shape_daily`, `raw_shape_summary`, `walkforward_mapping`, `oos_fit_score_lift`, `fixed_vs_ols_paired`, `topk_operational_lift`, `overheat_negative_diagnostics`, `segment_stability`, `annual_stability`, `bootstrap_effect_ci`, `decision_gate`, `observation_sample`
- Run telemetry: wall `547.54s`; maximum RSS `8,139,784,192 bytes`; swap `0`。

Validation: exact 15 non-empty tables、433,882 observations、Prime-only coverage `0101,0111`、same-date fixed/OLS pairing、completion dateがsignal dateより後、24 shape-gate rows、150 `ready` / 0 `flat` mappings、Top-K 12,956 complete / 340 incomplete audit rows、旧 `oos_*` flagsなし、price audit counts/hashes、manifest/summaryのv6→v7→v8 disposition、summary/manifest/decision gateの `neither` 一致を確認した。

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
  --run-id 20260719_prime_pit_technical_fit_shape_v8 \
  --notes 'PR 480 lineage repair at dadad21c: v6 superseded by v7 review-fixed frontier/flat mapping; v7 superseded by v8 for complete manifest and summary invalidation disposition.'
```
