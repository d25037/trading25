# Ranking Technical Fit Score Shape Evidence

## Published Readout

### Decision

**最終判断: `neither` — fixed 20D/60D endpoint return と OLS fitted move のどちらも勝者ではなく、Technical Fit Score を Ranking に導入しない。**

- Published run: `20260719_prime_pit_technical_fit_shape_v12`
- Immutable bundle: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v12/`
- Provenance commit: `85140932d6b785b41069db3ef7afe1d066cb0d5e` (`git_dirty=false`)

`decision_gate` は fixed / OLS の両方を `fails_adoption_gate`、最終比較を `neither` とした。両 family は sample contract を満たすが、20D OOS CI、ring replication、IC、period stability、および同一 near ring の期間別 raw-shape gate を同時に通らない。primary raw mapping の winner は各評価年で境界 `q1` / `q5` にあり、interior winner が存在しないため、`segment_stability` の primary shape-pair slice は `date_count=0`、effect metrics `NULL`、pass `false` として明示的に残した。

Ranking の Value Score と Long Hybrid Score がともに高い候補へ第三の high-is-good score を追加する根拠はない。`20D<0` と fixed `20D>=30%` overheat は診断に留め、candidate ring、mapping、primary gate、hard exclude を変更しない。

v12はpublication toolを固定した同一clean source commitからMarket v4を再実行した。selection-first clean cutによりobservationは429,764へ更新されたが、primary TOPIX evidenceとdecision gateの正式判断は`neither`のままである。v1–v11は削除・上書きせずimmutable archiveとして保持する。

### Main Findings

#### Coverage and decision

対象は `stock_master_daily` の exact signal-date Prime-equivalent membership (`0101`, `0111`) のみ。Value Score と Long Hybrid Score だけで mutually exclusive な三 ring を先に凍結し、その後に Prime-wide fixed / OLS level と completion-basis outcome を結合した。

| Ring | Observations | Dates | Median candidates/date | Completed 20D coverage |
| --- | ---: | ---: | ---: | ---: |
| `core_high_high` | 43,719 | 1,897 | 13 | 98.824% |
| `near_high_high_1` | 129,757 | 1,937 | 61 | 99.037% |
| `near_high_high_2` | 256,288 | 1,937 | 127 | 99.180% |

| Gate | Decision | Sufficient sample | Passed |
| --- | --- | --- | --- |
| fixed equal | `fails_adoption_gate` | yes | no |
| OLS equal | `fails_adoption_gate` | yes | no |
| fixed vs OLS | `neither` | yes | no |

#### Published artifact contract

通常の unit publication test はこの値を committed digest と README に対して hermetic に検証する。実 artifact 照合は `TRADING25_VERIFY_PUBLISHED_RESEARCH_ROOT` を設定した opt-in integration test で行う。

| key | published_value |
| --- | ---: |
| `observation_count` | `429764` |
| `fixed_core_oos_mean_lift_pct` | `0.2701` |
| `ols_core_oos_mean_lift_pct` | `0.1836` |
| `near1_fixed_minus_ols_mean_lift_pct` | `0.0448` |
| `fixed_top5_mean_lift_pct` | `-0.2126` |
| `ols_top5_mean_lift_pct` | `-0.2385` |

v12の`topk_operational_lift`は12,956 complete rowsと340 `incomplete_outcomes` audit rowsを区別する。未完了outcomeのmetricは`NULL`で、上表とadoption gateにはcomplete rowsだけを使う。

#### 20D OOS adoption evidence

lift は日付等ウェイトの top 30% minus bottom 30% mean TOPIX-excess return。CI は frozen 2,000-resample moving-block bootstrap。

| Family | Ring | Dates | Mean lift | 95% CI | Median IC | IC positive | Severe diff |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: |
| fixed | `core_high_high` | 839 | +0.270 pp | [-0.571, +1.190] | +0.0279 | 52.80% | -0.627 pp |
| fixed | `near_high_high_1` | 1,107 | -0.037 pp | [-0.507, +0.415] | +0.0014 | 49.41% | -1.249 pp |
| fixed | `near_high_high_2` | 1,108 | -0.228 pp | [-0.602, +0.140] | -0.0115 | 46.66% | -1.028 pp |
| OLS | `core_high_high` | 839 | +0.184 pp | [-0.618, +1.035] | +0.0296 | 53.16% | -0.769 pp |
| OLS | `near_high_high_1` | 1,107 | -0.082 pp | [-0.528, +0.359] | -0.0063 | 46.97% | -1.262 pp |
| OLS | `near_high_high_2` | 1,108 | -0.308 pp | [-0.713, +0.118] | -0.0154 | 44.13% | -0.743 pp |

fixed / OLS はともにcore point estimateが正でもCIが0を跨ぎ、near ringは負でreplicationがない。ICとperiod stabilityもadoption条件を同時に満たさない。

#### Same-near raw-shape gate

`segment_stability.analysis=raw_shape_pair_gate` は全6 raw score × 2 near ring × 2 required period の24行を持つ。各行は core と指定 near ring の selected-bin lift の小さい方、severe-loss deterioration の大きい方を同一 period slice で記録し、lift `>0` かつ severe deterioration `<=1 pp` のときだけ passする。同一 near ring が 2022–2023 と 2024+ の両方で passしなければ score-level gate は failする。

primary `fixed_equal_level` / `ols_equal_level` は評価年 mapping winner が境界 binで interior comparisonを定義できない。各 primary は2 near ×2 periodの4行を欠落させず、`date_count=0` / metrics `NULL` / pass `false` として保持する。旧 `raw_shape_summary.oos_*` 集約 flags は削除した。これにより期間ごとに異なる near が勝つ Simpson 型の混同や、period別 `+2 pp/-2 pp` severe deterioration の pooled maskingを許さない。

primary の全 ring における `shape_classification` は `unstable_shape` で、`interior_sweet_spot_confirmed` は0件だった。

#### Fixed versus OLS and Top-K

| Ring | Fixed minus OLS | 95% CI |
| --- | ---: | --- |
| `core_high_high` | +0.086 pp | [-0.267, +0.423] |
| `near_high_high_1` | +0.045 pp | [-0.142, +0.222] |
| `near_high_high_2` | +0.080 pp | [-0.059, +0.222] |

| Family | K | Mean lift | 95% CI | Severe diff | Sector HHI diff |
| --- | ---: | ---: | --- | ---: | ---: |
| fixed | 5 | -0.213 pp | [-0.712, +0.270] | +0.239 pp | +0.223 |
| fixed | 10 | -0.099 pp | [-0.594, +0.410] | -0.064 pp | +0.138 |
| OLS | 5 | -0.239 pp | [-0.860, +0.381] | +1.195 pp | +0.228 |
| OLS | 10 | -0.206 pp | [-0.762, +0.372] | +0.395 pp | +0.144 |

paired effect は全 ring で CI が0を跨ぐ。Top-K point estimateも負で、sector concentrationが悪化するため、failed family gateを救済しない。

### Data Plane / PIT Lineage

published v12 は physical `market.duckdb` schema v4 と `stock_price_adjustment_mode=local_projection_v2_event_time` を要求する。Technical Fit の株価 source は `stock_data_raw` のみで、`stock_data` fallback はない。

- signal feature: exact signal-date `basis_id` を full lookback の全 raw OHLCV rowへ適用。
- outcome: exact completion-date `basis_id` を signal / completion の両 endpointへ適用。
- adjusted OHLC: `raw OHLC * cumulative_factor`。
- adjusted volume: `ROUND(raw volume / cumulative_factor)`。
- segment coverage: factor validityで絞る前に exactly one を要求し、唯一の factor が finite / positive であることを別段階で検証。
- valuation: exact signal-date `daily_valuation.basis_version` と signal price basis の一致を要求。

| Audit item | v12 value |
| --- | ---: |
| canonical raw rows | 9,748,001 |
| signal feature rows | 4,735,924 |
| signal basis / segment rows | 3,724 / 5,684 |
| outcome requests / completed | 14,207,772 / 14,047,442 |
| completion basis / segment rows | 3,725 / 5,008 |

- signal basis SHA-256: `887e668c9fb4b8c31534a7367091d0384c997c025c7bf86b78670f4d5e415842`
- signal segment SHA-256: `6c93023fa755a64eca3524252d306619c5c7361f6c5ff12aeaf634f9e94c951f`
- completion basis SHA-256: `5a957544cebf4ae5368e402359e8a7705893d928f35372decc79b30cd8e4068a`
- completion segment SHA-256: `00c164249d971eae188d016bfb27542bfaf54bce0a597d0829c56224d6a4d5e3`
- forward outcome SHA-256: `8e011747c14e0f2749d08e1bdfddc44af76ac8b7d5d4aecfe7dc8725c53551ce`
- combined price projection SHA-256: `398bc1145ad24b85b8615c3ed2f420fd5e8dff62a4b0fb353228025aed07cfd2`
- invalidation disposition: `v1_v2_historical_archive_v3_superseded_by_v4_for_price_basis_gate_ci_hardening_v4_superseded_by_v5_for_explicit_failed_shape_slices_v5_superseded_by_v6_for_lineage_disposition_hardening_v6_superseded_by_v7_for_review_fixed_frontier_and_flat_mapping_v7_superseded_by_v8_for_lineage_disposition_hardening_v8_superseded_by_v9_for_completion_aligned_n225_endpoint_repair_v9_superseded_by_v10_for_missing_v8_v9_lineage_v10_superseded_by_v11_for_missing_v9_v10_lineage`

### Superseded immutable history

- v1: exact basis ID list / catalog evidence不足の provenance-only archive。
- v2: headlineはv3と一致するが、segment cardinalityを factor validityより先に監査しない gap がある。
- v3: valuation lineageはhardening済みだが、technical/ATR/liquidity/leadership/OLS価格を convenience `stock_data` から読んでおり、shape gateがperiod内の同一near pairを要求せず、通常CI testがdeveloper XDG artifactを要求したため superseded。
- v4: raw-price signal/completion basis、same-near gate、hermetic CI を導入したが、interior winner不存在時の primary `raw_shape_pair_gate` rowsを欠落させ `insufficient_evidence` としたため canonical publishせず superseded。
- v5: 全期待shape sliceを明示 fail rowとして保持したが、artifact内の `invalidation_disposition` がv3→v4までで止まり、v4→v5のcanonical lineageを記録しなかったため superseded。
- v6: v1–v5の履歴とv6 canonical理由をmanifest/summaryへ一貫記録した旧canonical artifact。review前のTop-K outcome frontierとnear-flat mapping contractのためv7がsupersedeする。
- v7: event-time adjustment frontier、明示的なTop-K outcome coverage、`0.01 pp` near-flat thresholdを同一clean commitで再実行したが、artifact内の`invalidation_disposition`がv6で止まったためv8がsupersedeする。
- v8: v6→v7 review fixとv7→v8 lineage repairをmanifest/summaryへ明示したが、N225がnominal session leadでbenchmark endpointを解決していたためv9がsupersedeする。
- v9: authoritative stock completion dateにN225 endpointを揃えたが、artifact内の`invalidation_disposition`がv7→v8で止まり、v8→v9 repair理由を記録しなかったためv10がsupersedeする。
- v10: v8→v9 N225 endpoint repairをmanifest/summaryへ明示したが、自身のv9→v10 transitionが欠落してchainが一世代遅れだったためv11がsupersedeする。
- v11: v9→v10の欠落理由とv10→v11のcanonical transitionを同時に事前記録した旧canonical artifact。
- v12: publication tool固定後の同一clean source commitから再実行した現行canonical artifact。v1–v12はすべてimmutableで、旧bundleを変更・削除しない。

### Interpretation

PIT-safe な価格へ切り替えても、fixed / OLS の順位づけは adoption に必要な再現性を得なかった。fixed core と OLS core / near1 の point estimate は一部正だが、CI、near-ring replication、IC、downsideを同時に満たさない。raw mappingは境界 binを選び、仮説である interior sweet spotを支持しない。したがって局所的なpoint estimateやTop-K liftを第三scoreの導入根拠へ昇格させず、正式状態を `neither` とする。

v12でもnear-flat guardrailの境界を維持し、実データの150 mappingはいずれもthreshold外だった。N225 endpointとcanonical publicationまで揃えてもprimary decisionは不変である。

### Caveats

- signal closeを含む after-close featureで、pre-open decidabilityはない。
- 5D / 20D / 60D close-to-close observation-level outcomeであり、portfolio P&Lではない。
- transaction cost、slippage、capacity、position sizing、executionを含まない。
- 2024+ は hypothesis-origin walk-forward periodで、独立したclean holdoutではない。
- exact-date Prime `0101/0111` のみで、Standard / Growthや他市場へ外挿しない。
- incomplete forward windowは効果集計から除外する。Top-Kの340行は`outcome_status=incomplete_outcomes`、効果metric `NULL`の監査行である。
- near-flat thresholdは将来のexpectancy spread `<=0.01 pp` mappingをneutral `0.5`へ束ねるcontractだが、v12 artifactでは該当rowがないため、flat regimeの実証結果ではない。

### Production Implication

- Ranking に `Technical Fit Score` column、badge、sort key、API field、materialization、UI を追加しない。
- fixed / OLS のどちらも第三 score として採用しない。
- `20D<0` は caution、overheat は sparse risk diagnostic のままにする。
- 本研究は after-close observation-level forward-return studyであり、portfolio construction、cost、slippage、capacity、executionを示さない。
- v12のclean rerunでもgateは変わらず、API、materialization、UIのfollow-onを開始しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_technical_fit_score_shape_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py` (shared event-time price projection: `daily_ranking_event_time_prices.py`)
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`
- Durable bundle: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v12/`
- Manifest: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v12/manifest.json`
- Results: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v12/results.duckdb`
- Summary: `~/.local/share/trading25/research/market-behavior/ranking-technical-fit-score-shape-evidence/20260719_prime_pit_technical_fit_shape_v12/summary.md`
- Provenance: source commit `85140932d6b785b41069db3ef7afe1d066cb0d5e`; `git_dirty=false`。artifact SHA-256はmanifest `6838d0ecc240ba0231e534a37ea25daf78ad4d1f7bb49ab2225f896b28cecbee`、results `b1e0b84ff539218740d7f7ea91728a483eb209d64fda0a2e485b1e5f1d206889`、summary `399b0226b99267577e0b5d4af30f877e1da0aeede44eb44b6ed1708f1de49473`。v1–v11はimmutable archiveとして保持する。
- Bundle tables: `ring_registry`, `raw_score_registry`, `coverage_attrition`, `raw_shape_daily`, `raw_shape_summary`, `walkforward_mapping`, `oos_fit_score_lift`, `fixed_vs_ols_paired`, `topk_operational_lift`, `overheat_negative_diagnostics`, `segment_stability`, `annual_stability`, `bootstrap_effect_ci`, `decision_gate`, `observation_sample`

Validation: exact 15 non-empty tables、429,764 observations、Prime-only coverage `0101,0111`、same-date fixed/OLS pairing、completion dateがsignal dateより後、24 shape-gate rows、150 `ready` / 0 `flat` mappings、Top-K 12,956 complete / 340 incomplete audit rows、旧 `oos_*` flagsなし、price audit counts/hashes、summary/manifest/decision gateの `neither` 一致を確認した。

Reproduce:

```bash
uv run --project apps/bt python \
  apps/bt/scripts/research/run_ranking_technical_fit_score_shape_evidence.py \
  --run-id 20260719_prime_pit_technical_fit_shape_v12
```
