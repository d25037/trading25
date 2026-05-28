# Ranking Short Red Evidence

Daily Ranking の long-side 色分けから独立して、short / red 候補を検証する runner-first research です。
既存の `ranking-color-evidence` を UI rule に直接拡張せず、`crowded_rerating`、`distribution_stress`、`stale_liquidity` を relative valuation、20D/60D technical state、ATR20/ATR60 と交差させ、20D close-to-close TOPIX excess を主軸に観察します。

## Published Readout

### Decision

この experiment は `Ranking` の赤色候補をすぐ UI rule 化するためのものではない。独立した evidence bundle として、`crowded_rerating + high valuation / no value`、`distribution_stress + weak trend`、`stale_liquidity + high valuation + weak trend` が long 回避または short candidate として十分に悪い forward distribution を持つかを検証した。

Primary outcome は既存の Ranking Color Evidence と揃え、`20D close-to-close TOPIX excess return` とする。`5D` / `10D` は timing、`60D` は持続性の補助確認に限定する。

初回 Prime run では、`distribution_stress_high_valuation` と `stale_high_valuation_weak_trend` を赤候補として優先する。`crowded_no_value` / `crowded_high_valuation` も左尾は重いが、mean が右尾で残るため、単純 short より「yellow から red への候補、または market/ATR 条件付き red」と読む。

### Main Findings

#### 結論: 20D主軸では stress high valuation と stale high valuation weak trend が最も赤に近い

Prime `2022-06-30` から `2026-05-14`、`1,609,210` stock-days。下表は `short_red_candidate_df` の 20D / 60D close-to-close TOPIX excess return。

| Candidate bucket | Horizon | Obs | Mean | Median | Win rate | Severe loss | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `crowded_no_value` | 20D | 57,209 | +0.588% | -1.121% | 45.55% | 19.23% | 左尾は重いが右尾も残る |
| `crowded_high_valuation` | 20D | 43,036 | +0.645% | -1.098% | 45.99% | 21.05% | high valuation で severe loss がさらに重い |
| `distribution_stress_weak_trend` | 20D | 141,388 | -0.077% | -1.177% | 44.38% | 15.01% | 素直な caution。単独 red には少し弱い |
| `distribution_stress_high_valuation` | 20D | 80,319 | -0.376% | -1.624% | 43.12% | 18.59% | 赤候補の中心 |
| `stale_high_valuation_weak_trend` | 20D | 28,659 | -1.100% | -1.309% | 39.64% | 5.79% | severe は低いが win rate がかなり悪い |
| `crowded_no_value` | 60D | 54,724 | +0.984% | -3.530% | 42.91% | 35.73% | 60Dでは左尾がかなり重い |
| `crowded_high_valuation` | 60D | 40,930 | +0.378% | -3.976% | 42.47% | 37.68% | crowded high valuation は長めで危険 |
| `distribution_stress_high_valuation` | 60D | 75,783 | -1.578% | -4.978% | 39.67% | 38.83% | 60Dでも最も赤に近い |
| `stale_high_valuation_weak_trend` | 60D | 27,148 | -2.701% | -3.174% | 34.68% | 24.55% | 持続的に弱い |

#### 結論: ATR20/ATR60 の過熱は crowded high valuation を悪化させるが、stress では救済/悪化が混ざる

`technical_atr_short_interaction_df` の 20D。`atr20_to_atr60_overheat` は `ATR20 change >= 25% AND ATR20/ATR60 >= 1.25`。

| Candidate bucket | Technical state | Obs | Median | Win rate | Severe loss | Read |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `crowded_no_value` | all | 57,209 | -1.121% | 45.55% | 19.23% | baseline |
| `crowded_no_value` | `atr20_to_atr60_overheat` | 10,677 | -1.646% | 43.87% | 21.79% | red 寄りに悪化 |
| `crowded_high_valuation` | all | 43,036 | -1.098% | 45.99% | 21.05% | baseline |
| `crowded_high_valuation` | `atr20_to_atr60_overheat` | 8,536 | -2.060% | 43.10% | 24.18% | crowded high valuation の明確な悪化条件 |
| `distribution_stress_high_valuation` | all | 80,319 | -1.624% | 43.12% | 18.59% | baseline |
| `distribution_stress_high_valuation` | `recent_20d_60d_negative` | 41,596 | -1.800% | 42.00% | 17.62% | trend weak で少し悪化 |
| `distribution_stress_high_valuation` | `atr20_to_atr60_overheat` | 4,847 | -0.563% | 47.84% | 15.95% | stress high valuation では overheat が単純悪化にならない |
| `stale_high_valuation_weak_trend` | all | 28,659 | -1.309% | 39.64% | 5.79% | baseline |
| `stale_high_valuation_weak_trend` | `atr20_acceleration` | 2,775 | -1.951% | 33.98% | 6.77% | stale では ATR加速が悪化寄り |

### Interpretation

この experiment は、既存の緑/青/黄を増やすためのものではない。特に `stale_liquidity` は既存 readout では return red というより investability warning と読まれているため、単独 red にはしない。`stale` は高valuationと弱trendが重なる場合だけ別表で検証する。

`crowded_rerating` は強い value confirmation があれば右尾が残る一方、value が無い場合や高valuationの場合は左尾 risk が重い。今回の run でも 20D severe loss は `crowded_no_value` で `19.23%`、`crowded_high_valuation` で `21.05%` と高い。ただし mean がプラスに残るため、単純 short rule ではなく「赤候補」または「ATR/market regime でさらに絞る候補」と読む。

`distribution_stress_high_valuation` は 20D median `-1.624%`、60D median `-4.978%` で、今回の中では最も素直な red candidate。`distribution_stress_weak_trend` 単独は悪いが、高valuationを重ねた方が赤の意味が明確になる。

`stale_high_valuation_weak_trend` は severe loss が 20D `5.79%` と低い一方、win rate が `39.64%` と悪い。これは急落 tail というより、上がりにくい / 放置されやすい bucket と読む。short candidate というより long 回避・低優先度 red に近い。

### Production Implication

現時点では production / UI rule を変更しない。後続PRで検討するなら優先順位は以下。

| Priority | Candidate | UI implication |
| --- | --- |
| 1 | `distribution_stress_high_valuation` | red overlay の第一候補 |
| 2 | `crowded_high_valuation + atr20_to_atr60_overheat` | crowded の red 昇格候補 |
| 3 | `stale_high_valuation_weak_trend` | short より long 回避 / muted red 候補 |
| 4 | `crowded_no_value` | yellow 維持か、market/ATR 条件付き red を追加検証 |

採用前には live Ranking replay を見て、現時点の銘柄リストが直感的に「赤」と読めるかを確認する。borrow / 約定 / capacity を見ていないため、short execution rule にはしない。

### Caveats

- Prime-only evidence から始め、Standard/Growth へ外挿しない。
- close-to-close diagnostic であり、pre-open screening rule ではない。
- short 実運用には borrow、約定、position sizing、risk cap が別途必要。
- mean が右尾で改善しても、median / severe-loss が悪い bucket は red 候補として扱う。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_short_red_evidence.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_short_red_evidence.py` |
| tests | `apps/bt/tests/unit/domains/analytics/test_ranking_short_red_evidence.py` |
| bundle | `/private/tmp/trading25-research/market-behavior/ranking-short-red-evidence/20260528_ranking_short_red_evidence_prime_v1` |
| result tables | `coverage_diagnostics_df`, `short_red_candidate_df`, `regime_valuation_interaction_df`, `technical_atr_short_interaction_df`, `stale_liquidity_short_diagnostics_df`, `live_ranking_replay_df`, `observation_sample_df` |

## Reproduction

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_short_red_evidence.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --horizons 5,10,20,60 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260528_ranking_short_red_evidence_prime_v1 \
  --min-observations 500
```
