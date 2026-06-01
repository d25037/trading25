# Rerating Bubble Regime Forward Response

`market-bubble-footprint` の regime を Daily Ranking の `neutral_rerating` / `crowded_rerating` と接続し、bubble footprint 悪化時にどの rerating bucket の forward TOPIX-excess が残るかを見る研究。

## Published Readout

### Decision

`blowoff_watch` では、rerating 系を「数カ月上昇狙い」として一括採用しない。特に `crowded_rerating` は 20D / 60D ともに悪く、no-value はさらに悪い。`neutral_rerating` も `blowoff_watch` では 60D が大きく崩れるため、数カ月 hold の主軸にはしない。

一方、`narrowing` では `neutral_rerating + medium/strong value confirmation` が 20D と 60D でまだ残る。したがって次の一手は、bubble footprint が `narrowing` までなら quality/value-backed rerating を短中期で狙い、`blowoff_watch` では holding horizon を短くし、`crowded_rerating` と value なしを落とす運用が妥当。

### Main Findings

#### 結論: `narrowing` では neutral rerating の value-backed bucket が残る

Publication slice `20260601_rerating_bubble_regime_2024_v1` は `2024-01-01` 以降、Prime / Standard / Growth、月次 anchor。下表は Prime、footprint horizon `120`、forward horizon `20/60`。

| Bubble regime | Liquidity regime | Value condition | Horizon | Obs | Median excess | Win | Severe loss |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `narrowing` | `neutral_rerating` | all | 20 | 2,799 | +0.94% | 55.48% | 7.43% |
| `narrowing` | `neutral_rerating` | all | 60 | 2,725 | +0.01% | 50.02% | 19.41% |
| `narrowing` | `neutral_rerating` | medium value | 20 | 758 | +1.74% | 59.63% | 3.96% |
| `narrowing` | `neutral_rerating` | medium value | 60 | 742 | +1.19% | 53.64% | 17.65% |
| `narrowing` | `neutral_rerating` | strong value | 20 | 230 | +2.31% | 62.61% | 3.48% |
| `narrowing` | `neutral_rerating` | strong value | 60 | 227 | +1.09% | 53.30% | 17.18% |

`narrowing` はまだ「買える regime」だが、value confirmation がない broad bucket では 60D severe loss が 19% 台まで上がる。数カ月狙いは broad rerating ではなく、medium/strong value confirmation へ絞る。

#### 結論: `crowded_rerating` は narrowing でも弱く、blowoff_watch では避ける

| Bubble regime | Liquidity regime | Value condition | Horizon | Obs | Median excess | Win | Severe loss |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `narrowing` | `crowded_rerating` | all | 20 | 421 | -1.90% | 40.86% | 21.62% |
| `narrowing` | `crowded_rerating` | all | 60 | 410 | -1.57% | 45.61% | 33.17% |
| `narrowing` | `crowded_rerating` | no value | 20 | 298 | -2.24% | 38.93% | 24.83% |
| `narrowing` | `crowded_rerating` | no value | 60 | 292 | -1.77% | 45.89% | 33.90% |
| `blowoff_watch` | `crowded_rerating` | all | 20 | 130 | -4.51% | 26.92% | 31.54% |
| `blowoff_watch` | `crowded_rerating` | all | 60 | 129 | -6.30% | 37.21% | 41.09% |
| `blowoff_watch` | `crowded_rerating` | no value | 20 | 96 | -5.83% | 26.04% | 33.33% |
| `blowoff_watch` | `crowded_rerating` | no value | 60 | 95 | -6.30% | 37.89% | 43.16% |

過去の Ranking readout と同じく、`crowded_rerating` は右尾を持つが左尾が重い。bubble footprint が悪化した局面では、数カ月上昇を狙う主戦場ではない。

#### 結論: blowoff_watch では neutral rerating も 60D が崩れる

| Bubble regime | Liquidity regime | Value condition | Horizon | Obs | Median excess | Win | Severe loss |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `blowoff_watch` | `neutral_rerating` | all | 20 | 857 | -0.67% | 46.09% | 8.87% |
| `blowoff_watch` | `neutral_rerating` | all | 60 | 856 | -7.61% | 30.84% | 43.57% |
| `blowoff_watch` | `neutral_rerating` | medium value | 20 | 242 | -0.10% | 49.59% | 5.79% |
| `blowoff_watch` | `neutral_rerating` | medium value | 60 | 241 | -7.62% | 33.61% | 46.89% |
| `blowoff_watch` | `neutral_rerating` | strong value | 20 | 65 | -0.34% | 46.15% | 4.62% |
| `blowoff_watch` | `neutral_rerating` | strong value | 60 | 65 | -11.58% | 26.15% | 53.85% |

この slice では、`blowoff_watch` の 60D は value confirmation でも救えない。短期 20D でも median はほぼ中立からマイナス。したがって「今から数カ月の上昇を狙う」なら、`blowoff_watch` では新規 rerating exposure を広げない。

### Interpretation

この研究は `market-bubble-footprint` の regime overlay と、Ranking の rerating bucket を接続する。bubble footprint は market state、rerating は individual stock state なので、単体では売買判断にしない。

結果は、「終盤入口でも上がる銘柄はある」という直感を一部支持するが、条件は狭い。`narrowing` では `neutral_rerating` の medium/strong value confirmation が 20D/60D ともに残る。一方で `blowoff_watch` では、`neutral_rerating` でも 60D が崩れ、`crowded_rerating` は明確に悪い。

したがって、足元のように複数 horizon が `blowoff_watch` / `crowded` に入る局面では「数カ月上昇を狙う」より「短期だけ、value-backed neutral に限定し、crowded/no-value を落とす」が自然。

### Production Implication

| Bubble regime | Rerating implication |
| --- | --- |
| `normal` | 既存 Ranking evidence を通常適用 |
| `narrowing` | `neutral_rerating + medium/strong value` を主戦場にする |
| `crowded` | `crowded_rerating` を value-backed に絞り、left-tail を明示 |
| `blowoff_watch` | 新規数カ月 hold を抑制。`crowded_rerating` と no-value は long 回避候補 |

Ranking UI に出すなら、最初は売買 signal ではなく `Market regime: blowoff watch` のような overlay / warning として出す。`crowded_rerating` を即 short にするのではなく、long avoidance / sizing haircut の候補として扱う。

### Caveats

- Publication slice は `2024-01-01` 以降。全期間 `2018+` run は重いため、runner は実装済みだが、今回の readout は tactical recent slice として読む。
- Outcome は close-to-close TOPIX-excess return。execution cost、portfolio sizing、turnover は含まない。
- `strong_value_confirmation` は sample が細る。特に `blowoff_watch` では過信しない。
- `bubble_regime` は crash timing signal ではなく market footprint label。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/market_bubble_footprint.py`
- Runner: `apps/bt/scripts/research/run_rerating_bubble_regime_forward_response.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_market_bubble_footprint.py`
- Bundle: `/private/tmp/trading25-research/market-behavior/rerating-bubble-regime-forward-response/20260601_rerating_bubble_regime_2024_v1/`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/market_bubble_footprint.py`
- Runner:
  - `apps/bt/scripts/research/run_rerating_bubble_regime_forward_response.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_rerating_bubble_regime_forward_response.py \
  --start-date 2024-01-01 \
  --signal-horizons 20,60 \
  --footprint-horizons 60,120 \
  --markets prime,standard,growth \
  --frequency monthly \
  --min-observations 30 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260601_rerating_bubble_regime_2024_v1
```
