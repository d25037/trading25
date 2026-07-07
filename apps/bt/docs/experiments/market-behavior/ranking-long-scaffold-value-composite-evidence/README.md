# Ranking Long Scaffold Value Composite Evidence

Daily Ranking Research Base を使い、`forward_per_percentile` と `pbr_percentile` を PIT-safe な同日 cross-sectional rank として合成し、既存 long scaffold の hard AND value 条件を単一 score に置き換えられるかを検証する。

合成 score は `low_forward_per_score = 1 - forward_per_percentile`、`low_pbr_score = 1 - pbr_percentile`、`value_composite_equal_score = (low_forward_per_score + low_pbr_score) / 2`。高いほど「低 forward PER + 低 PBR」の合成 value が強い。

## Published Readout

### Decision

Run: `20260707_long_scaffold_value_composite_prime_full_history_v3`

対象は Prime 全期間、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。publication run は `min_observations=100`。

結論:

- `value_composite_equal_score` は、`Long Hybrid Leadership + ATR20 Accel` 土台では 20D median excess と強く単調に相関する。20D の bucket-level Spearman は `1.000`、neutral rerating に絞っても `0.983`。
- したがって、PBR と forward PER を別々の hard AND 条件だけで扱うより、合成 score を ranking / priority diagnostic として持つ価値は高い。
- ただし `score >= 0.8` を既存 `Deep Value` の丸ごと置換にするのはまだ早い。`Deep Value + Long Hybrid + ATR20 Accel` より sample は増えるが、20D/60D median は落ちる。
- 現時点の実務解は、既存 `Deep Value` を eligibility として維持しつつ、`value_composite_equal_score >= 0.9` / `0.8..0.9` を priority ordering と tie-breaker に使うこと。hard filter としては `score >= 0.8` より、neutral + existing deep value の方が安定する。

### Main Findings

#### 結論: 合成 score は Long Hybrid + ATR20 Accel 土台で 20D median excess と強く相関する

| Scaffold | Horizon | Buckets | Pearson | Spearman | Top bucket median | Bottom bucket median | Spread |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| all_market | 20D | 9 | `0.997` | `1.000` | `-0.098%` | `-1.116%` | `+1.017pp` |
| long_hybrid_atr20_accel | 20D | 9 | `0.965` | `1.000` | `+1.345%` | `-1.508%` | `+2.853pp` |
| neutral_long_hybrid_atr20_accel | 20D | 9 | `0.971` | `0.983` | `+2.475%` | `-1.609%` | `+4.084pp` |
| long_hybrid_atr20_accel | 60D | 9 | `0.901` | `0.933` | `+0.857%` | `-2.297%` | `+3.155pp` |
| neutral_long_hybrid_atr20_accel | 60D | 9 | `0.960` | `0.967` | `+3.062%` | `-3.836%` | `+6.898pp` |

#### 結論: 20D では score bucket がほぼ単調に効く

| Scaffold | Score bucket | Obs | Codes | Median excess | p10 | Severe loss | Win rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| long_hybrid_atr20_accel | `>=0.90` | `2,374` | `98` | `+1.345%` | `-5.515%` | `1.60%` | `60.49%` |
| long_hybrid_atr20_accel | `0.80..0.90` | `2,845` | `229` | `+0.960%` | `-6.610%` | `3.13%` | `57.01%` |
| long_hybrid_atr20_accel | `0.70..0.80` | `3,692` | `318` | `+0.441%` | `-6.840%` | `4.31%` | `52.71%` |
| long_hybrid_atr20_accel | `0.60..0.70` | `3,734` | `404` | `-0.387%` | `-8.550%` | `7.61%` | `47.30%` |
| long_hybrid_atr20_accel | `<0.20` | `6,572` | `402` | `-1.508%` | `-14.130%` | `18.99%` | `43.24%` |
| neutral_long_hybrid_atr20_accel | `>=0.90` | `1,332` | `67` | `+2.475%` | `-4.188%` | `1.50%` | `70.72%` |
| neutral_long_hybrid_atr20_accel | `0.80..0.90` | `1,187` | `136` | `+2.336%` | `-5.877%` | `2.11%` | `66.39%` |
| neutral_long_hybrid_atr20_accel | `0.70..0.80` | `1,577` | `201` | `+1.373%` | `-6.318%` | `3.68%` | `58.59%` |
| neutral_long_hybrid_atr20_accel | `<0.20` | `2,094` | `251` | `-1.609%` | `-11.950%` | `15.19%` | `42.31%` |

#### 結論: composite は有用だが、既存 Deep Value の丸ごと置換ではない

| Scaffold | Horizon | Obs | Codes | Median excess | Mean excess | p10 | Severe loss | Win rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| long_hybrid_atr20_accel | 20D | `46,971` | `1,816` | `-0.616%` | `+0.228%` | `-10.059%` | `10.11%` | `46.42%` |
| value_composite_long_hybrid_atr20_accel (`score>=0.8`) | 20D | `5,219` | `249` | `+1.102%` | `+2.050%` | `-6.084%` | `2.43%` | `58.59%` |
| deep_value_long_hybrid_atr20_accel | 20D | `3,545` | `183` | `+1.370%` | `+2.614%` | `-5.670%` | `2.17%` | `60.76%` |
| neutral_deep_value_long_hybrid_atr20_accel | 20D | `1,845` | `111` | `+2.531%` | `+3.402%` | `-4.088%` | `1.25%` | `70.46%` |
| value_composite_long_hybrid_atr20_accel (`score>=0.8`) | 60D | `5,079` | `240` | `+0.717%` | `+3.577%` | `-13.748%` | `17.35%` | `52.55%` |
| deep_value_long_hybrid_atr20_accel | 60D | `3,460` | `178` | `+1.765%` | `+4.920%` | `-13.381%` | `16.21%` | `55.81%` |
| neutral_deep_value_long_hybrid_atr20_accel | 60D | `1,818` | `108` | `+3.239%` | `+6.905%` | `-9.164%` | `8.09%` | `63.81%` |

#### 結論: date-basket でも composite は positive だが、neutral + Deep Value が勝つ

`score>=0.8` の date-level equal-weight basket では、20D median date excess は `+1.429%`、60D は `+2.149%`。有効ではあるが、`neutral_deep_value_long_hybrid_atr20_accel` は 20D `+2.244%`、60D `+4.786%` で、date-level IR も高い。

| Scaffold | Horizon | Dates | Obs | Median date excess | p10 date | Positive date rate | Severe date rate | Date IR |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| value_composite_long_hybrid_atr20_accel | 20D | `1,010` | `5,219` | `+1.429%` | `-5.674%` | `61.19%` | `1.88%` | `0.277` |
| deep_value_long_hybrid_atr20_accel | 20D | `789` | `3,379` | `+1.354%` | `-5.518%` | `60.84%` | `2.28%` | `0.288` |
| neutral_deep_value_long_hybrid_atr20_accel | 20D | `491` | `1,791` | `+2.244%` | `-5.950%` | `65.17%` | `2.44%` | `0.371` |
| value_composite_long_hybrid_atr20_accel | 60D | `990` | `5,079` | `+2.149%` | `-10.568%` | `57.27%` | `11.62%` | `0.285` |
| deep_value_long_hybrid_atr20_accel | 60D | `769` | `3,294` | `+3.139%` | `-10.948%` | `60.08%` | `11.44%` | `0.339` |
| neutral_deep_value_long_hybrid_atr20_accel | 60D | `478` | `1,764` | `+4.786%` | `-8.876%` | `69.04%` | `7.95%` | `0.463` |

### Interpretation

今回の合成 score は、ユーザーの仮説どおり `forward PER` と `PBR` の PIT percentile を単一の連続指標にした方が、AND 条件より ranking / ordering に向いていることを示した。特に `Long Hybrid + ATR20 Accel` の土台では、score が上がるほど 20D median excess がほぼ単調に改善し、left-tail も軽くなる。

一方で、`score>=0.8` は `PBR<=20% AND Fwd PER<=20%` より広い。広げた分だけ observations と codes は増えるが、既存 Deep Value strict scaffold の中央値と tail を完全には超えない。つまり composite は「AND を廃止する置換条件」ではなく、「既存 value eligibility の内外を連続順位で並べる score」として使う方が自然。

`neutral` を残したときの改善が大きい点も重要。value composite が高くても liquidity/rerating state を無視すると 60D の安定性は落ちる。直近の long scaffold 研究と同じく、value axis だけで green にせず、`neutral_rerating` / `z -1..2` / sector / ATR の文脈で読む。

### Production Implication

- Ranking / Watchlist の long-side priority には、`value_composite_equal_score` を追加する価値がある。
- 初期 implementation では `score>=0.9` を high value composite、`0.8..0.9` を good value composite として、既存 `Deep Value` の中の ordering / tie-breaker に使う。
- `score>=0.8` だけで `Deep Value` hard condition を置き換えない。置換するなら portfolio lens、sector cap、turnover、cost、neutral/z overlay を含めて再検証する。
- `score<0.6` は Long Hybrid + ATR20 Accel の中でも 20D median がマイナスに落ちるため、priority-down / review cue として有効。

### Caveats

- outcome は 5D/20D/60D close-to-close TOPIX excess return。portfolio performance ではない。
- `value_composite_equal_score` は `forward_per_percentile` と `pbr_percentile` が両方ある観測だけに付く。coverage は Prime 全観測で `74.31%`。
- score は equal-weight。Standard の過去 value composite のような market-specific weight はまだ試していない。
- `score>=0.8` の date-basket は median daily observations が薄い日もあり、position sizing / capacity には使えない。
- Bundle 生成時点の local `market.duckdb` は live source。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_long_scaffold_value_composite_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_long_scaffold_value_composite_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_long_scaffold_value_composite_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-long-scaffold-value-composite-evidence/20260707_long_scaffold_value_composite_prime_full_history_v3/`
- Results tables: `long_scaffold_evidence_df`, `value_composite_bucket_evidence_df`, `long_scaffold_value_composite_bucket_evidence_df`, `value_composite_bucket_correlation_df`, `date_basket_evidence_df`, `coverage_diagnostics_df`
