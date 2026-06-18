# Ranking Liquidity Price Action Recomposition

Daily Ranking Research Base を使い、`liquidity_residual_z >= 1` の内側を 20D / 60D price action の4象限へ分解する。目的は、既存の `crowded_rerating` と `distribution_stress` をラベルそのものではなく、`20D > 0` / `60D > 0` の構成要素として再構成し、short-side の `Weak Sector` / `High PSR` とどう重なるかを検証すること。

## Published Readout

### Decision

Run: `20260618_liquidity_price_action_recomposition_prime_2024_v2`

対象は Prime、`analysis_start_date=2024-01-01`、forward outcome は 20D / 60D close-to-close TOPIX excess return。`liquidity_residual_z >= 1` を固定し、price action は strict に `recent_return_20d_pct > 0 / < 0` と `recent_return_60d_pct > 0 / < 0` で切った。

結論:

- mixed price action、つまり `20D > 0, 60D < 0` と `20D < 0, 60D > 0` は、どちらも `dual_positive_crowded` より悪い。ただし単体では `dual_negative_stress` より少し軽い。
- `distribution_stress` は一枚岩ではない。2024年以降 Prime の high-liquidity 観測では、`20D > 0, 60D < 0` が `14.4%`、`20D < 0, 60D > 0` が `16.8%`、`20D < 0, 60D < 0` が `34.4%`。
- short 側では、price-action の mixed 化そのものよりも `High PSR` と `Sector Weak` の重なりが強い。特に `dual_positive_crowded + High PSR + Sector Weak` は 60D median TOPIX excess `-13.933%`、severe loss `58.66%` で最も悪い。
- したがって、「crowded rerating を stress 方向へ分解する」次の実務読みにするなら、mixed bucket を単独 short label にするより、`liquidity_residual_z >= 1` の全象限で `High PSR` / `Sector Weak` を escalation overlay として優先する。

### Main Findings

#### 結論: mixed 20D/60D は crowded より弱いが、stress 本体ほどではない

| bucket | horizon | obs | median excess | win rate | severe loss | sector weak rate | median PSR percentile |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| dual_positive_crowded | 20D | 41,980 | -0.754% | 47.01% | 18.59% | 6.11% | 0.649 |
| recent20_positive_60d_negative | 20D | 18,033 | -1.051% | 45.34% | 16.47% | 8.62% | 0.573 |
| recent20_negative_60d_positive | 20D | 20,834 | -0.950% | 46.00% | 17.85% | 11.94% | 0.626 |
| dual_negative_stress | 20D | 42,407 | -1.438% | 43.26% | 15.49% | 17.32% | 0.586 |
| dual_positive_crowded | 60D | 39,357 | -2.268% | 45.35% | 33.53% | 6.41% | 0.642 |
| recent20_positive_60d_negative | 60D | 16,820 | -2.976% | 43.02% | 33.40% | 8.97% | 0.565 |
| recent20_negative_60d_positive | 60D | 19,182 | -3.138% | 43.48% | 35.25% | 12.62% | 0.624 |
| dual_negative_stress | 60D | 39,380 | -3.342% | 41.72% | 33.13% | 17.93% | 0.588 |

#### 結論: `-1 < liquidity z < 1` でも High PSR + Sector Weak は short 候補になる

Follow-up run: `20260619_liquidity_band_price_action_recomposition_prime_2024`

`liquidity_residual_z >= 1` だけでなく、`-1 < z < 1` と `z < -1` も同じ price-action / short-overlay grid で比較した。短期・中期の純粋な悪化幅は high-liquidity が最も鋭いが、`-1 < z < 1` でも `High PSR + Sector Weak` を重ねると sample が厚く、60D median が大きく悪化する。`z < -1` は一部の dual-positive + High PSR + Sector Weak が悪いが、stale/capacity caution として扱い、primary short にはしない。

| liquidity band | bucket | overlay | 60D obs | 60D median excess | 60D severe loss |
| --- | --- | --- | ---: | ---: | ---: |
| `z >= 1` | dual_positive_crowded | high_psr_sector_weak | 941 | -13.933% | 58.66% |
| `z >= 1` | recent20_negative_60d_positive | high_psr_sector_weak | 908 | -10.887% | 52.86% |
| `z < -1` | dual_positive_crowded | high_psr_sector_weak | 752 | -9.888% | 49.34% |
| `-1 < z < 1` | recent20_positive_60d_negative | high_psr_sector_weak | 1,503 | -9.596% | 49.17% |
| `-1 < z < 1` | dual_positive_crowded | high_psr_sector_weak | 3,854 | -9.295% | 47.72% |
| `-1 < z < 1` | dual_negative_stress | high_psr_sector_weak | 6,378 | -7.183% | 41.64% |
| `-1 < z < 1` | recent20_negative_60d_positive | high_psr_sector_weak | 2,696 | -6.758% | 40.50% |

#### 結論: short-side escalation は price-action bucket より High PSR / Sector Weak が支配的

| bucket | overlay | horizon | obs | median excess | win rate | severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dual_positive_crowded | all_high_liquidity | 20D | 41,980 | -0.754% | 47.01% | 18.59% |
| dual_positive_crowded | high_psr | 20D | 14,883 | -1.724% | 44.41% | 24.47% |
| dual_positive_crowded | sector_weak | 20D | 2,564 | -2.970% | 35.96% | 22.54% |
| dual_positive_crowded | high_psr_sector_weak | 20D | 951 | -5.609% | 31.65% | 34.28% |
| recent20_positive_60d_negative | all_high_liquidity | 20D | 18,033 | -1.051% | 45.34% | 16.47% |
| recent20_positive_60d_negative | high_psr | 20D | 4,927 | -1.266% | 45.32% | 21.25% |
| recent20_positive_60d_negative | sector_weak | 20D | 1,555 | -1.929% | 40.45% | 19.04% |
| recent20_negative_60d_positive | all_high_liquidity | 20D | 20,834 | -0.950% | 46.00% | 17.85% |
| recent20_negative_60d_positive | high_psr_sector_weak | 20D | 918 | -2.626% | 40.09% | 24.40% |
| dual_negative_stress | all_high_liquidity | 20D | 42,407 | -1.438% | 43.26% | 15.49% |
| dual_negative_stress | high_psr_sector_weak | 20D | 2,402 | -3.366% | 37.14% | 25.19% |
| dual_positive_crowded | all_high_liquidity | 60D | 39,357 | -2.268% | 45.35% | 33.53% |
| dual_positive_crowded | high_psr_sector_weak | 60D | 941 | -13.933% | 26.14% | 58.66% |
| recent20_positive_60d_negative | all_high_liquidity | 60D | 16,820 | -2.976% | 43.02% | 33.40% |
| recent20_positive_60d_negative | high_psr | 60D | 4,545 | -5.750% | 38.35% | 40.97% |
| recent20_positive_60d_negative | sector_weak | 60D | 1,509 | -6.053% | 33.27% | 39.83% |
| recent20_negative_60d_positive | all_high_liquidity | 60D | 19,182 | -3.138% | 43.48% | 35.25% |
| recent20_negative_60d_positive | high_psr_sector_weak | 60D | 908 | -10.887% | 29.52% | 52.86% |
| dual_negative_stress | all_high_liquidity | 60D | 39,380 | -3.342% | 41.72% | 33.13% |
| dual_negative_stress | high_psr_sector_weak | 60D | 2,355 | -6.543% | 35.58% | 42.08% |

### Interpretation

既存の `crowded_rerating` は `liquidity_residual_z >= 1` かつ 20D/60D がともに非負の positive-rerating label だが、今回 strict `> 0` で切っても high-liquidity の dual-positive 自体は 20D/60D とも median がマイナスだった。つまり crowded は long positive label ではなく、既存 readout と同じく adverse regime と読む。

混合bucketは、`20D > 0, 60D < 0` が「長めでは崩れているが短期で戻した」形、`20D < 0, 60D > 0` が「長めでは残っているが短期で崩れた」形。両者は `dual_positive_crowded` より悪いが、最も悪い単体bucketはおおむね `dual_negative_stress`。したがって mixed bucket 単独を新しい strongest short にする根拠は弱い。

一方、`High PSR` と `Sector Weak` を重ねると、price-action の違いを超えて左尾が厚くなる。特に dual-positive のまま sector が弱く PSR が高い群は、見た目は crowded rerating でも forward outcome はかなり悪い。これは「上がっていて流動性も集まっているが、売上対比で高く、セクターも弱い」状態を pure-short priority として読む方が自然。

### Production Implication

- `crowded_rerating` と `distribution_stress` の label 定義は変更しない。今回の研究は raw `liquidity_residual_z >= 1` 内部の診断軸として扱う。
- Daily Ranking の short-side priority では、mixed 20D/60D bucket を単独 escalation にしない。`High PSR`、`Sector Weak`、またはその overlap を優先する。
- `dual_positive_crowded + High PSR + Sector Weak` は、既存の `Crowded + PSR Overvalued + Sector Weak` と整合して強い short/caution 候補。UI に出すなら price-action ラベルより `High PSR` / `Sector Weak` の escalation chip が先。
- `20D < 0, 60D > 0` は 60D severe loss がやや重く、短期崩れの警戒として補助診断にできる。ただし `High PSR + Sector Weak` なしで primary short rule にするほどではない。
- `-1 < liquidity z < 1` は high-liquidity ほど鋭くないが、`High PSR + Sector Weak` と重なる場合は fallback short candidate として有効。特に `20D > 0, 60D < 0` と dual-positive は sample と悪化幅のバランスが良い。
- `liquidity z < -1` は stale/capacity caution が強い。dual-positive + `High PSR + Sector Weak` は悪いが、流動性制約が先に来るため、Ranking の primary short priority ではなく exception / low-capacity caution として扱う。

### Caveats

- outcome は 20D/60D close-to-close TOPIX excess return。short trade の borrow / execution / stop / sizing は含まない。
- `High PSR + Sector Weak` は強いが、bucket によっては sample が 1,000 件前後まで薄くなる。`recent20_positive_60d_negative + High PSR + Sector Weak` は `min_observations=500` を満たさない horizon があり、表には出していない。
- `crowded_rerating` の production label は現行 code では `>= 0` 判定を使うが、本研究の recomposition はユーザー指定に合わせて strict `> 0` / `< 0` で切った。ゼロ近辺の差は主結論に影響しにくいが、実装ラベルそのものの置換ではない。
- 対象は 2024年以降の Prime。過去全期間や Standard/Growth へは外挿しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_liquidity_price_action_recomposition.py`
- Module: `apps/bt/src/domains/analytics/ranking_liquidity_price_action_recomposition.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_liquidity_price_action_recomposition.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-liquidity-price-action-recomposition/20260618_liquidity_price_action_recomposition_prime_2024_v2/`
- Liquidity band follow-up bundle: `~/.local/share/trading25/research/market-behavior/ranking-liquidity-price-action-recomposition/20260619_liquidity_band_price_action_recomposition_prime_2024/`
- Results table: `price_action_bucket_evidence_df`, `short_overlay_evidence_df`
