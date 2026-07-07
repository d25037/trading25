# Ranking Short Value Composite Evidence

Daily Ranking Research Base を使い、`forward_per_percentile` と `pbr_percentile` を short-side の高バリュエーション composite として合成し、既存の `High PSR` / `Overvalued Breakdown` / short-red 系条件と同じ母集団で比較する。

合成 score は `high_fwd_per_pbr_composite_score = (forward_per_percentile + pbr_percentile) / 2`。高いほど「高 forward PER + 高 PBR」の合成割高が強い。long scaffold の `Long Hybrid + ATR20 Accel` 由来データは short 判断に使わず、short 専用に `Sector Weak`、`SMA5 count 0/1`、`Crowded Rerating`、`ATR20/60 overheat`、`High PSR` と交差する。

## Published Readout

### Decision

Run: `20260707_short_value_composite_prime_full_history_v1`

対象は Prime 全期間、forward outcome は 20D / 60D close-to-close TOPIX excess return。publication run は `min_observations=100`、tail threshold は excess return `10%`。

結論:

- fwd PER/PBR composite は空売り検索能力を持つ。単体 valuation axis では `score>=0.9` が `High PSR 90%` より 20D/60D median excess と downside tail を悪化させる。
- ただし short探索の最強条件は、既存研究どおり `Crowded + High PSR + Sector Weak` 系。60D median excess は `crowded_high_psr_sector_weak` が `-9.674%`、`crowded_high_psr_overheat_sector_weak` が `-10.672%`。
- fwd PER/PBR composite は PSR-inclusive 群を置換しない。PSRなしでも使える fallback / 補完軸として、`high_fpbr_breakdown` や `crowded_high_fpbr_sector_weak` を short watch に加えるのが自然。
- Broad な `overvalued_breakdown_core` は PSRを含む OR 条件にすると sample は増えるが、median はやや薄まる。pure-short priority は broad OR ではなく、`Crowded + Sector Weak` の中で PSR / fwd PER-PBR / overheat を別 chip として読む。

### Main Findings

#### 結論: 単体 valuation axis では high fwd PER/PBR composite は明確に悪い

`high_fwd_per_pbr_composite_90` は、`high_psr_90` より 20D/60D とも median excess が悪い。単体の割高検索軸としては fwd PER/PBR composite も十分に機能する。

| Valuation axis | Horizon | Obs | Codes | Median excess | Mean excess | Negative rate | Downside tail | Upside tail |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| High fwd PER/PBR composite 80% | 20D | `510,895` | `852` | `-1.108%` | `-0.412%` | `55.49%` | `14.54%` | `12.46%` |
| High fwd PER/PBR composite 90% | 20D | `200,067` | `480` | `-1.476%` | `-0.652%` | `56.43%` | `17.74%` | `14.14%` |
| High PSR 80% | 20D | `761,416` | `846` | `-0.912%` | `-0.259%` | `54.82%` | `12.58%` | `11.51%` |
| High PSR 90% | 20D | `379,580` | `517` | `-1.024%` | `-0.311%` | `55.03%` | `14.64%` | `12.89%` |
| High fwd PER/PBR composite 80% | 60D | `501,656` | `828` | `-3.107%` | `-1.237%` | `58.15%` | `31.54%` | `21.14%` |
| High fwd PER/PBR composite 90% | 60D | `196,895` | `462` | `-4.017%` | `-1.916%` | `59.53%` | `35.51%` | `22.00%` |
| High PSR 80% | 60D | `747,117` | `829` | `-2.598%` | `-0.914%` | `57.26%` | `29.14%` | `20.70%` |
| High PSR 90% | 60D | `372,668` | `506` | `-2.859%` | `-1.063%` | `57.29%` | `31.52%` | `21.99%` |

#### 結論: Breakdown 条件では fwd PER/PBR composite が PSR単独より少し鋭い

`Sector Weak + SMA5 count 0/1` を加えた broad breakdown では、`high_fpbr_breakdown` が `high_psr_breakdown` より 20D/60D median は悪い。ただし sample は PSRより薄く、upside tail もやや残る。

| Short condition | Horizon | Obs | Codes | Median excess | Mean excess | Negative rate | Downside tail | Upside tail |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Overvalued breakdown core | 20D | `176,300` | `1,637` | `-0.803%` | `-0.305%` | `54.76%` | `11.01%` | `10.19%` |
| Overvalued breakdown without PSR | 20D | `153,430` | `1,568` | `-0.867%` | `-0.346%` | `55.03%` | `11.53%` | `10.51%` |
| High fwd PER/PBR breakdown | 20D | `57,461` | `691` | `-1.104%` | `-0.513%` | `55.40%` | `14.63%` | `12.34%` |
| High PSR breakdown | 20D | `87,517` | `739` | `-0.858%` | `-0.365%` | `54.63%` | `12.74%` | `11.35%` |
| Overvalued breakdown core | 60D | `172,346` | `1,621` | `-2.597%` | `-1.070%` | `58.12%` | `27.19%` | `18.88%` |
| Overvalued breakdown without PSR | 60D | `149,962` | `1,550` | `-2.741%` | `-1.169%` | `58.41%` | `28.12%` | `19.12%` |
| High fwd PER/PBR breakdown | 60D | `56,549` | `680` | `-3.347%` | `-1.623%` | `58.99%` | `31.75%` | `20.56%` |
| High PSR breakdown | 60D | `86,338` | `729` | `-2.785%` | `-1.364%` | `58.14%` | `29.39%` | `19.85%` |

#### 結論: 最強の空売り検索はやはり `Crowded + High PSR + Sector Weak`

`Crowded Rerating + Sector Weak` に固定すると、High PSR が最も強い。fwd PER/PBR composite も悪いが、PSR 群より median と downside tail は弱い。ATR overheat を重ねると両方とも悪化するが、ここでも PSR が勝つ。

| Short condition | Horizon | Obs | Codes | Median excess | Mean excess | Negative rate | Downside tail | Upside tail |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Crowded overvalued sector weak | 20D | `5,256` | `392` | `-2.337%` | `-1.486%` | `59.23%` | `24.32%` | `15.66%` |
| Crowded high fwd/PBR sector weak | 20D | `2,578` | `213` | `-2.212%` | `-1.001%` | `57.60%` | `25.52%` | `17.03%` |
| Crowded high PSR sector weak | 20D | `3,008` | `228` | `-3.510%` | `-2.529%` | `61.54%` | `28.86%` | `14.66%` |
| Crowded high fwd/PBR overheat sector weak | 20D | `615` | `108` | `-3.658%` | `-1.746%` | `60.49%` | `29.27%` | `17.24%` |
| Crowded high PSR overheat sector weak | 20D | `735` | `112` | `-4.482%` | `-2.208%` | `61.09%` | `32.11%` | `17.96%` |
| Crowded overvalued sector weak | 60D | `5,180` | `389` | `-7.642%` | `-3.735%` | `63.90%` | `45.37%` | `22.57%` |
| Crowded high fwd/PBR sector weak | 60D | `2,526` | `211` | `-6.991%` | `-2.566%` | `62.31%` | `44.30%` | `24.62%` |
| Crowded high PSR sector weak | 60D | `2,981` | `226` | `-9.674%` | `-5.467%` | `66.66%` | `49.41%` | `20.50%` |
| Crowded high fwd/PBR overheat sector weak | 60D | `598` | `106` | `-9.053%` | `-3.212%` | `61.87%` | `48.49%` | `25.59%` |
| Crowded high PSR overheat sector weak | 60D | `731` | `111` | `-10.672%` | `-5.197%` | `66.07%` | `50.75%` | `21.75%` |

#### 結論: stress / stale では composite も使えるが、主力ではない

`distribution_stress` は high fwd/PBR と high PSR の差が小さい。`stale rally fade` では fwd/PBR の方が 60D median は悪いが、左尾は crowded 系ほど強くない。既存 short-red 研究どおり、stale は pure short というより relative red / long回避寄りに読む。

| Short condition | Horizon | Obs | Codes | Median excess | Mean excess | Negative rate | Downside tail | Upside tail |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Distribution stress high fwd/PBR sector weak | 60D | `10,071` | `295` | `-4.143%` | `-0.546%` | `58.67%` | `38.53%` | `26.56%` |
| Distribution stress high PSR sector weak | 60D | `14,836` | `330` | `-3.930%` | `-0.730%` | `58.36%` | `37.29%` | `25.67%` |
| Stale high fwd/PBR rally fade | 60D | `13,555` | `185` | `-3.383%` | `-2.523%` | `61.82%` | `27.88%` | `13.69%` |
| Stale high PSR rally fade | 60D | `26,730` | `227` | `-2.929%` | `-2.061%` | `60.15%` | `25.62%` | `14.74%` |

### Interpretation

今回の検証は、long hybrid のデータを流用せず、short 専用の母集団で作り直した。したがって結論は「long scaffold で良かったから short でも良い」ではなく、「short-side の overvaluation / sector weakness / SMA weakness / crowded / overheat の中で、fwd PER/PBR composite が PSR と比べてどこに立つか」になっている。

fwd PER/PBR composite は、単体の高割高 rank としては PSR より鋭い局面がある。特に `score>=0.9` や `high_fpbr_breakdown` は 60D median と downside tail が悪く、空売り候補検索の補助軸として有効。

一方で、既存の PSR 研究と整合して、`Crowded + High PSR + Sector Weak` は最も強い short-side confirmation のまま。PSR は売上ベースなので PER 欠損や利益の一時性に強く、crowded rerating の「売上対比で買われすぎ」を拾いやすい。今回の fwd PER/PBR composite はこの群を置換せず、PSRがない/薄い場合や、PBRとfwd PERが同時に高い銘柄を別chipで昇格するために使う。

Broad OR の `overvalued_breakdown_core` は実用上の recall は高いが、最強の short search ではない。pure-short priority は、`Crowded`、`Sector Weak`、`High PSR`、`ATR overheat`、または `High fwd/PBR composite` を個別に表示し、重なりを見て順位付けする方がよい。

### Production Implication

- short-side UI / watchlist には `high_fwd_per_pbr_composite_score` を diagnostic として追加する価値がある。
- 初期 chip は `High fwd/PBR composite >=0.9`、`>=0.8` の2段階が自然。
- pure-short priority の主軸は引き続き `Crowded + High PSR + Sector Weak`。`High fwd/PBR composite` は PSR-inclusive chip の置換ではなく補完。
- `Sector Weak + SMA5 count 0/1 + High fwd/PBR composite` は broad breakdown fallback として有効だが、単独 strategy rule にはしない。
- `Crowded + High fwd/PBR + Sector Weak + ATR overheat` は強いが sample が薄く、PSR overheat 群より upside tail が残るため、borrow/cost/position sizing 前提の次回 portfolio lens が必要。

### Caveats

- outcome は 20D/60D close-to-close TOPIX excess return。空売りの約定、貸株料、逆日歩、borrow availability、stop policy は未検証。
- 対象は Prime full-history。market-specific rule 化には Standard / Growth と period split の再確認が必要。
- `high_fwd_per_pbr_composite_score` は `forward_per_percentile` と `pbr_percentile` が両方ある観測だけに付く。coverage は Prime 全観測で `85.36%`。
- PSR は既存 `ranking-psr-valuation-evidence` と同じく research-only 算出で、永続 SoT 列ではない。
- `Crowded + overheat` 系は sample が薄く、portfolio lens と live replay なしで hard rule 化しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_short_value_composite_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_short_value_composite_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_short_value_composite_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-short-value-composite-evidence/20260707_short_value_composite_prime_full_history_v1/`
- Results tables: `observation_sample_df`, `coverage_diagnostics_df`, `valuation_axis_evidence_df`, `short_search_condition_evidence_df`
