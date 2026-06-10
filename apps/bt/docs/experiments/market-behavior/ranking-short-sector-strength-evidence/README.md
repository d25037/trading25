# Ranking Short Sector Strength Evidence

Daily Ranking の short / red 候補に PIT 33セクター強弱を重ね、個別銘柄がセクターベータを付与されることで TOPIX にさらに負けるかを検証する runner-first research です。

## Published Readout

### Decision

short 側の sector overlay は「long の逆で常に sector weak が良い」とは扱わない。`crowded_rerating` / `distribution_stress` の割高・no value 候補では `sector_weak` が明確な short tailwind になる。一方、`stale_overvalued` は sector weak よりも「stale そのもの」と「recent positive rally fade」の効果が強く、20D では `sector_strong` でも TOPIX excess は悪い。したがって production 表示では、short 候補の主条件を value / liquidity / technical で決め、sector score は候補ごとに downgrade / confirmation として読む。

Primary outcome は既存の short-red research と揃え、`20D close-to-close TOPIX excess return`。`60D` は持続性、`raw` / `TOPIX` / `excess` の併記で裸short と relative short を分ける。sector strength は 33業種ごとに、公式33業種指数 score と構成銘柄 score の平均を採用し、`score >= 0.8` を `sector_strong`、`score <= 0.2` を `sector_weak` とする。

### Main Findings

#### 結論: average score でも bad crowded の sector weak は売り側に効く

2026-06-01 update `20260601_ranking_short_sector_strength_average_score_v4` で、sector score は long 側と同じ average score に統一した。構成銘柄onlyは short 側では最も鋭いが、サービス業 `0060` のような公式指数主導の sector beta を過小評価する。index-only は buy 側を改善する一方で short の `sector_weak` が鈍る。平均scoreは、buy strong の 20D median `+2.014%` を維持しつつ、bad valuation crowded + `sector_weak` の 20D excess median を `crowded_overvalued` で `-3.309%`、`crowded_no_value` で `-3.171%` 残すため、production の単一 sector score として最もバランスが良い。

| Bucket | Sector | Obs | 20D excess median | Excess negative | Downside tail | Raw median | Read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `crowded_overvalued` | `sector_weak` | 3,364 | -1.623% | 56.81% | 22.27% | -1.487% | 最も純shortに近い |
| `crowded_overvalued` | `sector_strong` | 6,577 | -0.910% | 53.03% | 22.43% | +0.846% | relative short 寄り |
| `crowded_no_value` | `sector_weak` | 4,585 | -1.670% | 57.49% | 20.02% | -1.302% | 純short候補 |
| `crowded_no_value` | `sector_strong` | 8,920 | -1.192% | 54.64% | 20.95% | +0.162% | sector beta がrawを支える |
| `strong_low_value_sector_strong_short_prohibit` | `sector_strong` | 16,012 | +1.291% | 41.26% | 3.92% | n/a | short禁止側 |

したがって、売りbucketは `crowded_overvalued/no_value + sector_weak` を優先し、`sector_strong` は裸shortより relative short / long回避として扱う。`strong_low_value + sector_strong` は明確に short-prohibit。

#### 結論: crowded / stress の red 候補は sector weak でかなり悪化する

Prime `2022-07-01` から `2026-05-14`、`1,607,372` stock-days。下表は `short_candidate_sector_interaction_df` の 20D。

| Candidate bucket | Sector | Obs | Raw mean | TOPIX mean | Excess mean | Excess median | Negative excess | Downside excess tail | Upside excess tail | Read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `crowded_no_value` | weak | 5,335 | -1.867% | +0.789% | -2.655% | -3.628% | 65.66% | 25.38% | 11.79% | sector weak で naked/relative 両方が悪い |
| `crowded_no_value` | neutral | 41,722 | +2.243% | +1.275% | +0.968% | -0.647% | 52.50% | 17.69% | 18.66% | weak 以外は red には弱い |
| `crowded_no_value` | strong | 10,229 | +2.402% | +1.585% | +0.818% | -1.627% | 56.54% | 22.39% | 19.52% | 下方tailはあるが上方tailも大きい |
| `crowded_overvalued` | weak | 4,406 | -2.070% | +0.732% | -2.801% | -3.638% | 64.68% | 26.60% | 13.14% | Overvalued crowded は sector weak で赤に近い |
| `crowded_overvalued` | neutral | 30,925 | +2.424% | +1.410% | +1.014% | -0.569% | 52.06% | 19.36% | 19.88% | sector weak なしでは mean が残る |
| `distribution_stress_overvalued` | weak | 17,774 | -0.390% | +1.216% | -1.605% | -2.321% | 60.30% | 19.90% | 12.24% | stress red-risk の一番自然な強化条件 |
| `distribution_stress_overvalued` | neutral | 56,903 | +1.878% | +1.855% | +0.023% | -1.352% | 55.72% | 17.90% | 16.45% | caution 止まり |
| `distribution_stress_overvalued` | strong | 5,596 | +1.482% | +2.091% | -0.609% | -2.011% | 58.20% | 21.82% | 16.92% | excess は悪いが raw は強い |

#### 結論: crowded + ATR overheat は sector weak で red 昇格候補になる

`technical_sector_short_interaction_df` の 20D。`atr20_to_atr60_overheat` は `ATR20 change >= 25% AND ATR20/ATR60 >= 1.25`。

| Candidate bucket | Sector | Obs | Excess mean | Excess median | Negative excess | Downside excess tail | Upside excess tail | Read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `crowded_no_value + overheat` | weak | 937 | -3.188% | -3.953% | 65.10% | 27.43% | 13.13% | yellow から red-risk への昇格候補 |
| `crowded_no_value + overheat` | neutral | 7,248 | +0.745% | -0.898% | 53.34% | 19.00% | 18.24% | sector weak なしでは不十分 |
| `crowded_overvalued + overheat` | weak | 781 | -3.421% | -4.437% | 66.45% | 29.96% | 13.96% | crowded short の最有力条件 |
| `crowded_overvalued + overheat` | neutral | 5,719 | +0.539% | -1.344% | 54.59% | 21.44% | 18.90% | tail はあるが mean は残る |
| `distribution_stress_overvalued + overheat` | weak | 1,135 | -1.014% | -1.941% | 56.83% | 20.26% | 16.21% | stress では overheat より Overvalued + weak sector が主条件 |

#### 結論: stale overvalued は sector weak だけで説明しない

`stale_overvalued` はもともと TOPIX excess が悪い。sector weak は 60D では効くが、20D では sector strong でも悪く、単純な「弱セクターだけ short」とは言いにくい。むしろ `20D > 0 AND 60D > 0` の stale rally fade が sector を問わず悪い。

| Stale split | Sector | Horizon | Obs | Excess mean | Excess median | Negative excess | Downside excess tail | Upside excess tail |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `all_stale_overvalued` | weak | 20D | 8,242 | -1.041% | -0.933% | 57.63% | 7.12% | 4.48% |
| `all_stale_overvalued` | neutral | 20D | 40,677 | -1.197% | -1.469% | 61.31% | 6.89% | 4.52% |
| `all_stale_overvalued` | strong | 20D | 5,279 | -1.730% | -2.465% | 64.80% | 9.98% | 6.27% |
| `recent_20d_and_60d_positive` | weak | 60D | 3,214 | -4.574% | -4.422% | 67.70% | 30.37% | 9.83% |
| `recent_20d_and_60d_positive` | neutral | 60D | 18,315 | -3.269% | -4.105% | 67.43% | 27.80% | 10.95% |
| `recent_20d_and_60d_positive` | strong | 60D | 2,990 | -3.184% | -4.243% | 64.21% | 30.37% | 12.71% |

#### 結論: value confirmation は short 側にも必要。strong low value は short 禁止寄り

`short_value_sector_interaction_df` では、Overvalued / no value は sector weak で悪化し、strong low value は short に向かない。特に `strong_low_value_sector_strong_short_prohibit` は 20D excess mean `+2.037%`、60D excess mean `+5.464%` で、short 色に混ぜるべきではない。

| Priority condition | Horizon | Obs | Raw mean | TOPIX mean | Excess mean | Excess median | Negative excess | Upside excess tail |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `crowded_overvalued_overheat_sector_weak` | 20D | 781 | -2.877% | +0.544% | -3.421% | -4.437% | 66.45% | 13.96% |
| `crowded_no_value_overheat_sector_weak` | 20D | 937 | -2.630% | +0.559% | -3.188% | -3.953% | 65.10% | 13.13% |
| `distribution_stress_overvalued_sector_weak` | 20D | 17,774 | -0.390% | +1.216% | -1.605% | -2.321% | 60.30% | 12.24% |
| `stale_rally_fade_sector_weak` | 60D | 3,214 | +0.225% | +4.799% | -4.574% | -4.422% | 67.70% | 9.83% |
| `strong_low_value_sector_strong_short_prohibit` | 20D | 20,578 | +3.544% | +1.507% | +2.037% | +1.185% | 42.04% | 13.35% |
| `strong_low_value_sector_strong_short_prohibit` | 60D | 20,085 | +9.944% | +4.480% | +5.464% | +3.375% | 39.35% | 31.74% |

### Interpretation

short 側で sector weak が最も明確に効くのは、`crowded` の Overvalued / no value と、`distribution_stress_overvalued`。ここでは raw return も弱く、TOPIX が上がる中で個別がさらに負けているため、セクターベータが short thesis を補強している。

`stale_overvalued` は読み方が違う。stale は「弱セクターだから落ちる」というより、流動性が stale な割高銘柄が TOPIX に負けやすい bucket。sector strong でも 20D excess は悪く、sector weak に限定しすぎると stale red の本体を取りこぼす可能性がある。60D では `stale_rally_fade_sector_weak` が最も素直に悪いため、長めの relative short / long回避では sector weak を強い confirmation として扱える。

value confirmation は short 側でも重要。Overvalued と no value は short red の方向に寄るが、strong low value は sector strong と重なると明確に short 禁止。現在の short-side 配色を見直す場合も、long-side と同じく no value / Overvalued / strong value を分けるべきで、単に liquidity regime だけで赤にしない。

### Production Implication

Ranking に載せるなら、short-side sector overlay は以下の順序で扱う。

| Priority | Candidate | Sector use |
| --- | --- | --- |
| 1 | `crowded_overvalued + ATR overheat` | `sector_weak` で red-risk 昇格候補。`sector_neutral/strong` では caution |
| 2 | `crowded_no_value + ATR overheat` | `sector_weak` で red-risk 昇格候補。value なしの crowded を救済しない |
| 3 | `distribution_stress_overvalued` | `sector_weak` で short confirmation。strong sector では raw return が残るため弱める |
| 4 | `stale_overvalued` | sector で hard filter しない。`stale_rally_fade` と 60D sector weak を強化状態として表示 |
| 5 | `strong_low_value + sector_strong` | short 禁止 / downgrade。赤側に混ぜない |

#### 実務上の読み替え

`sector_weak` 単独を狙うのではなく、もともと short/red 候補として悪い個別条件に弱セクターベータが乗るかを見る。`crowded` / `stress` は `sector_weak` の有無で red-risk の強さがかなり変わるため、`crowded_overvalued + ATR overheat + sector_weak` と `distribution_stress_overvalued + sector_weak` を優先する。

一方で `stale_overvalued` は `sector_weak` を前提にしない。`stale_overvalued` 全体を base red / long回避に置き、`20D > 0 AND 60D > 0` の `stale_rally_fade` を本命の強化状態として扱う。20D では `sector_strong` でも TOPIX excess は悪いため、sector strong を機械的に除外しない。ただし裸shortでは強セクターベータによる逆行に注意し、`sector_weak` は安全寄り confirmation、`sector_strong` は relative short なら許容するが risk caution と読む。

例として `2914` 日本たばこ産業は 2026-05-28 時点で `stale_liquidity`、PBR percentile `0.819`、20D return `+4.61%`、60D return `+1.46%` のため、現行 Daily Ranking では赤 `Stale` + `Rally Fade` に該当する。一方、sector score は `0.33` の `sector_neutral` であり、これは `sector_weak` 型ではなく `stale_rally_fade` 型の relative red / long回避候補として読む。

### Caveats

- Prime-only evidence から始め、Standard/Growth へ外挿しない。
- close-to-close diagnostic であり、borrow、約定、capacity、risk cap は未評価。
- `sector_weak` は候補依存の overlay であり、すべての short 候補に対する hard gate ではない。
- sector score は TOPIX-relative の PIT state なので、raw return と excess return を必ず併記する。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_short_sector_strength_evidence.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_short_sector_strength_evidence.py` |
| tests | `apps/bt/tests/unit/domains/analytics/test_ranking_short_sector_strength_evidence.py` |
| bundle | `/private/tmp/trading25-research/market-behavior/ranking-short-sector-strength-evidence/20260529_ranking_short_sector_strength_prime_v1` |
| index-score bundle | `~/.local/share/trading25/research/market-behavior/ranking-short-sector-strength-evidence/20260601_ranking_short_sector_strength_index_score_v3` |
| average-score bundle | `~/.local/share/trading25/research/market-behavior/ranking-short-sector-strength-evidence/20260601_ranking_short_sector_strength_average_score_v4` |
| result tables | `coverage_diagnostics_df`, `short_candidate_sector_interaction_df`, `short_value_sector_interaction_df`, `stale_rally_sector_interaction_df`, `technical_sector_short_interaction_df`, `priority_short_sector_readout_df`, `observation_sample_df` |

## Reproduction

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_short_sector_strength_evidence.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --horizons 5,10,20,60 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260529_ranking_short_sector_strength_prime_v1 \
  --min-observations 500
```
