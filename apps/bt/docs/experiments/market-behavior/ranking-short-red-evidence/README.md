# Ranking Short Red Evidence

Daily Ranking の long-side 色分けから独立して、short / red 候補を検証する runner-first research です。
既存の `ranking-color-evidence` を UI rule に直接拡張せず、`crowded_rerating`、`distribution_stress`、`stale_liquidity` を relative valuation、20D/60D technical state、ATR20/ATR60 と交差させ、20D close-to-close TOPIX excess を主軸に観察します。

## Published Readout

### Decision

この experiment は `Ranking` の赤色候補をすぐ UI rule 化するためのものではない。独立した evidence bundle として、`crowded_rerating + Overvalued / no value`、`distribution_stress + Overvalued`、`stale_liquidity + Overvalued` が long 回避または short candidate として十分に悪い forward distribution を持つかを検証した。

Primary outcome は既存の Ranking Color Evidence と揃え、`20D close-to-close TOPIX excess return` とする。`5D` / `10D` は timing、`60D` は持続性の補助確認に限定する。

初回 Prime run では、short 用の赤は `stale_overvalued` 全体を base red として扱い、`20D>0 AND 60D>0` は base red 内の強化状態として別ラベルを付ける。これは `overheat` のような補助状態だが、ATR ではなく recent return による fade 状態なので、`stale_rally_fade` / `stale_overvalued_recent_positive` と呼ぶのが自然。`distribution_stress_overvalued` は下方tailも重いが逆行上方tailも大きいため、base red ではなく caution/red-risk に落とす。`stale_overvalued_weak_trend` は自然な OR 条件ではあるが、`stale_overvalued` 全体より悪いわけではないため、主結論からは外す。`crowded_no_value` / `crowded_overvalued` も左尾は重いが、mean が右尾で残るため、単純 short より「yellow から red への候補、または market/ATR 条件付き red」と読む。

Short 視点では、方向が曖昧な `win_rate` / `severe_loss` という long 由来の名前を使わない。result tables は raw / TOPIX / excess を併記し、short 側は `negative_*_return_rate_pct`、有利な下方tailは `downside_*_tail_rate_pct`、逆行上方tailは `upside_*_tail_rate_pct` として読む。

### Main Findings

#### 結論: 20D主軸では stress overvalued と stale overvalued が最も赤に近い

Prime `2022-06-30` から `2026-05-14`、`1,609,210` stock-days。下表は `short_red_candidate_df` と `stale_liquidity_short_diagnostics_df` の 20D / 60D close-to-close TOPIX excess return。`Negative excess` は forward excess return `< 0`、`Downside excess tail` は `<= -10%`、`Upside excess tail` は `>= +10%`。同じ tables には `mean_forward_raw_return_pct` / `median_forward_raw_return_pct` / `mean_topix_return_pct` も併記し、市場が強い局面で raw return と excess return を切り分ける。

| Candidate bucket | Horizon | Obs | Excess mean | Excess median | Negative excess | Downside excess tail | Upside excess tail | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `crowded_no_value` | 20D | 57,209 | +0.588% | -1.121% | 54.45% | 19.23% | 18.15% | 下方tailと上方tailが近く、裸shortには不安 |
| `crowded_overvalued` | 20D | 43,036 | +0.645% | -1.098% | 54.01% | 21.05% | 19.64% | Overvalued でも逆行tailが大きい |
| `distribution_stress_weak_trend` | 20D | 141,388 | -0.077% | -1.177% | 55.62% | 15.01% | 14.03% | 素直な caution。単独 red には少し弱い |
| `distribution_stress_overvalued` | 20D | 80,319 | -0.376% | -1.624% | 56.88% | 18.59% | 15.56% | 赤候補の中心 |
| `stale_overvalued` | 20D | 54,215 | -1.223% | -1.463% | 61.08% | 7.21% | 4.68% | tail より negative excess の非対称が良い |
| `stale_overvalued_weak_trend` | 20D | 28,659 | -1.100% | -1.309% | 60.36% | 5.79% | 4.09% | weak trend に絞ると少し弱まる |
| `crowded_no_value` | 60D | 54,724 | +0.984% | -3.530% | 57.09% | 35.73% | 27.00% | 60Dでは下方tailが重いが逆行tailも大きい |
| `crowded_overvalued` | 60D | 40,930 | +0.378% | -3.976% | 57.53% | 37.68% | 27.30% | crowded overvalued は長めで危険だが risk cap 必須 |
| `distribution_stress_overvalued` | 60D | 75,783 | -1.578% | -4.978% | 60.33% | 38.83% | 24.09% | 60Dでも最も赤に近い |
| `stale_overvalued` | 60D | 51,675 | -3.045% | -3.616% | 66.14% | 26.38% | 10.47% | 逆行tailが相対的に低く、long 回避/short候補として良い |
| `stale_overvalued_weak_trend` | 60D | 27,148 | -2.701% | -3.174% | 65.32% | 24.55% | 9.98% | 持続的に弱いが主条件より少し弱い |

#### 結論: stale overvalued は weak trend ではなく、上昇済み stale が最も short red に近い

`stale_overvalued_weak_trend` の `weak_trend` は `recent_return_20d_pct <= 0 OR recent_return_60d_pct <= 0`。この OR 条件は自然だが、実測では赤候補を強める条件ではなかった。追加集計では `20D > 0 AND 60D > 0` の方が forward 20D / 60D とも excess mean / median が悪く、negative excess も高い。stale overvalued は「既に弱いから悪い」ではなく、「流動性が stale なまま上がった割高が、その後に放置されやすい」と読む方がよい。

| Stale Overvalued split | 20D obs | 20D excess mean | 20D excess median | 20D negative excess | 20D upside excess tail | 60D obs | 60D excess mean | 60D excess median | 60D negative excess | 60D upside excess tail | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| all `stale_overvalued` | 54,215 | -1.223% | -1.463% | 61.08% | 4.68% | 51,675 | -3.045% | -3.616% | 66.14% | 10.47% | broad red base |
| `20D <= 0` | 21,937 | -1.041% | -1.292% | 60.10% | 4.37% | 20,748 | -2.721% | -3.156% | 65.24% | 9.90% | 弱trend単独では悪化しない |
| `60D <= 0` | 19,455 | -1.224% | -1.443% | 61.41% | 3.87% | 18,502 | -2.660% | -3.139% | 65.56% | 9.67% | 逆行tailは低いが mean は弱い |
| `20D <= 0 OR 60D <= 0` | 28,659 | -1.100% | -1.309% | 60.36% | 4.09% | 27,148 | -2.701% | -3.174% | 65.32% | 9.98% | 現行 weak_trend。主条件より弱い |
| `20D <= 0 AND 60D <= 0` | 12,733 | -1.189% | -1.479% | 61.51% | 4.22% | 12,102 | -2.674% | -3.083% | 65.55% | 9.39% | 20D は悪いが 60D は強まらない |
| `20D > 0 AND 60D > 0` | 25,521 | -1.360% | -1.650% | 61.87% | 5.34% | 24,492 | -3.430% | -4.169% | 67.07% | 11.02% | 最も short red に近い。上昇済み stale overvalued |

#### 結論: ATR20/ATR60 の過熱は crowded overvalued を悪化させるが、stress では救済/悪化が混ざる

`technical_atr_short_interaction_df` の 20D。`atr20_to_atr60_overheat` は `ATR20 change >= 25% AND ATR20/ATR60 >= 1.25`。

| Candidate bucket | Technical state | Obs | Excess median | Negative excess | Downside excess tail | Upside excess tail | Read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `crowded_no_value` | all | 57,209 | -1.121% | 54.45% | 19.23% | 18.15% | baseline |
| `crowded_no_value` | `atr20_to_atr60_overheat` | 10,677 | -1.646% | 56.13% | 21.79% | 17.65% | red 寄りに悪化 |
| `crowded_overvalued` | all | 43,036 | -1.098% | 54.01% | 21.05% | 19.64% | baseline |
| `crowded_overvalued` | `atr20_to_atr60_overheat` | 8,536 | -2.060% | 56.90% | 24.18% | 18.86% | crowded overvalued の明確な悪化条件 |
| `distribution_stress_overvalued` | all | 80,319 | -1.624% | 56.88% | 18.59% | 15.56% | baseline |
| `distribution_stress_overvalued` | `recent_20d_60d_negative` | 41,596 | -1.800% | 58.00% | 17.62% | 13.97% | trend weak で少し悪化 |
| `distribution_stress_overvalued` | `atr20_to_atr60_overheat` | 4,847 | -0.563% | 52.16% | 15.95% | 18.71% | overheat では逆行tailが増え、単純悪化にならない |
| `stale_overvalued_weak_trend` | all | 28,659 | -1.309% | 60.36% | 5.79% | 4.09% | baseline |
| `stale_overvalued_weak_trend` | `atr20_acceleration` | 2,775 | -1.951% | 66.02% | 6.77% | 4.86% | stale では ATR加速が悪化寄り |

### Interpretation

この experiment は、既存の緑/青/黄を増やすためのものではない。特に `stale_liquidity` は既存 readout では return red というより investability warning と読まれているため、単独 red にはしない。`stale` は割高と弱trendが重なる場合だけ別表で検証する。

`crowded_rerating` は強い value confirmation があれば右尾が残る一方、value が無い場合や割高の場合は下方 tail が重い。今回の run でも 20D downside excess tail は `crowded_no_value` で `19.23%`、`crowded_overvalued` で `21.05%` と高い。ただし 20D upside excess tail もそれぞれ `18.15%` / `19.64%` と大きく、excess mean もプラスに残るため、単純 short rule ではなく「赤候補」または「ATR/market regime でさらに絞る候補」と読む。

`distribution_stress_overvalued` は 20D excess median `-1.624%`、60D excess median `-4.978%` で、今回の中では最も素直な red-risk candidate。20D downside excess tail `18.59%` に対して upside excess tail `15.56%`、60D は `38.83%` 対 `24.09%` で、short 側の tail 非対称もある。ただし upside excess tail が大きいため base red ではなく caution/red-risk と読む。`distribution_stress_weak_trend` 単独は悪いが、割高を重ねた方が赤の意味が明確になる。

`stale_overvalued` は 20D excess mean `-1.223%` / median `-1.463%`、60D excess mean `-3.045%` / median `-3.616%`、60D negative excess `66.14%` で、急落 tail というより、TOPIXに負けやすい / 放置されやすい bucket と読む。生returnでは mean/median がプラスになる局面もあるため、裸short の赤ではなく relative red / long回避 / hedge前提short候補とする。60D downside excess tail `26.38%` に対して upside excess tail `10.47%` なので、逆行tail確認後も base red 候補として残る。さらに `20D > 0 AND 60D > 0` に絞ると 60D excess mean `-3.430%`、median `-4.169%`、negative excess `67.07%` まで悪化するため、`stale_overvalued` 全体を赤にしたうえで、上昇済み状態を `stale_rally_fade` / `stale_overvalued_recent_positive` のような強化状態として付けるのがよい。`weak_trend` OR に絞るとむしろ悪さが薄まるため、stale 側の補助状態は `stale_overvalued_weak_trend` ではなく `stale_overvalued_recent_positive` に寄せる。

### Production Implication

現時点では production / UI rule を変更しない。後続PRで検討するなら優先順位は以下。

| Priority | Candidate | UI implication |
| --- | --- |
| 1 | `stale_overvalued` | short red の base condition |
| 2 | `stale_overvalued + 20D>0 AND 60D>0` | base red 内の強化状態。`stale_rally_fade` / `stale_overvalued_recent_positive` として扱う |
| 3 | `distribution_stress_overvalued` | red-risk / caution。逆行上方tailが大きいため base red にはしない |
| 4 | `crowded_no_value` | yellow 維持か、market/ATR 条件付き red を追加検証 |

採用前には live Ranking replay を見て、現時点の銘柄リストが直感的に「赤」と読めるかを確認する。borrow / 約定 / capacity を見ていないため、short execution rule にはしない。

### Caveats

- Prime-only evidence から始め、Standard/Growth へ外挿しない。
- close-to-close diagnostic であり、pre-open screening rule ではない。
- short 実運用には borrow、約定、position sizing、risk cap が別途必要。
- short readout では raw / TOPIX / excess を併記し、裸short と relative/hedged short を混同しない。
- mean が右尾で改善しても、median / downside tail / upside tail の組み合わせで red 候補を扱う。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_short_red_evidence.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_short_red_evidence.py` |
| tests | `apps/bt/tests/unit/domains/analytics/test_ranking_short_red_evidence.py` |
| bundle | `/private/tmp/trading25-research/market-behavior/ranking-short-red-evidence/20260528_ranking_short_red_evidence_prime_v5` |
| result tables | `coverage_diagnostics_df`, `short_red_candidate_df`, `regime_valuation_interaction_df`, `technical_atr_short_interaction_df`, `stale_liquidity_short_diagnostics_df`, `stale_overvalued_trend_split_df`, `live_ranking_replay_df`, `observation_sample_df` |

## Reproduction

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_short_red_evidence.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --horizons 5,10,20,60 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260528_ranking_short_red_evidence_prime_v5 \
  --min-observations 500
```
