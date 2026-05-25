# Ranking Color Evidence

## Published Readout

### Decision

Ranking `Individual Stocks` の `PER` / `Fwd PER` / `Fwd P/OP` / `PBR` / `流動性Z` 色分け判定は、まず Prime 限定の evidence で決める。絶対 PER/PBR 水準は年次 valuation regime に依存するため使わず、target date の Prime cross-section percentile を使う。

専用runnerは `daily_valuation` を valuation SoT にして `statements` interval join を避ける。Prime-only run は 2016-04-01 から 2026-05-14 までを約8秒で完了した。

### Main Findings

#### 結論: valuation は低percentileが相対的に良く、高percentileは明確に悪い

Prime-only `ranking_color_evidence_df`。20d close-to-close TOPIX excess return の中央値 / win rate / severe-loss rate。

| Metric | Bucket | Observation | Median | Win rate | Severe loss | UI implication |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| PBR | cheapest 10% | 158,847 | +0.014% | 50.09% | 4.37% | `excellent` green の最有力 |
| PBR | cheapest 20% | 158,428 | -0.096% | 49.27% | 3.96% | `good` blue |
| PBR | middle 60% | 951,056 | -0.619% | 45.68% | 6.73% | neutral |
| PBR | expensive 20% | 158,604 | -1.370% | 42.54% | 12.68% | `bad` yellow |
| PBR | expensive 10% | 159,199 | -1.898% | 41.32% | 17.55% | `very_bad` red |
| Fwd PER | cheapest 10% | 148,069 | +0.010% | 50.08% | 5.29% | `excellent`/`good` 境界だが相対優位 |
| Fwd PER | cheapest 20% | 147,687 | +0.018% | 50.14% | 4.84% | `good` blue |
| Fwd PER | expensive 20% | 147,792 | -1.234% | 42.65% | 11.27% | `bad` yellow |
| Fwd PER | expensive 10% | 148,109 | -1.503% | 41.57% | 13.19% | `very_bad` red |
| PER | cheapest 10% | 149,607 | -0.176% | 48.75% | 5.76% | weak `good`; green は PBR/Fwd PER より弱い |
| PER | expensive 10% | 149,661 | -1.414% | 42.34% | 13.40% | `very_bad` red |
| Fwd P/OP | cheapest 10% | 139,648 | -0.177% | 48.74% | 6.00% | standalone green は弱い |
| Fwd P/OP | expensive 20% | 139,680 | -1.252% | 42.47% | 10.92% | `bad` yellow |
| Fwd P/OP | expensive 10% | 139,759 | -1.466% | 41.77% | 13.23% | `very_bad` red |

#### 結論: Fwd P/OP は Fwd PER の品質確認として使う

Prime-only `forward_per_pop_interaction_df`。

| Bucket | Observation | Median | Win rate | Severe loss | Interpretation |
| --- | ---: | ---: | ---: | ---: | --- |
| low Fwd PER + low Fwd P/OP | 195,970 | -0.132% | 49.04% | 5.41% | 低Fwd PERの質が確認できる。 |
| low Fwd PER + high Fwd P/OP | 4,130 | -0.275% | 47.94% | 6.30% | sample は小さい。red断定ではなく、low Fwd PER の緑を打ち消す警戒。 |
| low Fwd PER only | 54,533 | -0.211% | 48.47% | 5.57% | low+low より弱い。 |
| low Fwd P/OP only | 79,610 | -0.503% | 46.38% | 6.36% | standalone signal としては弱い。 |
| neither extreme | 774,749 | -0.727% | 44.99% | 7.34% | neutral baseline。 |

#### 結論: PERとの関係性は単独percentileを置き換えるほど強くない

Prime-only `per_relation_evidence_df`。`Fwd PER / PER` と `Fwd P/OP / PER` を target-date Prime cross-section percentile でbucket化した。

| Relation feature | Bucket | Observation | Median | Win rate | Severe loss | Interpretation |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `Fwd PER / PER` | lowest 10% | 141,500 | -0.771% | 45.26% | 9.60% | `Fwd PER < PER` 系の単純な緑判定は支持されない。 |
| `Fwd PER / PER` | middle 60% | 846,763 | -0.546% | 46.25% | 6.90% | relation は中位が最も無難。 |
| `Fwd PER / PER` | highest 10% | 141,466 | -0.849% | 44.27% | 8.15% | extreme high は悪いが、単独Fwd PER高位ほど強くない。 |
| `Fwd P/OP / PER` | lowest 10% | 132,909 | -0.868% | 44.67% | 10.29% | 低比率は良化ではない。 |
| `Fwd P/OP / PER` | middle 60% | 796,428 | -0.665% | 45.43% | 7.31% | relation 中位が相対的に普通。 |
| `Fwd P/OP / PER` | highest 10% | 133,152 | -0.749% | 44.78% | 7.57% | 高比率だけでは単独Fwd P/OP高位ほど悪くない。 |

この検討では、PERとの比率を市場内percentile化しただけでは「旧Fwd PER比較色」を復活させる根拠にも、Fwd P/OP standalone percentile を置き換える根拠にもならない。次の exact ratio level で、低PER前提の `Fwd PER / PER` だけを補助条件として採用する。

#### 結論: 低PERに限定した `Fwd PER / PER <= 0.8` は強い

Prime-only `low_per_relation_level_evidence_df`。`perPercentile <= 0.20` / `<= 0.10` を前提に、ratio level でbucket化した。

| PER scope | Relation feature | Ratio bucket | Observation | Median | Win rate | Severe loss | UI implication |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| low PER 20% | `Fwd PER / PER` | `<= 0.8x` | 10,619 | +0.916% | 55.33% | 4.82% | `excellent` green の追加条件 |
| low PER 20% | `Fwd PER / PER` | `0.8x - 1.0x` | 80,786 | +0.272% | 51.96% | 4.10% | `good` blue の追加条件 |
| low PER 20% | `Fwd PER / PER` | `1.0x - 1.25x` | 82,339 | -0.147% | 48.85% | 4.46% | neutral |
| low PER 20% | `Fwd PER / PER` | `> 1.25x` | 111,770 | -0.445% | 46.92% | 6.11% | weak caution |
| low PER 10% | `Fwd PER / PER` | `<= 0.8x` | 3,083 | +1.025% | 55.53% | 6.39% | sample は小さいが方向は同じ |
| low PER 20% | `Fwd P/OP / PER` | `<= 0.8x` | 103,195 | -0.104% | 49.20% | 4.81% | good には届かない |
| low PER 20% | `Fwd P/OP / PER` | `0.8x - 1.0x` | 57,604 | -0.101% | 49.26% | 4.74% | neutral |
| low PER 20% | `Fwd P/OP / PER` | `> 1.25x` | 54,009 | -0.540% | 46.20% | 6.86% | `bad` yellow の補助条件 |

この結果は、`Fwd PER < PER` の雑な比較をそのまま戻すものではない。`低PER` を前提に、さらに `Fwd PER / PER <= 0.8` まで改善している場合だけ、return distribution が明確に良い。

#### 結論: 流動性Zは「高いほど良い」ではない

Prime-only `liquidity_regime_evidence_df`。production regime 名に合わせて集計した。

| Regime | Observation | Median | Win rate | Severe loss | UI implication |
| --- | ---: | ---: | ---: | ---: | --- |
| `neutral_rerating` | 460,829 | -0.452% | 47.08% | 7.17% | 相対的には良いが green 断定は弱い。`good` blue 候補。 |
| `crowded_rerating` | 68,668 | -0.882% | 46.31% | 17.55% | 混雑・左尾risk。yellow寄りの caution。 |
| `distribution_stress` | 140,967 | -1.164% | 44.42% | 14.93% | `bad` yellow。 |
| `stale_liquidity` | 202,141 | -0.820% | 43.39% | 4.91% | returnより investability warning。赤ではなく muted/caution が妥当。 |
| `neutral` | 706,102 | -0.794% | 44.41% | 6.94% | neutral baseline。 |

#### 結論: rerating 系は value confirmation と掛け合わせて読む

`neutral_rerating` と `crowded_rerating` は regime 単独では意味が違う。`neutral_rerating` は median / win / tail が安定しやすく、`crowded_rerating` は tail risk が重い一方で value confirmation が付くと mean と右尾が大きく伸びる。

| Value condition | Regime | Observation | Mean | Median | Win rate | Severe loss | UI implication |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| all Prime | `neutral_rerating` | 460,829 | -0.011% | -0.452% | 47.08% | 7.17% | `good` blue |
| all Prime | `crowded_rerating` | 68,668 | +0.735% | -0.882% | 46.31% | 17.55% | valueなしは `bad` yellow |
| low PBR20 | `neutral_rerating` | 96,152 | +1.180% | +0.570% | 53.79% | 4.04% | blue候補。greenにはしない |
| low PBR20 | `crowded_rerating` | 9,427 | +1.827% | +0.168% | 50.67% | 10.19% | blue候補 |
| low PBR20 + low Fwd PER20 | `neutral_rerating` | 39,096 | +1.747% | +1.136% | 58.16% | 2.83% | 数字は良いが green から除外し blue |
| low PBR20 + low Fwd PER20 | `crowded_rerating` | 3,793 | +3.059% | +0.822% | 54.15% | 7.99% | `excellent` green。ただしtail注意 |
| low PER20 + `Fwd PER / PER <= 0.8` | `neutral_rerating` | 2,944 | +2.138% | +1.556% | 59.17% | 2.07% | `excellent` green |
| low PER20 + `Fwd PER / PER <= 0.8` | `crowded_rerating` | 991 | +4.454% | +3.215% | 64.68% | 6.96% | `excellent` green。ただしsample小さめ |

このため、`流動性Z` の色は raw z や regime 単独ではなく、`regime x value confirmation` で決める。`crowded_rerating` は「危険な過熱」だけではなく、「value が確認できると右尾が強いが、value がないと tail risk が重い rerating」と扱う。

#### 結論: TOPIX 調整局面では crowded_rerating の左尾がさらに重い

追加run `20260522_ranking_color_evidence_prime_topix_regime_v7` では、TOPIX の直近 20d / 60d return を個別銘柄 panel に持たせ、`topix_regime_liquidity_value_evidence_df` で `TOPIX regime x liquidity regime x value confirmation` を集計した。ここでも outcome は個別銘柄の 20d TOPIX excess return であり、TOPIX 自体の forward return ではない。

| TOPIX condition | Regime / value condition | Observation | Mean | Median | Win rate | Severe loss | Interpretation |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| all Prime | `neutral_rerating` all | 460,829 | -0.011% | -0.452% | 47.08% | 7.17% | baseline rerating |
| all Prime | `crowded_rerating` all | 68,668 | +0.735% | -0.882% | 46.31% | 17.55% | mean は強いが左尾が重い |
| TOPIX 20d < 0 | `neutral_rerating` all | 88,934 | -0.531% | -1.002% | 43.82% | 8.60% | 市場調整で全体に悪化 |
| TOPIX 20d < 0 | `crowded_rerating` all | 13,238 | -0.492% | -1.900% | 42.82% | 19.47% | crowded の左尾悪化が主因 |
| TOPIX 20d < 0 | `crowded_rerating` no value | 9,659 | -0.830% | -2.124% | 42.02% | 20.82% | valueなし crowded は最も警戒 |
| TOPIX 20d < 0 | `crowded_rerating` strong value | 808 | +1.978% | -0.125% | 49.75% | 9.65% | value は緩和するが all Prime ほど強くない |
| TOPIX 20d < 0, 60d > 0 | `neutral_rerating` all | 53,763 | +0.037% | -0.430% | 47.16% | 7.22% | 上昇トレンド内調整では neutral は崩れにくい |
| TOPIX 20d < 0, 60d > 0 | `crowded_rerating` all | 8,018 | -0.307% | -1.836% | 43.10% | 19.23% | pullback局面でも crowded は悪い |
| TOPIX 20d < 0, 60d > 0 | `crowded_rerating` no value | 5,769 | -0.862% | -2.235% | 41.78% | 21.29% | crowded悪化の中心 |
| TOPIX 20d < 0, 60d > 0 | `crowded_rerating` strong value | 585 | +3.156% | +0.541% | 51.97% | 6.15% | 強valueなら右尾は残るが sample は小さい |
| TOPIX 60d < 0 | `neutral_rerating` all | 64,903 | -0.872% | -1.290% | 41.93% | 8.93% | downtrend では neutral も悪化 |
| TOPIX 60d < 0 | `crowded_rerating` all | 10,074 | +0.037% | -1.028% | 45.69% | 17.56% | median は中立寄りでも左尾は大きい |
| TOPIX 60d < 0 | `crowded_rerating` no value | 7,456 | -0.040% | -1.090% | 45.52% | 17.93% | 60d下落でも tail警戒は残る |
| TOPIX 60d < 0 | `crowded_rerating` medium value | 1,285 | +0.743% | -0.540% | 47.78% | 13.39% | value は改善するが tail は高い |

この TOPIX 追加観点は、現在の `流動性Z` 配色をすぐ変えるというより、`crowded_rerating` の yellow 判定が特に TOPIX 20d<0 の市場調整局面で妥当だったことを裏付ける。強value確認がある `crowded_rerating` は調整局面でも mean は強いが、sample が小さく、TOPIX 20d<0 全体では median が大きく鈍るため、配色ルールに直接混ぜるなら別の TOPIX regime overlay として扱うほうがよい。

現在の UI 配色に合わせ、`neutral_rerating` / `crowded_rerating` の緑・青だけを非重複で切り出し、まず全体 baseline を置くと以下になる。green は3条件に限定する: `crowded_rerating` は `low PBR20 + low Fwd PER20` または `low PER20 + Fwd PER/PER <= 0.8`、`neutral_rerating` は `low PER20 + Fwd PER/PER <= 0.8` のみ。`neutral_rerating` の `low PBR20 + low Fwd PER20` は green から除外し blue に落とす。

| Scope | Regime | UI color | Observation | Mean | Median | Win rate | Severe loss |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| all Prime | `crowded_rerating` | green | 4,491 | +3.073% | +0.993% | 54.51% | 7.97% |
| all Prime | `crowded_rerating` | blue | 7,301 | +0.832% | -0.468% | 47.46% | 10.92% |
| all Prime | `neutral_rerating` | green | 2,944 | +2.138% | +1.556% | 59.17% | 2.07% |
| all Prime | `neutral_rerating` | blue | 457,885 | -0.025% | -0.464% | 47.01% | 7.20% |

同じ緑・青判定を TOPIX 条件別に分けると以下になる。ここが TOPIX 追加観点の主表である。

| TOPIX condition | Regime | UI color | Observation | Mean | Median | Win rate | Severe loss |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| TOPIX 20d < 0 | `crowded_rerating` | green | 808 | +1.978% | -0.125% | 49.75% | 9.65% |
| TOPIX 20d < 0 | `crowded_rerating` | blue | 1,213 | +0.967% | -0.666% | 46.41% | 10.72% |
| TOPIX 20d < 0 | `neutral_rerating` | blue | 88,436 | -0.546% | -1.010% | 43.75% | 8.62% |
| TOPIX 20d < 0, 60d > 0 | `crowded_rerating` | green | 585 | +3.156% | +0.541% | 51.97% | 6.15% |
| TOPIX 20d < 0, 60d > 0 | `crowded_rerating` | blue | 792 | +1.781% | -0.044% | 49.87% | 8.46% |
| TOPIX 20d < 0, 60d > 0 | `neutral_rerating` | blue | 53,412 | +0.016% | -0.441% | 47.08% | 7.23% |
| TOPIX 60d < 0 | `crowded_rerating` | blue | 848 | +0.345% | -0.924% | 44.46% | 12.50% |
| TOPIX 60d < 0 | `neutral_rerating` | blue | 64,562 | -0.877% | -1.299% | 41.89% | 8.95% |
| TOPIX 20d >= 0, 60d >= 0 | `crowded_rerating` | green | 3,469 | +3.256% | +0.966% | 54.97% | 7.38% |
| TOPIX 20d >= 0, 60d >= 0 | `crowded_rerating` | blue | 5,661 | +0.772% | -0.395% | 47.57% | 11.02% |
| TOPIX 20d >= 0, 60d >= 0 | `neutral_rerating` | green | 2,252 | +2.261% | +1.661% | 60.52% | 1.24% |
| TOPIX 20d >= 0, 60d >= 0 | `neutral_rerating` | blue | 339,911 | +0.130% | -0.312% | 47.97% | 6.87% |

#### 結論: 120D/150D は crowded green の品質確認には使えるが、blue 判定には混ぜない

2026-05-23 follow-up `20260523_ranking_color_liquidity_color_long_trend_prime_v2` では、上の `crowded_rerating` / `neutral_rerating` の green / blue 非重複判定だけに絞り、個別銘柄の `120d` / `150d` return が正かどうかで分解した。outcome は引き続き 20d close-to-close TOPIX excess return。`green/blue` 定義は上表の非重複 evidence に合わせ、`crowded blue` は `medium_value_confirmation AND NOT green` とした。

`crowded green` は long trend positive で大きく改善する。120d positive は observation `3,413`、median `+1.211%`、win rate `55.52%`、severe loss `8.00%`。120d non-positive は observation `773`、median `-0.505%`、win rate `46.57%`、severe loss `10.35%`。150d でも同方向だが、120d の方が positive / non-positive の median 差が大きい。

| Regime | UI color | Long condition | Observation | Mean | Median | Win rate | Severe loss | Read |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `crowded_rerating` | green | `120d > 0` | 3,413 | +3.555% | +1.211% | 55.52% | 8.00% | 採用候補の品質確認として有効 |
| `crowded_rerating` | green | `120d <= 0` | 773 | +0.312% | -0.505% | 46.57% | 10.35% | greenを弱める caution |
| `crowded_rerating` | green | `150d > 0` | 3,337 | +3.276% | +0.966% | 54.54% | 8.12% | 有効だが120dより少し鈍い |
| `crowded_rerating` | green | `150d <= 0` | 778 | +0.900% | -0.155% | 48.84% | 9.90% | caution だが120dほど切れない |

一方で `crowded blue` は long trend positive が逆に悪い。120d positive は median `-1.178%` / severe loss `10.99%`、120d non-positive は median `-0.582%` / severe loss `5.33%`。150d でも positive 側は median `-0.992%`、non-positive 側は `-1.251%` で、少なくとも「長期上昇なら blue を上げる」根拠にはならない。`crowded blue` は medium value だが strong value ではないため、長期上昇がむしろ crowded/extended な状態を拾っている可能性がある。

| Regime | UI color | Long condition | Observation | Mean | Median | Win rate | Severe loss | Read |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `crowded_rerating` | blue | `120d > 0` | 3,138 | -0.035% | -1.178% | 43.59% | 10.99% | blueを強めない |
| `crowded_rerating` | blue | `120d <= 0` | 882 | +2.475% | -0.582% | 47.05% | 5.33% | mean は右尾、median は弱い |
| `crowded_rerating` | blue | `150d > 0` | 3,099 | +0.071% | -0.992% | 44.18% | 10.16% | blueを強めない |
| `crowded_rerating` | blue | `150d <= 0` | 836 | +0.700% | -1.251% | 42.94% | 8.61% | 改善なし |

`neutral_rerating green` はもともと強く、long trend positive しか十分な観測が出ない。120d positive は median `+1.507%`、150d positive は `+1.651%` で、既存 green を補強するが、追加条件にすると sample を少し落とすだけで本質的な新情報は少ない。`neutral_rerating blue` では 120d/150d positive と non-positive の差が小さく、配色には混ぜないほうがよい。

| Regime | UI color | Long condition | Observation | Mean | Median | Win rate | Severe loss | Read |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `neutral_rerating` | green | `120d > 0` | 2,568 | +2.093% | +1.507% | 58.57% | 2.26% | 既存greenの補強 |
| `neutral_rerating` | green | `150d > 0` | 2,520 | +2.261% | +1.651% | 59.52% | 2.14% | 既存greenの補強 |
| `neutral_rerating` | blue | `120d > 0` | 340,928 | -0.053% | -0.512% | 46.76% | 7.46% | ほぼ使えない |
| `neutral_rerating` | blue | `120d <= 0` | 61,894 | -0.071% | -0.417% | 47.18% | 6.58% | ほぼ使えない |
| `neutral_rerating` | blue | `150d > 0` | 327,702 | +0.014% | -0.454% | 47.15% | 7.26% | ほぼ使えない |
| `neutral_rerating` | blue | `150d <= 0` | 64,645 | -0.243% | -0.515% | 46.44% | 7.25% | ほぼ使えない |

このため、120D/150D は `流動性Z` の green/blue を全面的に変える条件ではない。使うなら `crowded_rerating green` の補助 badge/overlay として、`120d <= 0` を green の弱め警告にするのが最も筋が良い。`150d` より `120d` の方が green の良否を切りやすい。`crowded blue` と `neutral blue` には入れない。

#### 結論: 高PER + 高PBRは大型・高ADVでも「良いreturn」には反転しない

2026-05-23 follow-up `20260523_high_valuation_size_liquidity_prime_v1` では、既存の Prime target-date percentile を使って `PER` / `PBR` の高valuation条件を定義し、時価総額と median ADV60 は絶対値bucketで切った。目的は「高PER・高PBRは全体では悪いが、大型・高売買代金ならよいのではないか」を検証すること。結果は、mega cap / high ADV では左尾riskはやや緩むが、20d TOPIX excess return の中央値はプラスに反転しない。

`high_valuation_size_liquidity_interaction_df`。20d close-to-close TOPIX excess return。

| Valuation condition | Market cap bucket | ADV60 bucket | Observation | Mean | Median | Win rate | Severe loss | Interpretation |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| high PER20 + high PBR20 | `cap_50_200bn` | `adv_50_300mn` | 15,120 | -1.97% | -2.10% | 37.9% | 13.5% | 高valuationの悪さが強い中型・中流動性。 |
| high PER20 + high PBR20 | `cap_200bn_1tn` | `adv_ge_1bn` | 25,553 | -1.22% | -2.04% | 41.2% | 18.2% | 高ADVでも中央値は悪い。 |
| high PER20 + high PBR20 | `cap_ge_1tn` | `adv_ge_1bn` | 34,790 | -0.57% | -1.35% | 43.1% | 12.9% | mega cap では悪さは緩むが、good bucket ではない。 |
| high Fwd PER20 + high PBR20 | `cap_50_200bn` | `adv_50_300mn` | 14,783 | -2.17% | -2.24% | 37.2% | 13.9% | forward valuation でも同方向。 |
| high Fwd PER20 + high PBR20 | `cap_200bn_1tn` | `adv_ge_1bn` | 25,005 | -1.22% | -1.94% | 41.5% | 17.1% | 高ADVは反転条件ではない。 |
| high Fwd PER20 + high PBR20 | `cap_ge_1tn` | `adv_ge_1bn` | 31,509 | -0.66% | -1.46% | 42.5% | 12.7% | mega cap でも bad 寄り。 |

一部に `cap_200bn_1tn / adv_50_300mn` の `high Fwd PER20 + high PBR20` が median `-0.14%` まで改善する小sample bucket（527 observations）はあるが、これは良いreturnと読むほど強くない。むしろ本筋は、時価総額・売買代金の絶対水準は高valuationの左尾riskを一部和らげる capacity / quality context であり、`PBR` / `PER` の high percentile red/yellow を green/blue へ反転させる根拠ではない。

### Interpretation

この readout は UI evidence layer であり、portfolio rule ではない。Prime の 20d TOPIX excess では、PBR と Fwd PER の低percentileが相対的に最も良く、高percentileは明確に悪い。したがって valuation coloring は percentile rank を使う。`Fwd PER / PER` や `Fwd P/OP / PER` のrelation percentile 単独は主軸にしないが、低PERを前提にした exact ratio level は補助条件として有効。

ただし、低valuation側の中央値は小さい。`excellent` green は「絶対的に強いalpha」ではなく、Prime cross-section の中で相対的に return distribution が良いことを示す UI cue として扱う。

流動性は raw z を直接色分けしない。`neutral_rerating` / `crowded_rerating` は value confirmation と掛け合わせて読む。`crowded_rerating` 単体は severe-loss rate が 17.55% と高いが、低PBR + 低Fwd PERや低PER + Fwd PER/PER改善が揃うと mean / median が強くなる。ただし TOPIX 20d<0 の市場調整局面では、`crowded_rerating` の median と左尾が大きく悪化する。`stale_liquidity` は20d severe loss は低いが、流動性・執行可能性の警告なので return tier と混ぜすぎない。

120D/150D の個別銘柄 trend は、`crowded_rerating green` だけで有用性が明確だった。特に `120d > 0` は green の品質確認として効き、`120d <= 0` は green を弱める警告になる。一方、`crowded blue` / `neutral blue` では long trend positive が改善条件にならず、`neutral green` は元々強いため追加条件としての情報量が小さい。

高PER・高PBRの大型/高ADV follow-up は、既存の赤/黄 valuation 判定を弱める材料ではあるが、反転材料ではない。大型・高ADVは「避けやすい危険」ではなく「bad がやや薄い expensive large liquid bucket」として扱い、valuation color そのものは引き続き high percentile を caution とする。

### Production Implication

Prime 判定としての初期mapping:

| Field | Initial production tier rule |
| --- | --- |
| `PBR` | `pbrPercentile <= 0.10` green, `<= 0.20` blue, `>= 0.80` yellow, `>= 0.90` red |
| `Fwd PER` | `perPercentile <= 0.20` かつ `forwardPer / per <= 0.8` green、同 `<= 1.0` blue。その他は `forwardPerPercentile <= 0.10` green, `<= 0.20` blue, `>= 0.80` yellow, `>= 0.90` red |
| `PER` | `perPercentile <= 0.20` blue, `>= 0.80` yellow, `>= 0.90` red. `<=0.10` green は弱めに扱う |
| `Fwd P/OP` | standalone cheap color は弱め。`forwardPerPercentile <= 0.20` かつ `forwardPOpPercentile <= 0.20` を blue、`perPercentile <= 0.20` かつ `forwardPOp / per > 1.25` は yellow、`forwardPOpPercentile >= 0.80` は yellow、`>= 0.90` は red |
| `流動性Z` | green は3条件に限定: `neutral_rerating` は low PER20 + Fwd PER/PER <= 0.8 のみ green、`crowded_rerating` は low PBR20 + low Fwd PER20 または low PER20 + Fwd PER/PER <= 0.8 のみ green。その他の `neutral_rerating` は blue、`crowded_rerating` は中valueあり blue・valueなし yellow。ただし `crowded_rerating` は PER/Fwd PER/Fwd P/OP/PBR のいずれかが high 20%、または PER/Fwd PER が両方 null（赤字・正のPER未成立を含む）なら、green 条件に該当しない限り yellow。`distribution_stress` / `stale_liquidity` は yellow。120D/150D は色そのものではなく、`crowded_rerating green` に限って `120d <= 0` caution overlay として扱う |

TOPIX regime は今回の配色には直接混ぜない。もし UI に入れるなら、`流動性Z` の色そのものではなく、`TOPIX 20d<0` かつ `crowded_rerating`、特に value confirmation なしを別の market-adjustment caution overlay として扱うほうが解釈が明確。

### Caveats

- 判定は Prime-only。Standard/Growth へ同じ色意味を外挿しない。
- 20d TOPIX excess return の UI evidence であり、annual portfolio lens / transaction cost lens ではない。
- local `market.duckdb` の coverage は最新runで Prime 1,717,700 observations / 1,920 codes / 1,004 dates。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_color_evidence.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_color_evidence.py` |
| prime bundle | `/tmp/trading25-research/market-behavior/ranking-color-evidence/20260522_ranking_color_evidence_prime_topix_regime_v7` |
| liquidity color long trend follow-up bundle | `/private/tmp/trading25-research/market-behavior/ranking-color-evidence/20260523_ranking_color_liquidity_color_long_trend_prime_v2` |
| high valuation size/liquidity follow-up bundle | `~/.local/share/trading25/research/market-behavior/ranking-color-evidence/20260523_high_valuation_size_liquidity_prime_v1` |
| result tables | `ranking_color_evidence_df`, `per_relation_evidence_df`, `low_per_relation_evidence_df`, `low_per_relation_level_evidence_df`, `forward_per_pop_interaction_df`, `liquidity_regime_evidence_df`, `topix_regime_liquidity_value_evidence_df`, `liquidity_color_long_trend_evidence_df`, `high_valuation_size_liquidity_interaction_df`, `coverage_diagnostics_df`, `observation_sample_df` |
