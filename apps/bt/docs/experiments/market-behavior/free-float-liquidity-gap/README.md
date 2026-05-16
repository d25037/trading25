# Free-Float Liquidity Gap

## Published Readout

### Decision

`free_float_liquidity_gap` は、売買代金を alpha として直接扱うのではなく、free-float 時価総額に対する流動性の過熱・枯渇を測る Phase 1 research として採用する。

この research は `log(ADV_N) ~ log(free_float_market_cap)` を市場別に回帰し、実際の売買代金が現在の free-float 規模に対してどれだけ上振れ・下振れしているかを `liquidity_residual_z` と `liquidity_implied_ffcap_gap_pct` で表す。`liquidity_implied_ffcap_gap_pct` は「理想時価総額」ではなく、現在の ADV 水準から逆算した **liquidity-implied free-float gap** として読む。

全期間 run では、Prime は free-float 時価総額で ADV をかなり説明できる一方、Standard は説明力が弱い。Standard の high residual bucket は平均 excess return では悪くないが、60-session median excess return と win rate が明確に悪く、crowding / exit-risk diagnostic として扱うのが自然。Prime の high residual はむしろ 60-session mean excess が強く、単純な hard exclude には向かない。

この readout は bundle `/tmp/trading25-research/market-behavior/free-float-liquidity-gap/phase1_20260511_fast` に基づく。対象期間は `2016-06-01` to `2026-05-08`、`ADV20/60`、`20/60 session` forward、`20 session` residual-change、`20 session` observation stride。

### Main Findings

#### 結論

| 観点 | 結果 | 判断 |
|---|---:|---|
| `prime` ADV60 regression R2 | 0.698 | free-float cap でかなり説明できる |
| `standard` ADV60 regression R2 | 0.188 | 規模だけでは説明しにくく residual の診断価値が高い |
| `growth` ADV60 regression R2 | 0.422 | Prime より粗く、Standard よりは関係が残る |
| `prime` ADV60 high residual 60d mean excess | +1.703% | high liquidity は crowding だけではなく re-rating/participation 候補 |
| `standard` ADV60 high residual 60d median excess | -5.174% | high residual は平均より中央値・勝率悪化を重視する |
| `growth` ADV60 high residual 60d median excess | -8.623% | high residual は高分散・低中央値 bucket |
| 実務判断 | market split 必須 | pooled `all` は sanity check に留める |

#### Regression Fit

| market | ADV window | observations | codes | beta | R2 | residual std | median ADV mn JPY | median free-float cap bn JPY |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| all | 20 | 395,214 | 3,727 | 1.012 | 0.639 | 1.400 | 60.9 | 16.9 |
| prime | 20 | 212,917 | 2,048 | 1.008 | 0.690 | 1.115 | 201.5 | 51.1 |
| standard | 20 | 135,991 | 1,666 | 0.672 | 0.199 | 1.514 | 9.4 | 5.9 |
| growth | 20 | 46,306 | 805 | 1.022 | 0.426 | 1.444 | 46.0 | 5.7 |
| all | 60 | 388,802 | 3,723 | 0.984 | 0.636 | 1.368 | 66.6 | 16.9 |
| prime | 60 | 209,672 | 2,048 | 0.995 | 0.698 | 1.081 | 208.9 | 51.1 |
| standard | 60 | 133,908 | 1,659 | 0.636 | 0.188 | 1.480 | 10.8 | 5.9 |
| growth | 60 | 45,222 | 799 | 0.975 | 0.422 | 1.384 | 54.3 | 5.7 |

#### Mean vs Median ADV60 Check

2026-05-13 follow-up で `ADV60` を rolling mean ではなく rolling median でも同条件比較した。mean bundle は `/tmp/trading25-research/market-behavior/free-float-liquidity-gap/phase1_20260513_mean_compare`、median bundle は `/tmp/trading25-research/market-behavior/free-float-liquidity-gap/phase1_20260513_median_compare`。

2026-05-17 follow-up で、median ADV60 の market split regression を Prime / Standard / Growth 別の scatter + regression line として bundle 出力に追加した。確認 bundle は `/tmp/trading25-research/market-behavior/free-float-liquidity-gap/phase1_20260517_median_regression_figures`、figure は `figures/median_adv60_regression_prime.png` / `figures/median_adv60_regression_standard.png` / `figures/median_adv60_regression_growth.png`。

| market | mean ADV60 R2 | median ADV60 R2 | mean high 60d excess mean % | median high 60d excess mean % | mean high 60d excess median % | median high 60d excess median % | high bucket overlap |
|---|---:|---:|---:|---:|---:|---:|---:|
| prime | 0.703 | 0.723 | +1.733 | +1.790 | -0.765 | -0.621 | 93.2% |
| standard | 0.193 | 0.265 | -0.030 | +0.266 | -5.140 | -4.641 | 85.6% |
| growth | 0.429 | 0.514 | -0.285 | +0.697 | -8.685 | -7.545 | 81.1% |

median ADV60 は短期 spike の影響を落とすため、各市場で R2 は改善した。特に Standard / Growth では fit の改善が大きい。ただし high residual bucket の解釈は反転しない。Prime は high residual が引き続き participation / re-rating 候補として残り、Standard / Growth は high residual の median excess と win rate がまだ弱い。したがって production implication は変えず、median ADV は robustness check として有用、Symbol Workbench や診断表示では mean と median の乖離を「一時的な売買集中」検出に使う余地がある。

同じ follow-up で、Phase 2/3 も mean / median ADV60 入力で再実行した。これは value ranking の liquidity floor ではなく、`recent_return_20d_pct` / `recent_return_60d_pct` と `liquidity_residual_z` を同時に見る Prime momentum interaction 側の確認。入力は Phase 2 bundle `/tmp/trading25-research/market-behavior/free-float-liquidity-regime-decomposition/phase2_20260513_mean_regime_compare` と `/tmp/trading25-research/market-behavior/free-float-liquidity-regime-decomposition/phase2_20260513_median_regime_compare`、Phase 3 bundle `/tmp/trading25-research/market-behavior/free-float-liquidity-prime-momentum-interaction/phase3_20260513_mean_momentum_compare` と `/tmp/trading25-research/market-behavior/free-float-liquidity-prime-momentum-interaction/phase3_20260513_median_momentum_compare`。

| check | mean ADV60 | median ADV60 | 判断 |
|---|---:|---:|---|
| `momentum_plus_liquidity` の `liquidity_residual_z` 係数 | +0.976% / 1sd | +1.001% / 1sd | median 非劣性 |
| 同 t-stat | 28.40 | 29.17 | median がやや強い |
| `momentum_liquidity_interaction_z` 係数 | +0.307% / 1sd | +0.357% / 1sd | median がやや強い |
| `positive_20d_60d + high_residual` vs neutral mean spread | +2.901% | +3.025% | median がやや強い |
| 同 median spread | +1.418% | +1.688% | median がやや強い |
| 同 win-rate spread | +5.283pt | +5.932pt | median がやや強い |
| Prime ADV60 residual high bucket overlap | 93.2% | 93.2% | 候補の大枠は維持 |

この確認では、median ADV60 へ変えても `return20/60 + liquidity residual` の主要結論は壊れない。むしろ Prime momentum interaction では係数・bucket spread が小幅に改善したため、free-float liquidity residual / regime 側の production default は median ADV60 へ寄せる方針でよい。ただし value ranking の `ADV60 >= 10mn` hard floor は別物で、median 化すると候補除外が強くなるため、この readout の default 化対象には含めない。

#### Residual Bucket

下表は 60-session forward excess return。high residual は「現在の ADV 水準から見ると free-float cap が足りない」状態、low residual はその逆。

| market | ADV | bucket | events | mean excess % | median excess % | win rate % | median implied gap % |
|---|---:|---|---:|---:|---:|---:|---:|
| prime | 20 | low | 42,332 | -0.761 | -1.605 | 53.00 | -73.0 |
| prime | 20 | high | 41,755 | +1.766 | -0.738 | 54.33 | +296.8 |
| prime | 60 | low | 41,666 | -0.795 | -1.666 | 53.49 | -72.3 |
| prime | 60 | high | 41,145 | +1.703 | -0.810 | 54.03 | +287.4 |
| standard | 20 | low | 26,723 | +0.077 | -1.473 | 57.01 | -93.2 |
| standard | 20 | high | 26,098 | +0.196 | -5.038 | 45.79 | +1,966.0 |
| standard | 60 | low | 26,369 | +0.006 | -1.497 | 56.62 | -93.9 |
| standard | 60 | high | 25,717 | +0.023 | -5.174 | 45.37 | +2,348.8 |
| growth | 20 | low | 8,702 | -1.373 | -4.783 | 45.84 | -82.8 |
| growth | 20 | high | 9,060 | +0.236 | -8.703 | 41.29 | +531.9 |
| growth | 60 | low | 8,529 | -1.234 | -4.746 | 46.51 | -83.5 |
| growth | 60 | high | 8,817 | -0.155 | -8.623 | 41.14 | +555.1 |

#### Residual Change Bucket

残差の急増・急低下だけを見ると、静的 high residual ほど強い差は出ない。Standard の ADV60 high change は 60d mean excess `+0.574%` だが median は `-2.925%`、win rate は `50.83%` で、単独 signal としては弱い。

| market | ADV | change bucket | events | mean excess % | median excess % | win rate % | median residual change |
|---|---:|---|---:|---:|---:|---:|---:|
| prime | 60 | low | 41,558 | -0.185 | -1.696 | 52.28 | -0.209 |
| prime | 60 | high | 40,335 | +0.220 | -1.912 | 53.88 | +0.224 |
| standard | 60 | low | 26,128 | +0.110 | -2.731 | 50.37 | -0.399 |
| standard | 60 | high | 25,705 | +0.574 | -2.925 | 50.83 | +0.390 |
| growth | 60 | low | 8,773 | -1.579 | -7.061 | 41.33 | -0.525 |
| growth | 60 | high | 8,750 | -0.059 | -6.708 | 43.04 | +0.417 |

### Interpretation

この Phase 1 は、`ADV60 >= 10mn` のような単純な liquidity gate を alpha として扱う研究ではない。売買代金の増減が意味を持つとすれば、それは「今の free-float 規模から見て異常に参加が増えている/減っている」状態であり、まず residual と residual change の market split を見る必要がある。

今回一番はっきりしたのは、`standard` の回帰説明力がかなり低いこと。free-float cap の大きさだけで自然な ADV を説明できないため、同じ high residual でも Prime より noisy で、median / win rate の悪化が大きい。これは「高ADVだから良い」ではなく「規模に対して売買が集まりすぎている銘柄は右尾と左尾が混ざる」と読むべき。

Prime は逆で、high residual の 60d mean excess が明確に強い。少なくとも Prime では high residual を hard exclude する根拠はない。むしろ re-rating / institutional participation / index-like liquidity discovery が混ざっている可能性がある。

Growth は high residual の mean は low より改善する場面があるが、median がかなり悪い。高分散 right-tail bucket として扱うべきで、単独 long signal にはしにくい。

### Production Implication

現時点では production ranking へ直接加点・減点しない。次に見る候補は以下。

| 用途 | 候補 |
|---|---|
| `standard` crowding diagnostic | high `liquidity_residual_z` を加点ではなく sizing haircut / caution 表示候補にする |
| `prime` participation diagnostic | high residual を hard exclude せず、re-rating か crowding かを別特徴で分解する |
| `growth` risk-state diagnostic | high residual は median 悪化が大きいため portfolio lens 前提 |
| stale liquidity diagnostic | low residual は low turnover / exit difficulty として別途 investability 検証する |

次の Phase 2 は、Standard high residual bucket を value ranking / pump-fade / breakout additive と join して、right-tail capture と left-tail budget cost のどちらが支配的かを見る。Prime は high residual を除外候補ではなく、quality participation と crowding の分解候補として扱う。

### Caveats

- free-float は `shares_outstanding - treasury_shares` の proxy であり、浮動株比率そのものではない。
- observation は close 時点の `ADV_N` を使うため、forward return は close-to-close の診断であり、寄付きで事前に建てられる signal ではない。
- `liquidity_implied_ffcap_gap_pct` は valuation target ではない。現在の ADV を正当化する規模感の逆算に過ぎない。
- `stock_master_daily` がある場合は observation date の exact row を市場区分 SoT とする。
- `liquidity_residual_change` は `change_window` 前の ADV と現在の free-float cap から近似する。短期の株式数変化は主眼ではない。
- ADV は capacity / execution diagnostic であり、単独 alpha として扱わない。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/free_float_liquidity_gap.py`
- runner: `apps/bt/scripts/research/run_free_float_liquidity_gap.py`
- bundle experiment id: `market-behavior/free-float-liquidity-gap`
- published bundle: `/tmp/trading25-research/market-behavior/free-float-liquidity-gap/phase1_20260511_fast`
- result tables: `observation_df`, `market_regression_df`, `residual_bucket_forward_return_df`, `residual_change_bucket_forward_return_df`, `market_sample_diagnostics_df`
