# ATR Expansion Forward Response

20D / 60D ATR の拡大を、日次 anchor からの forward TOPIX-excess return で見る独立実験です。
Ranking Color Evidence の technical 補強候補として、単純な long trend ではなく「短期ボラが伸びている状態」が continuation / exhaustion / left-tail warning のどれに近いかを確認します。

## Published Readout

### Decision

`20D(or60D) ATR の拡大` は、単独の positive ranking feature としては採用しない。特に `atr20_pct` / `atr60_pct` の上位 bucket は mean が右尾で持ち上がる一方、median は悪く、severe loss が大きい。

採用候補として残すのは、既存の rerating / 20D・60D return positive 系に重ねる補助特徴としての `atr20_change_20d_pct >= 25% AND atr20_to_atr60 < 1.25`。これは persistent runup 内で 20D forward excess の mean と median を改善し、左尾も大きく悪化させなかった。一方、`atr20_to_atr60 >= 1.25` まで短期ATRが突出する状態は過熱・荒れの warning として扱う。

2026-05-23 follow-up `20260523_atr_expansion_liquidity_color_overheat_excluded_prime_v4` では、Ranking Color Evidence の非重複色に合わせて `crowded_rerating` の green / blue / yellow、`neutral_rerating` の green / blue に絞り、repo の Ranking risk flag と同じ `OVERHEAT = recent_return_20d_pct >= 30.0` を除外した slice も追加した。

OVERHEAT 除外後も、ATR20加速は green 系の品質確認として有効寄り。`crowded_rerating green` の `atr20_acceleration_ex_overheat` は mean `+3.703%`、median `+1.570%`、severe `4.70%` で、全体 green より左尾が軽い。`neutral_rerating green` でも mean `+3.365%`、median `+3.057%`、severe `0.00%`。一方、`crowded_rerating yellow` は OVERHEAT 除外後も median `-1.099%`、ATR20加速でも median `-1.239%` なので救済しない。

### Main Findings

#### 結論

分析対象は local DB coverage 上 `2022-04-04` から `2026-05-14` の Prime、`1,717,700` stock-days。下表は `entry_mode=close_to_close`、`horizon=20`、forward TOPIX-excess return。

| slice | obs | mean | median | p10 | win | severe | interpretation |
|---|---:|---:|---:|---:|---:|---:|---|
| `atr20_pct top_10pct` | 169,432 | +0.463% | -0.945% | -13.502% | 46.29% | 17.14% | 絶対ボラ高は右尾も左尾も大きい。買いfeatureではない |
| `atr60_pct top_10pct` | 167,605 | +0.361% | -1.152% | -14.417% | 45.55% | 18.90% | 長めATR高も同様に risk-on ではなく荒れ |
| `atr20_to_atr60 top_10pct` | 170,750 | -0.010% | -0.637% | -9.262% | 46.07% | 8.43% | 短期ATR突出だけでは改善しない |
| `atr20_change_20d_pct top_10pct` | 166,497 | +0.028% | -0.674% | -9.306% | 45.87% | 8.58% | 加速単体も弱い |

#### return regime との交差

| return_regime | ATR state | obs | mean | median | severe | interpretation |
|---|---|---:|---:|---:|---:|---|
| `persistent_runup` | `atr20_acceleration` | 87,239 | +0.306% | -0.351% | 7.59% | 最も採用候補。20D/60D上昇中にATR20が加速、ただし短期ATR比は過熱していない |
| `persistent_runup` | `no_expansion` | 457,428 | -0.061% | -0.601% | 7.75% | baseline より `atr20_acceleration` が改善 |
| `persistent_runup` | `dual_expansion` | 104,604 | +0.019% | -0.605% | 9.61% | 比率も加速も高い状態は左尾が悪化 |
| `short_pullback_in_uptrend` | `atr20_acceleration` | 25,975 | -0.557% | -1.139% | 7.30% | 押し目局面のATR加速は買い材料ではない |
| `downtrend_decline` | `atr20_acceleration` | 46,266 | -0.483% | -1.170% | 7.98% | 下落中のATR加速も悪い |

#### ATR20/ATR60 pair

| ATR20 bucket | ATR60 bucket | obs | mean | median | severe | interpretation |
|---|---|---:|---:|---:|---:|---|
| `high_20pct` | `high_20pct` | 244,874 | +0.193% | -1.097% | 17.14% | 高ボラ銘柄は severe loss が大きく、右尾狙い以外では避ける |
| `high_20pct` | `middle_60pct` | 93,845 | +0.108% | -0.563% | 8.97% | 60Dまで高くない短期ボラ上昇は相対的にましだが、単独採用には弱い |
| `middle_60pct` | `middle_60pct` | 835,125 | -0.124% | -0.590% | 6.86% | 平常ボラ baseline |

#### Ranking Color Evidence 色別

下表は `liquidity_color_atr_interaction_df`、`entry_mode=close_to_close`、`horizon=20`。`all_atr` はその色全体、`overheat_excluded` は repo の Ranking risk flag と同じ `recent_return_20d_pct < 30.0`、`atr20_acceleration_ex_overheat` は `recent_return_20d_pct < 30.0 AND atr20_change_20d_pct >= 25 AND atr20_to_atr60 < 1.25`。

| Regime | UI color | ATR state | obs | mean | median | severe | interpretation |
|---|---|---|---:|---:|---:|---:|---|
| `crowded_rerating` | green | `all_atr` | 4,496 | +3.070% | +0.994% | 7.98% | baseline が強い |
| `crowded_rerating` | green | `overheat_excluded` | 4,366 | +3.097% | +0.949% | 7.79% | OVERHEAT除外でtailは少し改善、medianはほぼ維持 |
| `crowded_rerating` | green | `atr20_acceleration_ex_overheat` | 575 | +3.703% | +1.570% | 4.70% | green の品質確認として有効。sample は小さい |
| `crowded_rerating` | green | `overheat_only` | 130 | +2.185% | +2.961% | 14.62% | medianは高いがtailが重く、risk flag扱いは妥当 |
| `crowded_rerating` | blue | `all_atr` | 4,369 | +0.835% | -0.748% | 9.66% | blue は median が弱い |
| `crowded_rerating` | blue | `overheat_excluded` | 4,282 | +0.804% | -0.791% | 9.57% | OVERHEAT除外だけでは改善しない |
| `crowded_rerating` | blue | `atr20_acceleration_ex_overheat` | 520 | +2.017% | -0.109% | 5.19% | 左尾は改善するが median はマイナス |
| `crowded_rerating` | yellow | `all_atr` | 50,436 | +0.444% | -1.061% | 18.83% | yellow の左尾riskは大きい |
| `crowded_rerating` | yellow | `overheat_excluded` | 46,847 | +0.304% | -1.099% | 18.49% | OVERHEAT除外でも yellow は弱い |
| `crowded_rerating` | yellow | `atr20_acceleration_ex_overheat` | 6,362 | +0.402% | -1.239% | 19.02% | ATR加速は救済しない |
| `crowded_rerating` | yellow | `overheat_only` | 3,589 | +2.273% | -0.427% | 23.21% | 右尾はあるがtailが重すぎる |
| `neutral_rerating` | green | `all_atr` | 2,947 | +2.133% | +1.556% | 2.10% | baseline が強い |
| `neutral_rerating` | green | `overheat_excluded` | 2,917 | +2.056% | +1.497% | 2.09% | OVERHEAT除外後も強い |
| `neutral_rerating` | green | `atr20_acceleration_ex_overheat` | 376 | +3.365% | +3.057% | 0.00% | green の強化候補。ただし sample 小 |
| `neutral_rerating` | blue | `all_atr` | 434,055 | -0.000% | -0.433% | 7.08% | blue 全体は弱い |
| `neutral_rerating` | blue | `overheat_excluded` | 428,353 | -0.005% | -0.432% | 6.97% | OVERHEAT除外の効果は小さい |
| `neutral_rerating` | blue | `atr20_acceleration_ex_overheat` | 57,234 | +0.395% | -0.120% | 6.42% | 改善はあるが、色を上げるほどではない |
| `neutral_rerating` | blue | `overheat_only` | 5,702 | +0.386% | -0.544% | 15.50% | risk flag として妥当 |

### Interpretation

ATRの絶対水準が高い銘柄は、mean だけを見ると良く見えるが、median / p10 / severe loss が悪い。これは lottery-like な右尾と左尾が同時に増える状態で、Ranking Color Evidence の mean / median 切り分け改善とは性質が違う。

有用性があるのは「上昇トレンド中に、ATR20が20日前比で増えてきたが、ATR20/ATR60が1.25以上に飛び出していない」状態。価格 rerating に出来高・流動性が伴っている候補の technical confirmation としては検討できるが、ATRだけで candidate を増やすべきではない。

Ranking Color Evidence の色別に絞ると、この読みはより明確になる。`crowded green` と `neutral green` では OVERHEAT を除外した `atr20_acceleration` が mean / median を改善し、特に severe loss も悪化しない。一方、`crowded yellow` は OVERHEAT 除外でも median が悪く、ATR20加速でも救済されない。`neutral blue` は改善しても median がマイナスに残るため、色を上げる根拠には弱い。

### Production Implication

- `atr20_pct` / `atr60_pct` high bucket は positive filter ではなく risk annotation として扱う。
- 既存の crowded / neutral rerating green-blue 分解に重ねるなら、第一候補は `persistent_runup AND atr20_change_20d_pct >= 25 AND atr20_to_atr60 < 1.25`。
- `atr20_to_atr60 >= 1.25 AND atr20_change_20d_pct >= 25` は、continuation confirmation ではなく過熱 warning として別扱いにする。
- Ranking UI に混ぜるなら、`crowded_rerating green` と `neutral_rerating green` に対する positive overlay は `atr20_acceleration_ex_overheat`。
- 既存 repo 定義の `overheat` は `recent_return_20d_pct >= 30.0` の risk flag として維持する。ATR の `dual_expansion` は正式OVERHEATではなく、ATR-specific volatility warning として別名で扱う。
- `crowded blue` / `neutral blue` はATR20加速で多少改善するが、green昇格条件にはしない。

### Caveats

- ATR は anchor 日の OHLC を使うため、close 後に観測できる signal として扱う。
- rolling ATR は simple moving average of true range として計算する。
- market scope は PIT-safe な `stock_master_daily` exact-date を優先し、無い場合のみ `stocks` latest fallback を使う。
- local DB のこの run では、要求 start-date は `2016-04-01` だが実際の stock-day coverage は `2022-04-04` 以降だった。

### Source Artifacts

- runner: `apps/bt/scripts/research/run_atr_expansion_forward_response.py`
- domain: `apps/bt/src/domains/analytics/atr_expansion_forward_response.py`
- tests: `apps/bt/tests/unit/domains/analytics/test_atr_expansion_forward_response.py`
- bundle: `/private/tmp/trading25-research/market-behavior/atr-expansion-forward-response/20260523_atr_expansion_forward_response_prime_v1`
- liquidity color follow-up bundle: `/private/tmp/trading25-research/market-behavior/atr-expansion-forward-response/20260523_atr_expansion_liquidity_color_prime_v2`
- overheat excluded follow-up bundle: `/private/tmp/trading25-research/market-behavior/atr-expansion-forward-response/20260523_atr_expansion_liquidity_color_overheat_excluded_prime_v4`
- result tables: `coverage_diagnostics_df`, `atr_expansion_response_df`, `return_regime_interaction_df`, `atr_pair_interaction_df`, `liquidity_color_atr_interaction_df`, `observation_sample_df`

## Reproduction

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_atr_expansion_forward_response.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --output-root /private/tmp/trading25-research \
  --run-id 20260523_atr_expansion_forward_response_prime_v1 \
  --min-observations 1000
```
