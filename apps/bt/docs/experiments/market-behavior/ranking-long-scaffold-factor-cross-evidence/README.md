# Ranking Long Scaffold Factor Cross Evidence

Daily Ranking Research Base を使い、既存 long scaffold に以下の3条件を追加して、liquidity z の置換と `Fwd OP/OP` / `Good Fwd PER` の導入余地を検証する。

- `liquidity_z_0_to_2_rerating`: `0 < liquidity_residual_z < 2` かつ `20D >= 0` かつ `60D >= 0`
- `liquidity_z_minus1_to_2_rerating`: `-1 < liquidity_residual_z < 2` かつ `20D >= 0` かつ `60D >= 0`
- `fwd_op_op_gt_1_2`: `forecast_operating_profit_growth_ratio > 1.2`
- `good_fwd_per`: `forward_per_to_per_ratio <= 0.8`

## Published Readout

### Decision

Run: `20260703_long_scaffold_factor_cross_prime_full_history`

Thin companion: `20260703_long_scaffold_factor_cross_prime_full_history_min30`

`z=-1..2` follow-up: `20260703_long_scaffold_factor_cross_prime_full_history_zm1to2`

`z=-1..2` thin companion: `20260703_long_scaffold_factor_cross_prime_full_history_zm1to2_min30`

対象は Prime 全期間、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。publication run は `min_observations=100`、strict な3条件交差の sample thickness を見るため companion は `min_observations=30` で実行した。

結論:

- `0 < z < 2 AND 20D>=0 AND 60D>=0` は、`neutral_rerating` の単純置換候補として有力。`Deep Value + Long Hybrid Leadership + ATR20 Accel` に重ねると 20D/60D median は改善する。
- ただし z置換は sample をかなり絞り、left-tail は少し悪化する。既存 `neutral_rerating` を完全置換するというより、`z=1..2` を捨てない priority-up diagnostic として扱うのが妥当。
- `-1 < z < 2 AND 20D>=0 AND 60D>=0` は、`neutral_rerating` を拡張する置換として `z 0..2` より実務的。`z 0..2` ほど median は跳ねないが、sample を戻し、`Fwd OP/OP > 1.2` 交差の tail 悪化をかなり抑える。
- `Fwd OP/OP > 1.2` は、`Neutral + Deep Value + Long Hybrid + ATR20 Accel` の中では20Dで強い。単独 badge としては有効だが、`z 0..2` と同時に入れると sample が薄くなり、60D left-tail が悪化しやすい。
- `Good Fwd PER` は value confirmation として自然だが、既存 `Deep Value` 定義に一部内包される。strict scaffold 内では `Fwd OP/OP > 1.2` より sample が厚く、`z 0..2` との組み合わせは比較的安定する。
- 3条件全部載せは high-conviction に見えるが sample が薄く、severe loss が上がる。production hard filter にはしない。

### Main Findings

#### 結論: `z 0..2` は strict scaffold の median を上げるが、tail はやや重くなる

`Deep Value + Long Hybrid + ATR20 Accel` の中で `neutral_rerating` を `z 0..2 rerating` に置き換えると、20D median は `+2.521%` から `+3.636%`、60D median は `+3.239%` から `+5.671%` に上がる。一方で observation は半分未満になり、60D severe loss は `8.086%` から `10.040%` に悪化する。

| Scaffold | Horizon | Obs | Codes | Median excess | Win rate | Severe loss | p10 excess |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value | 20D | 295,636 | 771 | -0.064% | 49.532% | 4.152% | -7.067% |
| Neutral + Deep Value | 20D | 78,292 | 602 | +0.361% | 52.569% | 3.684% | -6.867% |
| z 0..2 + Deep Value | 20D | 34,630 | 429 | +0.354% | 52.264% | 5.391% | -7.767% |
| Deep Value + Long Hybrid + ATR20 Accel | 20D | 3,533 | 183 | +1.379% | 60.826% | 2.179% | -5.662% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | 20D | 1,841 | 111 | +2.521% | 70.397% | 1.249% | -4.101% |
| z 0..2 + Deep Value + Long Hybrid + ATR20 Accel | 20D | 775 | 80 | +3.636% | 70.323% | 2.452% | -5.340% |
| Deep Value + Long Hybrid + ATR20 Accel | 60D | 3,460 | 178 | +1.765% | 55.809% | 16.214% | -13.381% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | 60D | 1,818 | 108 | +3.239% | 63.806% | 8.086% | -9.164% |
| z 0..2 + Deep Value + Long Hybrid + ATR20 Accel | 60D | 757 | 77 | +5.671% | 69.089% | 10.040% | -10.007% |

#### 結論: `z -1..2` は neutral rerating 拡張としてバランスが良い

`z 0..2` は median を最大化しやすいが sample と tail の代償がある。`z -1..2` に広げると、`Neutral + Deep Value + Long Hybrid + ATR20 Accel` に近い sample を保ちながら、20D/60D median はわずかに改善する。`z=1..2` を捨てない一方で、`-1..0` の安定 bucket を戻すため、neutral 拡張としてはこちらの方が扱いやすい。

| Scaffold | Horizon | Obs | Codes | Median excess | Win rate | Severe loss | p10 excess | Median z |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | 20D | 1,841 | 111 | +2.521% | 70.397% | 1.249% | -4.101% | -0.204 |
| z 0..2 + Deep Value + Long Hybrid + ATR20 Accel | 20D | 775 | 80 | +3.636% | 70.323% | 2.452% | -5.340% | +0.361 |
| z -1..2 + Deep Value + Long Hybrid + ATR20 Accel | 20D | 2,029 | 118 | +2.639% | 70.478% | 1.676% | -4.317% | -0.150 |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | 60D | 1,818 | 108 | +3.239% | 63.806% | 8.086% | -9.164% | -0.215 |
| z 0..2 + Deep Value + Long Hybrid + ATR20 Accel | 60D | 757 | 77 | +5.671% | 69.089% | 10.040% | -10.007% | +0.357 |
| z -1..2 + Deep Value + Long Hybrid + ATR20 Accel | 60D | 2,006 | 115 | +3.652% | 64.008% | 8.674% | -9.551% | -0.153 |

#### 結論: `Fwd OP/OP > 1.2` は neutral strict scaffold の単独 badge としては強い

`Neutral + Deep Value + Long Hybrid + ATR20 Accel` 内では、`Fwd OP/OP > 1.2` が20D median `+4.726%`、60D median `+3.690%`。ただし 20D obs は `146`、60D obs は `145` と薄い。`z 0..2` replacement 後に同じ条件を足すと 20D/60D median は baseline より下がり、tail も悪化する。

| Scaffold | Factor | Horizon | Obs | Codes | Median excess | Win rate | Severe loss | p10 excess |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Long Hybrid + ATR20 Accel | z 0..2 rerating | 20D | 775 | 80 | +3.636% | 70.323% | 2.452% | -5.340% |
| Deep Value + Long Hybrid + ATR20 Accel | Fwd OP/OP > 1.2 | 20D | 405 | 51 | +3.655% | 67.407% | 4.938% | -6.115% |
| Deep Value + Long Hybrid + ATR20 Accel | Good Fwd PER | 20D | 903 | 85 | +1.933% | 62.016% | 4.097% | -6.390% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | Fwd OP/OP > 1.2 | 20D | 146 | 26 | +4.726% | 74.658% | 1.370% | -5.445% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | Good Fwd PER | 20D | 450 | 54 | +3.006% | 69.556% | 1.556% | -4.631% |
| z 0..2 + Deep Value + Long Hybrid + ATR20 Accel | Fwd OP/OP > 1.2 | 20D | 116 | 21 | +2.737% | 63.793% | 8.621% | -8.771% |
| z -1..2 + Deep Value + Long Hybrid + ATR20 Accel | Fwd OP/OP > 1.2 | 20D | 182 | 31 | +4.074% | 70.879% | 5.495% | -6.583% |
| z 0..2 + Deep Value + Long Hybrid + ATR20 Accel | Good Fwd PER | 20D | 253 | 38 | +3.636% | 68.379% | 5.138% | -6.562% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | Fwd OP/OP > 1.2 | 60D | 145 | 25 | +3.690% | 66.897% | 13.103% | -12.693% |
| z 0..2 + Deep Value + Long Hybrid + ATR20 Accel | Fwd OP/OP > 1.2 | 60D | 116 | 21 | +3.237% | 61.207% | 23.276% | -17.080% |
| z -1..2 + Deep Value + Long Hybrid + ATR20 Accel | Fwd OP/OP > 1.2 | 60D | 181 | 30 | +4.096% | 66.298% | 15.470% | -15.726% |

#### 結論: 条件を全部載せるほど良いわけではない

Thin companion で見ると、`Neutral + Deep Value + Long Hybrid + ATR20 Accel` の `Fwd OP/OP > 1.2 + Good Fwd PER` は20D median `+4.547%` と良いが obs `99`。3条件全部載せは obs `57` まで落ち、20D median `+3.474%`、60D severe loss `31.579%`。これは hard filter ではなく、exception review / badge の組み合わせとして読むべき薄さ。

| Scaffold | Combo | Horizon | Obs | Codes | Median excess | Win rate | Severe loss | p10 excess |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | z 0..2 + Fwd OP/OP > 1.2 | 20D | 80 | 16 | +4.387% | 67.500% | 2.500% | -6.658% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | z -1..2 + Fwd OP/OP > 1.2 | 20D | 146 | 26 | +4.726% | 74.658% | 1.370% | -5.445% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | z 0..2 + Good Fwd PER | 20D | 203 | 32 | +3.636% | 67.980% | 2.463% | -6.398% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | Fwd OP/OP > 1.2 + Good Fwd PER | 20D | 99 | 19 | +4.547% | 72.727% | 2.020% | -6.334% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | all three | 20D | 57 | 11 | +3.474% | 59.649% | 3.509% | -6.887% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | z -1..2 + Fwd OP/OP > 1.2 + Good Fwd PER | 20D | 99 | 19 | +4.547% | 72.727% | 2.020% | -6.334% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | z 0..2 + Good Fwd PER | 60D | 194 | 30 | +3.828% | 68.557% | 13.402% | -15.332% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | all three | 60D | 57 | 11 | +3.690% | 63.158% | 31.579% | -18.952% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | z -1..2 + Fwd OP/OP > 1.2 + Good Fwd PER | 60D | 98 | 18 | +4.834% | 69.388% | 19.388% | -15.805% |

### Interpretation

今回の3条件は all-market では standalone long signal ではない。20D/60D all-market median はいずれもマイナスで、既存の Deep Value / leadership / ATR scaffold が必要。

`liquidity_z_0_to_2_rerating` は、既存 `neutral_rerating` を単純に否定するものではない。`-1<z<1` の安定性を残しつつ、`z=1..2` を強い long scaffold 内で捨てすぎないための置換候補として有用。`z 0..2` は `-1..0` を捨てて `1..2` を入れる形なので median は上がるが、sample と left-tail には注意が必要。`z -1..2` はこの弱点を補い、neutral rerating の拡張としてはより自然。

`Fwd OP/OP > 1.2` は既存 growth readout と整合し、`Deep Value + Long Hybrid + ATR20 Accel` 系の priority badge になる。ただし `z 0..2` と同時に要求すると銘柄数が薄くなり、60D severe loss が悪化しやすい。`z -1..2` まで広げると 20D median は `+4.074%`、60D median は `+4.096%` に戻るため、拡張 neutral の中の priority badge としては使いやすい。それでも tail は neutral strict 単独より重く、hard eligibility ではなく Inspect priority / badge とする。

`Good Fwd PER` は独立の新主条件というより、Deep Value の中身を説明する補助軸。`z 0..2` と組み合わせても `Fwd OP/OP > 1.2` より sample が残るため、tie-breaker としては扱いやすい。

### Production Implication

- Daily Ranking の long-side filter では、`neutral_rerating` の上限 `z<1` を hard upper bound にしない。強い long scaffold 内では `-1<z<2 AND 20D>=0 AND 60D>=0` を neutral 拡張候補、`0<z<2` を高 median / sample-thin diagnostic として比較表示できるようにする。
- `Fwd OP/OP > 1.2` は `Deep Value + Long Hybrid + ATR20 Accel` 系の Inspect priority badge として有力。ただし `z 0..2` 置換後の hard filter にはしない。`z -1..2` 拡張内では導入余地があるが、tail caution を併記する。
- `Good Fwd PER` は value explanation / tie-breaker として扱う。既存 Deep Value を置き換えない。
- 3条件全部載せは sample-thin かつ 60D tail が重く、production hard filter にしない。
- portfolio construction、turnover、cost、sector cap は未検証。position sizing や最終採用には portfolio lens が必要。

### Caveats

- outcome は 5D/20D/60D close-to-close TOPIX excess return。portfolio performance ではない。
- publication run は `min_observations=100`。strict combo の一部は companion `min_observations=30` でのみ表示される。
- `Fwd OP/OP > 1.2` は `forecast_operating_profit_growth_ratio > 1.2` として評価した。
- `Good Fwd PER` は `forward_per_to_per_ratio <= 0.8`。既存 `strong_value_confirmation` の一部と重なる。
- `z -1..2` follow-up は既存 publication run の同一 runner / 同一 local market DB に `liquidity_z_minus1_to_2_rerating` を追加して再実行した。
- Bundle 生成時点の local `market.duckdb` は live source で、manifest は dirty worktree を記録している。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_long_scaffold_factor_cross_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_long_scaffold_factor_cross_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_long_scaffold_factor_cross_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-long-scaffold-factor-cross-evidence/20260703_long_scaffold_factor_cross_prime_full_history/`
- Thin companion: `~/.local/share/trading25/research/market-behavior/ranking-long-scaffold-factor-cross-evidence/20260703_long_scaffold_factor_cross_prime_full_history_min30/`
- `z -1..2` bundle: `~/.local/share/trading25/research/market-behavior/ranking-long-scaffold-factor-cross-evidence/20260703_long_scaffold_factor_cross_prime_full_history_zm1to2/`
- `z -1..2` thin companion: `~/.local/share/trading25/research/market-behavior/ranking-long-scaffold-factor-cross-evidence/20260703_long_scaffold_factor_cross_prime_full_history_zm1to2_min30/`
- Results tables: `long_scaffold_evidence_df`, `factor_condition_evidence_df`, `long_scaffold_factor_evidence_df`, `long_scaffold_factor_combo_evidence_df`, `coverage_diagnostics_df`
