# Free-Float Liquidity Regime Decomposition

## Published Readout

### Decision

`free_float_liquidity_regime_decomposition` は、`free_float_liquidity_gap` Phase 1 の high residual を「良い参加増」と「売られながらの出来高増」に分解する Phase 2 research として追加する。

Phase 1 の `liquidity_residual_z` は、free-float 時価総額に対して売買代金がどれだけ大きいかを測るだけで、価格方向を見ていない。そのため、8136 サンリオのような出来高を伴う下落と、4022 ラサ工業のような上昇を伴う re-rating を同じ high residual として扱ってしまう。Phase 2 では observation date 時点で観測済みの `recent_return_20d_pct` / `recent_return_60d_pct` を付与し、以下の状態に分ける。

| regime | 定義 | 読み |
|---|---|---|
| `rerating_participation` | high residual かつ 20d/60d return が両方プラス | 参加者増を伴う上昇 / discovery |
| `distribution_stress` | high residual かつ 20d/60d return のどちらかがマイナス | 売られながらの出来高 / stress participation |
| `stale_liquidity` | low residual かつ residual recovery なし | 規模対比で売買が薄い |
| `liquidity_recovery` | low residual だが residual change が急上昇 | 薄商いからの参加回復候補 |

### Main Findings

#### 結論

実データでは、ユーザー指摘の「8136 サンリオ型」と「4022 ラサ工業型」の違いは price-direction regime として分離できた。ただし Prime では `distribution_stress` も一律に悪いわけではなく、hard exclude ではなく risk state / sizing diagnostic として扱うのが妥当。

ADV60 / 60d forward excess return の主要結果:

| market | regime | obs | mean excess | median excess | win rate | 読み |
|---|---:|---:|---:|---:|---:|---|
| Prime | `rerating_participation` | 10,456 | +2.637% | -0.496% | 53.40% | 右尾 capture と整合 |
| Prime | `distribution_stress` | 18,766 | +1.878% | -0.754% | 54.48% | 悪くはないが stress 状態 |
| Prime | `liquidity_recovery` | 2,150 | -1.373% | -2.568% | 51.91% | 単独では弱い |
| Prime | `stale_liquidity` | 26,561 | -0.809% | -1.595% | 53.55% | capacity caution |
| Standard | `rerating_participation` | 7,599 | -0.312% | -6.282% | 42.43% | 上昇型でも追いかけ危険 |
| Growth | `rerating_participation` | 2,059 | +0.812% | -11.919% | 37.64% | 強い run-up 後の left-tail |

Prime ADV60 の observed regime 数:

| regime | obs | codes | median recent 20d | median recent 60d | median residual z |
|---|---:|---:|---:|---:|---:|
| `rerating_participation` | 10,634 | 910 | +8.457% | +18.318% | 1.461 |
| `distribution_stress` | 19,088 | 1,077 | -4.727% | -6.789% | 1.460 |
| `liquidity_recovery` | 2,162 | 602 | +3.735% | +6.732% | -1.358 |
| `stale_liquidity` | 26,648 | 1,022 | +0.636% | +0.748% | -1.380 |

最新 Prime snapshot は Phase 1 の 20-session observation grid に依存するため、全銘柄の latest screen ではない。2026-05-08 grid 上では、ADV60 で 6532 ベイカレントが `rerating_participation`、4480 メドレーが `distribution_stress` に分類された。全 Prime を毎営業日で拾う latest screen は、この runner とは別に daily scoring 化する。

### Interpretation

この Phase 2 は buy/sell signal を作るものではない。high residual を一律に良い/悪いと扱わず、価格方向と組み合わせて状態分類するための diagnostic である。

Prime では `rerating_participation` が `forward_eps_driven` の右尾 capture と整合的。`distribution_stress` も forward excess mean はプラスなので、単純除外ではなく position sizing / risk cap 候補として扱う。

Standard/Growth は違う。high residual かつ上昇型でも median と win rate が悪く、Prime の「参加者増」は市場横断で同じ意味にならない。特に Growth は run-up 後の left-tail が強いため、free-float liquidity gap を使うなら市場別に別物として扱う。

### Production Implication

現時点では production strategy に変更を入れない。次の判断対象は以下。

| 用途 | 候補 |
|---|---|
| Prime re-rating | `rerating_participation` の forward return / right-tail を確認 |
| Prime stress | `distribution_stress` の median / severe-loss を確認 |
| Standard/Growth caution | high residual が上昇型でも left-tail を持つか確認 |
| forward_eps_driven audit | 実トレード entry date に regime を join して tail を分解 |

`primeExTopix500` の production `forward_eps_driven` へ入れる前に、実際の entry trade ledger に `liquidity_regime` を join し、利益の右尾が `rerating_participation` に偏るか、損失 tail が `distribution_stress` / non-Prime high residual に偏るかを確認する。

### Caveats

- `recent_return_*d_pct` は observation date の close までを含むため、pre-open signal ではなく close-to-close diagnostic。
- high residual threshold は default `liquidity_residual_z >= 1.0`。
- 20d/60d return の符号で分類する単純な Phase 2 であり、材料、決算、需給イベントはまだ区別しない。
- Phase 1 の free-float は `shares_outstanding - treasury_shares` proxy であり、浮動株そのものではない。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/free_float_liquidity_regime_decomposition.py`
- runner: `apps/bt/scripts/research/run_free_float_liquidity_regime_decomposition.py`
- input experiment id: `market-behavior/free-float-liquidity-gap`
- bundle experiment id: `market-behavior/free-float-liquidity-regime-decomposition`
- latest result bundle: `/tmp/trading25-research/market-behavior/free-float-liquidity-regime-decomposition/phase2_20260511_regime_strict`
- result tables: `enriched_observation_df`, `regime_forward_return_df`, `market_regime_diagnostics_df`, `latest_prime_regime_df`
