# Free-Float Liquidity Prime Momentum Interaction

## Published Readout

### Decision

`free_float_liquidity_prime_momentum_interaction` は、Prime に絞って free-float liquidity residual が price momentum の単なる言い換えか、それとも momentum trade の中で追加的な right-tail diagnostic になるかを確認する Phase 3 research。

Phase 2 の `enriched_observation_df` を入力にし、Prime の observation に対して以下を比較する。

| view | 内容 |
|---|---|
| `momentum_only` | `recent_return_20d_pct` / `recent_return_60d_pct` だけで forward excess を説明 |
| `liquidity_only` | `liquidity_residual_z` だけで forward excess を説明 |
| `momentum_plus_liquidity` | momentum と liquidity residual を同時投入 |
| `momentum_liquidity_interaction` | momentum / liquidity / interaction を同時投入 |
| bucket | positive momentum と high residual の組み合わせを確認 |

### Main Findings

#### 結論

Prime では、free-float liquidity residual は単なる price momentum の言い換えではなく、momentum trade の右尾を支える participation/capacity diagnostic の一部である可能性が高い。

ADV60 / 60d forward excess return の回帰では、`momentum_plus_liquidity` にしても `liquidity_residual_z` の係数が強く残る。一方、`momentum_only` の説明力はかなり小さい。

| model | factor | obs | R2 | coef per 1sd | t-stat | 読み |
|---|---|---:|---:|---:|---:|---|
| `momentum_only` | `recent_return_20d_pct` | 203,288 | 0.0004 | -0.352 | -8.58 | 20d 過熱はむしろ短期反動寄り |
| `momentum_only` | `recent_return_60d_pct` | 203,288 | 0.0004 | +0.290 | +7.06 | 60d momentum は弱くプラス |
| `liquidity_only` | `liquidity_residual_z` | 204,973 | 0.0041 | +0.985 | +29.09 | liquidity residual が単独で残る |
| `momentum_plus_liquidity` | `liquidity_residual_z` | 203,288 | 0.0045 | +0.986 | +28.85 | momentum control 後も残る |
| `momentum_liquidity_interaction` | `momentum_liquidity_interaction_z` | 203,170 | 0.0048 | +0.323 | +8.37 | momentum と residual の組み合わせもプラス |

bucket でも同じ方向。ADV60 / 60d で `positive_20d_60d + high_residual` は、`positive_20d_60d + neutral_residual` を mean excess で +2.935%、median excess で +1.292%、win rate で +4.951pt 上回る。

| comparison | mean spread | median spread | win-rate spread |
|---|---:|---:|---:|
| positive momentum high residual - neutral residual | +2.935% | +1.292% | +4.951pt |
| positive momentum high residual - low residual | +3.659% | +1.437% | +7.544pt |
| high residual positive momentum - mixed/negative momentum | +0.729% | +0.269% | +0.660pt |

つまり、Prime では「上がっているから強い」だけではなく、「上がっていて、かつ free-float 規模対比で売買代金が異常に大きい」状態の方が明確に良い。ただし high residual 自体も強く、positive momentum と mixed/negative momentum の差は小さいため、源泉の中心は price momentum 単体ではなく participation/liquidity residual 側にある。

### Interpretation

この research は「momentum と完全に独立した alpha」を証明するものではない。だが、少なくとも Prime の pooled diagnostic では、20d/60d momentum を control しても liquidity residual が残り、interaction もプラスだった。

解釈としては、Prime の momentum trade の一部は、単なる price trend ではなく、free-float cap 対比で売買代金が増える re-rating / institutional participation 状態を取っている可能性がある。Phase 2 の `rerating_participation` はこの一部を見ている。

### Production Implication

現時点では production strategy に変更を入れない。次は `forward_eps_driven` の実 entry ledger に `liquidity_residual_z` / `momentum_state` / `liquidity_state` を join し、利益右尾が high residual に偏るかを確認する。

仮に同じ構図が実 trade でも出るなら、Prime では entry candidate の hard filter よりも、right-tail conviction / sizing / risk budget の diagnostic として使うのが自然。

### Caveats

- 入力は Phase 2 の observation grid であり、毎営業日の full latest screen ではない。
- `recent_return_*d_pct` は observation date close までを使う diagnostic であり、pre-open signal ではない。
- 回帰は pooled OLS の簡易診断で、sector/date fixed effect はまだ入れていない。
- 目的変数は forward excess return で、portfolio execution / turnover / cost までは見ていない。
- R2 は低く、単独 predictor としての強さではなく、large-N diagnostic と bucket spread の方向性として読む。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/free_float_liquidity_prime_momentum_interaction.py`
- runner: `apps/bt/scripts/research/run_free_float_liquidity_prime_momentum_interaction.py`
- input experiment id: `market-behavior/free-float-liquidity-regime-decomposition`
- bundle experiment id: `market-behavior/free-float-liquidity-prime-momentum-interaction`
- latest result bundle: `/tmp/trading25-research/market-behavior/free-float-liquidity-prime-momentum-interaction/phase3_20260511_prime_momentum_interaction`
- result tables: `prime_panel_df`, `factor_regression_df`, `interaction_bucket_df`, `momentum_residual_summary_df`
