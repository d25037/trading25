# Standard Value Pump/Fade Decomposition

Standard value ranking の上位候補を、普通の低PBR・小型・低forward PER 候補と、
過去に大きく噴いて沈静化した pump/fade 的候補に分解する研究。

## Published Readout

### Decision

Standard value ranking の違和感は本物。現行上位には microcap / low ADV /
high volatility / 2年高値からの深い drawdown / 大型月足後の fade がかなり混ざる。

初期実装では月足終値からの fade と排他的 `pattern_bucket` に寄せていたが、
人間がチャートで見ている「噴いた高値からの崩落」を弱く見積もるため、
月足高値基準の `faded_after_large_month_high` と、非排他的な
`deep_drawdown_after_large_month` を追加した。これにより `2673` / `6276` / `7603`
は全て `deep_high_fade_after_large_month` として捕捉できる。

ただし、historical outcome ではこの bucket も単純な除外対象ではない。
60d の upside 到達率は高く、右尾を持つ一方、p10 / severe loss も重い。
したがって、これは alpha source というより
`right-tail capture + left-tail budget cost` の bucket として扱う。

この research は、Standard value+breakout を quality strategy として読むべきではない、
という確認でもある。過去成績の一部は、普通の割安再評価ではなく、
仕手的に見える再噴火 optionality から来ている可能性が高い。

### Main Findings

#### 結論: current top 25 は「普通の value」だけではない。

| Current top25 diagnostic | Count |
| --- | ---: |
| `risk_score>=3` | 7 |
| `faded_after_large_month_high` | 4 |
| `deep_drawdown_after_large_month` | 5 |
| `faded_after_large_month_high OR deep_drawdown_after_large_month` | 5 |

| Current top25 pattern | Count |
| --- | ---: |
| `plain_value` | 13 |
| `active_rebound` | 4 |
| `deep_high_fade_after_large_month` | 4 |
| `deep_2y_drawdown` | 3 |
| `deep_drawdown_after_large_month` | 1 |

#### 結論: チャート感覚との sanity check は一致した。

| Code | Human read | Quant pattern | Risk | Key diagnostics |
| --- | --- | --- | ---: | --- |
| `6138` | まとも寄り | `active_rebound` | 2 | 2年DD `-4.66%`, high fade `+7.82%` |
| `2673` | 仕手っぽい | `deep_high_fade_after_large_month` | 4 | high fade `-72.84%`, 2年DD `-72.84%` |
| `6276` | 仕手っぽい | `deep_high_fade_after_large_month` | 4 | high fade `-60.14%`, 2年DD `-60.14%` |
| `7603` | かなり仕手っぽい | `deep_high_fade_after_large_month` | 4 | high fade `-85.51%`, close fade `-75.98%` |

`7603` は 2025-06 月足で high `635`、close `383`、2026-05-07 close `92`。
月足終値基準でも `-75.98%` だが、月足高値基準では `-85.51%` で、
人間の「吹いた後に上場来安値圏まで沈んだ」感覚に近い。

#### 結論: risk score は「除外」ではなく、右尾と左尾の同居を示す。

| Top | Risk bucket | Horizon | Events | Mean | Median | P10 | Severe | Upside20 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `top_25` | `ordinary_value_score_0_1` | 60 | 318 | `+10.17%` | `+6.89%` | `-14.62%` | `16.35%` | `33.96%` |
| `top_25` | `watch_score_2` | 60 | 768 | `+8.54%` | `+2.25%` | `-17.19%` | `21.35%` | `38.02%` |
| `top_25` | `pump_fade_like_score_ge3` | 60 | 564 | `+6.96%` | `+2.46%` | `-20.87%` | `24.47%` | `47.52%` |

#### 結論: 月足高値 fade は右尾も左尾も大きい。

| Top | Pattern | Horizon | Events | Mean | Median | P10 | Severe | Upside20 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `top_25` | `plain_value` | 60 | 720 | `+6.40%` | `+3.14%` | `-14.59%` | `17.22%` | `32.22%` |
| `top_25` | `deep_high_fade_after_large_month` | 60 | 98 | `+13.31%` | `+1.47%` | `-21.83%` | `32.65%` | `59.18%` |
| `top_25` | `deep_drawdown_after_large_month` | 60 | 92 | `+4.88%` | `-1.80%` | `-23.51%` | `23.91%` | `50.00%` |

`deep_high_fade_after_large_month` は mean と upside20 が高い一方、severe loss も
`32.65%` と重い。hard filter ではなく sizing / risk budget の対象にするのが自然。

### Interpretation

`pump/fade` は「仕手に乗るから悪い」と単純化できない。historical top25 では
`deep_high_fade_after_large_month` の 60d mean は `+13.31%` で、`plain_value`
の `+6.40%` より高い。さらに `+20%` upside 到達率は `59.18%` まで上がる。
これは、沈静化後に再度資金が戻る right-tail が実際にあることを示す。

一方で、同じ bucket の 60d p10 は `-21.83%`、severe loss rate は `32.65%`。
つまり、この family は平均だけを見ると魅力的だが、同じサイズで持つと
portfolio の左尾を太くする。

読みとしては、Standard value の中に二種類が混ざっている。
ひとつは普通の低期待・割安再評価候補、もうひとつは過去の投機 episode が残る
再噴火待ち候補。後者は alpha ではなく convex / lottery 的な exposure に近い。
そのため、production では「良い会社を安く買う」ではなく、
`speculative value sleeve` として別枠の risk budget を割り当てる前提で扱う。

### Production Implication

Ranking には `pump/fade` diagnostic を表示する。特に以下は画面上で分ける価値がある。

- `faded_after_large_month_high`: 月足高値から大きく沈んでいる。
- `deep_drawdown_after_large_month`: 大型月足後に2年高値から深いDD。
- `deep_high_fade_after_large_month`: 上記2つが重なった最も仕手っぽい bucket。

次の実装候補は、hard exclude ではなく `risk badge` と `position-size haircut`。
portfolio lens では `risk_score>=3` を 0.5x、`deep_high_fade_after_large_month`
を 0.25x、`plain_value` は 1.0x のような sizing 分岐を比較する。

### Caveats

過去検証は `annual-value-breakout-periodic-rebalance` の PIT-safe rebalance panel
から候補を作る。current Ranking examples は現行画面の違和感確認用に別途付与するため、
historical outcome evidence とは分けて読む。

本研究は event-level 20d / 60d outcome であり、同時保有、position sizing、
コスト、約定可能性、税、板厚は未反映。`pump/fade` の定義は月足近似であり、
ニュース起点・仕手性そのものを識別しているわけではない。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/standard_value_pump_fade_decomposition.py`
- Runner: `apps/bt/scripts/research/run_standard_value_pump_fade_decomposition.py`
- Bundle: `/tmp/trading25-research/market-behavior/standard-value-pump-fade-decomposition/20260508_standard_value_pump_fade_v2_high_flags/`
- Results DB: `/tmp/trading25-research/market-behavior/standard-value-pump-fade-decomposition/20260508_standard_value_pump_fade_v2_high_flags/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/standard-value-pump-fade-decomposition/20260508_standard_value_pump_fade_v2_high_flags/summary.md`

## Current Surface

- `candidate_event_df`: PIT candidate + pump/fade diagnostics + forward outcomes。
- `risk_score_summary_df`: speculative risk score bucket ごとの 20d / 60d outcome。
- `pattern_summary_df`: `deep_high_fade_after_large_month` / `plain_value` などの pattern bucket outcome。
- `flag_summary_df`: microcap、low ADV、high volatility、deep drawdown、large candle high fade の単独分解。
- `current_snapshot_df`: 最新 Ranking 上位の説明用 diagnostics。

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_standard_value_pump_fade_decomposition.py \
  --output-root /tmp/trading25-research
```
