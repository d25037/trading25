# Annual Prime Value PBR Absorption

[`annual-value-composite-selection`](../annual-value-composite-selection/README.md)
で Prime top `5%` の最適配分が小型 + 低 `forward PER` 寄りになったことを受け、
Prime 全体では独立性が残る低 `PBR` が、top slice では他の2軸に吸収されるのかを確認する研究。

## Published Readout

### Decision

Prime top `5%` では、低 `PBR` は主役ではない。`PBR` weight `0%` と現行 `5%` はほぼ同等で、`10%` までは許容範囲だが、`20%` 以上へ上げると CAGR / Sharpe が落ちる。したがって Prime の Ranking score は小型 + 低 `forward PER` を主軸にし、低 `PBR` は少量の補助 weight または tie-breaker として扱う。

### Why This Research Was Run

`annual-fundamental-confounder-analysis` では、Prime 全体の cross-section で低 `PBR` が小型・低 `forward PER` と同時に入れても独立して残った。一方、Prime top `5%` の portfolio lens では `PBR` weight が `5%` まで下がった。両者の差が「PBR の無効化」なのか、「強選抜時に小型・低 `forward PER` へ吸収される」のかを分けるために実行した。

### Data Scope / PIT Assumptions

入力は v3 positive-ratio value bundle `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive/`。分析対象は `prime` / liquidity `none` / top `5%`。score は既存 bundle の `scored_panel_df` を使い、`PBR` weight を固定値で振り、残り weight を現行 `prime_size_tilt` の小型:低 `forward PER` 比率で按分した。portfolio lens は既存 annual value と同じ annual open-to-close equal-weight daily close path。

### Main Findings

#### 結論

| PBR weight | Size weight | Forward PER weight | Events | CAGR | Sharpe | MaxDD | Annual mean | Year t |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `0.0%` | `48.9%` | `51.1%` | `721` | `26.68%` | `1.43` | `-32.00%` | `27.44%` | `3.21` |
| `5.0%` | `46.5%` | `48.5%` | `721` | `26.98%` | `1.44` | `-31.84%` | `27.68%` | `3.23` |
| `10.0%` | `44.1%` | `45.9%` | `721` | `26.96%` | `1.44` | `-31.92%` | `27.75%` | `3.25` |
| `20.0%` | `39.2%` | `40.8%` | `721` | `25.75%` | `1.39` | `-31.59%` | `26.54%` | `3.16` |
| `33.3%` | `32.6%` | `34.0%` | `721` | `24.71%` | `1.34` | `-31.17%` | `25.59%` | `3.20` |

#### 結論

| Variant vs PBR 5% | Added | Dropped | Overlap vs baseline | Jaccard |
| --- | ---: | ---: | ---: | ---: |
| `PBR 0%` | `30` | `30` | `95.84%` | `92.01%` |
| `PBR 10%` | `22` | `22` | `96.95%` | `94.08%` |
| `PBR 20%` | `66` | `66` | `90.85%` | `83.23%` |
| `PBR 33%` | `106` | `106` | `85.30%` | `74.37%` |

#### 結論

| Variant | Direction | Events | Mean return | P10 return | Median PBR | Median market cap bn | Median forward PER |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `PBR 0%` | added by variant | `30` | `23.84%` | `-29.47%` | `1.12` | `6.6` | `6.10` |
| `PBR 0%` | dropped from PBR 5% | `30` | `29.81%` | `-10.11%` | `0.16` | `11.7` | `3.81` |
| `PBR 20%` | added by variant | `66` | `26.88%` | `-14.57%` | `0.17` | `15.3` | `3.01` |
| `PBR 20%` | dropped from PBR 5% | `66` | `39.00%` | `-12.34%` | `0.78` | `6.0` | `4.34` |
| `PBR 33%` | added by variant | `106` | `21.51%` | `-21.26%` | `0.18` | `15.9` | `3.19` |
| `PBR 33%` | dropped from PBR 5% | `106` | `35.16%` | `-13.95%` | `0.66` | `6.6` | `4.21` |

### Interpretation

Prime 全体の回帰で低 `PBR` が独立して残ることと、Prime top `5%` で `PBR` の採用 weight が小さいことは矛盾しない。上位 `5%` まで強く絞ると、小型 + 低 `forward PER` がほとんどの候補を決め、`PBR 0%` と `PBR 5%` の入替は全体の `4.16%` に留まる。少量の `PBR` は `PBR 0%` が落とす低PBR・低forward PER銘柄を拾い戻すためわずかに効くが、`20%` 以上にすると小型 exposure を削りすぎて悪化する。

「悪い低PBRを拾う」というより、Prime top slice では `PBR` を強く入れるほど「より低PBRだが相対的に大きい銘柄」へ寄り、過去のこの annual portfolio lens では、むしろ外れた小型寄り候補の方が平均 return が高かった。

### Production Implication

Prime の `prime_size_tilt` は、現行の `small 46.5% / low PBR 5% / low forward PER 48.5%` を維持する。`PBR` を完全に外すほどの差はないが、Prime で `PBR` を Standard 並みに増やす根拠はない。Ranking UI では Prime と Standard の score profile を引き続き分ける。

### Caveats

これは annual open-to-close equal-weight portfolio lens であり、実コスト、スリッページ、capacity、turnover は含まない。`PBR` の独立性そのものは cross-sectional regression / Fama-MacBeth の結果で別途確認済みであり、本研究は Prime top `5%` の採用 weight と入替銘柄の診断に限定する。

### Source Artifacts

- Bundle: `/tmp/trading25-research/market-behavior/annual-prime-value-pbr-absorption/20260504_prime_top5_pbr_absorption/`
- Input bundle: `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive/`
- Domain: `apps/bt/src/domains/analytics/annual_prime_value_pbr_absorption.py`
- Runner: `apps/bt/scripts/research/run_annual_prime_value_pbr_absorption.py`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_prime_value_pbr_absorption.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_prime_value_pbr_absorption.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_prime_value_pbr_absorption.py \
  --input-bundle /tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive \
  --output-root /tmp/trading25-research
```
