# Ranking Core Factor Regime Breakdown

`neutral_rerating blue + sector_strong + low_pbr20_low_fwd_per20` の momentum-value core が、2026 partial で何を失っているのかを、value / momentum / ATR / sector の factor lens で分解する研究。

## Published Readout

### Decision

2026 partial で壊れている主因は、core の value anchor 全体というより、`core + atr20_acceleration_ex_overheat` overlay。2022-2025 では ATR20 acceleration が core の high-conviction confirmation として強かったが、2026 では同じ overlay が 20D / 60D ともに負けている。

一方、2026 の `core_momentum_20_60_top20` は 20D / 60D ともに強い。したがって 2026 regime では「value + momentum」はまだ残るが、「value + ATR20 acceleration」は late-stage / volatility expansion になっている可能性が高い。

Production implication は、2026 相場では `atr20_acceleration_ex_overheat` を単純な sizing boost にしないこと。core 内の優先順位は `momentum_20_60_top20` を上位に置き、ATR20 acceleration は直近 regime filter なしでは confidence badge にしない。

### Main Findings

#### 結論: 2026 は value が弱く、momentum は相対的に改善している

Primary run `20260530_ranking_core_factor_regime_breakdown_prime_v4` は `2016-04-01` から `2026-05-14`、Prime、horizon `5/10/20/60`、`min_observations=20`。実カバレッジは `2022-04-04` から `2026-05-14`、観測母集団は `1,717,700` stock-days。

| Horizon | Year group | Factor | Obs | Code | Median TOPIX ex | Win | Severe | Read |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 20D | `2022_2025_history` | `low_value` | 113,886 | 419 | +0.446% | 53.4% | 3.1% | value は小幅プラス |
| 20D | `2026_partial` | `low_value` | 7,978 | 181 | -1.931% | 39.4% | 9.9% | 2026 は value が明確に弱い |
| 20D | `2022_2025_history` | `momentum_20_60_top20` | 154,466 | 1,840 | -0.722% | 46.2% | 11.5% | pure momentum は過去弱い |
| 20D | `2026_partial` | `momentum_20_60_top20` | 13,480 | 680 | +0.068% | 50.3% | 15.3% | 2026 は改善するが tail は重い |
| 20D | `2022_2025_history` | `high_valuation_momentum` | 52,422 | 878 | -1.626% | 42.9% | 16.9% | 過去は悪い |
| 20D | `2026_partial` | `high_valuation_momentum` | 5,030 | 281 | +0.433% | 51.3% | 20.4% | 2026 は勝つが左尾は重い |
| 20D | `2022_2025_history` | `value_momentum` | 9,424 | 218 | +1.576% | 59.9% | 4.5% | value + momentum は強い |
| 20D | `2026_partial` | `value_momentum` | 576 | 52 | +2.376% | 60.8% | 8.9% | 2026 でも残る |

#### 結論: 2026 で壊れたのは core + ATR20 acceleration

`core` 内の 20D TOPIX excess。

| Year | Slice | Obs | Code | Median | Win | Severe | Read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 2022 | `core_all` | 1,160 | 75 | +5.037% | 73.4% | 1.7% | 強い |
| 2022 | `core_atr20_acceleration_ex_overheat` | 296 | 45 | +9.441% | 81.8% | 0.7% | ATR boost が効く |
| 2023 | `core_all` | 3,263 | 113 | +2.174% | 64.2% | 5.9% | 強い |
| 2023 | `core_atr20_acceleration_ex_overheat` | 506 | 73 | +3.224% | 77.9% | 0.0% | ATR boost が効く |
| 2024 | `core_all` | 1,880 | 100 | +0.404% | 53.3% | 1.5% | 弱いがプラス |
| 2024 | `core_atr20_acceleration_ex_overheat` | 341 | 47 | +0.890% | 58.4% | 1.5% | 少し改善 |
| 2025 | `core_all` | 4,607 | 102 | +1.819% | 63.2% | 1.2% | 強い |
| 2025 | `core_atr20_acceleration_ex_overheat` | 802 | 68 | +3.086% | 75.3% | 0.5% | ATR boost が効く |
| 2026 | `core_all` | 683 | 50 | +0.789% | 53.6% | 8.1% | core 自体は小幅プラス |
| 2026 | `core_atr20_acceleration_ex_overheat` | 116 | 25 | -1.901% | 37.1% | 8.6% | ATR overlay が逆効果 |
| 2026 | `core_momentum_20_60_top20` | 341 | 27 | +3.545% | 68.9% | 4.1% | core 内 momentum は強い |
| 2026 | `core_without_momentum_20_60_top20` | 342 | 43 | -2.320% | 38.3% | 12.0% | momentum 不在が負ける |

60D でも同じ方向。2026 の `core_atr20_acceleration_ex_overheat` は median `-3.263%`、win `41.1%`、severe `20.5%`。一方、`core_momentum_20_60_top20` は median `+8.652%`、win `69.3%`、severe `9.8%`。

#### 結論: 2026 の core 悪化は sector 偏りもある

2026 `core_all` の 20D sector contribution。

| Sector | Obs | Code | Median | Win | Severe | Read |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `銀行業` | 435 | 19 | +2.919% | 66.2% | 0.2% | core のプラス寄与 |
| `電気･ガス業` | 57 | 6 | -11.847% | 10.5% | 56.1% | 大きな悪化要因 |
| `非鉄金属` | 51 | 2 | -0.230% | 45.1% | 15.7% | 弱い |
| `鉄鋼` | 51 | 4 | -6.715% | 5.9% | 11.8% | 弱い |

2026 `core_atr20_acceleration_ex_overheat` は sector別に見ると `銀行業` が 61 obs / 13 code で median `-0.630%`。過去は ATR boost が銀行を含む core の加速確認として効いたが、2026 では銀行内でも短期ATR加速が追随遅れになっている。

### Interpretation

2026 partial は「pure momentum が全面的に強く、value が全滅」という単純な絵ではない。broad な `low_value` は明確に弱いが、`value_momentum` は 2026 でも 20D median `+2.376%` と残っている。つまり value anchor 自体よりも、momentum confirmation の種類が問題。

過去の high-conviction overlay だった `atr20_acceleration_ex_overheat` は、2026 では late-stage volatility expansion になっている可能性が高い。20D / 60D の momentum top20 は core 内で強いので、2026 では「価格 momentum の強さ」は必要だが、「ATR20 が加速していること」は良い confirmation ではない。

Sector では銀行 core はまだ機能している。一方で電気・ガス、鉄鋼、非鉄金属が 2026 の core を悪化させている。したがって 2026 の負けは core 全体の構造破綻ではなく、factor regime と sector rotation の組み合わせ。

### Production Implication

2026 regime では、`core + atr20_acceleration_ex_overheat` を sizing boost に使わない。`core + momentum_20_60_top20` を優先し、ATR20 acceleration は別の regime guard が入るまで positive badge から外す。

| Rule candidate | Implication |
| --- | --- |
| `core` | 維持。2026 でも小幅プラス |
| `core + momentum_20_60_top20` | 2026 の優先候補。momentum-value として自然 |
| `core + atr20_acceleration_ex_overheat` | 2022-2025 は強いが 2026 で崩壊。直近では boost しない |
| `low_value` broad | 2026 は弱い。core 外の value 拡張は避ける |
| `high_valuation_momentum` | 2026 は改善するが severe が重く、core long へ混ぜない |
| `電気･ガス業` / `鉄鋼` core | 2026 caution。sector-level guard 候補 |

### Caveats

- 2026 は `2026-05-14` までの partial year。
- factor signal は daily close-to-close forward TOPIX excess の association study であり、実行コスト、turnover、capacity は含まない。
- `momentum_20_60_top20` は `date x market_scope` 内の 20D / 60D return percentile 上位20%。
- ATR signal は close 後に確定するため、production entry timing は別途検証が必要。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/ranking_core_factor_regime_breakdown.py`
- Runner: `apps/bt/scripts/research/run_ranking_core_factor_regime_breakdown.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_core_factor_regime_breakdown.py`
- Bundle: `/tmp/trading25-research/market-behavior/ranking-core-factor-regime-breakdown/20260530_ranking_core_factor_regime_breakdown_prime_v4/`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/ranking_core_factor_regime_breakdown.py`
- Runner:
  - `apps/bt/scripts/research/run_ranking_core_factor_regime_breakdown.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_core_factor_regime_breakdown.py \
  --output-root /tmp/trading25-research \
  --run-id 20260530_ranking_core_factor_regime_breakdown_prime_v4 \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --horizons 5,10,20,60 \
  --min-observations 20
```
