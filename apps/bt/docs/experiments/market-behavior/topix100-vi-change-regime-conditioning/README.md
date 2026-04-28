# TOPIX100 VI Change Regime Conditioning

`日経VI` の前日比を regime にして、TOPIX100 銘柄群の `price vs SMA20 gap x volume_sma_20_80` split がその後どう振る舞うかを調べる実験です。

## Published Readout

### Decision

VI change は `SMA Q10 Low` bounce の主フィルタにはしない。`VI weak` では `Q10 Low` の 10d 平均が高く見えるが、Holm 補正後の paired / Wilcoxon は残らないため、後続の production 候補では `TOPIX close` / `NT ratio` conditioning を優先し、VI は volatility regime diagnostic として扱う。

### Why This Research Was Run

先行する `topix100-price-vs-sma-q10-bounce` と `topix100-price-vs-sma-q10-bounce-regime-conditioning` では、`SMA50 Q10 Low` の反発は地合いで読みやすさが変わった。この研究は、同じ bounce 系列に `日経VI` 前日比 regime を重ね、volatility の上昇/低下が `Q10 Low vs Middle` の説明力を追加するかを確認するために実行した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。available range は `2016-07-19 -> 2026-03-30`、analysis range は `2016-07-21 -> 2026-03-31`。対象は latest `TOPIX100` constituent approximation、valid dates は `2,365`。`VI = options_225_data.base_volatility` を日次 1 値に正規化し、`VI change = (vi - prev_vi) / prev_vi` から `weak` / `neutral` / `strong` regime を作る。stock 側は `price_vs_sma_20_gap x volume_sma_20_80` split、forward horizon は `t_plus_1` / `t_plus_5` / `t_plus_10`。

### Main Findings

#### `VI weak` は 10d の headline spread が最大だが、統計的には弱い。

| Regime | Hypothesis | Mean diff | Paired t Holm | Wilcoxon Holm |
| --- | --- | ---: | ---: | ---: |
| `weak` | `Q10 Low vs Middle High` | `+0.4698%` | `0.792755` | `1.000000` |
| `weak` | `Q10 Low vs Middle Low` | `+0.3956%` | `1.000000` | `1.000000` |
| `weak` | `Q10 Low vs Q10 High` | `+0.3804%` | `1.000000` | `1.000000` |
| `strong` | `Q10 Low vs Middle High` | `+0.3501%` | `1.000000` | `1.000000` |
| `neutral` | `Q10 Low vs Middle Low` | `+0.1837%` | `0.334458` | `1.000000` |

#### 各 regime の `Q10 Low` 平均は高いが、regime 間の sample size 差が大きい。

| Regime | Date count | Mean VI change | `Q10 Low` 10d mean | `Middle High` 10d mean | `Middle Low` 10d mean |
| --- | ---: | ---: | ---: | ---: | ---: |
| `weak` | `240` | `-13.07%` | `+1.5489%` | `+1.0791%` | `+1.1533%` |
| `neutral` | `1,847` | `-0.56%` | `+0.7165%` | `+0.5405%` | `+0.5328%` |
| `strong` | `278` | `+18.61%` | `+1.2763%` | `+0.9261%` | `+1.1255%` |

### Interpretation

VI regime は反発が強く見える日を説明している可能性はあるが、`weak` / `strong` は sample が小さく、統計的な読み筋にはならない。特に `VI weak` の headline は、volatility 低下局面で market 全体の forward return が高くなる影響も含む。先行研究で有望だった `TOPIX close` / `NT ratio` の方が、`Q10 Low vs Middle` を安定的に読む条件としては優先度が高い。

### Production Implication

この研究単体では entry filter にしない。production に近い設計では、まず `SMA50 Q10 Low` と `TOPIX close` / `NT ratio` の条件を優先し、VI は risk dashboard や regime annotation として添える。VI を使う場合も、`weak` を買いシグナルにするのではなく、volatility compression 後の rebound observation として別途 OOS で検証する。

### Caveats

`options_225_data.base_volatility` から作る VI は local data の正規化に依存し、`0` placeholder や複数値 conflict 日を除外している。`weak` / `strong` regime は date count が少なく、Holm 補正後の有意性は出ていない。日足 close-to-close の観察で、手数料、slippage、portfolio construction、execution timing は未評価。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_vi_change_regime_conditioning.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_vi_change_regime_conditioning.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-vi-change-regime-conditioning/20260331_173436_de1d187c`
- Tables: `results.duckdb`

## Purpose

- `日経VI` 上昇日の risk-off 環境で、TOPIX100 内の相対強弱が翌営業日以降にどう効くかを見る。
- `VI down / neutral / up` の 3 折りたたみ regime でも、price/volume split の差が残るかを確認する。
- 既存の `TOPIX close` / `NT ratio` conditioning notebook と並べて、volatility regime の説明力を比較できるようにする。

## Scope

- Market input:
  - `VI = options_225_data.base_volatility`
  - `VI change = (vi - prev_vi) / prev_vi`
- Stock universe:
  - latest `TOPIX100` constituent approximation (`TOPIX Core30` + `TOPIX Large70`)
- Split panel:
  - `price_vs_sma_20_gap = (close / sma20) - 1`
  - `volume_sma_20_80`
- Horizons:
  - `t_plus_1`
  - `t_plus_5`
  - `t_plus_10`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_vi_change_regime_conditioning.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_vi_change_regime_conditioning.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix100_vi_change_regime_conditioning.py`

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix100_vi_change_regime_conditioning.py
```

この command は
`~/.local/share/trading25/research/market-behavior/topix100-vi-change-regime-conditioning/<run_id>/`
へ bundle を保存します。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Notes

- `base_volatility > 0` を満たす値だけを候補にし、日次で 1 値に収束する日だけを VI series として使います。
- `0` placeholder の混在日は正値を優先し、複数の正値が残る conflict 日は除外します。
- 現時点では `index_master` / `indices_data` へ materialize せず、read-time synthetic exposure を SoT とします。
