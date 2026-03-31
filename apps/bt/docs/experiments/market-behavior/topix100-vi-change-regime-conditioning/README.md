# TOPIX100 VI Change Regime Conditioning

`日経VI` の前日比を regime にして、TOPIX100 銘柄群の `price vs SMA20 gap x volume_sma_20_80` split がその後どう振る舞うかを調べる実験です。

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

- Notebook:
  - `apps/bt/notebooks/playground/topix100_vi_change_regime_conditioning_playground.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_vi_change_regime_conditioning.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix100_vi_change_regime_conditioning.py`

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/topix100_vi_change_regime_conditioning_playground.py
```

## Notes

- `base_volatility > 0` を満たす値だけを候補にし、日次で 1 値に収束する日だけを VI series として使います。
- `0` placeholder の混在日は正値を優先し、複数の正値が残る conflict 日は除外します。
- 現時点では `index_master` / `indices_data` へ materialize せず、read-time synthetic exposure を SoT とします。
