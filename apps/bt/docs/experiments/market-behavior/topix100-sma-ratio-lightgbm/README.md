# TOPIX100 SMA Ratio LightGBM

TOPIX100 の `price/volume SMA ratio` 6 特徴を使い、既存の hand-crafted composite baseline と `LightGBM lambdarank` を walk-forward OOS で比較する実験です。

## Purpose

- fixed split `<= 2021-12-31 / >= 2022-01-01` だけでは過学習と signal を切り分けにくい問題を解消する。
- `t_plus_1` / `t_plus_5` / `t_plus_10` の cross-sectional rank を pure OOS walk-forward で評価し、LightGBM が research tool として使えるかを判定する。
- baseline の feature / composite selection を split ごとに train-only で再実行し、LightGBM との比較を leakage なしで揃える。

## Scope

- Universe:
  - `TOPIX100`
- Features:
  - `price_sma_5_20`
  - `price_sma_20_80`
  - `price_sma_50_150`
  - `volume_sma_5_20`
  - `volume_sma_20_80`
  - `volume_sma_50_150`
- Horizons:
  - `t_plus_1`
  - `t_plus_5`
  - `t_plus_10`
- Models:
  - walk-forward baseline composite
  - walk-forward `LightGBM LGBMRanker(objective="lambdarank")`
- Walk-forward config:
  - `train_window=756`
  - `test_window=126`
  - `step=126`
- Gate:
  - `t_plus_5` / `t_plus_10` で `overall OOS spread > 0`
  - `median split spread > 0`
  - `positive split share >= 60%`

## Source Of Truth

- Notebook:
  - [`apps/bt/notebooks/playground/topix100_sma_ratio_rank_future_close_playground.py`](/Users/shinjiroaso/dev/trading25/apps/bt/notebooks/playground/topix100_sma_ratio_rank_future_close_playground.py)
- Domain logic:
  - [`apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py`](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py)
  - [`apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py`](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py)
- Tests:
  - [`apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py`](/Users/shinjiroaso/dev/trading25/apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py)
  - [`apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close.py`](/Users/shinjiroaso/dev/trading25/apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close.py)

## Latest Baseline

- [baseline-2026-03-31.md](./baseline-2026-03-31.md)

## Current Read

- walk-forward OOS gate は `passed` です。`t_plus_5` と `t_plus_10` の両方で overall spread・median split spread・positive split share が gate を通過しました。
- OOS では baseline がかなり弱く、`t_plus_10` は `-0.1017%` の負 spread でした。一方 LightGBM は `t_plus_5=+0.2920%`、`t_plus_10=+0.4593%` まで残っています。
- `t_plus_1` は gate 対象外ですが、OOS でも LightGBM は `+0.0500%`、baseline は `-0.0516%` で、短期でも baseline より整っています。
- 重要度は全 horizon で `price_sma_50_150` が最上位です。`t_plus_5` / `t_plus_10` では `volume_sma_50_150` も上位に入り、既存 baseline が寄りがちだった短期窓より、長期 trend / participation が効いています。
- fixed split diagnostic では LightGBM がさらに強く見えますが、walk-forward OOS に落とすと spread は大きく縮みます。signal 自体は残るが、fixed split の headline は過大評価だった、という整理です。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt --group research python - <<'PY'
from src.shared.config.settings import get_settings
from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    run_topix100_sma_ratio_rank_future_close_research,
)
from src.domains.analytics.topix100_sma_ratio_rank_future_close_lightgbm import (
    run_topix100_sma_ratio_rank_future_close_lightgbm_research,
)

base_result = run_topix100_sma_ratio_rank_future_close_research(
    get_settings().market_db_path
)
lightgbm_result = run_topix100_sma_ratio_rank_future_close_lightgbm_research(
    base_result
)
print(lightgbm_result.walkforward.comparison_summary_df)
print(lightgbm_result.walkforward.exploratory_gate_df)
PY
```

Notebook で確認する場合:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/topix100_sma_ratio_rank_future_close_playground.py
```

## Next Questions

- `price_sma_50_150` / `volume_sma_50_150` を中心にした deterministic composite を作ると、LightGBM に近い OOS spread を simpler rule で再現できるか。
- LightGBM は OOS gate を通ったので、次段では regularization と early stopping を加えたうえで spread の安定性がさらに上がるか。
- split ごとの baseline 選択が頻繁に揺れているので、feature family の選択自体を walk-forward 安定度で制約した方がよいか。
