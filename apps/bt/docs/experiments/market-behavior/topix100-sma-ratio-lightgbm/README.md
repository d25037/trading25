# TOPIX100 SMA Ratio LightGBM

## Published Readout

### Decision
- Invalidated. 旧 headline は production、Ranking、Screening、strategy selection evidence として使わない。

### Why This Research Was Run
- 旧 runner-first research を fallback-free publication surface に移行するため、現時点の扱いを明示する。
- 旧 `Current Read` / baseline は chat や `summary.json` ではなく、この README 上で triage する。

### Data Scope / PIT Assumptions
- Triage status: `Invalidated`.
- Blocker: TOPIX100 walk-forward universe is not proven PIT-safe.
- walk-forward OOS の model evaluation より前に、各 signal date の TOPIX100 membership を PIT 解決した rerun が必要。旧 positive gate は production / stock-selection evidence として使わない。

### Main Findings
#### 結論: 旧 headline は採用判断に使わない

| Item | Disposition |
| --- | --- |
| Old readout | historical context only |
| Publication source | this README `Published Readout` |
| Bundle `summary.json` | not a publication source |
| Required action | Invalidated |

### Interpretation
- この readout は旧数値を有効化するものではない。fallback / legacy 構造を排除するため、旧 research の現在の扱いを source markdown に固定する。
- PIT-safe でない可能性がある universe、membership、market grouping、または exploratory branch は、再実行なしに production evidence へ昇格しない。

### Production Implication
- 現時点では UI / strategy に反映しない。runner を PIT-safe に修正して rerun し、結果が確認できた場合だけ新しい readout として再採用する。

### Caveats
- 旧 baseline の数値は下の既存セクションに残るが、`Published Readout` より優先しない。
- 再実行する場合は `market.duckdb` schema v3、signal-date membership、`stock_master_daily` / `index_membership_daily` の source を README に明記する。

### Source Artifacts
- Experiment: `market-behavior/topix100-sma-ratio-lightgbm`
- Existing runner / baseline references remain below this section.
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

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

- Runner:
  - `apps/bt/scripts/research/run_topix100_sma_ratio_rank_future_close_lightgbm.py`
- Baseline runner:
  - `apps/bt/scripts/research/run_topix100_sma_ratio_rank_future_close.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py`
  - `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close.py`

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
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt --group research python \
  apps/bt/scripts/research/run_topix100_sma_ratio_rank_future_close_lightgbm.py
```

bundle は `~/.local/share/trading25/research/market-behavior/topix100-sma-ratio-lightgbm/<run_id>/`
に `manifest.json`, `results.duckdb`, `summary.md` として保存されます。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

LightGBM の bundle 再現は上の dedicated runner を使います。

## Next Questions

- `price_sma_50_150` / `volume_sma_50_150` を中心にした deterministic composite を作ると、LightGBM に近い OOS spread を simpler rule で再現できるか。
- LightGBM は OOS gate を通ったので、次段では regularization と early stopping を加えたうえで spread の安定性がさらに上がるか。
- split ごとの baseline 選択が頻繁に揺れているので、feature family の選択自体を walk-forward 安定度で制約した方がよいか。
