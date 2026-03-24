# TOPIX Close / Stock Overnight

TOPIX の当日引け変動を event day として bucket 化し、個別銘柄群の翌営業日 overnight リターンを観察する実験です。

## Purpose

- `TOPIX が大きく動いて引けた日` の翌朝に、どの銘柄群が相対的に強いかを把握する。
- 単純な bucket 平均だけでなく、`どの bucket がどれくらい頻繁に起きるか` を含めて解釈する。

## Scope

- Event definition:
  - `topix_close_return = (topix_close - prev_topix_close) / prev_topix_close`
- Trade definition:
  - `stock_overnight_return = (next_open - event_close) / event_close`
- Stock groups:
  - `PRIME`
  - `TOPIX100`
  - `TOPIX500`
  - `PRIME ex TOPIX500`

## Source Of Truth

- Notebook:
  - [`apps/bt/notebooks/playground/topix_close_stock_overnight_distribution_playground.py`](/Users/shinjiroaso/dev/trading25/apps/bt/notebooks/playground/topix_close_stock_overnight_distribution_playground.py)
- Domain logic:
  - [`apps/bt/src/domains/analytics/topix_close_stock_overnight_distribution.py`](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix_close_stock_overnight_distribution.py)
- Tests:
  - [`apps/bt/tests/unit/domains/analytics/test_topix_close_stock_overnight_distribution.py`](/Users/shinjiroaso/dev/trading25/apps/bt/tests/unit/domains/analytics/test_topix_close_stock_overnight_distribution.py)

## Latest Baseline

- [baseline-2026-03-24.md](./baseline-2026-03-24.md)

## Current Read

- `-1% < TOPIX close < 1%` が発生日の大半を占めるため、頻度加重で見るとこの bucket の性格が全体像を大きく決める。
- `TOPIX100` は `TOPIX close >= 2%` の翌朝が非常に強く、強い地合いの continuation に最も反応する。
- `PRIME ex TOPIX500` は極端日よりも、平常日からやや強い日でじわっと積み上がる傾向が強い。
- `TOPIX500` は極端な上昇日に恩恵はあるが、最頻出 bucket での優位が相対的に弱い。
- `TOPIX close <= -2%` の翌朝は全群で平均マイナス寄りで、急落翌朝の continuation risk が残る。

## Reproduction

```bash
uv run --project apps/bt python - <<'PY'
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    run_topix_close_stock_overnight_distribution,
)

result = run_topix_close_stock_overnight_distribution(
    "/Users/shinjiroaso/.local/share/trading25/market-timeseries/market.duckdb",
    sample_size=0,
)
print(result.summary_df)
PY
```

Notebook で確認する場合:

```bash
uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/topix_close_stock_overnight_distribution_playground.py
```

## Next Questions

- `TOPIX close >= 2%` の翌朝は、寄り付き売り前提で `TOPIX100` を優先すべきか。
- `-2% < TOPIX close <= -1%` と `<= -2%` の境界で、mean reversion から continuation へ切り替わる条件は何か。
- セクターや出来高など、market regime 以外の条件を重ねると優位性が sharpen するか。
