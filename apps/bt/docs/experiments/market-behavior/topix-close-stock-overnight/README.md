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

- Runner:
  - `apps/bt/scripts/research/run_topix_close_stock_overnight_distribution.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_close_stock_overnight_distribution.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix_close_stock_overnight_distribution.py`

## Latest Baseline

- [baseline-2026-03-24.md](./baseline-2026-03-24.md)

## Current Read

- 現行 baseline の TOPIX close 標準偏差は `1.1599%` で、bucket は `±1σ = 1.16%`, `±2σ = 2.32%` を使う。
- `-1.16% < TOPIX close < 1.16%` が 76.95% を占めるため、頻度加重ではこの中立帯の性格が全体像を最も強く決める。
- `TOPIX100` は `1.16% <= TOPIX close < 2.32%` の翌朝が最も強く、平均 overnight return は `0.8929%`、頻度加重後でも全 group で首位。
- ただし `TOPIX close >= 2.32%` まで行くと全 group で翌朝平均がマイナスに反転し、continuation は `+1σ ~ +2σ` 帯に集中している。
- `PRIME ex TOPIX500` は最頻出の中立帯で最も強く、極端日の当たり外れより日常的な drift の積み上がりで効く。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_close_stock_overnight_distribution.py \
  --sample-size 0
```

この command は
`~/.local/share/trading25/research/market-behavior/topix-close-stock-overnight/<run_id>/`
へ bundle を保存します。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- `TOPIX close >= +2σ` の翌朝は、指数の follow-through ではなく reversion 前提に切り替えるべきか。
- `+1σ ~ +2σ` の continuation と `>= +2σ` の反転を分ける条件を、出来高・先物・NT 倍率で説明できるか。
- セクターや出来高など、market regime 以外の条件を重ねると優位性が sharpen するか。
