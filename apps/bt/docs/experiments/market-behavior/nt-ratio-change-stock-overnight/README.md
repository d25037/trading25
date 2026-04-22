# NT Ratio Change / Stock Overnight

NT 倍率の前日比を event day として z-score bucket 化し、個別銘柄群の翌営業日 overnight リターンを観察する実験です。

## Purpose

- `日経平均優位が急に進んだ日` と `TOPIX 優位が急に進んだ日` の翌朝に、どの銘柄群が相対的に強いかを把握する。
- 単純な bucket 平均だけでなく、`どの NT bucket がどれくらい頻繁に起きるか` を含めて解釈する。
- TOPIX の絶対方向ではなく、`日経平均 / TOPIX の相対回転` が翌朝の gap に効いているかを見る。

## Scope

- Event definition:
  - `nt_ratio = N225_UNDERPX close / TOPIX close`
  - `nt_ratio_return = (nt_ratio - prev_nt_ratio) / prev_nt_ratio`
- Bucket definition:
  - `μ-2σ`
  - `μ-1σ`
  - `μ+1σ`
  - `μ+2σ`
- Trade definition:
  - `stock_overnight_return = (next_open - event_close) / event_close`
- Stock groups:
  - `PRIME`
  - `TOPIX100`
  - `TOPIX500`
  - `PRIME ex TOPIX500`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_nt_ratio_change_stock_overnight_distribution.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/nt_ratio_change_stock_overnight_distribution.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_nt_ratio_change_stock_overnight_distribution.py`

## Latest Baseline

- [baseline-2026-03-24.md](./baseline-2026-03-24.md)

## Current Read

- `μ-1σ < NT return < μ+1σ` が発生日の大半を占めるため、頻度加重で見るとこの bucket の性格が全体像を大きく決める。
- `NT return <= μ-2σ` の翌朝は全群で平均プラス幅が大きく、`TOPIX 優位が急進した日の翌朝` は broad に強い。
- `NT return >= μ+2σ` の翌朝は全群で平均マイナスに転じ、`日経平均優位が急進した日の翌朝` は broad に弱い。
- `TOPIX100` は tail への反応が最も大きく、`NT <= μ-2σ` と `NT >= μ+2σ` の差が最も広い。
- `PRIME ex TOPIX500` は extremes でも方向は同じだが、`TOPIX100` より振れ幅が小さく、平常帯の積み上がりが相対的に安定している。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_nt_ratio_change_stock_overnight_distribution.py \
  --sample-size 0
```

この command は
`~/.local/share/trading25/research/market-behavior/nt-ratio-change-stock-overnight/<run_id>/`
へ bundle を保存します。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- `NT return` をさらに `TOPIX close return` で分類すると、相対回転シグナルの中身はどう変わるか。
- `TOPIX100` の tail sensitivity はセクター構成によるものか、それとも index concentration の効果か。
- z-score bucket より quantile bucket の方が、tail 件数不足に対して安定するか。
