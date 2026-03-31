# NT Ratio Change x TOPIX Close / Stock Overnight

NT 倍率の前日比と TOPIX 当日引け変動を joint bucket 化し、個別銘柄群の翌営業日 overnight リターンを観察する実験です。

## Purpose

- `相対回転` と `市場全体の絶対方向` を同時に見たとき、翌朝の gap 優位性がどう変わるかを把握する。
- `日経平均優位 / TOPIX 優位` という rotation と、`TOPIX の強弱` を分離し、どの組み合わせが最も強いかを見る。
- `panic reversion`, `healthy risk-on`, `divergent stress` のような regime を joint cell として切り出す。

## Scope

- Event definition:
  - `nt_ratio = N225_UNDERPX close / TOPIX close`
  - `nt_ratio_return = (nt_ratio - prev_nt_ratio) / prev_nt_ratio`
  - `topix_close_return = (topix_close - prev_topix_close) / prev_topix_close`
- Bucket definition:
  - NT ratio return: `μ-2σ`, `μ-1σ`, `μ+1σ`, `μ+2σ`
  - TOPIX close return: `0%` 基準の `±1σ`, `±2σ`
- Trade definition:
  - `stock_overnight_return = (next_open - event_close) / event_close`
- Stock groups:
  - `PRIME`
  - `TOPIX100`
  - `TOPIX500`
  - `PRIME ex TOPIX500`

## Source Of Truth

- Notebook:
  - `apps/bt/notebooks/playground/nt_ratio_change_topix_close_stock_overnight_distribution_playground.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/nt_ratio_change_topix_close_stock_overnight_distribution.py`
- Related helpers:
  - `apps/bt/src/domains/analytics/nt_ratio_change_stock_overnight_distribution.py`
  - `apps/bt/src/domains/analytics/topix_close_stock_overnight_distribution.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_nt_ratio_change_topix_close_stock_overnight_distribution.py`

## Latest Baseline

- [baseline-2026-03-24.md](./baseline-2026-03-24.md)

## Current Read

- `μ-1σ < NT return < μ+1σ` かつ `-1.16% < TOPIX close < 1.16%` の central-central cell が 61.91% を占め、`PRIME` / `PRIME ex TOPIX500` の全体像を大きく決める。
- 最も使いやすい positive regime は `NT central x TOPIX +1σ~+2σ` で、`TOPIX100` の平均 overnight return は `1.3835%`、頻度加重寄与でも最大。
- `TOPIX <= -2σ` でも NT が弱い (`<= μ-1σ`) と翌朝は broad に強く、`panic + TOPIX relative strength` の reversion pocket が見える。
- 逆に `TOPIX <= -2σ` なのに NT が強い (`+1σ~+2σ`) と全 group で最悪になり、`divergent stress` が overnight long の危険 regime になる。
- group の総合順位は `TOPIX100 > PRIME ex TOPIX500 > PRIME > TOPIX500` だが、`TOPIX100` は `healthy risk-on`、`PRIME ex TOPIX500` は central-central の積み上がりで勝っている。

## TOPIX100に絞った考察

- joint note での `TOPIX100` は、単なる `大型株バスケット` ではなく、`絶対方向と相対回転が噛み合ったときだけ鋭く効く regime amplifier` として読むのが正確です。`PRIME ex TOPIX500` のように central-central を積み上げて勝つタイプではありません。
- いちばん素直な買い場は `NT central x TOPIX +1σ~+2σ` です。日数は `155` 日あり、平均 overnight return は `1.3835%`、寄与は `0.0919%` と全セル最大です。これは `TOPIX が適度に強いが、相場の主導権が日経225の一部大型株へ偏りすぎていない` 状態で、TOPIX100 の continuation が最もきれいに出ています。
- もう一つの買い場は `TOPIX <= -2σ` かつ `NT <= μ-1σ` です。件数は `16` 日と少ないものの、`<= μ-2σ` で `1.7542%`、`μ-2σ~μ-1σ` で `1.6984%` と非常に強く、`panic の中でも TOPIX 側が主導` しているときの reversion long に使えます。つまり TOPIX100 は `risk-on continuation` だけでなく、`TOPIX 主導の panic rebound` にも反応します。
- 逆に避けるべきなのは、`市場全体は弱いのに relative には日経優位` という食い違いです。最悪セルは `TOPIX <= -2σ` かつ `NT +1σ~+2σ` で、平均 `-2.5265%` と全 group 中の worst です。ここでは mega-cap defensive flow が出ており、TOPIX100 long は broad market の弱さをまともに食らいます。
- もう一つの誤用は `TOPIX を強いほど良い` と考えて `>= +2σ` を追うことです。`NT central x TOPIX >= +2σ` でも `-0.5878%` なので、TOPIX100 の本命は `moderately strong` であって `blow-off top` ではありません。
- 実装に落とすなら、TOPIX100 は `1. TOPIX +1σ~+2σ かつ NT central では積極利用`, `2. TOPIX <= -2σ かつ NT <= μ-1σ では反発狙いで限定利用`, `3. TOPIX <= -2σ かつ NT が日経側, または TOPIX >= +2σ では明確に回避` という 3 分岐が自然です。

## Reproduction

```bash
uv run --project apps/bt python - <<'PY'
from src.domains.analytics.nt_ratio_change_topix_close_stock_overnight_distribution import (
    run_nt_ratio_change_topix_close_stock_overnight_distribution,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    get_topix_close_return_stats,
)

db_path = "~/.local/share/trading25/market-timeseries/market.duckdb"
topix_stats = get_topix_close_return_stats(
    db_path,
    sigma_threshold_1=1.0,
    sigma_threshold_2=2.0,
)
result = run_nt_ratio_change_topix_close_stock_overnight_distribution(
    db_path,
    sigma_threshold_1=1.0,
    sigma_threshold_2=2.0,
    topix_close_threshold_1=topix_stats.threshold_1,
    topix_close_threshold_2=topix_stats.threshold_2,
    sample_size=0,
)
print(result.nt_ratio_stats)
print(result.joint_day_counts_df)
print(result.summary_df)
PY
```

Notebook で確認する場合:

```bash
uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/nt_ratio_change_topix_close_stock_overnight_distribution_playground.py
```

## Next Questions

- `TOPIX <= -2σ` かつ `NT +1σ~+2σ` の worst regime は、先物主導の mega-cap defensive flow なのか。
- `NT central x TOPIX +1σ~+2σ` の強さを、TOPIX100 内の sector concentration で説明できるか。
- joint bucket を rolling sigma や quantile で切り直したとき、same-cell の優位性はどこまで安定するか。
