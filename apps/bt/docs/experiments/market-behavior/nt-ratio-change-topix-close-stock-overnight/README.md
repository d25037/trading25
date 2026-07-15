# NT Ratio Change x TOPIX Close / Stock Overnight

## Published Readout

### Decision
- Rerun required. 旧結果は historical context として残すが、production、Ranking、Screening、strategy selection evidence には使わない。

### Why This Research Was Run
- 旧 runner-first research を fallback-free publication surface に移行するため、現時点の扱いを明示する。
- 旧 `Current Read` / baseline は chat や `summary.json` ではなく、この README 上で triage する。

### Data Scope / PIT Assumptions
- Triage status: `Rerun required`.
- Blocker: joint market-state readout needs PIT universe confirmation.
- NT ratio と TOPIX close の joint regime は有用候補だが、個別銘柄 universe / group 解決を PIT-safe に確認するまで production evidence にしない。

### Main Findings
#### 結論: 旧 headline は採用判断に使わない

| Item | Disposition |
| --- | --- |
| Old readout | historical context only |
| Publication source | this README `Published Readout` |
| Bundle `summary.json` | not a publication source |
| Required action | Rerun required |

### Interpretation
- この readout は旧数値を有効化するものではない。fallback / legacy 構造を排除するため、旧 research の現在の扱いを source markdown に固定する。
- PIT-safe でない可能性がある universe、membership、market grouping、または exploratory branch は、再実行なしに production evidence へ昇格しない。

### Production Implication
- 高価値候補として残す場合は、signal-date universe と Market v4 event-time source を明示した runner で再実行してから採用判断する。

### Caveats
- 旧 baseline の数値は下の既存セクションに残るが、`Published Readout` より優先しない。
- 再実行する場合は Market schema v4 / `local_projection_v2_event_time` を必須とし、signal-date membership、`stock_master_daily` / `index_membership_daily`、event-time basis lineage の source を README に明記する。旧 schema v3 run は historical provenance に限る。

### Source Artifacts
- Experiment: `market-behavior/nt-ratio-change-topix-close-stock-overnight`
- Existing runner / baseline references remain below this section.
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

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

- Runner:
  - `apps/bt/scripts/research/run_nt_ratio_change_topix_close_stock_overnight_distribution.py`
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
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_nt_ratio_change_topix_close_stock_overnight_distribution.py \
  --sample-size 0
```

この command は
`~/.local/share/trading25/research/market-behavior/nt-ratio-change-topix-close-stock-overnight/<run_id>/`
へ bundle を保存します。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- `TOPIX <= -2σ` かつ `NT +1σ~+2σ` の worst regime は、先物主導の mega-cap defensive flow なのか。
- `NT central x TOPIX +1σ~+2σ` の強さを、TOPIX100 内の sector concentration で説明できるか。
- joint bucket を rolling sigma や quantile で切り直したとき、same-cell の優位性はどこまで安定するか。
