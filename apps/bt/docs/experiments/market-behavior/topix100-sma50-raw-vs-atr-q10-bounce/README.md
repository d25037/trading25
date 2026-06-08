# TOPIX100 SMA50 Raw vs ATR Q10 Bounce

## Published Readout

### Decision
- Invalidated. 旧 headline は production、Ranking、Screening、strategy selection evidence として使わない。

### Why This Research Was Run
- 旧 runner-first research を fallback-free publication surface に移行するため、現時点の扱いを明示する。
- 旧 `Current Read` / baseline は chat や `summary.json` ではなく、この README 上で triage する。

### Data Scope / PIT Assumptions
- Triage status: `Invalidated`.
- Blocker: TOPIX100 decile universe is not proven PIT-safe.
- `TOPIX100` decile partition の historical membership が publication source で証明されていないため、旧 bounce headline は撤回し、PIT-safe rerun 待ちにする。

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
- Experiment: `market-behavior/topix100-sma50-raw-vs-atr-q10-bounce`
- Existing runner / baseline references remain below this section.
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

`SMA50` からの乖離をそのまま使う `raw_gap` と、`ATR14` で標準化した `atr_gap_14` を同じ `Q10 / middle x volume` bounce frame で比較する実験です。runner-first 導線では reproducible bundle を保存し、playground は bundle viewer + representative sample chart audit として使います。

## Purpose

- `price_vs_sma_50_gap` だけで見えていた `Q10 Low` の強さが、ATR 正規化後も残るかを確認する。
- 特に `Middle High` / `Middle Low` 側の見た目の違いを、単純乖離ではなく volatility-adjusted distance で説明できるかを見る。
- `Q10 Low vs ...` の統計差と representative chart の両方を揃え、次段の signal 設計で raw gap / ATR gap のどちらを採るべきか判断する。

## Scope

- Universe:
  - `TOPIX100`
- Signal variants:
  - `raw_gap = (close / sma50) - 1`
  - `atr_gap_14 = (close - sma50) / atr14`
- Volume feature:
  - default `volume_sma_5_20`
- Bounce slice:
  - `middle_volume_high`
  - `middle_volume_low`
  - `q10_volume_high`
  - `q10_volume_low`
- Horizons:
  - `t_plus_1`
  - `t_plus_5`
  - `t_plus_10`
- Outputs:
  - `q10 / middle` split summary
  - pairwise significance
  - `Q10 Low vs ...` hypothesis / scorecard
  - representative sample chart candidates

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_sma50_raw_vs_atr_q10_bounce.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_sma50_raw_vs_atr_q10_bounce.py`
  - `apps/bt/src/domains/analytics/topix_rank_future_close_core.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix100_sma50_raw_vs_atr_q10_bounce.py`
  - `apps/bt/tests/unit/scripts/test_run_topix100_sma50_raw_vs_atr_q10_bounce.py`

## Latest Baseline

- [baseline-2026-03-31.md](./baseline-2026-03-31.md)

## Current Read

- `raw_gap` は従来の読みを保ちつつ、`atr_gap_14` は high-range names の見た目を相対的に鈍らせるので、`middle` bucket の顔つきが変わるかを直接見比べられます。
- `volume_sma_5_20` を固定しているので、今回の差分は price distance scaling に集中しています。
- playground の representative sample charts は、各 bucket の median-like event を analysis 前半・中盤・後半から 1 件ずつ拾い、統計差と chart impression を同じ画面で確認する前提です。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix100_sma50_raw_vs_atr_q10_bounce.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

notebook は latest bundle を既定で読みます。fresh analysis は `Mode = Run Fresh Analysis` に切り替えたときだけ実行されます。

## Next Questions

- `raw_gap` で強かった `Q10 Low vs Middle High` は、`atr_gap_14` でも残るか、それとも high-ATR middle names の再配置で薄まるか。
- representative sample chart の印象差は、実際に `atr14` の大きさで説明できるか。
- 次段で regime conditioning を足すなら、raw / ATR のどちらを基準 signal に採るべきか。
