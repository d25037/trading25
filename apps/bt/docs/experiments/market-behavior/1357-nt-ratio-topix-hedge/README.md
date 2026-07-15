# 1357 x NT Ratio / TOPIX Hedge

## Published Readout

### Decision
- Archive. 現行 production、Ranking、Screening、strategy selection evidence には使わない。

### Why This Research Was Run
- 旧 runner-first research を fallback-free publication surface に移行するため、現時点の扱いを明示する。
- 旧 `Current Read` / baseline は chat や `summary.json` ではなく、この README 上で triage する。

### Data Scope / PIT Assumptions
- Triage status: `Archive`.
- Blocker: ETF hedge diagnostic only.
- 個別銘柄 universe には依存しないが、現行 production / Ranking / Screening の判断材料ではないため archive context として残す。

### Main Findings
#### 結論: 旧 headline は採用判断に使わない

| Item | Disposition |
| --- | --- |
| Old readout | historical context only |
| Publication source | this README `Published Readout` |
| Bundle `summary.json` | not a publication source |
| Required action | Archive |

### Interpretation
- この readout は旧数値を有効化するものではない。fallback / legacy 構造を排除するため、旧 research の現在の扱いを source markdown に固定する。
- PIT-safe でない可能性がある universe、membership、market grouping、または exploratory branch は、再実行なしに production evidence へ昇格しない。

### Production Implication
- Research catalog 上は historical / descriptive context として残し、再利用が必要になった時だけ新規 runner/readout として起こす。

### Caveats
- 旧 baseline の数値は下の既存セクションに残るが、`Published Readout` より優先しない。
- 再実行する場合は Market schema v4 / `local_projection_v2_event_time` を必須とし、signal-date membership、`stock_master_daily` / `index_membership_daily`、event-time basis lineage の source を README に明記する。旧 schema v3 run は historical provenance に限る。

### Source Artifacts
- Experiment: `market-behavior/1357-nt-ratio-topix-hedge`
- Existing runner / baseline references remain below this section.
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

`1357` を使って日本株ロングの下落局面をどこまでヘッジできるかを、`TOPIX` の絶対方向と `NT倍率` の変化で調べる実験です。

## Purpose

- `TOPIX` 下落日に `1357` を使うだけで十分か、それとも `NT倍率` の回転を併用しないと follow-through を取り損ねるのかを確かめる。
- `翌営業日短期` と `3〜5営業日保有` の両方で、どの regime が broad equity long に対して実用的なヘッジになるかを見る。
- `PRIME` / `TOPIX100` / `TOPIX500` / `PRIME ex TOPIX500` の proxy long basket ごとに、どのルールと hedge weight が最も downside を抑えるかを比較する。

## Scope

- Market inputs:
  - `TOPIX close return = (close - prev_close) / prev_close`
  - `NT ratio = N225_UNDERPX close / TOPIX close`
  - `NT ratio return = (nt_ratio - prev_nt_ratio) / prev_nt_ratio`
- Rule families:
  - `shock-only`
  - `trend-only`
  - `hybrid`
- Hedge targets:
  - `next_overnight`
  - `next_intraday`
  - `next_close_to_close`
  - `forward_3d_close_to_close`
  - `forward_5d_close_to_close`
- Proxy long baskets:
  - `PRIME`
  - `TOPIX100`
  - `TOPIX500`
  - `PRIME ex TOPIX500`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_1357_nt_ratio_topix_hedge.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/hedge_1357_nt_ratio_topix.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_1357_nt_ratio_topix_hedge.py`

## Latest Baseline

- [baseline-2026-03-26.md](./baseline-2026-03-26.md)

## Current Read

- `TOPIX` 単独の急落は `1357` の翌日 follow-through を十分に説明しません。`TOPIX <= -2σ` でも `NT倍率` が `TOPIX` 側へ回る日と `日経` 側へ回る日で結果が真逆になります。
- `next_close_to_close` と `forward_5d_close_to_close` の strongest cell は、どちらも `TOPIX <= -2σ` かつ `NT +1σ~+2σ` で、`broad weakness + Nikkei relative strength` が `1357` にとって最も素直な follow-through regime です。
- 逆に `TOPIX <= -2σ` なのに `NT <= μ-2σ` は `1357` にとって worst cell で、panic 後の reversion をまともに食らいます。
- `trend_macd_negative` は `TOPIX` に対する一般的な `EMA MACD(12/26/9, adjust=False)` の histogram < 0 で判定します。
- ルール評価では、strict な discovery / validation 両通過の shortlist は今回も出ませんでした。EMA 版の fallback top 3 は `TOPIX500` / `TOPIX100` に対する `forward_3d_close_to_close x trend_macd_negative` の `fixed_0.30〜0.40` が上位です。
- ただし `trend_macd_negative` は hedge には効いても、`1357` 単独ではかなり悪く、`forward_3d_close_to_close` の active-only 累積は overall で `-99.91%` 近辺まで減価します。`1357` 単独の売買ルールとしては、sample は少ないものの `shock_joint_adverse x next_close_to_close` の方がまだ素直です。
- `hybrid_bearish_joint` は実データ上で active day が極端に少なく、現状では research note の対象にはなるものの、実用ルールとしてはまだ未成熟です。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_1357_nt_ratio_topix_hedge.py
```

この command は
`~/.local/share/trading25/research/market-behavior/1357-nt-ratio-topix-hedge/<run_id>/`
へ bundle を保存します。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- `trend_macd_negative` が broad に効きすぎる一方で carry drag も大きいので、`NT倍率` filter を足して active day をどこまで減らせるか。
- `shock_joint_adverse` は日数が少ないので、`TOPIX <= -1σ` と `NT >= +1σ` の周辺を rolling quantile で切り直すと安定するか。
- `TOPIX100` が shortlist を独占した理由を、proxy basket beta と sector concentration で説明できるか。
