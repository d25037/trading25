# Stock Intraday / Overnight Share

## Published Readout

### Decision
- Archive. 現行 production、Ranking、Screening、strategy selection evidence には使わない。

### Why This Research Was Run
- 旧 runner-first research を fallback-free publication surface に移行するため、現時点の扱いを明示する。
- 旧 `Current Read` / baseline は chat や `summary.json` ではなく、この README 上で triage する。

### Data Scope / PIT Assumptions
- Triage status: `Archive`.
- Blocker: descriptive microstructure diagnostic.
- intraday / overnight share の descriptive diagnostic として残すが、現行 strategy / Ranking の根拠には使わない。

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
- 再実行する場合は `market.duckdb` schema v3、signal-date membership、`stock_master_daily` / `index_membership_daily` の source を README に明記する。

### Source Artifacts
- Experiment: `market-behavior/stock-intraday-overnight-share`
- Existing runner / baseline references remain below this section.
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

個別銘柄の値動きを `当日 intraday (open -> close)` と `翌営業日 overnight (close -> next open)` に分解し、どの銘柄群がどちら主導かを観察する実験です。

## Purpose

- `値動きの主戦場が立会中か立会外か` を、銘柄群ごとに比較する。
- 単純な日次平均ではなく、各銘柄の累積 absolute log-return を使って、`どこで値幅が生まれたか` を測る。
- size / liquidity の違いが `overnight 主導` と `intraday 主導` にどう表れるかを見る。

## Scope

- Session pair definition:
  - `intraday_log_return(t) = log(C_t / O_t)`
  - `overnight_log_return(t) = log(O_{t+1} / C_t)`
- Share definition:
  - `intraday_share = Σ|intraday_log_return| / Σ(|intraday_log_return| + |overnight_log_return|)`
  - `overnight_share = Σ|overnight_log_return| / Σ(|intraday_log_return| + |overnight_log_return|)`
- Stock groups:
  - `TOPIX100`
  - `TOPIX500`
  - `PRIME ex TOPIX500`
  - `STANDARD`
  - `GROWTH`
- Default notebook filter:
  - `min_session_count = 60`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_stock_intraday_overnight_share.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/stock_intraday_overnight_share.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_stock_intraday_overnight_share.py`

## Latest Baseline

- [baseline-2026-03-26.md](./baseline-2026-03-26.md)

## Current Read

- 長期 median では `TOPIX100` が最も overnight 寄りで、中央値は `overnight 47.52% / intraday 52.48%`。とはいえ過半は still intraday で、`完全な overnight 主導` ではない。
- `TOPIX500` は `overnight 44.42%`、`PRIME ex TOPIX500` は `38.02%`、`STANDARD` は `38.42%`、`GROWTH` は `35.76%`。大型から小型へ行くほど intraday 寄りになる size gradient がはっきり出る。
- `overnight_share > 50%` の銘柄比率は `TOPIX100 15%` が突出し、`TOPIX500 3.63%`, `STANDARD 4.77%`, `PRIME ex TOPIX500 0.09%`, `GROWTH 0%`。`overnight 主導銘柄は large-cap に偏る` と読める。
- 直近 60 営業日の日次 group share では `TOPIX100` の平均 overnight share が `48.90%` まで上がり、長期平均より overnight 側に寄っている。一方 `GROWTH` は `36.81%` で、最近も構造はほぼ変わらない。
- `TOPIX500` は `TOPIX100` を含むため、broad large-cap proxy と pure mega-cap proxy を分けて見たいときは `TOPIX100` と `PRIME ex TOPIX500` の両方を併記するのが妥当。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_stock_intraday_overnight_share.py \
  --min-session-count 60
```

この command は
`~/.local/share/trading25/research/market-behavior/stock-intraday-overnight-share/<run_id>/`
へ bundle を保存します。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- `TOPIX100` の overnight 優位銘柄は、半導体・資源・グローバル景気敏感に偏っているか。
- market regime を `TOPIX close`, `NT ratio`, `USDJPY`, `VIX` などで切ると、overnight share の序列はどこで反転するか。
- `STANDARD` と `GROWTH` の intraday 優位は、板の薄さによる日中ノイズなのか、ニュースの消化タイミングによるものか。
- equal-weight 集計ではなく、売買代金や時価総額で重み付けすると `TOPIX100` の overnight 優位はどこまで強まるか。
