# Stock Intraday / Overnight Share

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

- Notebook:
  - `apps/bt/notebooks/playground/stock_intraday_overnight_share_playground.py`
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
uv run --project apps/bt python - <<'PY'
import sys
sys.path.insert(0, "apps/bt")

from src.domains.analytics.stock_intraday_overnight_share import (
    run_stock_intraday_overnight_share_analysis,
)

result = run_stock_intraday_overnight_share_analysis(
    "~/.local/share/trading25/market-timeseries/market.duckdb",
    min_session_count=60,
)
print(result.group_summary_df)
PY
```

Notebook で確認する場合:

```bash
uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/stock_intraday_overnight_share_playground.py
```

## Next Questions

- `TOPIX100` の overnight 優位銘柄は、半導体・資源・グローバル景気敏感に偏っているか。
- market regime を `TOPIX close`, `NT ratio`, `USDJPY`, `VIX` などで切ると、overnight share の序列はどこで反転するか。
- `STANDARD` と `GROWTH` の intraday 優位は、板の薄さによる日中ノイズなのか、ニュースの消化タイミングによるものか。
- equal-weight 集計ではなく、売買代金や時価総額で重み付けすると `TOPIX100` の overnight 優位はどこまで強まるか。
