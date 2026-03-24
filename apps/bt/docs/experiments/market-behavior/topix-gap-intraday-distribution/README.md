# TOPIX Gap / Intraday Distribution

TOPIX の寄り付き gap を event day として bucket 化し、個別銘柄群の当日 intraday リターンと、そこから導かれる簡易 rotation ルールを観察する実験です。

## 目的

- `TOPIX が窓開けして始まった日` に、どの銘柄群が当日中に相対的に強いかを把握する。
- gap の方向と強さに応じて、`TOPIX500` と `PRIME ex TOPIX500` のどちらを intraday で選ぶべきかを検討する。
- 単純な bucket 平均だけでなく、`どの gap bucket がどれくらい頻繁に起きるか` も含めて解釈する。

## スコープ

- Event definition:
  - `topix_gap_return = (topix_open - prev_topix_close) / prev_topix_close`
- Trade definition:
  - `stock_intraday_return = (close - open) / open`
- Stock groups:
  - `PRIME`
  - `TOPIX100`
  - `TOPIX500`
  - `PRIME ex TOPIX500`
- Fixed rotation rule:
  - `weak => TOPIX500 long`
  - `strong => PRIME ex TOPIX500 long`
  - `neutral => flat`

## Source Of Truth

- Notebook:
  - [`apps/bt/notebooks/playground/topix_gap_intraday_distribution_playground.py`](/Users/shinjiroaso/dev/trading25/apps/bt/notebooks/playground/topix_gap_intraday_distribution_playground.py)
- Domain logic:
  - [`apps/bt/src/domains/analytics/topix_gap_intraday_distribution.py`](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix_gap_intraday_distribution.py)
- Tests:
  - [`apps/bt/tests/unit/domains/analytics/test_topix_gap_intraday_distribution.py`](/Users/shinjiroaso/dev/trading25/apps/bt/tests/unit/domains/analytics/test_topix_gap_intraday_distribution.py)

## Latest Baseline

- [baseline-2026-03-24.md](./baseline-2026-03-24.md)

## Current Read

- `-1% < gap < 1%` が event day の大半を占め、その bucket では全群とも intraday 平均がわずかにマイナス。
- `-2% < gap <= -1%` では `TOPIX100` と `TOPIX500` の intraday が最も強く、moderate negative gap に対する反発が見える。
- `PRIME` と `PRIME ex TOPIX500` は最頻出 bucket のマイナス寄与が重く、頻度加重で見ると総合期待値は小幅マイナス。
- 簡易 rotation は `neutral` 日を flat にすることで、gap 条件が出た日にだけ資金を使う構造になっている。
- rotation の実績では `weak => TOPIX500` 側の寄与が `strong => PRIME ex TOPIX500` より大きい。

## 再現方法

```bash
uv run --project apps/bt python - <<'PY'
from src.domains.analytics.topix_gap_intraday_distribution import (
    run_topix_gap_intraday_distribution,
)

result = run_topix_gap_intraday_distribution(
    "/Users/shinjiroaso/.local/share/trading25/market-timeseries/market.duckdb",
    sample_size=0,
)
print(result.summary_df)
print(result.rotation_overall_summary_df)
PY
```

Notebook で確認する場合:

```bash
uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/topix_gap_intraday_distribution_playground.py
```

## 次に見るべき点

- `-2% < gap <= -1%` で `TOPIX100` と `TOPIX500` が強い理由を、セクター構成や寄り付きの売買代金で分解できるか。
- `gap <= -2%` と `gap >= 2%` は件数が少ないため、閾値固定ではなく z-score 的な正規化の方が安定するか。
- fixed rotation を `TOPIX100` を含む選択ルールに変えると、trade frequency と drawdown のバランスが改善するか。
