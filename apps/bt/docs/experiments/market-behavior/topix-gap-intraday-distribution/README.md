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

- Runner:
  - `apps/bt/scripts/research/run_topix_gap_intraday_distribution.py`
- Notebook:
  - `apps/bt/notebooks/playground/topix_gap_intraday_distribution_playground.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_gap_intraday_distribution.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix_gap_intraday_distribution.py`

## Latest Baseline

- [baseline-2026-03-24.md](./baseline-2026-03-24.md)

## Current Read

- 現行 baseline の TOPIX gap 標準偏差は `0.6751%` で、bucket は `±1σ = 0.68%`, `±2σ = 1.35%` を使う。
- `-0.68% < gap < 0.68%` が 70.56% を占め、この帯では `TOPIX100` と `TOPIX500` はほぼフラット、`PRIME` と `PRIME ex TOPIX500` は小幅マイナス。
- `gap <= -1.35%` では全 group が明確にプラスで、特に `TOPIX100` / `TOPIX500` が `0.26%` 台と強い。
- `0.68% <= gap < 1.35%` では `PRIME` / `PRIME ex TOPIX500` が優位に回り、`TOPIX100` / `TOPIX500` は逆に弱い。
- sigma ベースにすると `strong` 側のサンプルが十分増え、rotation では `weak` と `strong` の両方が寄与する構図になった。

## 再現方法

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_gap_intraday_distribution.py \
  --sample-size 0
```

この command は
`~/.local/share/trading25/research/market-behavior/topix-gap-intraday-distribution/<run_id>/`
へ bundle を保存します。

Notebook で確認する場合:

```bash
uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/topix_gap_intraday_distribution_playground.py
```

## 次に見るべき点

- `gap <= -2σ` で `TOPIX100` / `TOPIX500` が強い理由を、セクター構成や寄り付き売買代金で分解できるか。
- `+1σ ~ +2σ` と `>= +2σ` の strong side を分けると、`PRIME ex TOPIX500` の使い方はさらに sharpen するか。
- fixed rotation を `TOPIX100` を含む選択ルールに変えると、trade frequency と drawdown のバランスが改善するか。
