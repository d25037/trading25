# 1357 x NT Ratio / TOPIX Hedge

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

- Notebook:
  - `apps/bt/notebooks/playground/hedge_1357_nt_ratio_topix_playground.py`
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
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python - <<'PY'
from src.shared.config.settings import get_settings
from src.domains.analytics.hedge_1357_nt_ratio_topix import (
    run_1357_nt_ratio_topix_hedge_research,
)

result = run_1357_nt_ratio_topix_hedge_research(get_settings().market_db_path)
print(result.shortlist_df)
print(
    result.joint_forward_summary_df[
        (result.joint_forward_summary_df["split"] == "overall")
        & (result.joint_forward_summary_df["target_name"] == "next_close_to_close")
    ]
)
PY
```

Notebook で確認する場合:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/hedge_1357_nt_ratio_topix_playground.py
```

## Next Questions

- `trend_macd_negative` が broad に効きすぎる一方で carry drag も大きいので、`NT倍率` filter を足して active day をどこまで減らせるか。
- `shock_joint_adverse` は日数が少ないので、`TOPIX <= -1σ` と `NT >= +1σ` の周辺を rolling quantile で切り直すと安定するか。
- `TOPIX100` が shortlist を独占した理由を、proxy basket beta と sector concentration で説明できるか。
