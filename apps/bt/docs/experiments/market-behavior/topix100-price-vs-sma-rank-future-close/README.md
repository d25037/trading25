# TOPIX100 Price vs SMA Rank / Future Close

TOPIX100 の `price / SMA20|50|100` を単独特徴として使い、decile と price/volume split で将来 `close` / `return` を観察する実験です。volume 条件は既存 SMA20 研究と同じ `volume_sma_20_80` を使います。

## Purpose

- 既存の `price vs SMA20` notebook を、`SMA20 / SMA50 / SMA100` の feature family として比較可能にする。
- `Q1` を「SMA を大きく上回る銘柄群」、`Q10` を「SMA を大きく下回る銘柄群」として、将来 `t_plus_1 / t_plus_5 / t_plus_10` の decile 構造を確認する。
- `volume_sma_20_80` high/low split を掛け合わせて、price deviation の極端群に turnover regime が追加の説明力を持つかを見る。

## Scope

- Universe:
  - `TOPIX100`
- Price features:
  - `price_vs_sma_20_gap`
  - `price_vs_sma_50_gap`
  - `price_vs_sma_100_gap`
- Volume feature:
  - `volume_sma_20_80`
- Horizons:
  - `t_plus_1`
  - `t_plus_5`
  - `t_plus_10`
- Outputs:
  - decile summary / significance
  - price bucket summary / significance
  - price x volume split summary / significance

## Source Of Truth

- Notebook:
  - `apps/bt/notebooks/playground/topix100_price_vs_sma_rank_future_close_playground.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_price_vs_sma_rank_future_close.py`
  - `apps/bt/src/domains/analytics/topix100_price_vs_sma20_rank_future_close.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma_rank_future_close.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma20_rank_future_close.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma20_regime_conditioning.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix100_vi_change_regime_conditioning.py`

## Latest Baseline

- [baseline-2026-03-31.md](./baseline-2026-03-31.md)

## Current Read

- `price_vs_sma_20_gap` は全体として弱く、`Q1-Q10` spread は `t_plus_1=-0.0334%`、`t_plus_5=-0.0227%`、`t_plus_10=+0.0107%` でした。`t_plus_10` だけ decile 全体の Friedman 検定は有意ですが、extreme decile 差は小さいです。
- `price_vs_sma_50_gap` と `price_vs_sma_100_gap` は continuation ではなく mean-reversion 寄りです。`Q1-Q10` spread は `t_plus_5/t_plus_10` で負に傾き、`Q10` が `Q1` を上回ります。
- しかも `Q10 Low vs Middle Low` は `SMA50` と `SMA100` の `t_plus_5/t_plus_10` でかなり強いです。長期 SMA を大きく下回り、かつ volume 条件が low の銘柄群は bounce 候補として観察価値があります。
- したがって、この feature family は「高 rank を買う continuation signal」としては弱い一方、「低 rank の反発」を見る mean-reversion research の入口としては有望です。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python - <<'PY'
from src.shared.config.settings import get_settings
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    run_topix100_price_vs_sma_rank_future_close_research,
)

result = run_topix100_price_vs_sma_rank_future_close_research(
    get_settings().market_db_path
)
print(result.decile_future_summary_df)
print(result.group_hypothesis_df)
print(result.split_hypothesis_df)
PY
```

Notebook で確認する場合:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/topix100_price_vs_sma_rank_future_close_playground.py
```

## Next Questions

- `price_vs_sma_50_gap` / `price_vs_sma_100_gap` の `Q10` 側を主役にして、bounce 専用 notebook へ切り出した方が読みやすいか。
- `volume_sma_20_80` ではなく、より長期の turnover proxy を重ねると `Q10` 反発の選別が改善するか。
- continuation ではなく mean-reversion 仮説に切り替えるなら、TOPIX close / VI change regime との相性はどの bucket で強いか。
