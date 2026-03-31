# TOPIX100 Price vs SMA Q10 Bounce

TOPIX100 の `price / SMA20|50|100` 研究から、`Q10` 側の bounce 仮説だけを切り出した実験です。`middle` と `q10` を対象に、`volume_sma_20_80` の high/low を掛け合わせて `Q10 Low` がどこまで優位かを見ます。

## Purpose

- `price / SMA` family の中で continuation ではなく mean-reversion 側に寄っていた `Q10` を主役にする。
- `Q10 Low vs Q10 High`
- `Q10 Low vs Middle Low`
- `Q10 Low vs Middle High`

この 3 仮説を feature / horizon ごとに並べ、どの `SMA` が bounce 観察に向くかを判定する。

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
  - `q10 / middle` volume split summary
  - pairwise significance
  - `Q10 Low vs ...` hypothesis table
  - daily spread scorecard

## Source Of Truth

- Notebook:
  - [`apps/bt/notebooks/playground/topix100_price_vs_sma_q10_bounce_playground.py`](/Users/shinjiroaso/dev/trading25/apps/bt/notebooks/playground/topix100_price_vs_sma_q10_bounce_playground.py)
- Domain logic:
  - [`apps/bt/src/domains/analytics/topix100_price_vs_sma_q10_bounce.py`](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix100_price_vs_sma_q10_bounce.py)
  - [`apps/bt/src/domains/analytics/topix100_price_vs_sma_rank_future_close.py`](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix100_price_vs_sma_rank_future_close.py)
- Tests:
  - [`apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma_q10_bounce.py`](/Users/shinjiroaso/dev/trading25/apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma_q10_bounce.py)
  - [`apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma_rank_future_close.py`](/Users/shinjiroaso/dev/trading25/apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma_rank_future_close.py)

## Latest Baseline

- [baseline-2026-03-31.md](./baseline-2026-03-31.md)

## Current Read

- `SMA20` は bounce slice でも弱いです。`t_plus_10` で平均差は出るものの、positive share はほぼ `50%` で、安定した反発パターンとは言いにくいです。
- `SMA50` は最も分かりやすい bounce feature です。`Q10 Low` は `Middle High` / `Middle Low` / `Q10 High` の全部に対して `t_plus_5` と `t_plus_10` で正の差を持ち、Holm 補正後も多くが残ります。
- `SMA100` も `Middle` 相手には強いですが、`Q10 High` との差はほぼ無いです。つまり `SMA100` は「Q10 bucket 内の volume split」より「Q10 と middle の位置差」で読んだ方が素直です。
- 次段で regime conditioning を重ねるなら、第一候補は `price_vs_sma_50_gap` の `Q10 Low`、第二候補は `price_vs_sma_100_gap` の `Q10 Low vs Middle` です。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python - <<'PY'
from src.shared.config.settings import get_settings
from src.domains.analytics.topix100_price_vs_sma_q10_bounce import (
    run_topix100_price_vs_sma_q10_bounce_research,
)

result = run_topix100_price_vs_sma_q10_bounce_research(
    get_settings().market_db_path
)
print(result.q10_low_scorecard_df)
print(result.q10_low_hypothesis_df)
PY
```

Notebook で確認する場合:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/topix100_price_vs_sma_q10_bounce_playground.py
```

## Next Questions

- `SMA50 Q10 Low` を same-day TOPIX close / NT ratio / VI regime に重ねると、bounce の positive share はさらに上がるか。
- `SMA100` では `Q10 High` との差が弱いので、volume 条件を `20/80` から長期 proxy へ替えると bucket 内分離が改善するか。
- 実運用の候補としては `Q10 Low` 単体より、`Q10 Low - Middle Low` spread を安定化させる market regime filter の方が重要ではないか。
