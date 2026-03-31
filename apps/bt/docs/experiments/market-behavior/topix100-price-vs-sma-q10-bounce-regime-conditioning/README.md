# TOPIX100 Price vs SMA Q10 Bounce Regime Conditioning

`price / SMA` の `Q10 bounce` 研究に対して、same-day `TOPIX close return` と `NT ratio return` の regime を重ねる実験です。runner-first 導線では `SMA50 Q10 Low + volume_sma_5_20` を既定 bundle に保存し、notebook は viewer として使います。

## Purpose

- `SMA50 Q10 Low` の反発が、どの market regime で強まるかを確認する。
- 同じ `Q10 Low` でも、`Q10 High` / `Middle Low` / `Middle High` に対する優位性が regime でどう変わるかを見る。
- 次段で production 寄りの filter 仮説に寄せるなら、`TOPIX close` と `NT ratio` のどちらが先かを判断する。

## Scope

- Universe:
  - `TOPIX100`
- Price feature:
  - default `price_vs_sma_50_gap`
  - helper 自体は `price_vs_sma_20_gap` / `price_vs_sma_100_gap` も受け付ける
- Volume feature:
  - `volume_sma_20_80`
- Bounce slice:
  - `middle_volume_high`
  - `middle_volume_low`
  - `q10_volume_high`
  - `q10_volume_low`
- Regime types:
  - `topix_close`
  - `nt_ratio`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_price_vs_sma_q10_bounce_regime_conditioning.py`
- Notebook viewer:
  - `apps/bt/notebooks/playground/topix100_price_vs_sma50_q10_bounce_regime_conditioning_playground.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_price_vs_sma_q10_bounce_regime_conditioning.py`
  - `apps/bt/src/domains/analytics/topix100_price_vs_sma_q10_bounce.py`
  - `apps/bt/src/domains/analytics/research_bundle.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma_q10_bounce_regime_conditioning.py`
  - `apps/bt/tests/unit/scripts/test_run_topix100_price_vs_sma_q10_bounce_regime_conditioning.py`

## Latest Baseline

- [baseline-2026-03-31.md](./baseline-2026-03-31.md)

## Current Read

- `SMA50 Q10 Low` は unconditional でも強いですが、regime を掛けると `neutral` と `strong` がさらに読みやすくなります。
- `TOPIX close` では `neutral` と `strong` で `Q10 Low vs Middle` がかなり強いです。特に `t_plus_10` の `Q10 Low vs Middle High` は `neutral=+0.4008%`、`strong=+0.7759%` でした。
- `NT ratio` では `neutral` が最も整っています。`t_plus_10` の `Q10 Low vs Middle High=+0.4432%`、`Q10 Low vs Middle Low=+0.4327%` で、補正後 p-value もかなり低いです。
- 一方 `weak` regime は平均差は大きく見えても sample が少なく、統計は安定しません。first filter 候補は `weak` 回避より `neutral/strong` 優先です。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix100_price_vs_sma_q10_bounce_regime_conditioning.py
```

Notebook で確認する場合:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt marimo edit \
  apps/bt/notebooks/playground/topix100_price_vs_sma50_q10_bounce_regime_conditioning_playground.py
```

notebook は latest bundle を既定で読みます。fresh analysis は `Mode = Run Fresh Analysis` に切り替えたときだけ実行されます。

## Next Questions

- `TOPIX close strong` と `NT ratio neutral` のどちらが out-of-sample で安定するか。
- `Q10 Low vs Middle` を優先し、`Q10 Low vs Q10 High` は補助条件に留めた方が良いか。
- 次段で `VI change` を重ねるなら、`SMA50` の `Q10 Low` に対して追加説明力があるか。
