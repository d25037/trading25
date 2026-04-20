# Speculative Volume-Surge Prime Pullback Tradeable

`deepest pullback label` の ex post family を、
実際に売買可能な daily rule へ落とした実験です。

`プライム` の surge episode に対して、
最初の pullback close が `0-10%` もしくは `10-20%`
へ入った時点で signal とみなし、
翌営業日寄りで入り、
`20営業日 hold` または `initial peak reclaim` で手仕舞います。

## Purpose

- `deepest label family` をそのまま結論に使わず、
  `real-time に観測可能な first-entry rule` へ変換する。
- `0-10% entry` と `10-20% entry` を別戦略として比較する。
- exit を `20営業日 hold or peak reclaim` に固定し、
  実運用に近い return / asymmetry を確認する。
- 各 entry cohort がその後どの `deepest pullback family` に属したかを見て、
  ex post family と整合するかを確かめる。
- `pullback speed` が結果に効くかを切り出す。

## Primary Definitions

- Universe:
  - `market_name = プライム`
- Primary surge:
  - `event close >= +10%`
  - `volume_ratio_20d >= 10x`
- Base price:
  - `base_close = surge 前日終値`
- Initial peak:
  - `max(high[t0..t0+5])`
- Entry signal:
  - initial peak 後
  - initial peak strict reclaim 前
  - `t0+20営業日` まで
  - その区間で `close / base_close - 1` が
    `0-10%` or `10-20%` に最初に入った日
- Entry:
  - signal 翌営業日 `Open`
- Exit:
  - holding window 内で `high > initial_peak_price` になった最初の日に
    `initial_peak_price` で利確
  - それが無ければ `20営業日後 Close`

## Interpretation Guardrail

- 同じ episode が
  `10-20% entry` と `0-10% entry` の両方に入ることがあります。
  これは `entry bucket 別の独立 rule backtest` として扱います。
- `deepest label` は依然として ex post です。
  この研究では、
  `tradeable entry cohort が最終的にどの family に流れたか`
  を alignment table として読むのが主眼です。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_speculative_volume_surge_prime_pullback_tradeable.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/speculative_volume_surge_prime_pullback_tradeable.py`
- Deepest-label family reference:
  - `apps/bt/src/domains/analytics/speculative_volume_surge_prime_pullback_profile.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_speculative_volume_surge_prime_pullback_tradeable.py`
  - `apps/bt/tests/unit/scripts/test_run_speculative_volume_surge_prime_pullback_tradeable.py`

## Latest Baseline

- [baseline-2026-04-20.md](./baseline-2026-04-20.md)

## Current Read

- 2016-04-18 から 2026-04-17 の `プライム` surge episode `1,372` 件に対して、
  tradeable entry は `1,568` 件でした。
  - `10-20% entry`: `934`
  - `0-10% entry`: `634`
- 同じ episode が両 bucket に入る overlap は `542` 件あり、
  `10-20%` で先に signal が出て、その後 `0-10%` まで押すケースがかなりあります。
- median trade return は
  - `0-10% entry`: `+1.8%`
  - `10-20% entry`: `+2.0%`
  で、どちらもプラスでした。
- ただし path-wise asymmetry はかなり違い、
  - `0-10% entry`: `+2.4%`
  - `10-20% entry`: `+0.2%`
  でした。
  つまり `10-20%` は利確 target で勝っているが、
  途中経路の upside/downside はほぼ拮抗です。
- deepest-family alignment を見ると、
  `0-10% entry` の `73.2%` は最終的にも `0-10% family` に残りましたが、
  `10-20% entry` は
  - `10-20% family`: `41.6%`
  - `0-10% family`: `45.9%`
  - `<0% family`: `12.4%`
  と割れます。
  したがって `10-20%` は continuation 入口としては使える一方、
  `ex post family` との整合はそれほど高くありません。
- speed では、
  `0-10% entry` は `1-2日` が最良で `+2.4%`,
  `3-5日` は `-0.1%` と鈍ります。
  一方 `10-20% entry` は
  `6-10日` が `+3.6%` と最良でしたが sample は `54` 件です。
  実務上は
  `0-10% は早押しが良く、10-20% は中速でも崩れない`
  くらいの読みが妥当です。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_speculative_volume_surge_prime_pullback_tradeable.py \
  --output-root /tmp/trading25-research
```

bundle は
`/tmp/trading25-research/market-behavior/speculative-volume-surge-prime-pullback-tradeable/<run_id>/`
に保存されます。
