# Speculative Volume-Surge Pullback Edge

`+10% close × 10x volume` の初回 surge 後に pullback してきたとき、
その時点の終値が `surge 前日 close` 比でどこにいると
その後の upside/downside 非対称性が良いかを見る実験です。

## Purpose

- 初回 surge 自体の強さではなく、`pullback の現在位置` を state として読む。
- `surge 前日 close` を base とし、pullback close の位置を bucket 化する。
- その state から将来 20/40/60 営業日で
  `future max high` と `future min low` を見て、
  `upside - downside` の asymmetry を比較する。
- `2発目の派手な shock` に限定せず、
  その後の通常の continuation / failure もまとめて読む。

## Primary Definitions

- Primary surge:
  - `event close >= +10%`
  - `volume_ratio_20d >= 10x`
- Base price:
  - `base_close = surge 前日終値`
- Initial peak:
  - `max(high[t0..t0+5])`
- Pullback state:
  - initial peak 後、まだ peak reclaim していない間に、
    close が各 bucket に最初に入った時点
- Pullback buckets:
  - `<-10%`
  - `-10% to 0%`
  - `0-10%`
  - `10-20%`
  - `20-35%`
  - `35-50%`
  - `50%+`
- Future edge read:
  - `future_max_upside_pct = future max high / pullback close - 1`
  - `future_max_downside_pct = 1 - future min low / pullback close`
  - `asymmetry_pct = upside - downside`
  - `peak_reclaim = future max high > initial peak`

## State Semantics

- bucket の単位は `episode` ではなく `state observation` です。
- 1 つの episode が pullback 途中で複数 bucket を通る場合、
  各 bucket に 1 回ずつ入ります。
- したがって bucket 同士は mutually exclusive ではありません。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_speculative_volume_surge_pullback_edge.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/speculative_volume_surge_pullback_edge.py`
- Upstream episode definition:
  - `apps/bt/src/domains/analytics/speculative_volume_surge_follow_on.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_speculative_volume_surge_pullback_edge.py`
  - `apps/bt/tests/unit/scripts/test_run_speculative_volume_surge_pullback_edge.py`

## Latest Baseline

- [baseline-2026-04-20.md](./baseline-2026-04-20.md)

## Current Read

- 2016-04-18 から 2026-04-17 までで、
  primary surge episode は `11,190` 件、
  pullback state observation は `27,428` 件でした。
- `mean asymmetry` は極端な multi-bagger に引っ張られるため、
  実務的な read は `median asymmetry` と `upside > downside rate` を優先します。
- 40 日 read の中央値では、
  `base 比 +10% 未満` の pullback はまだ upside 優勢で、
  `+10% 超` からほぼフラットないし downside 優勢に変わります。
  - `<-10%`: median asymmetry `+5.9%`, upside>downside `60.6%`
  - `-10% to 0%`: `+3.5%`, `57.3%`
  - `0-10%`: `+1.5%`, `52.9%`
  - `10-20%`: `-0.2%`, `49.3%`
  - `20-35%`: `-0.7%`, `48.5%`
  - `35-50%`: `-3.8%`, `45.3%`
  - `50%+`: `-12.4%`, `40.1%`
- 20 日ではこの傾向がもっと明確で、
  `+10% 超` bucket は median asymmetry がすでにマイナスです。
- 60 日まで伸ばすと `10-20%` はわずかにプラスへ戻りますが、
  strongest zone は依然として `base 以下`、
  `35%+` は引き続き downside 優勢です。
- 一方で `peak reclaim rate` は高い位置の bucket ほど上がるため、
  `reclaim しやすさ` と `downside cushion` は同じではありません。
  40 日では `<-10%` の reclaim は `11.8%` しかありませんが、
  `20-35%` は `52.7%` あります。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_speculative_volume_surge_pullback_edge.py \
  --output-root /tmp/trading25-research
```

bundle は
`/tmp/trading25-research/market-behavior/speculative-volume-surge-pullback-edge/<run_id>/`
に保存されます。

## Next Questions

- `future_primary_event` を pullback state 後に再計算し、
  `2発目も +10% × 10x volume` に限定した read を追加する。
- state を `各 bucket 初回到達` ではなく
  `first pullback` / `deepest pullback` / `fixed offset` に変えると、
  解釈しやすさと sample の偏りがどう変わるかを見る。
- `price bucket` と `ADV20` で edge が強いので、
  as-of market cap や turnover rate を追加して
  小型・低流動性仮説を直接検証する。
