# Speculative Volume-Surge Follow-On

急激な出来高増加と価格急騰が同時に出た初動を 1 episode にまとめ、
その day0 前日終値を基準にした初動の伸びが、日を空けた後の
`おかわり breakout` とどう関係するかを見る実験です。

## Purpose

- `close >= +10%` かつ `volume / trailing average >= 10x` の day を speculative surge とみなす。
- 同一銘柄の近接トリガーを 20 営業日 cooldown で 1 episode にまとめる。
- 初動 0/1/3/5 営業日で観測できる `base_close -> initial max high` を主特徴にして、
  後日 breakout の有無を比較する。
- `full_extension_20d/60d` は descriptive read として残し、実務寄りの read は
  `initial_extension_0d/1d/3d/5d` だけで分ける。

## Primary Definitions

- Base price:
  - `base_close = event day の前日終値`
- Primary trigger:
  - `event_close_return >= +10%`
  - `volume_ratio_20d >= 10x`
- Initial extension:
  - `max(high[t0..t0+k]) / base_close - 1`, `k in {0,1,3,5}`
- Follow-on breakout:
  - 初動 window の高値を超える高値が、gap 後の観測 window に出るか
  - current default compact read:
    - extension window `5d`
    - gap `10d`
    - follow-on window `40d`
- Secondary surge:
  - gap 後 window の中で、再び `+10%` かつ `10x volume` の primary trigger day が出るか

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_speculative_volume_surge_follow_on.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/speculative_volume_surge_follow_on.py`
- Shared references:
  - `apps/bt/src/domains/analytics/jpx_daily_price_limits.py`
  - `apps/bt/src/domains/analytics/research_bundle.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_speculative_volume_surge_follow_on.py`
  - `apps/bt/tests/unit/scripts/test_run_speculative_volume_surge_follow_on.py`

## Latest Baseline

- [baseline-2026-04-20.md](./baseline-2026-04-20.md)

## Current Read

- 2016-04-18 から 2026-04-17 までで、
  primary trigger 該当 day は `13,154` 件、
  cooldown 後の primary episode は `11,190` 件でした。
- compact read (`initial 5d`, `gap 10d`, `follow-on 40d`) では、
  初動 5 日までの伸びが大きいほど `おかわり breakout` はむしろ減りました。
  - `10-20%`: breakout follow-on `56.1%`
  - `20-35%`: `50.7%`
  - `35-50%`: `43.2%`
  - `50-100%`: `35.9%`
  - `100%+`: `31.1%`
- 一方で `secondary surge` の再点火率は、
  大きく走った bucket のほうが少し高く、
  `10-20%` の `7.0%` に対して `35-50%` 以上はおおむね `10%` 前後でした。
- control cohort を見ると、
  price-only (`+10% だが 10x volume ではない`) のほうが
  breakout follow-on は高く、`63.9%` でした。
  primary `price+volume` は `48.2%`、volume-only は `55.5%` です。
- size / liquidity の compact table では、
  large sample に限定すると `プライム` と `TOPIX Small 1/2` の
  `10-35%` bucket に高い breakout 率が集まりました。
  この read は `sample count >= 100` で再確認する必要があります。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_speculative_volume_surge_follow_on.py \
  --output-root /tmp/trading25-research
```

bundle は
`/tmp/trading25-research/market-behavior/speculative-volume-surge-follow-on/<run_id>/`
に保存されます。

## Next Questions

- `breakout high` ではなく `later close > initial max high` にすると傾向は残るか。
- `primary trigger` を `+10%` ではなく `exact stop_high` / `outside_standard_upper` に寄せると、
  反転型と継続型の混ざり方は変わるか。
- `same-day crowding` や `episode count per date` を入れると、
  price+volume episode の breakout 率は改善するか。
- `ADV20` だけでなく as-of `market cap` を足すと、
  small-cap 仮説をより直接に説明できるか。
