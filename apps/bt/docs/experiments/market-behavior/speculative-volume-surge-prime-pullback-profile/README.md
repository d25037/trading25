# Speculative Volume-Surge Prime Pullback Profile

`+10% close × 10x volume` の surge を起こした `プライム` 銘柄だけを取り出し、
`浅い pullback の continuation` が本当に有利かを見る実験です。

既存の pullback-state 研究とは違い、こちらは
`1 episode = 1 exclusive label` にしています。

## Purpose

- `プライム` の surge episode を `スタンダード/グロース` と分けて扱う。
- 各 episode について、
  surge 後 20 営業日以内・initial peak reclaim 前の
  `deepest close` を 1 本だけ取る。
- その deepest pullback の深さごとに、
  `20/40/60営業日` の close return と asymmetry を比べる。
- `深押し待ち` より `浅い continuation` のほうが良いのかを、
  exclusive label で検証する。

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
- Exclusive pullback label:
  - initial peak 後
  - `t0+20営業日` まで
  - initial peak を reclaim する前
  - その区間での `deepest close`
- Pullback buckets:
  - `<0%`
  - `0-10%`
  - `10-20%`
  - `20-35%`
  - `35%+`

## Interpretation Guardrail

- この label は `ex post` です。
- つまり「その後 20 日の間で最も深かった押し」を見て family 分けしているので、
  その時点でリアルタイムに bucket を知っていた前提ではありません。
- これは trade rule というより、`episode family profile` です。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_speculative_volume_surge_prime_pullback_profile.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/speculative_volume_surge_prime_pullback_profile.py`
- Upstream episode definition:
  - `apps/bt/src/domains/analytics/speculative_volume_surge_follow_on.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_speculative_volume_surge_prime_pullback_profile.py`
  - `apps/bt/tests/unit/scripts/test_run_speculative_volume_surge_prime_pullback_profile.py`

## Latest Baseline

- [baseline-2026-04-20.md](./baseline-2026-04-20.md)

## Current Read

- 2016-04-18 から 2026-04-17 までで、
  `プライム` surge episode は `1,372` 件、
  exclusive deepest-pullback profile は `1,244` 件でした。
- primary horizon `20d` では、
  `35%+` を除く主要 bucket はすべて median close return がプラスでした。
  - `<0%`: `+6.7%`
  - `0-10%`: `+6.2%`
  - `10-20%`: `+6.9%`
  - `20-35%`: `+5.4%`
- したがって `プライムでは深押しするまで待った方が良い` とは言えません。
  むしろ大きなサンプルでは `10-20%` が最良、`20-35%` でやや鈍る形です。
- ただし `35%+` は `26` episodes しかなく、
  `+11.2%` は outlier 影響を疑うべきです。
- `reclaim rate` は深くなるほど上がり、
  `<0%` の `12.0%` に対して
  `10-20%` は `81.0%`、`20-35%` は `88.4%` でした。
  つまりプライムでは `close return` と `reclaim` が同じ方向を向きやすいです。
- ADV20 で切ると、
  `50m-200m × <0%` が `+8.1%`,
  `<50m × 10-20%` が `+8.2%`,
  `200m-1000m × 0-10%` が `+7.4%` で、
  最良 bucket は流動性帯ごとに少し違います。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_speculative_volume_surge_prime_pullback_profile.py \
  --output-root /tmp/trading25-research
```

bundle は
`/tmp/trading25-research/market-behavior/speculative-volume-surge-prime-pullback-profile/<run_id>/`
に保存されます。

## Next Questions

- exclusive deepest label を使ったまま、
  `+5% / +10% 到達率` と `stop/timeout 実現 return` を足す。
- `20-35%` が鈍るのは `利益確定による再加速不足` なのか、
  `深押し化する episode の質` なのかを、
  sector / market cap / turnover で切り分ける。
- `35%+` は sample が小さいため、
  longer history か stop-high 条件 tightening で安定性を再確認する。
