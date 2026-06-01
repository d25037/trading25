# Market Bubble Footprint

市場全体の bubble footprint を、breadth、concentration、valuation pressure、return dispersion、上位寄与で月次監視する研究。

## Published Readout

### Decision

現在の日本株市場は、`2026-05-29` 時点で 20D / 120D / 252D が `blowoff_watch`、60D が `crowded` と判定される。ただしこれは「即クラッシュ」ではなく、`breadth_narrowing + valuation_pressure + return_dispersion + cap_weight_leadership` が多くの horizon で同時点灯した終盤入口の警戒 regime と読む。

12カ月 breadth はまだ 66.88% あり、2025-2026 の上昇は少数銘柄だけの相場ではない。一方で 20D / 60D breadth と SMA breadth は細り、割高時価総額シェアは 24% 台まで上がっている。したがって「数カ月上昇を取りに行く余地はあるが、ここから breadth が戻らず集中と割高化だけが進むなら危険度を上げる」という監視用途に使う。

### Main Findings

#### 結論: 最新 snapshot は 20D / 120D / 252D が `blowoff_watch`

Primary run `20260601_market_bubble_footprint_2016_legacy_mapped_v2` は `2016-01-01` 以降、Prime / Standard / Growth、月次 snapshot、lookback `20/60/120/252`。2022年の東証市場再編前は legacy market code を current scope へ mapping する。

| Horizon | Regime | Score | Breadth up | SMA50 above | SMA200 above | Top10 mcap | Top10 positive contribution | Expensive mcap | P90-P10 spread | TOPIX return | Cap-weight return | Equal-weight return |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20 | `blowoff_watch` | 4 | 42.88% | 38.19% | 43.26% | 10.10% | 18.38% | 24.05% | 30.61% | 5.94% | 6.66% | 1.07% |
| 60 | `crowded` | 3 | 24.77% | 38.17% | 43.37% | 10.10% | 21.31% | 24.06% | 39.90% | 0.47% | 1.40% | -5.15% |
| 120 | `blowoff_watch` | 4 | 48.86% | 38.27% | 43.62% | 10.12% | 10.90% | 24.05% | 67.66% | 17.47% | 18.32% | 8.59% |
| 252 | `blowoff_watch` | 4 | 66.88% | 38.36% | 43.90% | 10.13% | 14.23% | 24.08% | 115.35% | 44.40% | 44.36% | 30.56% |

2016年以降の月次履歴では、`blowoff_watch` は horizon 別に 20D が3回、60Dが1回、120Dが2回、252Dが1回だけ。いずれかの horizon が `blowoff_watch` になった月は4回で、4 horizon すべてが同時に `blowoff_watch` になった月はない。足元は「過去最悪」ではないが、複数 horizon が同時に warning へ入った珍しい局面。

#### 結論: 12カ月の上昇寄与は半導体/AI隣接だけではないが、電気機器の寄与が突出

252D positive contribution の上位は、キオクシア、ソフトバンクグループ、村田製作所、アドバンテスト、東京エレクトロンが中心。電気機器 sector は positive contribution の 31.55% を占める。

| Rank | Code | Name | Sector | 252D return | Latest mcap | Positive contribution |
| ---: | --- | --- | --- | ---: | ---: | ---: |
| 1 | 285A | キオクシアホールディングス | 電気機器 | 2864.88% | 35.96T JPY | 7.31% |
| 2 | 9984 | ソフトバンクグループ | 情報･通信業 | 287.68% | 42.79T JPY | 6.97% |
| 3 | 6981 | 村田製作所 | 電気機器 | 375.31% | 18.89T JPY | 3.18% |
| 4 | 6857 | アドバンテスト | 電気機器 | 259.82% | 19.16T JPY | 3.09% |
| 5 | 8035 | 東京エレクトロン | 電気機器 | 123.40% | 24.72T JPY | 2.91% |
| 6 | 8306 | 三菱UFJフィナンシャル・グループ | 銀行業 | 54.03% | 35.59T JPY | 2.71% |
| 7 | 9983 | ファーストリテイリング | 小売業 | 68.02% | 26.20T JPY | 2.26% |
| 8 | 8316 | 三井住友フィナンシャルグループ | 銀行業 | 68.13% | 22.27T JPY | 1.95% |
| 9 | 8411 | みずほフィナンシャルグループ | 銀行業 | 94.99% | 17.91T JPY | 1.88% |
| 10 | 8058 | 三菱商事 | 卸売業 | 75.87% | 20.41T JPY | 1.87% |

| Sector | Obs | Equal-weight return | Cap-weight return | Positive contribution |
| --- | ---: | ---: | ---: | ---: |
| 電気機器 | 223 | 108.26% | 98.30% | 31.55% |
| 銀行業 | 78 | 100.47% | 72.65% | 11.22% |
| 卸売業 | 270 | 26.82% | 55.47% | 7.66% |
| 非鉄金属 | 31 | 164.89% | 347.25% | 5.79% |
| 情報･通信業 | 582 | 3.14% | 27.15% | 7.78% |

### Interpretation

`blowoff_watch` は crash timing signal ではない。今回の score は、足元 breadth の悪化、割高時価総額シェア、return dispersion、cap-weight leadership を同じ snapshot で読む warning regime である。

重要なのは 252D breadth がまだ広いこと。相場全体は過去12カ月で広く上がっており、典型的な「指数だけ上がり個別は死んでいる」局面ではない。ただし 20D / 60D breadth、SMA50/SMA200 breadth が低く、直近では参加銘柄が細っている。2016年以降の履歴でも、60D breadth は低位、expensive mcap share と return dispersion は高位にある。ここから指数だけが上がり、breadth が回復しなければ、終盤入口から終盤本体へ進む可能性を上げる。

AI/半導体隣接色は強いが、銀行・卸売・非鉄金属も寄与しているため、純粋な一本足ではない。したがって monitor は「AIバブル断定」ではなく、Ranking / rerating research の market regime overlay として使う。

### Production Implication

Ranking / research では以下の扱いにする。

| Regime | Implication |
| --- | --- |
| `normal` | rerating 系を通常評価する |
| `narrowing` | value confirmation と sector strength を優先する |
| `crowded` | `crowded_rerating` を雑に買わず、no-value と高valuationを警戒する |
| `blowoff_watch` | 短期上昇余地は否定しないが、holding horizon を短くし、強value以外の rerating exposure を落とす候補にする |

この monitor 単体で売買判断はしない。次の判断は `rerating-bubble-regime-forward-response` のように、bucket 別 forward response と接続して行う。

### Caveats

- Market universe は Prime / Standard / Growth。2022年以前は legacy market code を current scope に寄せる。
- `stock_master_daily` がある場合は target-date exact join、なければ `stocks` latest fallback。
- `expensive_mcap_share` は `forward_per > 40 OR pbr > 5` の単純 rule。
- `bubble_regime` は crash 予測ではなく、market footprint の warning label。
- Sector contribution は current market database の `sector_33_name` に依存する。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/market_bubble_footprint.py`
- Runner: `apps/bt/scripts/research/run_market_bubble_footprint.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_market_bubble_footprint.py`
- Bundle: `/private/tmp/trading25-research/market-behavior/market-bubble-footprint/20260601_market_bubble_footprint_2016_legacy_mapped_v2/`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/market_bubble_footprint.py`
- Runner:
  - `apps/bt/scripts/research/run_market_bubble_footprint.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_market_bubble_footprint.py \
  --start-date 2016-01-01 \
  --return-horizons 20,60,120,252 \
  --markets prime,standard,growth \
  --frequency monthly \
  --output-root /private/tmp/trading25-research \
  --run-id 20260601_market_bubble_footprint_2016_legacy_mapped_v2
```
