# TOPIX100 Price vs SMA Q10 Bounce

TOPIX100 の `price / SMA20|50|100` 研究から、`Q10` 側の bounce 仮説だけを切り出した実験です。runner-first 導線では選択した `price_feature` / `volume_feature` の組み合わせを bundle に保存し、notebook は bundle viewer として使います。

## Published Readout

### Decision

`SMA50 Q10 Low` を後続の regime conditioning に渡す第一候補として採用する。ただし、この研究単体は production rule ではなく、先行研究 `topix100-price-vs-sma-rank-future-close` で見えた「continuation ではなく oversold bounce」という方向を、`Q10 Low vs ...` 仮説に絞って確認した中間研究として扱う。

### Why This Research Was Run

前段の `price / SMA` rank study では、`Q1` continuation より `SMA50` / `SMA100` の `Q10` 反発が自然に見えた。この研究では `Q10 Low vs Q10 High`、`Q10 Low vs Middle Low`、`Q10 Low vs Middle High` の 3 比較に絞り、どの SMA horizon が rebound candidate として一番扱いやすいかを決めるために実行した。

### Data Scope / PIT Assumptions

入力は `~/.local/share/trading25/market-timeseries/market.duckdb` の snapshot。分析範囲は `2016-08-19 -> 2026-03-27`、valid dates は `2,344`、warmup/filter 後の stock-day rows は `232,484`。対象は `TOPIX100`。特徴量は signal date 時点の `price_vs_sma_20_gap` / `price_vs_sma_50_gap` / `price_vs_sma_100_gap` と volume split、forward horizon は `t_plus_5` / `t_plus_10` を中心に読む。

### Main Findings

#### `SMA50 Q10 Low` は 5d/10d の両方で最も素直な bounce candidate だった。

| Feature | Horizon | Hypothesis | Mean diff | Positive share | Paired t Holm | Wilcoxon Holm |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `price_vs_sma_50_gap` | `t_plus_5` | `Q10 Low vs Q10 High` | `+0.1762%` | `51.7%` | `0.001917` | `0.03162` |
| `price_vs_sma_50_gap` | `t_plus_5` | `Q10 Low vs Middle Low` | `+0.2309%` | `51.2%` | `0.000140` | `0.02250` |
| `price_vs_sma_50_gap` | `t_plus_5` | `Q10 Low vs Middle High` | `+0.2698%` | `53.0%` | `0.00000748` | `0.000182` |
| `price_vs_sma_50_gap` | `t_plus_10` | `Q10 Low vs Middle High` | `+0.4757%` | `51.8%` | `0.00000000783` | `0.0000695` |

#### `SMA100` は `Q10` bucket 内の volume split より、middle との差で読むべきだった。

| Feature | Horizon | Hypothesis | Mean diff | Positive share | Paired t Holm | Wilcoxon Holm |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `price_vs_sma_100_gap` | `t_plus_10` | `Q10 Low vs Q10 High` | `+0.0159%` | `51.2%` | `1.0000` | `0.9929` |
| `price_vs_sma_100_gap` | `t_plus_10` | `Q10 Low vs Middle Low` | `+0.3047%` | `52.1%` | `0.000148` | `0.002250` |
| `price_vs_sma_100_gap` | `t_plus_10` | `Q10 Low vs Middle High` | `+0.3251%` | `52.9%` | `0.000117` | `0.002097` |

#### `SMA20` は平均差だけなら正だが、positive share がほぼ 50% で主軸には弱い。

| Feature | Horizon | Hypothesis | Mean diff | Positive share | Paired t Holm | Wilcoxon Holm |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `price_vs_sma_20_gap` | `t_plus_10` | `Q10 Low vs Q10 High` | `+0.1965%` | `51.0%` | `0.0279` | `0.1367` |
| `price_vs_sma_20_gap` | `t_plus_10` | `Q10 Low vs Middle Low` | `+0.2048%` | `50.0%` | `0.0253` | `0.2801` |
| `price_vs_sma_20_gap` | `t_plus_10` | `Q10 Low vs Middle High` | `+0.2328%` | `51.0%` | `0.0115` | `0.2801` |

### Interpretation

先行研究で見えた `SMA50` / `SMA100` の低rank反発は、`Q10 Low` に限定しても消えなかった。ただし positive share は `51-53%` 程度で、単純に `SMA50 Q10 Low` を買うだけでは薄い。`SMA50` は `Q10 High` と middle の両方に勝つため候補として最も整理されている一方、`SMA100` は `Q10 High` との差がなく、volume split ではなく「大きく下がった位置そのもの」の説明に近い。

### Production Implication

production に近づけるには、この段階では entry rule ではなく candidate generator として使う。次は `SMA50 Q10 Low` を same-day `TOPIX close` / `NT ratio` regime に重ね、`Q10 Low vs Middle` の差がどの market state で残るかを検証する。`SMA100` は第二候補、`SMA20` は優先度を下げる。

### Caveats

これは `TOPIX100` の日足 observation で、手数料、スリッページ、同時保有、capacity は未評価。`SMA50` でも positive share は高くないため、平均差だけで production edge と読まない。`Q10 Low` は crash continuation を含みうるため、後続では market regime や tail-risk filter が必要。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_price_vs_sma_q10_bounce.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_price_vs_sma_q10_bounce.py`
- Baseline: `apps/bt/docs/experiments/market-behavior/topix100-price-vs-sma-q10-bounce/baseline-2026-03-31.md`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-price-vs-sma-q10-bounce/20260331_173029_de1d187c`
- Tables: `results.duckdb`

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
  - default `volume_sma_20_80`
  - runner では `volume_sma_5_20` / `volume_sma_20_80` / `volume_sma_50_150` を指定可能
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

- Runner:
  - `apps/bt/scripts/research/run_topix100_price_vs_sma_q10_bounce.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_price_vs_sma_q10_bounce.py`
  - `apps/bt/src/domains/analytics/topix100_price_vs_sma_rank_future_close.py`
  - `apps/bt/src/domains/analytics/research_bundle.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma_q10_bounce.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma_rank_future_close.py`
  - `apps/bt/tests/unit/scripts/test_run_topix100_price_vs_sma_q10_bounce.py`

## Latest Baseline

- [baseline-2026-03-31.md](./baseline-2026-03-31.md)

## Current Read

- `SMA20` は bounce slice でも弱いです。`t_plus_10` で平均差は出るものの、positive share はほぼ `50%` で、安定した反発パターンとは言いにくいです。
- `SMA50` は最も分かりやすい bounce feature です。`Q10 Low` は `Middle High` / `Middle Low` / `Q10 High` の全部に対して `t_plus_5` と `t_plus_10` で正の差を持ち、Holm 補正後も多くが残ります。
- `SMA100` も `Middle` 相手には強いですが、`Q10 High` との差はほぼ無いです。つまり `SMA100` は「Q10 bucket 内の volume split」より「Q10 と middle の位置差」で読んだ方が素直です。
- 次段で regime conditioning を重ねるなら、第一候補は `price_vs_sma_50_gap` の `Q10 Low`、第二候補は `price_vs_sma_100_gap` の `Q10 Low vs Middle` です。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix100_price_vs_sma_q10_bounce.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

notebook は latest bundle を既定で読みます。新規 run は notebook ではなく runner script から実行します。

## Next Questions

- `SMA50 Q10 Low` を same-day TOPIX close / NT ratio / VI regime に重ねると、bounce の positive share はさらに上がるか。
- `SMA100` では `Q10 High` との差が弱いので、volume 条件を `20/80` から長期 proxy へ替えると bucket 内分離が改善するか。
- 実運用の候補としては `Q10 Low` 単体より、`Q10 Low - Middle Low` spread を安定化させる market regime filter の方が重要ではないか。
