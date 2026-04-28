# TOPIX100 Price/SMA50 Decile Partitions

TOPIX100 の `Price / SMA50` decile を contiguous partition に分け、どの境界で high / middle / low を読むべきかを比較する実験です。先行する `SMA50 Q10 Low` bounce を、単一 Q10 ではなく decile partition の設計問題として読み直します。

## Published Readout

### Decision

`Price / SMA50` は low-ratio 側の反発候補として残すが、境界は単純な `Q10` 固定だけにしない。price-only では `Q1 | Q2-Q3 | Q4-Q10` が balanced な候補、low-volume edge では `Q1 | Q2-Q9 | Q10` が最も分かりやすい。どちらも production rule ではなく、後続の ATR 正規化・regime conditioning・OOS 検証に渡す partition candidate として扱う。

### Why This Research Was Run

`topix100-price-vs-sma-q10-bounce` では `SMA50 Q10 Low` が最も素直な bounce candidate だった。一方で `Q10` だけを extreme として固定すると、low-ratio 側のどこまでを候補に含めるべきかが曖昧だった。この研究では contiguous decile partition を総当たりし、`Q1` continuation、middle、low-ratio rebound の境界を決めるために実行した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。available range は `2016-03-25 -> 2026-04-01`、analysis range は `2016-06-08 -> 2026-04-01`。対象は latest `TOPIX100` constituent approximation、stock-day rows は `237,784`、valid dates は `2,397`。price feature は `Price / SMA50`、volume feature は `Volume SMA 5 / 20`。candidate partitions は `36` 通り、forward horizon は `t_plus_10` を中心に読む。

### Main Findings

#### price-only partition は `Q1 | Q2-Q3 | Q4-Q10` が balanced だが、low side の定義は広めになる。

| Candidate | Horizon | `High vs Low` | `High vs Middle` | `Low vs Middle` | Wilcoxon hits |
| --- | --- | ---: | ---: | ---: | ---: |
| `Q1 | Q2-Q3 | Q4-Q10` | `t_plus_10` | `+0.1119%` | `+0.2211%` | `+0.1092%` | `3` |
| `Q1 | Q2-Q6 | Q7-Q10` | `t_plus_10` | `+0.0629%` | `+0.1934%` | `+0.1306%` | `3` |
| `Q1-Q8 | Q9 | Q10` | `t_plus_10` | `-0.3218%` | `-0.1425%` | `+0.1793%` | `3` |
| `Q1 | Q2-Q9 | Q10` | `t_plus_10` | `-0.1556%` | `+0.1691%` | `+0.3247%` | `2` |

#### low-volume edge だけを見ると `Q10` 単独が最も読みやすい。

| Candidate | Hypothesis | Mean diff | Positive share | Paired t Holm | Wilcoxon Holm |
| --- | --- | ---: | ---: | ---: | ---: |
| `Q1 | Q2-Q9 | Q10` | `Low Volume Low vs Middle Volume High` | `+0.4969%` | `54.8%` | `2.70e-13` | `9.36e-11` |
| `Q1 | Q2-Q9 | Q10` | `Low Volume Low vs Middle Volume Low` | `+0.4771%` | `54.1%` | `8.87e-13` | `5.63e-11` |
| `Q1-Q2 | Q3-Q9 | Q10` | `Low Volume Low vs Middle Volume High` | `+0.4933%` | `54.4%` | `1.08e-13` | `3.13e-11` |
| `Q1-Q3 | Q4-Q9 | Q10` | `Low Volume Low vs Middle Volume High` | `+0.4910%` | `54.8%` | `6.68e-14` | `1.31e-11` |

#### decile profile では `Q10` の 10d mean は高いが、`Q1` も弱くない。

| Decile | Mean `Price / SMA50` | Mean `Volume SMA 5 / 20` | 10d mean | 10d median |
| --- | ---: | ---: | ---: | ---: |
| `Q1 Highest Ratio` | `1.1263` | `1.0539` | `+0.8124%` | `+0.6130%` |
| `Q2` | `1.0703` | `1.0088` | `+0.5858%` | `+0.4932%` |
| `Q3` | `1.0478` | `0.9917` | `+0.5969%` | `+0.5152%` |
| `Q10 Lowest Ratio` | `0.9131` | `1.0536` | `+0.9559%` | `+0.6781%` |

### Interpretation

この結果は、`SMA50 Q10 Low` の反発が本物の候補である一方、price-only では high-ratio 側も一定に強いことを示す。したがって `Q10` は「市場全体の戻り局面で強い low-ratio bucket」として読み、`Q1` continuation と同じ ranking rule に混ぜない方がよい。low-volume split は `Q10` 単独で最も明瞭なので、先行研究の `Q10 Low` 仮説は維持できる。

### Production Implication

production へ進めるなら、`Q10 Low` を candidate generator として残しつつ、plain `Price / SMA50` だけでなく ATR 正規化や market regime を重ねる。`Q1 | Q2-Q3 | Q4-Q10` は partition diagnostic、`Q1 | Q2-Q9 | Q10` は low-volume rebound diagnostic として分けて扱う。

### Caveats

これは fixed snapshot の observational partition search であり、walk-forward OOS ではない。36 通りの partition を比較しているため、multiple testing と境界選択 bias がある。return は日足 forward return で、手数料、slippage、turnover、portfolio capacity は未評価。bundle は `git_dirty: true` の run なので、再利用前に current runner で再現確認する。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_price_to_sma50_decile_partitions.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_price_to_sma50_decile_partitions.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-price-to-sma50-decile-partitions/20260403_153217_5d376da6`
- Tables: `results.duckdb`

## Purpose

- `SMA50 Q10 Low` bounce を decile partition 設計として読み直す。
- high / middle / low の contiguous boundary を比較し、price-only と low-volume edge を分けて判断する。
- 後続の ATR 正規化・regime conditioning・OOS 検証へ渡す partition candidate を作る。

## Scope

- Universe:
  - `TOPIX100`
- Price feature:
  - `Price / SMA50`
- Volume feature:
  - `Volume SMA 5 / 20`
- Candidate partitions:
  - `36`
- Horizons:
  - `t_plus_10` を中心に読む

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_price_to_sma50_decile_partitions.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_price_to_sma50_decile_partitions.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix100-price-to-sma50-decile-partitions/20260403_153217_5d376da6`

## Current Read

- price-only では `Q1 | Q2-Q3 | Q4-Q10` が balanced な candidate。
- low-volume edge では `Q1 | Q2-Q9 | Q10` の `Low Volume Low vs Middle` が明瞭。
- `Q10` は維持するが、plain SMA50 だけで production rule にしない。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix100_price_to_sma50_decile_partitions.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- ATR 正規化すると `Q10` の rebound edge は同じ bucket に残るか。
- `Q1` continuation と `Q10` rebound は同じ strategy family に混ぜず、別候補として扱うべきか。
- partition boundary は walk-forward OOS でも安定するか。
