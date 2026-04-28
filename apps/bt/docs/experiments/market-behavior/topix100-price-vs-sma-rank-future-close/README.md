# TOPIX100 Price vs SMA Rank / Future Close

TOPIX100 の `price / SMA20|50|100` を単独特徴として使い、decile と price/volume split で将来 `close` / `return` を観察する実験です。runner-first 導線では `volume_sma_5_20 / 20_80 / 50_150` をまとめて保存し、notebook は bundle viewer として使います。

## Published Readout

### Decision

この研究は continuation signal ではなく、`SMA50` / `SMA100` を大きく下回った `Q10` 側の mean-reversion 仮説へ進むための先行研究として扱う。`Q1` high-rank を買う発想は優先度を下げ、後続の `topix100-price-vs-sma-q10-bounce` と regime conditioning で `Q10 Low` を主役にする。

### Why This Research Was Run

既存の `price vs SMA20` notebook を、`SMA20 / SMA50 / SMA100` の feature family として比較可能にし、`Q1` continuation と `Q10` rebound のどちらが研究対象として自然かを切り分けるために実行した。volume split を重ねることで、単なる price deviation ではなく turnover regime が反発候補を分けるかも確認した。

### Data Scope / PIT Assumptions

入力は `~/.local/share/trading25/market-timeseries/market.duckdb` の snapshot。分析範囲は `2016-08-19 -> 2026-03-27`、valid dates は `2,344`、warmup/filter 後の stock-day rows は `232,484`。対象は `TOPIX100` で、特徴量は signal date までに観測できる `price_vs_sma_20_gap` / `price_vs_sma_50_gap` / `price_vs_sma_100_gap` と `volume_sma_5_20` / `20_80` / `50_150`。forward horizon は `t_plus_1` / `t_plus_5` / `t_plus_10`。

### Main Findings

#### `Q1` continuation は弱く、`SMA50` / `SMA100` はむしろ `Q10` 側が強い。

| Feature | Horizon | `Q1` mean | `Q10` mean | `Q1-Q10` |
| --- | --- | ---: | ---: | ---: |
| `price_vs_sma_20_gap` | `t_plus_10` | `+0.7737%` | `+0.7631%` | `+0.0107%` |
| `price_vs_sma_50_gap` | `t_plus_5` | `+0.3938%` | `+0.4809%` | `-0.0871%` |
| `price_vs_sma_50_gap` | `t_plus_10` | `+0.8206%` | `+0.9487%` | `-0.1282%` |
| `price_vs_sma_100_gap` | `t_plus_10` | `+0.8731%` | `+0.9046%` | `-0.0315%` |

#### `Q10 Low vs Middle Low` は `SMA50` と `SMA100` の 5d/10d で反発候補として読める。

| Feature | Horizon | Mean diff | Paired t Holm | Wilcoxon Holm |
| --- | --- | ---: | ---: | ---: |
| `price_vs_sma_50_gap` | `t_plus_5` | `+0.2309%` | `0.000393` | `0.0630` |
| `price_vs_sma_50_gap` | `t_plus_10` | `+0.4206%` | `0.000000556` | `0.001220` |
| `price_vs_sma_100_gap` | `t_plus_5` | `+0.1604%` | `0.0479` | `0.6742` |
| `price_vs_sma_100_gap` | `t_plus_10` | `+0.3047%` | `0.000495` | `0.007500` |

### Interpretation

この結果は `price / SMA` が「強いものをさらに買う」signal ではないことを示す。特に `SMA50` / `SMA100` では decile 構造自体は有意だが、方向は高rank continuation ではなく、長期 SMA を下回った低rank側の反発に寄っている。`SMA20` は方向が曖昧で、主軸にするには弱い。

### Production Implication

この研究単体では production rule にしない。後続研究では `Q10 Low` を切り出し、`SMA50` を第一候補、`SMA100` を第二候補として、volume split と market regime で rebound の安定性を確認する。実装面では `Q1` continuation のランキング導線より、oversold bounce の候補生成・除外条件として扱う。

### Caveats

これは `TOPIX100` の日足 close-to-close forward return を使った観察研究で、手数料、スリッページ、同時保有、capacity は見ていない。`Q1-Q10` の pairwise は Holm 補正後に強く残っておらず、decile 全体構造と extreme spread を混同しない。`git_dirty: true` の bundle に基づくため、再現時は runner と current market DB で再確認する。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_price_vs_sma_rank_future_close.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_price_vs_sma_rank_future_close.py`
- Baseline: `apps/bt/docs/experiments/market-behavior/topix100-price-vs-sma-rank-future-close/baseline-2026-03-31.md`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-price-vs-sma-rank-future-close/20260331_173014_de1d187c`
- Tables: `results.duckdb`

## Purpose

- 既存の `price vs SMA20` notebook を、`SMA20 / SMA50 / SMA100` の feature family として比較可能にする。
- `Q1` を「SMA を大きく上回る銘柄群」、`Q10` を「SMA を大きく下回る銘柄群」として、将来 `t_plus_1 / t_plus_5 / t_plus_10` の decile 構造を確認する。
- `volume_sma_5_20 / 20_80 / 50_150` high/low split を掛け合わせて、price deviation の極端群に turnover regime が追加の説明力を持つかを見る。

## Scope

- Universe:
  - `TOPIX100`
- Price features:
  - `price_vs_sma_20_gap`
  - `price_vs_sma_50_gap`
  - `price_vs_sma_100_gap`
- Volume features:
  - `volume_sma_5_20`
  - `volume_sma_20_80`
  - `volume_sma_50_150`
- Horizons:
  - `t_plus_1`
  - `t_plus_5`
  - `t_plus_10`
- Outputs:
  - decile summary / significance
  - price bucket summary / significance
  - price x volume split summary / significance

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_price_vs_sma_rank_future_close.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_price_vs_sma_rank_future_close.py`
  - `apps/bt/src/domains/analytics/research_bundle.py`
  - `apps/bt/src/domains/analytics/topix100_price_vs_sma20_rank_future_close.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma_rank_future_close.py`
  - `apps/bt/tests/unit/domains/analytics/test_research_bundle.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma20_rank_future_close.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix100_price_vs_sma20_regime_conditioning.py`
  - `apps/bt/tests/unit/domains/analytics/test_topix100_vi_change_regime_conditioning.py`
  - `apps/bt/tests/unit/scripts/test_run_topix100_price_vs_sma_rank_future_close.py`

## Latest Baseline

- [baseline-2026-03-31.md](./baseline-2026-03-31.md)

## Current Read

- `price_vs_sma_20_gap` は全体として弱く、`Q1-Q10` spread は `t_plus_1=-0.0334%`、`t_plus_5=-0.0227%`、`t_plus_10=+0.0107%` でした。`t_plus_10` だけ decile 全体の Friedman 検定は有意ですが、extreme decile 差は小さいです。
- `price_vs_sma_50_gap` と `price_vs_sma_100_gap` は continuation ではなく mean-reversion 寄りです。`Q1-Q10` spread は `t_plus_5/t_plus_10` で負に傾き、`Q10` が `Q1` を上回ります。
- しかも `Q10 Low vs Middle Low` は `SMA50` と `SMA100` の `t_plus_5/t_plus_10` でかなり強いです。長期 SMA を大きく下回り、かつ volume 条件が low の銘柄群は bounce 候補として観察価値があります。
- したがって、この feature family は「高 rank を買う continuation signal」としては弱い一方、「低 rank の反発」を見る mean-reversion research の入口としては有望です。

## Reproduction

Runner-first の canonical path:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix100_price_vs_sma_rank_future_close.py
```

この command は `~/.local/share/trading25/research/market-behavior/topix100-price-vs-sma-rank-future-close/<run_id>/`
へ `manifest.json + results.duckdb + summary.md` を保存します。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

notebook は latest bundle を既定で読みます。新規 run は notebook ではなく runner script から実行します。

## Next Questions

- `price_vs_sma_50_gap` / `price_vs_sma_100_gap` の `Q10` 側を主役にして、bounce 専用 notebook へ切り出した方が読みやすいか。
- `volume_sma_20_80` ではなく、より長期の turnover proxy を重ねると `Q10` 反発の選別が改善するか。
- continuation ではなく mean-reversion 仮説に切り替えるなら、TOPIX close / VI change regime との相性はどの bucket で強いか。
