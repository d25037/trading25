# TOPIX100 SMA Ratio Rank / Future Close

TOPIX100 の price/volume SMA ratio を cross-sectional rank と composite で比較し、future return に残る単純な ratio signal を観察する実験です。先行する `price / SMA` bounce 系列を、より広い `price_sma_*` / `volume_sma_*` feature family に拡張します。

## Published Readout

### Decision

この研究は production rule ではなく、`price / SMA` bounce 系列を SMA ratio family に広げるための先行研究として扱う。`Q10 Low` の 10d 反発と `price_sma_5_20 + volume_sma_5_20` composite は候補に残すが、固定 split の validation だけでは足りないため、後続の regime conditioning と walk-forward LightGBM / deterministic composite で再検証する。

### Why This Research Was Run

`topix100-price-vs-sma-rank-future-close` と `topix100-price-vs-sma-q10-bounce` では、単独の `price / SMA` feature で continuation より低rank反発が目立った。この研究では `price_sma_5_20` / `20_80` / `50_150` と `volume_sma_5_20` / `20_80` / `50_150` を同じ ranking frame に載せ、単独 feature と composite のどちらが後続研究に値するかを見た。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。available range は `2016-03-25 -> 2026-03-31`、analysis range は `2016-11-02 -> 2026-03-31`。対象は `TOPIX100`、constituents は `100`、stock-day rows は `227,684`、ranked events は `1,366,104`、valid dates は `2,296`。特徴量は signal date までの price/volume SMA ratio から作り、forward horizon は `t_plus_1` / `t_plus_5` / `t_plus_10`。

### Main Findings

#### `Q10 Low` は 10d で middle に対して強いが、`Q10 High` との差は小さい。

| Horizon | Hypothesis | Mean diff | Paired t Holm | Wilcoxon Holm |
| --- | --- | ---: | ---: | ---: |
| `t_plus_1` | `Q10 Low vs Middle Low` | `+0.0574%` | `0.100281` | `1.000000` |
| `t_plus_5` | `Q10 Low vs Middle High` | `+0.1895%` | `0.003266` | `0.040201` |
| `t_plus_10` | `Q10 Low vs Middle Low` | `+0.3834%` | `0.000004` | `0.001331` |
| `t_plus_10` | `Q10 Low vs Middle High` | `+0.3208%` | `0.000169` | `0.006847` |
| `t_plus_10` | `Q10 Low vs Q10 High` | `+0.0670%` | `0.358056` | `0.219623` |

#### 単独 feature では `price_sma_5_20` の 5d/10d が validation でも同方向に残った。

| Feature | Horizon | Selection side | Discovery spread | Validation spread | Robustness spread |
| --- | --- | --- | ---: | ---: | ---: |
| `price_sma_5_20` | `t_plus_5` | high | `+0.0715%` | `+0.0483%` | `+0.0483%` |
| `price_sma_5_20` | `t_plus_10` | high | `+0.1103%` | `+0.2409%` | `+0.1103%` |
| `price_sma_50_150` | `t_plus_1` | high | `+0.0231%` | `+0.0018%` | `+0.0018%` |

#### Composite は短期より 5d/10d で読みやすく、`price_sma_5_20 + volume_sma_5_20` が候補に残った。

| Horizon | Composite | Feature pair | Discovery spread | Validation spread | Direction |
| --- | --- | --- | ---: | ---: | --- |
| `t_plus_1` | `rank_product` | `price_sma_50_150` + `volume_sma_5_20` | `-0.0041%` | `+0.0029%` | inconsistent |
| `t_plus_5` | `rank_mean` | `price_sma_5_20` + `volume_sma_5_20` | `+0.0767%` | `+0.0997%` | consistent |
| `t_plus_10` | `rank_mean` | `price_sma_5_20` + `volume_sma_5_20` | `+0.1286%` | `+0.1379%` | consistent |

### Interpretation

`price / SMA` bounce で見えた低rank反発は、ratio family でも `Q10 Low vs Middle` として残る。ただし `Q10 Low vs Q10 High` は弱く、extreme bucket 内の volume discrimination というより、middle より大きく売られたものの戻りを拾っている。単純 composite は 5d/10d では方向が揃うが、spread は薄く、fixed split だけで採用するには弱い。

### Production Implication

この研究は後続研究の feature shortlist として使う。`price_sma_5_20 + volume_sma_5_20` は deterministic composite 候補、`Q10 Low vs Middle` は regime conditioning の読み筋として残す。production 化するなら、固定 split ではなく walk-forward OOS と market regime で安定性を確認してからにする。

### Caveats

固定 discovery/validation split の観察であり、walk-forward OOS ではない。turnover、手数料、同時保有、capacity、execution timing は未評価。`summary.md` の headline は `+0.0032%` 表記だが、table 値は decimal return であり、ここでは percent 表記に換算して読む。bundle は `git_dirty: true` の run なので、再利用時は current runner で再現確認する。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_sma_ratio_rank_future_close.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-sma-ratio-rank-future-close/20260331_173340_de1d187c`
- Tables: `results.duckdb`

## Purpose

- `price / SMA` bounce 系列を、price/volume SMA ratio family に拡張する。
- 単独 feature と composite ranking のどちらが後続研究に値するかを切り分ける。
- `t_plus_1` / `t_plus_5` / `t_plus_10` の horizon ごとに、fixed split validation で方向が残るかを見る。

## Scope

- Universe:
  - `TOPIX100`
- Features:
  - `price_sma_5_20`
  - `price_sma_20_80`
  - `price_sma_50_150`
  - `volume_sma_5_20`
  - `volume_sma_20_80`
  - `volume_sma_50_150`
- Horizons:
  - `t_plus_1`
  - `t_plus_5`
  - `t_plus_10`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_sma_ratio_rank_future_close.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py`
  - `apps/bt/src/domains/analytics/research_bundle.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix100-sma-ratio-rank-future-close/20260331_173340_de1d187c`

## Current Read

- `Q10 Low vs Middle` は 10d で最も強く、低rank反発の観察は ratio family でも残る。
- `Q10 Low vs Q10 High` は弱く、volume split の bucket 内判別より、middle との差を読む方が自然。
- `price_sma_5_20 + volume_sma_5_20` の simple composite は 5d/10d で同方向に残るが、spread は薄い。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix100_sma_ratio_rank_future_close.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- `Q10 Low vs Middle` は market regime を重ねると sample と spread のバランスが改善するか。
- `price_sma_5_20 + volume_sma_5_20` は walk-forward OOS でも baseline として残るか。
- 後続の LightGBM が見つける長期 feature importance と、この deterministic composite の差はどこにあるか。
