# TOPIX100 SMA Ratio Regime Conditioning

TOPIX100 の SMA ratio ranking に same-day `TOPIX close` / `NT ratio` regime を重ね、`Q10 Low` bounce と middle spread がどの market state で読みやすいかを観察する実験です。

## Published Readout

### Decision

`nt_ratio neutral` は統計的に最も読みやすく、`topix_close strong` は平均差が大きいが sample が小さい。したがって、この研究は「regime を掛けると読める場所がある」という先行研究として残し、production rule にはせず、後続の walk-forward LightGBM / deterministic composite で regime feature の必要性を確認する。

### Why This Research Was Run

前段の `topix100-sma-ratio-rank-future-close` では、`Q10 Low vs Middle` が 10d で残り、`price_sma_5_20 + volume_sma_5_20` composite も fixed split では同方向だった。一方で平均差は薄く、地合いの影響を受ける可能性が高かったため、same-day `TOPIX close` と `NT ratio` の regime を重ねて、反発が集中する状態を探した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。available range は `2016-03-25 -> 2026-03-31`、analysis range は `2016-11-02 -> 2026-03-31`。対象は `TOPIX100`、constituents は `100`、valid dates は `2,296`。regime は signal date までに観測できる same-day market return から作り、sigma thresholds は `1.0` / `2.0`。forward return は `t_plus_1` / `t_plus_5` / `t_plus_10` を見る。

### Main Findings

#### 10d の `Q10 Low vs Middle` は `nt_ratio neutral` が一番 clean に残った。

| Regime type | Regime | Horizon | Hypothesis | Mean diff | Paired t Holm | Wilcoxon Holm |
| --- | --- | --- | --- | ---: | ---: | ---: |
| `topix_close` | strong | `t_plus_10` | `Q10 Low vs Middle High` | `+0.5740%` | `0.182955` | `0.505599` |
| `topix_close` | strong | `t_plus_10` | `Q10 Low vs Middle Low` | `+0.4778%` | `0.318638` | `0.595581` |
| `nt_ratio` | neutral | `t_plus_10` | `Q10 Low vs Middle Low` | `+0.4484%` | `0.000004` | `0.000730` |
| `topix_close` | neutral | `t_plus_10` | `Q10 Low vs Middle Low` | `+0.3818%` | `0.000195` | `0.027063` |
| `nt_ratio` | neutral | `t_plus_10` | `Q10 Low vs Middle High` | `+0.3660%` | `0.000430` | `0.004616` |

#### 平均 return は strong regime が大きいが、neutral regime の方が sample と統計のバランスが良い。

| Regime type | Regime | `Q10 Low` mean | `Middle High` mean | `Middle Low` mean | Date count |
| --- | --- | ---: | ---: | ---: | ---: |
| `topix_close` | strong | `+1.2506%` | `+0.6766%` | `+0.7729%` | `282` |
| `nt_ratio` | neutral | `+0.8410%` | `+0.4750%` | `+0.3926%` | `1,728` |
| `topix_close` | neutral | `+0.8140%` | `+0.5042%` | `+0.4322%` | `1,748` |
| `nt_ratio` | weak | `+1.4274%` | `+1.1634%` | `+1.0795%` | `281` |

### Interpretation

regime conditioning は `Q10 Low` bounce を完全に作るものではないが、どこで読むべきかをかなり整理する。`topix_close strong` は headline spread が大きい一方、sample が小さく補正後 p-value が残りにくい。`nt_ratio neutral` は spread がやや小さいが、sample が大きく、`Q10 Low vs Middle` が統計的に残る。これは前段の fixed split finding を「地合いが過度に荒れていない状態の反発」として読む方向を支持する。

### Production Implication

この研究単体では entry filter にしない。production に近づける場合は、`nt_ratio neutral` を regime feature 候補として残し、`topix_close strong` は sample-limited な upside observation として補助に留める。次段では walk-forward OOS の LightGBM / deterministic composite で、regime feature を入れたときに 5d/10d spread と positive split share が改善するかを確認する。

### Caveats

regime bucket は sample size が大きく異なり、strong/weak の headline は不安定になりやすい。これは observational study で、cost、slippage、turnover、capacity、portfolio construction は未評価。same-day market regime は実行時刻の扱いに注意が必要で、production では entry timing と観測可能性を別途固定する必要がある。bundle は `git_dirty: true` の run なので、再利用時は current runner で再現確認する。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_sma_ratio_regime_conditioning.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_sma_ratio_regime_conditioning.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-sma-ratio-regime-conditioning/20260331_173425_de1d187c`
- Tables: `results.duckdb`

## Purpose

- SMA ratio rank study で残った `Q10 Low vs Middle` を market regime ごとに読み直す。
- `topix_close` と `nt_ratio` のどちらが conditioning variable として有用かを比較する。
- 後続の model / deterministic composite に regime feature を入れる価値があるかを判断する。

## Scope

- Universe:
  - `TOPIX100`
- Regime types:
  - `topix_close`
  - `nt_ratio`
- Sigma thresholds:
  - `1.0`
  - `2.0`
- Horizons:
  - `t_plus_1`
  - `t_plus_5`
  - `t_plus_10`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_sma_ratio_regime_conditioning.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_sma_ratio_regime_conditioning.py`
  - `apps/bt/src/domains/analytics/research_bundle.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix100-sma-ratio-regime-conditioning/20260331_173425_de1d187c`

## Current Read

- `topix_close strong` は平均差が大きいが、sample が小さく統計は残りにくい。
- `nt_ratio neutral` は `Q10 Low vs Middle` が 10d で clean に残り、後続の feature candidate として最も扱いやすい。
- regime conditioning は signal を確定するというより、fixed split の bounce observation をどの地合いで読むかを整理する役割が大きい。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix100_sma_ratio_regime_conditioning.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- `nt_ratio neutral` を明示 feature にした deterministic composite は walk-forward OOS で spread が改善するか。
- `topix_close strong` の headline spread は longer sample や different threshold でも残るか。
- 後続の LightGBM は regime feature を使わずに同じ情報を price/volume ratio から近似しているのか。
