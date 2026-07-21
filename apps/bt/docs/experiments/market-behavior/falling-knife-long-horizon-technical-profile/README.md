# Falling Knife Long-Horizon Technical Profile

> Historical pre-v4 readout. The schema v3 parent bundle and recorded counts
> below are provenance only, not current production evidence. Any rerun or
> adoption decision must use Market schema v5 / `provider_adjusted_v1` with
> exact provider-window/current-basis lineage and
> publish a new parent and child readout.

## Published Readout

### Decision

annual value research で強かった `rebound_from_252d_low_pct` は、falling knife ではそのまま hard positive feature にしない。PIT master v3 の falling-knife events では、非Growthを優先する既存方針は維持される。一方で、長期テクニカル単体の候補としては `range_position_252d` の低〜中位、または `drawdown_from_252d_high_pct` の深い側を「平均リターンを取りに行くが左尾が残る feature」として分けて見るのが自然。Standard では `rebound_from_252d_low_pct` high bucket が mean を押し上げるが severe loss も悪化するため、annual value の反転進捗 feature をそのまま falling knife の risk filter にするのは危ない。

### Why This Research Was Run

falling-knife 系の既存 readout では、急落イベントは平均ではプラスでも左尾が問題であり、Growth / low quality / Overvalued が bad-tail に偏ることが分かっていた。一方、annual value research では Standard 市場で `rebound_from_252d_low_pct`、`return_252d_pct`、1年 range 位置が銘柄選択補助として効いていた。そこで、falling knife の signal date close 時点で同じ長期テクニカル context を付与し、Standard の「1年安値から戻り始めた銘柄」が falling knife でも良いのか、それとも急落局面では別の risk profile になるのかを確認した。

### Data Scope / PIT Assumptions

入力は `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260506_falling_knife_reversal_v3_pit_master` の event bundle。分析期間は `2022-05-02 -> 2026-04-30`、horizon は `20` sessions、baseline events は `65,804`。feature は `market.duckdb` v3 の `stock_data` から各 event の `signal_date` close までで計算し、post-signal price は使わない。`return_252d_pct`、`rebound_from_252d_low_pct`、`drawdown_from_252d_high_pct`、`range_position_252d`、`price_to_sma250`、`sma250_slope_20d_pct` を付与した。252d range feature の coverage は `63,458 / 65,804` events、`96.43%`。250本未満の履歴は削除せず、`history_class` に分けて残した。

### Main Findings

#### 結論: 非Growth優先は長期テクニカルを足しても変わらない。

| Market | Events | Mean | Median | Non-rebound | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: |
| Prime | `26,324` | `2.26%` | `1.41%` | `40.67%` | `5.87%` |
| Standard | `25,755` | `2.07%` | `0.49%` | `46.22%` | `6.42%` |
| Growth | `13,725` | `1.49%` | `-0.24%` | `51.80%` | `16.54%` |

#### 結論: Standard の `rebound_from_252d_low_pct` high は平均を上げるが、左尾も悪化する。

| Standard feature | Low bucket mean | High bucket mean | High-low mean | Low bucket severe | High bucket severe |
| --- | ---: | ---: | ---: | ---: | ---: |
| `rebound_from_252d_low_pct` | `2.16%` | `3.24%` | `+1.09pt` | `4.58%` | `14.38%` |
| `return_252d_pct` | `3.95%` | `1.50%` | `-2.45pt` | `8.50%` | `12.42%` |
| `range_position_252d` | `3.93%` | `1.65%` | `-2.28pt` | `6.53%` | `7.56%` |
| `drawdown_from_252d_high_pct` | `3.73%` | `1.10%` | `-2.63pt` | `13.10%` | `1.53%` |

#### 結論: pooled rule では非Growthが最も素直で、長期テクニカル単独は補助に留まる。

| Rule | Kept | Kept fraction | Kept mean | Kept severe | Severe reduction | Mean cost |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `keep_non_growth` | `52,079` | `79.14%` | `2.17%` | `6.14%` | `+2.17pt` | `-0.14pt` |
| `keep_standard` | `25,755` | `39.14%` | `2.07%` | `6.42%` | `+1.89pt` | `-0.04pt` |
| `exclude_deep_252d_drawdown` | `57,434` | `87.28%` | `1.92%` | `6.80%` | `+1.51pt` | `+0.10pt` |
| `keep_low_to_mid_range_position` | `31,730` | `48.22%` | `2.40%` | `7.69%` | `+0.62pt` | `-0.37pt` |
| `keep_rebounded_from_low` | `31,729` | `48.22%` | `2.16%` | `9.69%` | `-1.37pt` | `-0.14pt` |

#### 結論: `range_position_252d` は「高いほど良い」ではなく、低〜中位の方が読みやすい。

| Segment | Events | Mean | Median | Non-rebound | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: |
| Standard range low | `13,174` | `2.58%` | `0.45%` | `46.75%` | `5.81%` |
| Standard range high | `12,581` | `1.54%` | `0.54%` | `45.66%` | `7.06%` |
| Growth range low | `9,261` | `1.71%` | `0.00%` | `51.14%` | `15.86%` |
| Growth range high | `4,464` | `1.03%` | `-0.63%` | `53.16%` | `17.94%` |

### Interpretation

falling knife は annual value とは regime が違う。annual value の Standard では `rebound_from_252d_low_pct` が「反転進捗」として有効だったが、falling knife では rebound-high bucket が平均を上げても severe loss も大きくなる。これは「1年安値から戻っている銘柄が良い」という単純な話ではなく、戻った後に再急落している銘柄には右尾と左尾が同居している、という読みになる。

一方、`range_position_252d` の低〜中位は Standard / Growth の両方で range-high より素直に見える。特に pooled rule の `keep_low_to_mid_range_position` は mean を `2.02% -> 2.40%` に上げ、severe loss を `8.31% -> 7.69%` に下げる。ただし非Growth rule より severe loss reduction は弱いので、第一条件ではなく補助 diagnostic として扱う。

### Production Implication

次の検証では、長期テクニカルを単独 hard filter にせず、非Growth / quality / valuation の risk gate に重ねる。優先候補は `range_position_252d` の低〜中位、または `rebound_from_252d_low_pct` high を「右尾も左尾も増える bucket」として sizing / exit 側で扱う設計。Standard 専用に使うなら、`rebound_from_252d_low_pct` high は alpha 加点ではなく、severe loss 悪化を同時に持つ feature として portfolio lens で確認する。

### Caveats

この分析は event-level の 20営業日 outcome であり、同時保有、約定コスト、position sizing、capacity は未反映。bucket は market 内 quantile で、live rule にするには train-only calibration が必要。`drawdown_from_252d_high_pct` の deep side は平均が良くても severe loss が悪い bucket があり、平均だけで採用してはいけない。manifest は `git_dirty: true` を示す。

### Source Artifacts

- Output bundle: `/tmp/trading25-research/market-behavior/falling-knife-long-horizon-technical-profile/20260506_falling_knife_long_horizon_technical_v3_pit_master`
- Summary markdown: `/tmp/trading25-research/market-behavior/falling-knife-long-horizon-technical-profile/20260506_falling_knife_long_horizon_technical_v3_pit_master/summary.md`
- Results DB: `/tmp/trading25-research/market-behavior/falling-knife-long-horizon-technical-profile/20260506_falling_knife_long_horizon_technical_v3_pit_master/results.duckdb`
- Manifest: `/tmp/trading25-research/market-behavior/falling-knife-long-horizon-technical-profile/20260506_falling_knife_long_horizon_technical_v3_pit_master/manifest.json`
- Input bundle: `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260506_falling_knife_reversal_v3_pit_master`

## Purpose

`falling-knife-reversal-study` の event bundle に、signal date close 時点の長期テクニカル context を付与し、annual value research で有効だった `return_252d_pct` / `rebound_from_252d_low_pct` / `range_position_252d` が falling knife でも使えるかを検証する。

## Feature Construction

- `return_252d_pct`: signal date close / 252 sessions prior close - 1
- `rebound_from_252d_low_pct`: signal date close / 252d low - 1
- `drawdown_from_252d_high_pct`: signal date close / 252d high - 1
- `range_position_252d`: `(close - 252d low) / (252d high - 252d low)`
- `price_to_sma250`: signal date close / SMA250
- `sma250_slope_20d_pct`: SMA250 / SMA250 20 sessions ago - 1

Feature lookup は `signal_date` 以下の最新 price row を使う。falling knife の entry は翌営業日 open なので、signal date close は entry 前に観測済みとして扱う。

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_long_horizon_technical_profile.py \
  --input-bundle /tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260506_falling_knife_reversal_v3_pit_master
```

Useful options:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_long_horizon_technical_profile.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle \
  --horizon-days 20 \
  --bucket-count 5 \
  --severe-loss-threshold -0.10
```

## Tables

- `enriched_event_df`: falling-knife event plus signal-date long-horizon technical features.
- `technical_feature_summary_df`: feature coverage and distribution.
- `technical_bucket_summary_df`: feature bucket x market return/tail summary.
- `technical_rule_summary_df`: candidate kept/removed rule comparison.
- `interaction_summary_df`: market x range/rebound/history interactions.
- `feature_rank_df`: high-low / best-worst feature ranking by market.
