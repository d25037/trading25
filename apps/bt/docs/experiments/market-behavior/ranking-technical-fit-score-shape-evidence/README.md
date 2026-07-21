# Ranking Technical Fit Score Shape Evidence

Value Score / Long Hybrid Score の fixed-return-free candidate rings 内で、fixed return と OLS slope の nonlinear fit を比較する研究です。Technical Fit Score を Ranking へ導入せず、既存 API、materialization、UI は変更しません。

## Published Readout

> [!WARNING]
> **Status: `historical_archive`; `rerun_required`.** This Market v4 evidence
> is retained only as a historical candidate. It must not drive production,
> thresholds, or Ranking decisions before a physical Market v5
> `market.duckdb` rerun with
> `stock_price_adjustment_mode=provider_adjusted_v1`, signal-date PIT
> membership, and provider-vintage/current-basis provenance.

### Decision

Historical v4 run の判定は fixed/OLS のどちらも採用しない、でした。Market v5 rerun 前の current Ranking decision ではありません。

### Main Findings

historical な結果値は Historical Metrics 表に限定します。

**Historical measurement only:** the following Market v4 metrics retain their
prior provenance, but must not drive production, thresholds, or Ranking
decisions before the required Market v5 `provider_adjusted_v1` rerun.

### Interpretation

Technical Fit Score の導入根拠としては扱わず、Market v5 rerun の比較候補としてのみ保存します。

### Production Implication

実運用 Daily Ranking は変更しません。

### Caveats

Prime 相当 universe の observation-level research です。

### Source Artifacts

historical bundle の `manifest.json`、`results.duckdb`、`summary.md` と schema-v3 publication digest を provenance として保持します。

## Historical Publication Identity

| Field | Value |
| --- | --- |
| experiment_id | `market-behavior/ranking-technical-fit-score-shape-evidence` |
| run_id | `20260719_prime_pit_technical_fit_shape_v13` |
| decision | `neither` |
| source_commit | `e33f76f1a8fecb1f8c3c731b3692c1e10dd123d4` |
| git_dirty | `false` |

## Historical Metrics

| Metric | Value |
| --- | --- |
| fixed_core_oos_mean_lift_pct | `0.2700659415` |
| fixed_top5_mean_lift_pct | `-0.2125659412` |
| near1_fixed_minus_ols_mean_lift_pct | `0.0448338176` |
| observation_count | `429764` |
| ols_core_oos_mean_lift_pct | `0.1835736534` |
| ols_top5_mean_lift_pct | `-0.2385391574` |
| topk_complete_row_count | `12956` |
| topk_incomplete_row_count | `340` |

## Decision

Historical v4 run の判定は fixed/OLS のどちらも採用しない、です。上表の値は historical `results.duckdb` から publication verifier が再計算し、README と一致した provenance である。run 固有の数値は historical readout としてのみ保持する。

Historical run は Market v4 `local_projection_v2_event_time`、exact signal-date universe、`daily_valuation` / `stock_data_raw` basis を記録した。これは Market v5 `provider_adjusted_v1` contract の検証済み evidence ではなく、実運用 Daily Ranking への変更でもない。
