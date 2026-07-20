# Ranking Technical Fit Score Shape Evidence

Value Score / Long Hybrid Score の fixed-return-free candidate rings 内で、fixed return と OLS slope の nonlinear fit を比較する研究です。Technical Fit Score を Ranking へ導入せず、既存 API、materialization、UI は変更しません。

## Published Readout

### Decision

fixed/OLS のどちらも採用しません。

### Main Findings

canonical な結果値は Published Metrics 表に限定します。

### Interpretation

Technical Fit Score の導入根拠として扱いません。

### Production Implication

実運用 Daily Ranking は変更しません。

### Caveats

Prime 相当 universe の observation-level research です。

### Source Artifacts

canonical bundle の `manifest.json`、`results.duckdb`、`summary.md` と schema-v3 publication digest を検証対象とします。

## Publication Identity

| Field | Value |
| --- | --- |
| experiment_id | `market-behavior/ranking-technical-fit-score-shape-evidence` |
| run_id | `20260719_prime_pit_technical_fit_shape_v13` |
| decision | `neither` |
| source_commit | `e33f76f1a8fecb1f8c3c731b3692c1e10dd123d4` |
| git_dirty | `false` |

## Published Metrics

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

fixed/OLS のどちらも採用しません。上表の値は canonical `results.duckdb` から publication verifier が再計算し、README と完全一致を検証します。run 固有の数値はこの表だけを canonical readout とします。

Market v4 `local_projection_v2_event_time`、exact signal-date universe、`daily_valuation` / `stock_data_raw` basis を検証済みです。service-local recomputation、basis fallback、`stock_data` fallback はありません。これは Research の判定であり、実運用 Daily Ranking への変更ではありません。
