# Ranking Trend Acceleration Conditional Lift

既存 Daily Ranking 候補内で、20D/60D の trend acceleration を追加する価値を検証する研究です。candidate selection、Ranking API、materialization、UI は変更しません。

## Published Readout

### Decision

追加導入を棄却します。

### Main Findings

canonical な結果値は Published Metrics 表に限定します。

### Interpretation

trend acceleration を独立した導入根拠として扱いません。

### Production Implication

実運用 Daily Ranking は変更しません。

### Caveats

Prime 相当 universe の observation-level research です。

### Source Artifacts

canonical bundle の `manifest.json`、`results.duckdb`、`summary.md` と schema-v3 publication digest を検証対象とします。

## Publication Identity

| Field | Value |
| --- | --- |
| experiment_id | `market-behavior/ranking-trend-acceleration-conditional-lift` |
| run_id | `20260719_prime_price_pit_conditional_lift_v9` |
| decision | `reject_introduction` |
| source_commit | `e33f76f1a8fecb1f8c3c731b3692c1e10dd123d4` |
| git_dirty | `false` |

## Published Metrics

| Metric | Value |
| --- | --- |
| binary_gate_pass_count | `0` |
| binary_gate_total | `7` |
| continuous_gate_pass_count | `1` |
| continuous_gate_total | `7` |
| observation_count | `182773` |
| topk_complete_row_count | `23363` |
| topk_incomplete_row_count | `220` |

## Decision

追加導入を棄却します。上表の値は canonical `results.duckdb` から publication verifier が再計算し、README と完全一致を検証します。run 固有の数値はこの表だけを canonical readout とします。

価格は `stock_data_raw` の event-time basis を使い、signal/completion date の exact basis と next-open endpoint を検証済みです。`stock_data` fallback はありません。これは Research の判定であり、実運用 Daily Ranking への変更ではありません。
