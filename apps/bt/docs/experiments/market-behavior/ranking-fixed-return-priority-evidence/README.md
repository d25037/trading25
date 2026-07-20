# Ranking Fixed Return Priority Evidence

fixed 20D/60D return を候補抽出に使わない long scaffold 内で、既存 Ranking priority の妥当性を検証する研究です。Ranking priority、badge、API、UI は変更しません。

## Published Readout

### Decision

証拠不足です。

### Main Findings

canonical な結果値は Published Metrics 表に限定します。

### Interpretation

fixed return priority の追加・置換根拠として扱いません。

### Production Implication

実運用 Daily Ranking は変更しません。

### Caveats

Prime 相当 universe の observation-level research です。

### Source Artifacts

canonical bundle の `manifest.json`、`results.duckdb`、`summary.md` と schema-v3 publication digest を検証対象とします。

## Publication Identity

| Field | Value |
| --- | --- |
| experiment_id | `market-behavior/ranking-fixed-return-priority-evidence` |
| run_id | `20260719_prime_price_pit_fixed_return_priority_v12` |
| decision | `insufficient_evidence` |
| source_commit | `e33f76f1a8fecb1f8c3c731b3692c1e10dd123d4` |
| git_dirty | `false` |

## Published Metrics

| Metric | Value |
| --- | --- |
| observation_count | `4785` |
| strict_value_observation_count | `3640` |
| topk_complete_row_count | `4263` |
| topk_incomplete_row_count | `3` |
| value_extension_observation_count | `1145` |

## Decision

証拠不足です。上表の値は canonical `results.duckdb` から publication verifier が再計算し、README と完全一致を検証します。run 固有の数値はこの表だけを canonical readout とします。

価格は `stock_data_raw` の event-time basis を使い、signal/completion date の exact basis と next-open endpoint を検証済みです。`stock_data` fallback はありません。これは Research の判定であり、実運用 Daily Ranking への変更ではありません。
