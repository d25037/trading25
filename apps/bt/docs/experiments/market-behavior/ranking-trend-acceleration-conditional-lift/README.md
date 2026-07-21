# Ranking Trend Acceleration Conditional Lift

既存 Daily Ranking 候補内で、20D/60D の trend acceleration を追加する価値を検証する研究です。candidate selection、Ranking API、materialization、UI は変更しません。

## Published Readout

> [!WARNING]
> **Status: `historical_archive`; `rerun_required`.** This Market v3 evidence
> is retained only as a historical candidate. It must not drive production,
> thresholds, or Ranking decisions before a physical Market v5
> `market.duckdb` rerun with
> `stock_price_adjustment_mode=provider_adjusted_v1`, signal-date PIT
> membership, and provider-vintage/current-basis provenance.

### Decision

Historical v3 run の判定は追加導入の棄却でした。Market v5 rerun 前の current Ranking decision ではありません。

### Main Findings

historical な結果値は Historical Metrics 表に限定します。

**Historical measurement only:** the following Market v3 metrics retain their
prior provenance, but must not drive production, thresholds, or Ranking
decisions before the required Market v5 `provider_adjusted_v1` rerun.

### Interpretation

trend acceleration を独立した導入根拠としては扱わず、Market v5 rerun の比較候補としてのみ保存します。

### Production Implication

実運用 Daily Ranking は変更しません。

### Caveats

Prime 相当 universe の observation-level research です。

### Source Artifacts

historical bundle の `manifest.json`、`results.duckdb`、`summary.md` と schema-v3 publication digest を provenance として保持します。

## Historical Publication Identity

| Field | Value |
| --- | --- |
| experiment_id | `market-behavior/ranking-trend-acceleration-conditional-lift` |
| run_id | `20260719_prime_price_pit_conditional_lift_v9` |
| decision | `reject_introduction` |
| source_commit | `e33f76f1a8fecb1f8c3c731b3692c1e10dd123d4` |
| git_dirty | `false` |

## Historical Metrics

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

Historical v3 run の判定は追加導入の棄却です。上表の値は historical `results.duckdb` から publication verifier が再計算し、README と一致した provenance である。run 固有の数値は historical readout としてのみ保持する。

Historical run は `stock_data_raw` の event-time basis、signal/completion date の exact basis、next-open endpoint を記録した。これは Market v5 `provider_adjusted_v1` contract の検証済み evidence ではなく、実運用 Daily Ranking への変更でもない。
