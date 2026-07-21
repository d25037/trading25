# Ranking Fixed Return Priority Evidence

fixed 20D/60D return を候補抽出に使わない long scaffold 内で、既存 Ranking priority の妥当性を検証する研究です。Ranking priority、badge、API、UI は変更しません。

## Published Readout

> [!WARNING]
> **Status: `historical_archive`; `rerun_required`.** This Market v3 evidence
> is retained only as a historical candidate. It must not drive production,
> thresholds, or Ranking decisions before a physical Market v5
> `market.duckdb` rerun with
> `stock_price_adjustment_mode=provider_adjusted_v1`, signal-date PIT
> membership, and provider-vintage/current-basis provenance.

### Decision

Historical v3 run の判定は証拠不足でした。Market v5 rerun 前の current Ranking decision ではありません。

### Main Findings

historical な結果値は Historical Metrics 表に限定します。

**Historical measurement only:** the following Market v3 metrics retain their
prior provenance, but must not drive production, thresholds, or Ranking
decisions before the required Market v5 `provider_adjusted_v1` rerun.

### Interpretation

fixed return priority の追加・置換根拠としては扱わず、Market v5 rerun の比較候補としてのみ保存します。

### Production Implication

実運用 Daily Ranking は変更しません。

### Caveats

Prime 相当 universe の observation-level research です。

### Source Artifacts

historical bundle の `manifest.json`、`results.duckdb`、`summary.md` と schema-v3 publication digest を provenance として保持します。

## Historical Publication Identity

| Field | Value |
| --- | --- |
| experiment_id | `market-behavior/ranking-fixed-return-priority-evidence` |
| run_id | `20260719_prime_price_pit_fixed_return_priority_v12` |
| decision | `insufficient_evidence` |
| source_commit | `e33f76f1a8fecb1f8c3c731b3692c1e10dd123d4` |
| git_dirty | `false` |

## Historical Metrics

| Metric | Value |
| --- | --- |
| observation_count | `4785` |
| strict_value_observation_count | `3640` |
| topk_complete_row_count | `4263` |
| topk_incomplete_row_count | `3` |
| value_extension_observation_count | `1145` |

## Decision

Historical v3 run の判定は証拠不足です。上表の値は historical `results.duckdb` から publication verifier が再計算し、README と一致した provenance である。run 固有の数値は historical readout としてのみ保持する。

Historical run は `stock_data_raw` の event-time basis、signal/completion date の exact basis、next-open endpoint を記録した。これは Market v5 `provider_adjusted_v1` contract の検証済み evidence ではなく、実運用 Daily Ranking への変更でもない。
