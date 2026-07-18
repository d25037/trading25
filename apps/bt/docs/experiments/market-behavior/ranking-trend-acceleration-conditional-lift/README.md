# Ranking Trend Acceleration Conditional Lift

既存の Daily Ranking long candidate の中で、rolling log-price OLS による短期・長期 trend acceleration が、forward TOPIX-excess return の追加的な優先順位情報になるかを検証する。候補選定は既存の valuation、liquidity、ATR、momentum のみで固定し、OLS slope は候補抽出には使わない。

対象は signal date 時点の Prime 相当 universe のみである。`0101`（市場再編前の東証一部）と `0111`（Prime）を `stock_master_daily` の exact-date membership で解決し、Standard / Growth は対象外とする。

## Published Readout

### Decision

Decision: 実行結果のpublication待ち

Task 4 で durable bundle の全 decision-gate input を確認した後、この placeholder を実際の判断へ置き換える。現時点では production Ranking の fixed `20D/60D`、候補抽出、API、UI は変更しない。

### Main Findings

#### 結論

未実行のため、数値的な結論はまだない。runner は `core_long`、`momentum_value`、`neutral_rerating_good`、`earnings_priority`、`aggressive_rerating` の named group と、重複を独立成功として数えない mutually exclusive slice を別々に出力する。

| 確認項目 | 実行後に確認する根拠 |
| --- | --- |
| 連続列 | candidate/date 内の margin rank、20D IC、top-minus-bottom lift と bootstrap CI |
| binary badge | triple-minus-control paired-date 20D lift、win rate、segment direction、tail |
| 運用順序 | K=5 / K=10 の priority lift、turnover、rank stability |

### Interpretation

この研究は after-close の observation-level evidence であり、portfolio performance や執行可能性を示すものではない。2024年以降は仮説の起点を再現する期間であり、historical replication（2017-2023）と区別して読む。

### Production Implication

連続列、badge、または既存 fixed endpoint semantics の変更は、この実験だけでは導入しない。decision gate を満たした場合も、production API/materialization/UI の追加は別途承認済み設計を必要とする。fixed `20D/60D` の置換は本研究の対象外である。

### Caveats

- OLS feature は signal date の close までを含むため、pre-open use は未検証である。
- candidate group の overlap は replication count を水増ししない。nested `earnings_priority` は独立 family として数えない。
- incomplete forward window は除外する。結果は Prime 相当 universe と設定した horizons に限定される。
- 取引費用、capacity、portfolio construction、execution timing は含まれない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_trend_acceleration_conditional_lift.py`
- Module: `apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py`
- Bundle root: `~/.local/share/trading25/research/market-behavior/ranking-trend-acceleration-conditional-lift/`
- Result tables: `coverage_diagnostics_df`, `candidate_registry_df`, `conditional_binary_lift_df`, `fixed_incremental_2x2_df`, `continuous_rank_lift_df`, `topk_priority_lift_df`, `segment_stability_df`, `bootstrap_effect_ci_df`, `decision_gate_df`, `observation_sample_df`
