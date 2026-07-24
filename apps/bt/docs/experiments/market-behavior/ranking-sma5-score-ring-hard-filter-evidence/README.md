# Ranking SMA5 Score-Ring Hard-Filter Evidence

`value_composite_equal_score` と `long_hybrid_leadership_score` の frozen
score ring 内で、SMA5 state を entry hard filter / exit trigger にしたときの
OOS portfolio evidence を検証する。

## Published Readout

### Decision

Run: `20260724_prime_v5_sma5_score_ring_hard_filter_v1`

- Entry family: `insufficient_evidence`
- Exit family: `insufficient_evidence`
- Combined family: `not_evaluated`
- `production_candidate`: なし

Entry / exit のどの単独 rule も統計ゲートと運用ゲートの両方を通過しなかった。
したがって combined rule は confirmatory 評価せず、strategy / API / UI は変更しない。

### Why This Research Was Run

先行研究では、強い long scaffold の内側で weak SMA5 state、ATR20 正規化の
上方乖離、3日連続の `Close < SMA5` に条件付き効果が見られた。本研究では
それらを outcome 確認前に固定した entry / exit rule として、Market v5 の
position-state / equal-weight portfolio lens で再検証した。

### Data Scope / PIT Assumptions

- Effective period: `2018-01-04..2026-07-21`
- Prime exact-date membership (`0101`, `0111`)
- Market schema v5 / `provider_adjusted_v1`
- Primary: `core_high_high`, 60 sessions
- OOS: `2022-01-01..2024-12-31`、holdout: `2025-01-01..2026-07-21`
- Paired moving-block bootstrap: block 20、2,000 resamples、seed `20260724`
- Frozen registry: 150 variants / 450 cost-level executions

`value_composite_equal_score` に必要な finite positive `forward_per` と `pbr` が
同日 valuation に揃う code-date だけを研究母集団にした。元の Prime
3,621,883 code-date のうち 3,402,913（`93.95%`）が eligible で、218,970
code-date は outcome attach 前に除外した。

### Main Findings

#### 結論: weak-state avoidance も chase avoidance も entry hard filter には採用しない

10bps 控除後の primary OOS では、すべての entry rule が baseline を下回った。
`E3_avoid_atr20_chase` は OOS でほぼ中立だったが、holdout では悪化し、
bootstrap CI もゼロを跨いだ。

| Rule | OOS trades / dates | OOS delta | 95% CI | Holm p | IR lift | Holdout delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `E1_close_above_sma5` | `1,268 / 303` | `-2.377 bps` | `[-5.046, +0.053]` | `0.231884` | `-0.337` | `-1.712 bps` |
| `E2_count_ge_2` | `1,420 / 361` | `-1.616 bps` | `[-3.700, +0.487]` | `0.413793` | `-0.235` | `-1.111 bps` |
| `E3_avoid_atr20_chase` | `1,516 / 380` | `-0.130 bps` | `[-1.617, +1.510]` | `0.883558` | `+0.023` | `-2.815 bps` |

`E4_count_ge_2_and_avoid_chase` は E2 / E3 が事前ゲートを通らなかったため
confirmatory rule ではなく `not_evaluated` とした。holdout を見た後の threshold
変更や追加 rule 探索は行っていない。

#### 結論: exit は左尾を軽くする場合があるが、統計ゲートを通らない

`X2` / `X3` / `X4` は OOS の tail と10/20bps cost sensitivityでは改善した。
ただし全ruleで bootstrap CI がゼロを跨ぎ、Holm補正後 p は `0.05` 未満に
ならなかった。`X4` は OOS IR lift が `+0.149` と要求値 `+0.15` にも届かず、
holdout delta は負へ反転した。

| Rule | OOS trades / dates | OOS delta | 95% CI | Holm p | IR lift | Tail improvement | Holdout delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `X1_close_below_sma5` | `1,477 / 334` | `-3.837 bps` | `[-13.982, +5.032]` | `0.917541` | `-0.688` | `-17.83%` | `+9.596 bps` |
| `X2_count_le_1` | `1,477 / 334` | `+2.487 bps` | `[-3.203, +7.639]` | `0.917541` | `-0.043` | `+16.10%` | `+0.062 bps` |
| `X3_below_streak_ge_3` | `1,477 / 334` | `+3.520 bps` | `[-3.096, +10.045]` | `0.917541` | `+0.032` | `+13.69%` | `+5.257 bps` |
| `X4_atr20_below_le_neg1` | `1,477 / 334` | `+3.639 bps` | `[-0.954, +8.277]` | `0.511744` | `+0.149` | `+22.02%` | `-0.741 bps` |

#### 結論: combined family は評価対象に上げない

| Family | Pre-holdout pass | Final decision | Production candidate |
| --- | --- | --- | --- |
| Entry | なし | `insufficient_evidence` | なし |
| Exit | なし | `insufficient_evidence` | なし |
| Combined | prerequisite 不成立 | `not_evaluated` | なし |

### Interpretation

Weak-state avoidance（`E1` / `E2`）は、強い score ring 内の一時的な押し目まで
捨て、OOS と holdout の両方で平均差を悪化させた。Chase avoidance（`E3`）は
OOS ではほぼ中立だが holdout で悪化し、hard ban の根拠にはならない。

Exit 側の `X2` / `X3` / `X4` は左尾を軽くする一方、IR lift が弱い。悪化局面を
早く切る効果と同時に、その後回復する winner も早期に切るためと解釈できる。
この winner truncation を上回る再現性は bootstrap / holdout で確認できなかった。

### Production Implication

統計ゲートと運用ゲートの両方を通過した rule はない。SMA5 hard filter / cash
exit を production strategy、Daily Ranking UI、API に自動反映しない。`X2` /
`X3` / `X4` の tail diagnostic は研究上の観察として残すが、別の事前固定研究
なしに threshold や rule を変更しない。

### Caveats

- Signal と fill が同じ Close の `close_proxy_same_session` であり、optimistic。
- Portfolio は active positions の等ウェイト近似で、資金制約・sector cap・
  position sizing を再現しない。
- Round-trip 10bps を base、20bps を stress とした。実slippage、capacity、
  market impact は未反映。
- 2026年は `2026-07-21` までの incomplete year。
- Value input が揃わない 218,970 code-date は研究対象外。うち8,473 code-date
  では shared validator と pre-disclosure / statementless valuation lineage の
  契約不整合も確認しており、本研究では unusable value input として局所除外した。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_sma5_score_ring_hard_filter_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py`, `apps/bt/tests/unit/scripts/test_run_ranking_sma5_score_ring_hard_filter_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-score-ring-hard-filter-evidence/20260724_prime_v5_sma5_score_ring_hard_filter_v1/`
- Bundle files: `manifest.json`, `results.duckdb`, `summary.md`
- Source commit: `92e6e765d578d245eb90483e2fa03e9143de85df`
- Results tables: `rule_registry_df`, `coverage_diagnostics_df`,
  `trade_ledger_df`, `portfolio_daily_df`, `entry_rule_evidence_df`,
  `exit_rule_evidence_df`, `combined_rule_evidence_df`,
  `annual_stability_df`, `bootstrap_effect_ci_df`, `cost_sensitivity_df`,
  `decision_gate_df`, `observation_sample_df`
