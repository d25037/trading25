# Daily Ranking SMA5 Score-Ring Hard-Filter Evidence Design

## Objective

Market v5 の Daily Ranking Research Base を使い、`value_composite_equal_score`
と `long_hybrid_leadership_score` で事前固定した long 候補集合の内側で、
SMA5 と終値の位置関係が entry hard filter または exit trigger として
統計的・運用的に有効かを検証する。

最近の `20D/60D` Technical Fit Score 研究は、Market v4 の historical evidence
では fixed return / OLS slope のどちらも adoption gate を通過しなかった。
一方、先行 SMA5 研究では SMA5 単独の候補生成力は弱いものの、強い long
scaffold 内で `sma5_above_count_5d=0/1`、3日連続の `Close < SMA5`、
ATR20 正規化乖離に条件付きの効果が見られた。本研究はその差を Market v5
`provider_adjusted_v1` 上の position-state / portfolio lens で再検証する。

## Scope

- 対象は signal-date 時点で Prime (`0101`, `0111`) に属する日本株とする。
- long side のみを扱う。
- Value / Long Hybrid の2スコアは候補集合の定義にのみ使い、SMA5 を第三の
  ranking score にはしない。
- API、Daily Ranking UI、strategy signal registry、production strategy は
  初期研究では変更しない。
- outcome を確認した後の閾値変更、追加ルール探索、short-side 検証は別研究
  とする。

## Data and Provenance

- physical `market.duckdb` schema v5 を必須とする。
- `sync_metadata.stock_price_adjustment_mode=provider_adjusted_v1` を必須とする。
- price は Market v5 の provider-adjusted consumer SoT を使い、signal-date
  universe は `stock_master_daily` から exact-date で解決する。
- Value、Long Hybrid、SMA5、ATR20 は
  `apps/bt/src/domains/analytics/daily_ranking_research_base.py` と
  `daily_ranking_feature_builders.py` の generation-bound builder を再利用する。
- service-local recomputation、latest membership fallback、旧 Market v4 basis
  fallback は行わない。
- 2スコアまたは必要な technical feature が欠ける行は補完せず、coverage
  diagnostics に記録する。

## Candidate Rings

候補集合は outcome attach 前に次の nested ring として固定する。

| Ring | Predicate | Role |
| --- | --- | --- |
| `core_high_high` | Value `>= 0.8` AND Long Hybrid `>= 0.8` | Primary |
| `near_high_high_1` | Value `>= 0.7` AND Long Hybrid `>= 0.7` | Robustness |
| `near_high_high_2` | Value `>= 0.6` AND Long Hybrid `>= 0.6` | Robustness |

Primary adoption decision は `core_high_high` だけで行う。Near ring は sample
拡張時に効果の符号が大きく反転しないかを確認する。

## Frozen Hypotheses

1. Entry: 強い2スコア候補でも、SMA5 状態が弱い、または上方へ過熱している
   entry を拒否すると、無条件 entry より OOS 成績が改善する。
2. Exit: 保有後に SMA5 状態が悪化した当日 Close で exit すると、SMA5 exit
   なしより左尾または drawdown が改善する。
3. Combined: entry と exit を独立に合格させた場合だけ、両者を組み合わせた
   効果が維持される。
4. Null: SMA5 hard gate は turnover を増やすだけで、統計・運用の二重基準
   を満たさない。

## Rule Registry

### Entry variants

| Rule | Predicate |
| --- | --- |
| `E0_no_sma5_filter` | score ring のみ |
| `E1_close_above_sma5` | `Close >= SMA5` |
| `E2_count_ge_2` | `sma5_above_count_5d >= 2` |
| `E3_avoid_atr20_chase` | `sma5_atr20_deviation < +1.0` |
| `E4_count_ge_2_and_avoid_chase` | E2 AND E3 |

`E4` は E2 と E3 が単独で有望な場合だけ confirmatory combined rule として
扱う。それ以外の E4 結果は exploratory と明記する。

### Exit variants

| Rule | Predicate |
| --- | --- |
| `X0_no_sma5_exit` | SMA5 exit なし |
| `X1_close_below_sma5` | `Close < SMA5` |
| `X2_count_le_1` | `sma5_above_count_5d <= 1` |
| `X3_below_streak_ge_3` | `Close < SMA5` が3営業日連続 |
| `X4_atr20_below_le_neg1` | `sma5_atr20_deviation <= -1.0` |

## Position-State and Execution Accounting

本研究はユーザーの実運用を近似し、当日 Close を含む signal を同じ当日 Close
で約定させる。これは厳密には future-leak 的な optimistic execution なので、
`close_proxy_same_session` と命名し、PIT-safe execution と区別して manifest、
summary、Published Readout の Caveats に明記する。

Position state は次の順序で処理する。

1. `score ring AND entry rule` が false から true へ遷移した当日 Close で entry
   する。
2. entry 日の price move は return に含めず、最初の exposure は当日 Close
   から翌営業日 Close までとする。
3. exit rule が成立した当日 Close で exit し、その Close 以後の return は
   含めない。
4. score ring 離脱は全 variant 共通の Close exit とする。
5. exit 後は entry eligibility が一度 false になり、再び true になるまで
   re-entry しない。
6. 最大保有期間は primary 60営業日、robustness 20営業日とする。
7. 上場廃止または末尾欠損は最後の有効 Close で強制終了し、欠損 return を
   zero-return winner として扱わない。

Entry、exit、ring-exit、time-exit が同日に重なる場合は、既存 position を
先に exit し、同日 re-entry は許可しない。

## Portfolio and Trade Outputs

二つの会計 view を生成する。

1. Trade ledger: 各 code の entry / exit、holding sessions、gross/net return、
   exit reason、signal feature snapshot を保持する。
2. Portfolio path: 各日の active position を等ウェイト化し、TOPIX excess
   return、NAV、IR、drawdown、expected shortfall、exposure、turnover proxy
   を集計する。

取引コストは round-trip 10bps を base、20bps を stress とする。entry と
exit に半分ずつ配賦し、gross と net の両方を保存する。

## Time Splits

長期 leadership feature が complete になる最初の日以降を使い、次の期間を
固定する。

- Discovery: 2018-01-01 から 2021-12-31
- Validation: 2022-01-01 から 2024-12-31
- Final holdout: 2025-01-01 から利用可能な最終日

feature availability により実際の開始日が遅い場合は、期間ラベルを変えず
coverage diagnostics に effective first date を記録する。holdout は rule、
threshold、decision gate を固定した後に一度だけ評価する。

## Statistical Adoption Gate

Primary comparison は `core_high_high`、60-session cap、各 entry/exit variant
対対応する `E0` / `X0` とする。

統計ゲートは次をすべて満たす必要がある。

- paired date-level portfolio return delta の moving-block bootstrap 95% CI
  がゼロを跨がない。
- final holdout でも効果の方向が同じである。
- entry family と exit family を分けて Holm 補正し、adjusted `p < 0.05`。
- OOS に最低200 trades、100 signal dates がある。
- Near 1/2 ring と20-session capで効果の符号が大きく反転しない。

Bootstrap は固定 seed を使い、block length と resample count を manifest に
保存する。serial correlation を無視した iid bootstrap は primary gate に
使わない。

## Operational Adoption Gate

統計ゲートに加えて次をすべて満たす。

- OOS annualized IR が baseline より `0.15` 以上改善する。
- 最大 drawdown または5% expected shortfall が相対 `10%` 以上改善する。
- round-trip 10bps 控除後も改善が残る。
- 20bps stress cost で結論が反転しない。
- turnover が baseline の `1.5` 倍以下である。
- 暦年別効果が過半数の年で正で、特定の1年だけに依存しない。

Entry と exit は独立に合否判定する。両方が合格した場合だけ combined variant
を confirmatory に評価する。全ゲートを通過しても、same-Close execution の
近似があるため最終判定は `production_candidate` までとし、production strategy
を自動変更しない。

## Domain and Runner Design

新しい実験 ID は
`market-behavior/ranking-sma5-score-ring-hard-filter-evidence` とする。

- Domain:
  `apps/bt/src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py`
- Runner:
  `apps/bt/scripts/research/run_ranking_sma5_score_ring_hard_filter_evidence.py`
- Unit tests:
  `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py`
- Runner tests:
  `apps/bt/tests/unit/scripts/test_run_ranking_sma5_score_ring_hard_filter_evidence.py`
- Canonical note:
  `apps/bt/docs/experiments/market-behavior/ranking-sma5-score-ring-hard-filter-evidence/README.md`

Position accounting は既存 vectorbt adapter に寄せ、新しい独自 execution engine
を増やさない。Domain module は rule classification、state transition、
aggregation、decision gate を担当し、runner は I/O と bundle orchestration
に限定する。

## Bundle Contract

Runner は標準の `manifest.json + results.duckdb + summary.md` を出力し、
`results.duckdb` に最低限次の table を保存する。

- `rule_registry_df`
- `coverage_diagnostics_df`
- `trade_ledger_df`
- `portfolio_daily_df`
- `entry_rule_evidence_df`
- `exit_rule_evidence_df`
- `combined_rule_evidence_df`
- `annual_stability_df`
- `bootstrap_effect_ci_df`
- `cost_sensitivity_df`
- `decision_gate_df`
- `observation_sample_df`

`summary.json`、`published_summary=`、module-local `_build_published_summary()`
は導入しない。Publication SoT は canonical README の `## Published Readout`
とし、`Decision`、`Main Findings`、`Interpretation`、
`Production Implication`、`Caveats`、`Source Artifacts` を日本語で記録する。

## Testing and Failure Handling

Unit tests は少なくとも次を固定する。

- entry 日 return を含めない。
- exit Close 以後の return を含めない。
- false-to-true 遷移だけで entry する。
- exit 後に条件回復なしで re-entry しない。
- score ring 離脱を全 variant 共通 exit とする。
- exit と entry が同日に重なっても same-day re-entry しない。
- 20 / 60 session cap を正しく適用する。
- 欠損、上場廃止、末尾 open position を明示的に処理する。
- discovery / validation / holdout を跨ぐ future-derived threshold または
  summary を使わない。
- moving-block bootstrap が固定 seed で再現可能である。
- Holm adjustment と decision boundary が仕様通りである。
- Market v5 / provider-adjusted provenance 不一致で fail closed する。
- 同じ入力から同じ bundle table と summary を再生成できる。

## Verification

実装後は少なくとも次を実行する。

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py \
  tests/unit/scripts/test_run_ranking_sma5_score_ring_hard_filter_evidence.py
python3 scripts/check-research-guardrails.py
python3 scripts/skills/audit_skills.py --strict-legacy
scripts/prepush-ci.sh --research
```

実データ run 後は `manifest.json`、`summary.md`、`results.duckdb` の decision
metrics が canonical README と一致することを確認し、closeout は README の
Published Readout を更新して初めて完了とする。
