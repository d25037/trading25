# Ranking SMA5 Score-Ring Stateful Rotation Evidence

Value × Long Hybrid の frozen score ring で保有銘柄が X2 / X3 / X4 に初めて
到達した日に、同じringのhealthy basketへ一度だけ持ち替え、各targetの次trigger
まで保有した累積returnを、同じ期間sourceを持ち続けた場合と比較する。

## Published Readout

### Decision

Run: `20260724_prime_v5_sma5_score_ring_stateful_rotation_v1`

- X2 `count <= 1`: `stateful_rotation_candidate`
- X3 `Close < SMA5` 3営業日連続: `insufficient_evidence`
- X4 `SMA5/ATR20 deviation <= -1`: `insufficient_evidence`

X2はCore・10bps控除後のpaired中央値が `+49.71 bps`、positive event rateが
`59.50%`、positive yearが `5/9` だった。Near1 / Near2の10bps後も
`+1.22 / +11.99 bps`、Core・20bps後も `+39.71 bps` で、事前に固定した
全条件を通過した。

これはproduction採用ではなく、X2を使うrecursive portfolio研究へ進める価値が
ある、という探索的判断である。strategy、Daily Ranking、API、UIは変更しない。

### Data Scope

- Effective period: `2018-01-04..2026-07-21`
- Prime exact-date membership (`0101`, `0111`)
- Market schema v5 / `provider_adjusted_v1`
- Source: `E0_no_sma5_filter` / 60-session cap のbaseline保有中に最初に発生した
  X2 / X3 / X4
- Target: 同日・同一ringでX2 / X3 / X4のすべてに非該当の銘柄
- Holding: targetごとに次trigger、ring離脱、60営業日、データ終端の最初まで
- Counterfactual: targetと同じ終了日までsourceを継続保有
- Cost: event-level paired deltaからrotation時に一度だけ `0 / 10 / 20 bps`

既に2025年以降を観測した後のexploratory follow-upであり、独立したholdoutでは
ない。

### Main Findings

以下はCore ringの
`target累積return - 同期間source累積return - rotation cost` である。

| Trigger | Availability | 保有日数中央値 | 0bps中央値 | 10bps中央値 | 10bps勝率 | 正の年 | 20bps中央値 | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| X2 | 98.04% | 4 | +59.71 bps | +49.71 bps | 59.50% | 5/9 | +39.71 bps | candidate |
| X3 | 95.64% | 4 | +43.90 bps | +33.90 bps | 55.17% | 6/9 | +23.90 bps | insufficient |
| X4 | 89.36% | 2 | -22.71 bps | -32.71 bps | 44.94% | 4/9 | -42.71 bps | insufficient |

10bps後のring robustnessは次のとおり。

| Trigger | Core | Near1 | Near2 |
| --- | ---: | ---: | ---: |
| X2 | +49.71 bps | +1.22 bps | +11.99 bps |
| X3 | +33.90 bps | -0.31 bps | +10.94 bps |
| X4 | -32.71 bps | -10.13 bps | -6.51 bps |

X3はCoreでは強く、20bpsと年別方向も通過したが、Near1・10bpsが
`-0.31 bps`とわずかに負だったため固定条件上は不合格とした。境界的な結果で
あり、X2の次点として観察対象には残す。X4は全ringで負であり、持ち替えtrigger
として支持されない。

Targetの終了理由はholding capではなく、次の短期状態変化かring離脱が大半だった。
Coreのholding cap到達率はいずれも `0.5%` 未満で、実際の比較期間はX2 / X3で
中央値4営業日、X4で2営業日だった。

### One-Day Researchとの違い

前回の翌日だけの比較では、Core・10bps中央値はX2 `+3.43 bps`、
X3 `-5.45 bps`、X4 `-31.75 bps`で、全triggerを不採用とした。

今回、コストを一度だけ負担して次triggerまで保有すると、X2は
`+49.71 bps`へ拡大し、Near ringと20bpsでも正を維持した。X3もCoreでは
`+33.90 bps`へ改善したが、Near1がほぼゼロで固定判定には届かなかった。
X4は複数日化しても改善せず、前回と同じ方向だった。

したがって、翌日returnだけでは捉えにくかったX2の複数日rotation効果はある。
一方、これはone-hop episodeの平均であり、実際に資金を連続運用したNAVではない。

### Interpretation

X2は翌日だけでは小さかった差が、次triggerまでの中央値4営業日では拡大した。
一度だけの売買コストに対し、healthy状態の銘柄を数日保有する効果が残った形で
ある。X3にも同様の可能性はあるがringを少し変えると消えるため、X2より確度が
低い。X4はsource継続保有に劣り、rotation用途には適さない。

### Production Implication

今回はX2をproduction ruleへ昇格しない。次に行う価値があるのは、X2をtriggerに
したrecursive portfolioで、実際の資金制約、保有重複、連続rotation、売買コストを
含むNAVを確認することである。X3は比較対象、X4は候補外とする。

### Caveats

- signal、target選択、持ち替えを同じCloseで行うoptimisticな近似。
- source eventとtarget episodeは重複し、観測は独立ではない。
- 既にholdoutを観測した後のexploratory follow-up。
- targetは等ウェイト近似。保有重複、資金制約、流動性、slippage、market impact、
  capacityは未評価。
- 2026年は `2026-07-21` までのincomplete year。

### Source Artifacts

- Runner:
  `apps/bt/scripts/research/run_ranking_sma5_score_ring_stateful_rotation_evidence.py`
- Module:
  `apps/bt/src/domains/analytics/ranking_sma5_score_ring_stateful_rotation_evidence.py`
- Test:
  `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_stateful_rotation_evidence.py`
- Bundle:
  `~/.local/share/trading25/research/market-behavior/ranking-sma5-score-ring-stateful-rotation-evidence/20260724_prime_v5_sma5_score_ring_stateful_rotation_v1/`
- Bundle files: `manifest.json`, `results.duckdb`, `summary.md`
- Results tables: `stateful_rotation_summary_df`, `stateful_rotation_annual_df`,
  `stateful_rotation_exit_reason_df`, `stateful_rotation_decision_df`,
  `stateful_rotation_event_df`, `coverage_diagnostics_df`
