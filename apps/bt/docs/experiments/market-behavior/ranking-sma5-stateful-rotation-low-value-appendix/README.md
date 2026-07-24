# Ranking SMA5 Stateful Rotation Low-Value Appendix

Long Hybridは強いがValueは悪い群で、X2 / X3 / X4到達日に同じringのhealthy
basketへ一度だけ持ち替え、各targetの次triggerまでの累積returnを同期間の
source継続保有と比較したappendixである。

## Published Readout

### Decision

Run: `20260724_prime_v5_sma5_stateful_rotation_low_value_appendix_v1`

- X2 `count <= 1`: `insufficient_evidence`
- X3 `Close < SMA5` 3営業日連続: `insufficient_evidence`
- X4 `SMA5/ATR20 deviation <= -1`: `insufficient_evidence`
- `stateful_rotation_candidate`: なし

Low-Value Core・10bpsではX2が `-14.43 bps`、X3が `-25.42 bps`だった。
X4だけは `+37.20 bps`、positive event rate `53.23%`、Core・20bpsでも
`+27.20 bps`と改善した。しかしNear1が `-1.70 bps`、正の年も `4/9`に
留まり、固定した判定条件を満たさなかった。

したがって、Low-Value群でもX2 / X3 / X4を機械的なstateful rotation ruleへ
採用しない。X4はこの群に限った観察候補として残すが、production昇格は行わない。

### Data Scope

- Effective period: `2018-01-04..2026-07-21`
- Prime exact-date membership (`0101`, `0111`)
- Market schema v5 / `provider_adjusted_v1`
- Ring:
  - Core: Long Hybrid `>= 0.7`、Value `<= 0.2`
  - Near1: Long Hybrid `>= 0.7`、Value `<= 0.3`
  - Near2: Long Hybrid `>= 0.7`、Value `<= 0.4`
- Source: `E0_no_sma5_filter` / 60-session cap のbaseline保有中に最初に発生した
  X2 / X3 / X4
- Target: 同日・同一ringでX2 / X3 / X4のすべてに非該当の銘柄
- Holding: targetごとに次trigger、ring離脱、60営業日、データ終端の最初まで
- Counterfactual: targetと同じ終了日までsourceを継続保有
- Cost: event-level paired deltaからrotation時に一度だけ `0 / 10 / 20 bps`

### Main Findings

以下は `target累積return - 同期間source累積return - rotation cost` の
event-level paired比較である。

| Trigger | Core availability | Core保有日数中央値 | Core 10bps | 勝率 | 正の年 | Near1 10bps | Near2 10bps | Core 20bps | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| X2 | 99.83% | 5 | -14.43 bps | 48.22% | 4/9 | -2.82 bps | +1.00 bps | -24.43 bps | insufficient |
| X3 | 99.78% | 5 | -25.42 bps | 47.04% | 4/9 | -11.83 bps | +3.76 bps | -35.42 bps | insufficient |
| X4 | 100.00% | 4 | +37.20 bps | 53.23% | 4/9 | -1.70 bps | +2.15 bps | +27.20 bps | insufficient |

持ち替え先の不足は問題ではなかった。Coreでもavailabilityは
`99.78%..100.00%`、target数中央値はX2 / X3 / X4で `68 / 76 / 65`だった。
holding cap到達は各triggerで `0.01%`以下で、targetは主に次の短期triggerか
ring離脱で終了した。

### High-Value Stateful Resultとの比較

通常のValue × Long Hybrid high-Value研究のCore・10bps中央値は、
X2 `+49.71 bps`、X3 `+33.90 bps`、X4 `-32.71 bps`だった。Low-Valueでは
それぞれ `-14.43 / -25.42 / +37.20 bps`となり、方向がすべて反転した。

| Trigger | High-Value Core 10bps | Low-Value Core 10bps | Low - High |
| --- | ---: | ---: | ---: |
| X2 | +49.71 bps | -14.43 bps | -64.14 bps |
| X3 | +33.90 bps | -25.42 bps | -59.32 bps |
| X4 | -32.71 bps | +37.20 bps | +69.91 bps |

### Interpretation

X2のstateful rotation効果は「セクターが強い」だけでは再現せず、高Value群との
組み合わせに依存していた。一方、Low-Valueでは深い下方乖離であるX4後の
持ち替えに改善の兆候がある。ただしValue上限を `0.3`へ広げるだけで中央値が
わずかに負となり、暦年方向も過半数に届かないため、rule化の根拠にはしない。

### Production Implication

このappendixからstrategy、Daily Ranking、API、UIは変更しない。High-Value群の
X2候補をLow-Value群へ一般化せず、Low-Value X4は追加の観察材料に留める。

### Caveats

- appendixとして既存結果を見た後のexploratory分析であり、独立holdoutではない。
- signal、target選択、持ち替えを同じCloseで行うoptimisticな近似。
- source eventとtarget episodeは重複し、観測は独立ではない。
- targetは等ウェイト近似。保有重複、資金制約、流動性、slippage、market impact、
  capacityは未評価。
- 2026年は `2026-07-21`までのincomplete year。

### Verification

`stateful_rotation_event_df`の15,937 eventsから、0 / 10 / 20bpsの全27 summary行と
全243 annual行をread-onlyで独立再計算し、保存値と一致した。Decision、coverage、
holding days、exit reasonの件数・比率も整合した。

### Source Artifacts

- Runner:
  `apps/bt/scripts/research/run_ranking_sma5_stateful_rotation_low_value_appendix.py`
- Module:
  `apps/bt/src/domains/analytics/ranking_sma5_stateful_rotation_low_value_appendix.py`
- Test:
  `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_stateful_rotation_low_value_appendix.py`
- Bundle:
  `~/.local/share/trading25/research/market-behavior/ranking-sma5-stateful-rotation-low-value-appendix/20260724_prime_v5_sma5_stateful_rotation_low_value_appendix_v1/`
- Bundle files: `manifest.json`, `results.duckdb`, `summary.md`
- Results tables: `stateful_rotation_summary_df`, `stateful_rotation_annual_df`,
  `stateful_rotation_exit_reason_df`, `stateful_rotation_decision_df`,
  `stateful_rotation_event_df`, `coverage_diagnostics_df`
