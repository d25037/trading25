# Ranking SMA5 Score-Ring Rotation Evidence

Value × Long Hybrid の frozen score ring で保有中の銘柄が X2 / X3 / X4 に
初めて到達した日に、同じ ring の健全な銘柄 basket へ持ち替えた方が翌営業日
Close までの return を改善するかを比較する。

## Published Readout

### Decision

Run: `20260724_prime_v5_sma5_score_ring_rotation_v1`

- X2 `count <= 1`: `insufficient_evidence`
- X3 `Close < SMA5` 3営業日連続: `insufficient_evidence`
- X4 `SMA5/ATR20 deviation <= -1`: `insufficient_evidence`
- `rotation_candidate`: なし

X2 は Core の10bps控除後中央値が `+3.43 bps`、outperform率が `51.50%`、
positive yearが `6/9` で、Core内だけなら弱い改善が見えた。しかし20bpsでは
`-6.57 bps` に反転し、Near1 / Near2 の10bpsでも負だったため、持ち替え
候補の固定条件を満たさなかった。X3 / X4 はCoreの10bps時点で負だった。

したがって、今回の Value × Long Hybrid score ring では、X2 / X3 / X4を
機械的な同日rotation ruleとして採用しない。

### Data Scope

- Effective period: `2018-01-04..2026-07-21`
- Prime exact-date membership (`0101`, `0111`)
- Market schema v5 / `provider_adjusted_v1`
- Source: `E0_no_sma5_filter` / 60-session cap のbaseline保有中に最初に発生した
  X2 / X3 / X4
- Target: 同日・同一ringでX2 / X3 / X4のすべてに非該当の銘柄等ウェイトbasket
- Outcome: 当日Closeから翌営業日Close
- Cost: rotation側だけに `0 / 10 / 20 bps`

このfollow-upは既に2025年以降のholdoutを観測した後の探索的研究であり、
confirmatory evidenceではない。

### Target Availability

持ち替え先は大半の日で存在した。Coreではringが狭いため、X4のavailabilityが
最も低かった。

| Ring | Trigger | Source outcomes | Paired events | Availability | Target数中央値 |
| --- | --- | ---: | ---: | ---: | ---: |
| Core | X2 | 204 | 200 | 98.04% | 17 |
| Core | X3 | 686 | 658 | 95.92% | 14 |
| Core | X4 | 498 | 449 | 90.16% | 13 |
| Near1 | X2 | 922 | 917 | 99.46% | 56 |
| Near1 | X3 | 3,205 | 3,183 | 99.31% | 55 |
| Near1 | X4 | 2,037 | 1,946 | 95.53% | 44 |
| Near2 | X2 | 1,550 | 1,550 | 100.00% | 152 |
| Near2 | X3 | 5,625 | 5,625 | 100.00% | 129 |
| Near2 | X4 | 3,616 | 3,598 | 99.50% | 114 |

### Main Findings

以下は `rotation return - source return - cost` のpaired比較である。

| Trigger | Ring | 0bps median | 10bps median | 10bps outperform | 20bps median |
| --- | --- | ---: | ---: | ---: | ---: |
| X2 | Core | +13.43 bps | +3.43 bps | 51.50% | -6.57 bps |
| X2 | Near1 | +3.34 bps | -6.66 bps | 47.66% | -16.66 bps |
| X2 | Near2 | +0.14 bps | -9.86 bps | 46.00% | -19.86 bps |
| X3 | Core | +4.55 bps | -5.45 bps | 48.18% | -15.45 bps |
| X3 | Near1 | +3.99 bps | -6.01 bps | 47.97% | -16.01 bps |
| X3 | Near2 | -0.18 bps | -10.18 bps | 46.33% | -20.18 bps |
| X4 | Core | -21.75 bps | -31.75 bps | 42.09% | -41.75 bps |
| X4 | Near1 | -0.34 bps | -10.34 bps | 47.28% | -20.34 bps |
| X4 | Near2 | -4.41 bps | -14.41 bps | 45.28% | -24.41 bps |

Core・10bpsの暦年中央値が正だった年は、X2が `6/9`、X3が `4/9`、X4が
`1/9`。X2だけは年別方向が過半数で正だったが、ring robustnessと20bps
stressを通過しなかった。

### Interpretation

旧scaffoldの研究ではX2 / X3 / X4非該当basketへのrotationが有望だったが、
今回のValue × Long Hybrid score ringでは再現しなかった。特にX4はsourceを
持ち続ける方が翌日成績が良く、機械的に健康銘柄へ入れ替える根拠はない。

X2はCoreの低コスト条件に限ればわずかに有望だが、対象ringを広げると消え、
20bpsでも反転する。個人運用で候補の質や売買コストを個別判断するための
watch signalには残せるが、自動rotation ruleにはしない。

### Production Implication

X2 / X3 / X4をValue × Long Hybrid score ring内の自動持ち替えruleにはしない。
strategy、Daily Ranking、API、UIは変更せず、X2のCore低コスト条件は個人運用の
手動watch材料にだけ残す。

### Caveats

- signal、target選択、持ち替えを同じCloseで行うoptimisticな近似。
- outcomeは翌営業日Closeまでの1日だけで、中期の持ち替え効果は未評価。
- 既にholdoutを観測した後のexploratory follow-upであり、独立holdoutではない。
- target basketは等ウェイト近似。実際の保有重複、資金配分、流動性、
  slippage、market impact、capacityは未評価。
- 2026年は `2026-07-21` までのincomplete year。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_sma5_score_ring_rotation_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_sma5_score_ring_rotation_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_rotation_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-score-ring-rotation-evidence/20260724_prime_v5_sma5_score_ring_rotation_v1/`
- Bundle files: `manifest.json`, `results.duckdb`, `summary.md`
- Results tables: `rotation_summary_df`, `rotation_annual_df`,
  `rotation_decision_df`, `coverage_diagnostics_df`, `rotation_event_df`
