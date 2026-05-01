# Range Break Dynamic PIT Pruning

## Published Readout

### Decision

`range_break_v15` の dynamic PIT 版 `unknown_20260501_100647` を source artifact とし、`primeExTopix500` の entry-date universe gate 後に増えた trades を、PIT-safe な事前特徴で 350-450 trades 近辺へ圧縮できるかを検証する。

### Main Findings

#### 結論

`range_break_v15` dynamic PIT baseline は trades `1007`、entry signals `8619`、trade-level 平均 return `4.23%`、profit factor `1.63` だった。2016-2021 の discovery segment だけで閾値を決め、全期間へ適用した候補では、`target_low_pbr` が trades `359`、平均 return `6.12%`、profit factor `2.12`、severe loss rate `22.6%` で最も良い trade-quality pruning になった。より trades `400` に近い候補では `target_high_topix_risk_adjusted_return_60` が trades `405`、平均 return `4.99%`、profit factor `1.81` だった。

| candidate | trades | avg return | win rate | profit factor | severe loss | avg hold | approx concurrent | gross at 20% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline_all | 1007 | 4.23% | 33.8% | 1.63 | 30.2% | 81.5d | 33.6 | 672.8% |
| target_low_pbr | 359 | 6.12% | 36.5% | 2.12 | 22.6% | 86.4d | 12.7 | 254.2% |
| target_high_topix_risk_adjusted_return_60 | 405 | 4.99% | 35.3% | 1.81 | 27.9% | 81.0d | 13.5 | 268.9% |
| target_low_breakout_60d_runup | 416 | 4.08% | 36.1% | 1.82 | 19.5% | 84.6d | 14.4 | 288.6% |
| target_low_forward_per | 374 | 3.48% | 37.7% | 1.66 | 21.9% | 87.9d | 13.5 | 269.4% |

#### validation 側の見え方

2022-2026 の validation segment だけで見ると、baseline は trades `344`、平均 return `5.10%`。`target_low_pbr` は validation trades `148`、平均 return `5.53%`、win rate `36.5%` で baseline より質は改善したが、件数は大きく削られる。`target_high_topix_risk_adjusted_return_60` は validation trades `142`、平均 return `8.91%`、win rate `41.5%` で後半局面への効きが強い。

### Interpretation

`range_break_v15` は dynamic PIT によって universe が 1454 銘柄から 2012 銘柄へ広がり、baseline trades が 1007 まで増えた。今回の実測平均 holding days は `81.5` 日で、当初メタ仮説の `40` 日よりかなり長い。そのため 400 trades へ絞っても、20% allocation なら平均同時保有は `13` positions 前後、概算 gross は `250-290%` になりやすい。trade count だけでなく `max_concurrent_positions` または daily entry ranking が必要。

### Production Implication

第一候補は `target_low_pbr`。ただし target midpoint 400 からはやや少ないため、production 化では `PBR <= 0.97` を単独で入れるより、`PBR` を ranking score 化して daily top-N / max concurrent と組み合わせる方が自然。第一段の移植用 YAML として `experimental/robustness/range_break_v15_target_low_pbr.yaml` を生成した。第二候補は market regime overlay の `topix_risk_adjusted_return_60 >= 1.81` で、400 trades 近辺に最も近い。次段では `low PBR` と `supportive TOPIX risk-adjusted regime` を portfolio-level backtest で検証する。

### Caveats

この研究は baseline trade ledger の pruning 診断であり、pruned entries を使った portfolio-level re-simulation ではない。資金拘束、cash sharing、同時保有、entry ranking の効果は次段の再バックテストで検証する。`PBR` / `forward PER` は entry 日以前に解決した fundamentals から算出しているが、最終実装時は signal runtime 側でも同じ PIT helper を使う必要がある。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_range_break_dynamic_pit_pruning.py`
- Domain: `apps/bt/src/domains/analytics/range_break_dynamic_pit_pruning.py`
- Strategy YAML: `apps/bt/config/strategies/experimental/robustness/range_break_v15_target_low_pbr.yaml`
- Source backtest: `~/.local/share/trading25/backtest/results/range_break_v15/unknown_20260501_100647.*`
- Bundle: `~/.local/share/trading25/research/strategy-audit/range-break-dynamic-pit-pruning/20260501_102735_db623107`
