# Range Break Trade Archetype Decomposition

## Purpose

`production/range_break_v15` の実トレード台帳を起点に、range break の何が強く、何が return 劣化の原因になっているかを分解する。

この研究は全銘柄 panel ではない。実際に `range_break_v15` の entry/exit 条件を通過して約定した trade だけを対象に、entry 前営業日時点で PIT-safe に以下を付与する。

- 200日高値 breakout からの距離と 20/60/120 日 run-up
- Bollinger 上限までの距離、RSI、出来高比
- rolling beta、売買代金 range、TOPIX regime
- PBR / forward PER / market cap
- return 上位/下位 bucket の特徴差

## Published Readout

### Decision

2026-05-01 に `production/range_break_v15` を `market.duckdb` v3 の `primeExTopix500` universe で再分解した。universe は window start 日の `stock_master_daily` で PIT 解決し、entry features は entry 前営業日 close 時点で付与した。

結論は、`range_break` 概念自体は捨てない。v3 full-history では 929 trades、avg +3.48%、median -4.03%、severe loss 29.82% で、中央値と左尾が劣化の主因。一方で holdout 6m は 54 trades、avg +17.07%、median +0.67%、severe loss 9.26% まで戻っており、直近だけなら momentum breakout として機能している。再構成の焦点は「勝ちサブセット探し」より、長期 run-up / overheat が重なった左尾をどう小さくするか。

### Main Findings

#### 結論: v3 full では中央値がマイナス、holdout は改善している

| window | trades | symbols | win rate | avg trade | median trade | worst | p05 | p95 | avg holding |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train_pre_holdout | 867 | 593 | 32.18% | +2.75% | -4.31% | -40.68% | -20.77% | +45.56% | 79.0d |
| holdout_6m | 54 | 54 | 50.00% | +17.07% | +0.67% | -26.82% | -13.00% | +51.89% | 70.7d |
| full | 929 | 619 | 32.40% | +3.48% | -4.03% | -40.68% | -20.48% | +46.75% | 79.8d |

`range_break` は低勝率・右尾依存。full の p95 は +46.75% まであるので alpha source は残るが、median -4.03% と severe loss 29.82% が production return を削っている。

#### 結論: full-history の Standard 部分は弱いが、holdout は Prime のみ

| window | market_scope | trades | avg trade | median trade | win rate | severe loss | worst | median PBR | median forward PER | median mcap bn JPY |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| full | all | 929 | +3.48% | -4.03% | 32.40% | 29.82% | -40.68% | 0.94 | 15.85 | 23.5 |
| full | prime | 797 | +4.05% | -4.01% | 33.00% | 28.61% | -35.78% | 0.97 | 15.85 | 25.8 |
| full | standard | 132 | -0.00% | -4.27% | 28.79% | 37.12% | -40.68% | 0.79 | 15.78 | 12.8 |
| holdout_6m | all/prime | 54 | +17.07% | +0.67% | 50.00% | 9.26% | -26.82% | 1.04 | 16.26 | 44.9 |

v3 の PIT stock master で見ると、full-history の `primeExTopix500` 約定には historical Standard が 132 trades 入る。Standard 側は severe loss が 37.12% と濃く、full の劣化に効いている。ただし holdout は Prime のみなので、直近改善を Standard 排除だけで説明することはできない。

#### 結論: 低 return は過去 run-up と高めの valuation に寄る

| window | return bucket | trades | avg trade | median trade | severe loss | median 60d run-up | median RSI10 | median volume ratio | median beta | median TOPIX 60d | median TV oku | median PBR | median forward PER |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| full | low_return_q20 | 186 | -18.31% | -17.02% | 100.00% | 52.40% | 73.65 | 1.85 | 0.89 | +3.93% | 4.79 | 1.33 | 21.04 |
| full | middle_return_q20_q80 | 557 | -3.67% | -4.03% | 16.34% | 33.41% | 72.05 | 1.80 | 0.84 | +4.36% | 2.77 | 0.87 | 14.54 |
| full | high_return_q80 | 186 | +46.67% | +29.52% | 0.00% | 42.10% | 72.87 | 1.83 | 0.90 | +3.60% | 3.77 | 1.00 | 15.46 |
| holdout_6m | low_return_q20 | 11 | -11.52% | -9.32% | 45.45% | 20.27% | 76.13 | 1.81 | 0.83 | +9.86% | 5.79 | 1.04 | 20.24 |
| holdout_6m | high_return_q80 | 11 | +84.51% | +39.55% | 0.00% | 38.33% | 65.48 | 1.75 | 1.01 | +8.98% | 12.30 | 0.97 | 18.48 |

低 return bucket は full で median forward PER 21.04、PBR 1.33 と割高寄り。holdout でも低 return は RSI が高く、forward PER も高い。単純な「breakout が強いほど良い」ではなく、すでに伸びた高PER銘柄を掴むと左尾化しやすい。

#### 結論: run-up Q5 は右尾も左尾も濃いので、hard exclude は期待値を落とす

| feature | Q1 median | Q1 avg | Q1 severe | Q5 median | Q5 avg | Q5 severe |
|---|---:|---:|---:|---:|---:|---:|
| breakout_60d_runup_pct | 10.63% | +0.67% | 15.14% | 87.03% | +8.68% | 42.47% |
| breakout_120d_runup_pct | 10.23% | +1.43% | 17.84% | 97.92% | +6.40% | 41.40% |
| rsi10 | 53.97 | +2.49% | 25.41% | 88.79 | +6.98% | 31.72% |
| trading_value_ma_15_oku | 1.11 | +1.08% | 17.84% | 16.86 | +11.23% | 36.56% |
| volume_ratio_value | 1.68 | +0.18% | 34.05% | 2.39 | +5.13% | 35.48% |

60/120日 run-up Q5 は severe loss が 40% 超だが avg return も高い。これは `range_break` の momentum right-tail と、成熟し過ぎた overheat bad-tail が同じ場所に混ざっているという意味。forward EPS driven と同様、単純除外より haircut / risk cap の検証が先。

#### 結論: overlay は左尾を少し下げるが、平均を犠牲にする

| rule | window | kept trades | kept avg | kept median | kept severe | kept worst | delta avg | delta severe |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| overheat_overlap_ge2_excluded | full | 741 | +2.06% | -3.88% | 27.13% | -35.17% | -1.42pt | -2.69pt |
| supportive_topix_and_no_overheat_overlap | full | 555 | +1.93% | -3.80% | 25.59% | -35.17% | -1.55pt | -4.23pt |
| breakout_not_extended_120d | full | 743 | +2.75% | -3.65% | 26.92% | -29.07% | -0.73pt | -2.90pt |
| volume_not_extreme_q80 | holdout_6m | 43 | +20.12% | +1.54% | 6.98% | -26.82% | +3.05pt | -2.28pt |
| overheat_overlap_ge2_excluded | holdout_6m | 43 | +8.54% | +2.51% | 4.65% | -15.20% | -8.52pt | -4.61pt |

full-history では overheat overlap 除外は severe loss を下げるが avg も下がる。holdout では overheat 除外が worst を -15.20% まで改善する一方、avg を +17.07% から +8.54% へ落とす。`volume_not_extreme_q80` は holdout で良いが full では弱いので、単独採用には早い。

#### 結論: value overlay は left-tail control にはなるが alpha source としては不安定

| scope | window | candidate | trades | avg trade | median trade | severe loss | delta avg | delta severe |
|---|---|---|---:|---:|---:|---:|---:|---:|
| all | full | low_forward_per_q1 | 228 | +2.89% | -3.28% | 22.81% | -0.59pt | -7.01pt |
| all | full | low_pbr_q1 | 249 | +3.47% | -4.31% | 24.10% | -0.01pt | -5.72pt |
| prime | full | low_forward_per_q1 | 198 | +1.78% | -3.21% | 20.71% | -2.27pt | -7.90pt |
| standard | full | low_forward_per_q1 | 30 | +11.60% | -1.39% | 33.33% | +11.60pt | -3.79pt |
| all/prime | holdout_6m | small_market_cap_q1 | 17 | +41.60% | +2.51% | 17.65% | +24.53pt | +8.39pt |
| all/prime | holdout_6m | value_core_low_pbr_low_fper_small_cap | 4 | -5.67% | -4.65% | 50.00% | -22.74pt | +40.74pt |

低forward PER / 低PBRは full の severe loss を下げるが avg は上がらない。Standard の低forward PERは強いが 30 trades の historical subset。holdout の小型は平均が高いが severe loss も悪化し、value core は標本 4 trades で明確に悪い。value は entry 採用条件ではなく、左尾・valuation risk の診断軸として残す。

### Interpretation

`range_break_v15` は「上昇の初動を買う」より「一定以上伸びた銘柄の continuation 右尾を取りに行く」戦略として振る舞っている。したがって、run-up を完全に切ると alpha も落ちる。問題は、長期 run-up・高RSI・高出来高・高forward PER が重なる mature breakout を同じサイズで持つこと。

v3 化で return が劣化した理由としては、PIT stock master と raw/adjusted projection の修正により、過去の Standard trade や調整後系列の扱いがより正確になったことが大きい。full-history の Standard 部分は avg 0% 近辺、severe loss 37% で、旧 snapshot 的な見え方より弱い。一方で holdout は Prime のみで改善しているため、range break 概念そのものが消えたわけではない。

### Production Implication

現時点で production YAML に hard filter は追加しない。次の検証候補は以下。

1. `overheat_overlap_ge2` を exclude ではなく 0.5x sizing / max exposure cap として portfolio-level backtest する。
2. `breakout_not_extended_120d` は full の worst を -29.07% まで改善するため、exit/risk cap として検証する。
3. high forward PER / high PBR を hard exclude せず、mature breakout overlap の一条件として加える。
4. Standard historical trade は別 slice として、現行 production universe で再投入価値があるかを切り出す。

### Caveats

- 指標は trade-level。CAGR や portfolio equity curve の改善ではない。
- overlay 閾値は同一 window/group 内の分位で、production rule にする場合は train-only calibration が必要。
- market-derived features は entry 前営業日 close 時点で付与する。
- `holdout_6m` は 54 trades と小標本。
- range break はファンダ条件を持たないため、statements 欠損銘柄の PBR / forward PER / market cap は欠損扱いにしている。

### Source Artifacts

- Bundle: `~/.local/share/trading25/research/strategy-audit/range-break-trade-archetype-decomposition/20260501_range_break_trade_archetype_v3_prime_ex_topix500`
- Results DB: `~/.local/share/trading25/research/strategy-audit/range-break-trade-archetype-decomposition/20260501_range_break_trade_archetype_v3_prime_ex_topix500/results.duckdb`
- Summary: `~/.local/share/trading25/research/strategy-audit/range-break-trade-archetype-decomposition/20260501_range_break_trade_archetype_v3_prime_ex_topix500/summary.md`
- Runner: `uv run --project apps/bt python apps/bt/scripts/research/run_range_break_trade_archetype_decomposition.py --dataset primeExTopix500 --holdout-months 6 --run-id 20260501_range_break_trade_archetype_v3_prime_ex_topix500`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_range_break_trade_archetype_decomposition.py \
  --dataset primeExTopix500 \
  --holdout-months 6 \
  --run-id 20260501_range_break_trade_archetype_v3_prime_ex_topix500
```

## Artifact Tables

- `dataset_summary_df`: dataset/universe の期間と holdout 境界。
- `scenario_summary_df`: `range_break_v15` の single-name 実トレード集計。
- `trade_ledger_df`: 実トレード台帳。
- `enriched_trade_df`: 実トレードに entry 時点特徴量を付与した明細。
- `market_scope_summary_df`: market scope ごとの成績と中央値特徴量。
- `feature_bucket_summary_df`: breakout / overheat / liquidity / beta / market regime bucket。
- `overlay_candidate_summary_df`: bad-tail pruning / supportive TOPIX / liquidity / beta overlay 候補。
- `value_feature_bucket_summary_df`: PBR / forward PER / market cap / value composite bucket。
- `value_overlay_candidate_summary_df`: 低PBR・低forward PER・小型の value overlay 候補。
- `return_bucket_summary_df`: high return / low return trade の特徴 profile。
