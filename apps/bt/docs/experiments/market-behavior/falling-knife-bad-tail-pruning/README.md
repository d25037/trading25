# Falling Knife Bad-Tail Pruning

## Published Readout

### Decision

production へ直接持ち込む候補としては、`exclude_growth_or_q5_rar` を第一候補にする。severe loss rate を `10.62%` から `8.68%` へ下げ、kept median を `0.50%` から `0.70%` へ上げ、mean return cost も `-0.09pt` と悪化しないため、左尾削減と rebound exposure の維持のバランスが最も良い。`exclude_deep_60d_drawdown` は severe loss reduction が最大だが、kept mean が `1.29%` から `0.90%` に落ち、右尾 p90 も `12.73%` から `10.43%` に縮むため、単独 production rule としては強すぎる。

### Why This Research Was Run

前段の `falling-knife-reversal-study` では、急落を拾う setup が平均ではプラスを残す一方、グロースや deep drawdown で severe loss が集中することが分かった。この follow-on では OHLC や event 定義を再計算せず、前段 event bundle を固定して、`catch_next_open` の 20d return に対して悪いサブセットを除外できるかを検証した。

### Data Scope / PIT Assumptions

入力 bundle は `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260427_110323_78e01df6`、input run id は `20260427_110323_78e01df6`。分析範囲は `2016-04-21 -> 2026-04-23`、horizon は `20` sessions、severe loss threshold は `-10.00%`、baseline trades は `155,548`。前段の signal は signal date close までの特徴量だけで作られ、entry は翌営業日 open なので、この pruning study も前段 event_df の PIT 前提を継承する。pruning rule は事後 return を条件に使わず、market、Daily Risk Adjusted Return bucket、falling-knife 条件 flag、condition_count で kept / removed を比較した。

### Main Findings

#### baseline は平均プラスだが、20d の severe loss が無視できない。

| Scope | Mean | Median | P10 | P90 | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline | `1.29%` | `0.50%` | `-10.42%` | `12.73%` | `10.62%` |

#### 第一候補は `exclude_growth_or_q5_rar`。左尾を下げつつ rebound exposure を残しやすい。

| Metric | Value |
| --- | ---: |
| removed trades | `46,583` |
| removed fraction | `29.95%` |
| kept trades | `108,965` |
| kept mean | `1.38%` |
| kept median | `0.70%` |
| kept p10 | `-9.09%` |
| kept p90 | `12.00%` |
| kept severe loss | `8.68%` |
| severe loss reduction | `1.94pt` |
| removed severe loss share | `42.74%` |

#### `exclude_deep_60d_drawdown` は左尾削減は最大だが、反発右尾も削りすぎる。

| Metric | Value |
| --- | ---: |
| removed trades | `35,354` |
| removed fraction | `22.73%` |
| kept severe loss | `8.21%` |
| kept mean | `0.90%` |
| kept median | `0.45%` |
| kept p90 | `10.43%` |

#### `exclude_growth` 単独でも有効だが、market hard exclusion としては別途検証が必要。

| Scope | Trades | Mean | Median | P10 | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: |
| kept after `exclude_growth` | n/a | `1.36%` | `0.62%` | n/a | `9.11%` |
| removed Growth segment | `20,944` | `0.88%` | `-0.78%` | `-16.19%` | `20.34%` |
| removed fraction | `13.46%` | n/a | n/a | n/a | n/a |

#### Daily Risk Adjusted Return は「低いほど悪い」ではなく、bucket の読み替えが必要。

| Bucket | Mean | Median | Severe loss |
| --- | ---: | ---: | ---: |
| `unbucketed` | n/a | n/a | `25.65%` |
| `Q5_highest` | n/a | n/a | `12.94%` |
| `Q1_lowest` | `2.01%` | `1.05%` | `11.65%` |

### Interpretation

bad-tail pruning は「一番大きく severe loss を減らす rule」を選ぶ問題ではない。`deep_60d_drawdown` や `condition_count >= 3` は危険局面をよく捕まえるが、同時に removed 側の mean や p90 が高く、急反発の右尾も削る。`exclude_growth_or_q5_rar` は、グロースの構造的な左尾と Daily RAR Q5/unbucketed 近辺の不安定さを合わせて取り除き、kept 側の median と p10 を改善しながら mean を維持できる点が実用的。

### Production Implication

falling-knife 系の production 化は、positive selection よりも bad-tail exclusion を先に入れる。第一候補は `market_name == グロース OR risk_adjusted_bucket == Q5_highest` の除外で、実装前には既存 strategy universe、売買頻度、capacity、保有重複への影響を確認する。より defensive な profile が必要な場合だけ `deep_60d_drawdown` 除外を追加検討するが、その場合は mean cost `0.40pt` と p90 低下を許容する前提にする。

### Caveats

この readout は `20260427_111758_4e990514` bundle の単一 run に基づく。manifest は `git_dirty: true` を示しており、入力 event bundle も local temporary snapshot 由来である。rule は 20d `catch_next_open` return の分布に対する後段評価で、実際の production では同時保有数、約定コスト、position sizing、既存 entry/exit との重なりで効果が薄まる可能性がある。`Q5_highest` の意味は前段の Daily Risk Adjusted Return bucket 定義に依存するため、lookback や bucket 生成方法を変えた場合は再検証が必要。

### Source Artifacts

- Bundle: `/tmp/trading25-research/market-behavior/falling-knife-bad-tail-pruning/20260427_111758_4e990514`
- Summary: `/tmp/trading25-research/market-behavior/falling-knife-bad-tail-pruning/20260427_111758_4e990514/summary.md`
- Published numbers: `/tmp/trading25-research/market-behavior/falling-knife-bad-tail-pruning/20260427_111758_4e990514/summary.json`
- Tables: `/tmp/trading25-research/market-behavior/falling-knife-bad-tail-pruning/20260427_111758_4e990514/results.duckdb` (`rule_summary_df`, `segment_summary_df`)
- Manifest: `/tmp/trading25-research/market-behavior/falling-knife-bad-tail-pruning/20260427_111758_4e990514/manifest.json`
- Input bundle: `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260427_110323_78e01df6`

## Purpose

`falling-knife-reversal-study` の event bundle を入力に、リバウンドの平均・右尾を残しながら severe loss を減らせる除外ルールを比較する。

この研究は OHLC を再計算しない。前段 research の `event_df` を固定し、`catch_next_open` の trade-level return 分布に対して rule ごとの kept / removed を集計する。

## Baseline

Default は `catch_return_20d` を使い、`-10%` 以下を severe loss とする。

比較する指標:

- kept fraction / removed fraction
- mean / median
- p10 / p90
- severe loss rate
- severe loss rate reduction
- mean return cost
- removed severe loss share

## Rule Candidates

初期候補:

- Growth market exclusion
- Daily Risk Adjusted Return `Q5_highest` exclusion
- `Q5_highest or unbucketed`
- `deep_60d_drawdown`
- `deep_20d_drop`
- `sharp_5d_drop`
- `condition_count >= 3`
- `condition_count >= 4`
- Growth x Daily Risk Adjusted Return `Q5_highest`
- Growth x `deep_60d_drawdown`
- Daily Risk Adjusted Return `Q5_highest` x `deep_60d_drawdown`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_bad_tail_pruning.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle
```

If `--input-bundle` is omitted, the latest `falling-knife-reversal-study` bundle under the same output root is used.

## Tables

- `rule_summary_df`: rule-level kept / removed comparison.
- `segment_summary_df`: market, Daily Risk Adjusted Return bucket, and condition-count segment distributions.

## Interpretation

The preferred rule is not necessarily the one with the largest severe-loss reduction. It should reduce severe-loss rate without destroying the median return or removing too much of the right-tail rebound.
