# Falling Knife Fundamental Quality Pruning

> Historical pre-v4 readout. The schema v3 parent bundle and recorded counts
> below are provenance only, not current production evidence. Any rerun or
> adoption decision must use Market schema v4 /
> `local_projection_v2_event_time` with exact event-time basis lineage and
> publish a new parent and child readout.

## Published Readout

### Decision

現時点の採用候補は「low quality を一律除外」ではなく、PIT master v3 rerun でも `Growth or Daily RAR Q5 highest when low quality` を次の portfolio-lens 検証に回す。この rule は 65,804 events のうち 86.86% を残し、20営業日 severe loss rate を baseline 8.31% から 7.18% へ下げながら、mean return は +2.02% から +2.08%、median return は +0.74% から +0.86% に改善した。`exclude_missing_or_low_quality` は severe loss rate を 6.88% まで下げるが、34.32% の events を落とすため、初期の production rule としては広すぎる。

### Why This Research Was Run

先行の falling-knife study では Growth market の左尾が悪く、Growth を単純に避けるべきか、Growth に多い低 quality 銘柄だけを避ければ足りるのかが未分解だった。この実験は、落ちるナイフの bad tail が市場区分そのものから来ているのか、forecast EPS、利益、営業CF、FCF margin、自己資本比率で見た fundamental quality から来ているのかを切り分けるために実行した。

### Data Scope / PIT Assumptions

入力は `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260506_falling_knife_reversal_v3_pit_master` の event bundle。分析期間は 2022-05-02 から 2026-04-30、horizon は 20 sessions、severe loss は `catch_return_20d <= -10%`、baseline events は 65,804。statement coverage は 94.13%。前段 event は `market.duckdb` v3 の `stock_data` と `stock_master_daily` の同日 join で作られている。fundamental join は各 `signal_date` 時点で `disclosed_date <= signal_date` の行だけに限定し、その後で最新 row を選んでいる。`quality_score` は forecast EPS > 0、Profit > 0、OperatingCashFlow > 0、simple FCF margin > 0、equity ratio >= 30% の5条件の合計で、`quality_score >= 3` を high quality とした。

### Main Findings

#### baseline は平均プラスだが、20d severe loss が pruning 対象になる。

| Scope | Mean | Median | P10 | P90 | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline | `+2.02%` | `+0.74%` | `-8.86%` | `+12.99%` | `8.31%` |

#### `exclude_missing_or_low_quality` は左尾を最も下げる quality-only rule だが、削除量が大きすぎる。

| Metric | Value |
| --- | ---: |
| kept events | `43,220` |
| kept fraction | `65.68%` |
| kept severe loss | `6.88%` |
| removed severe loss | `11.05%` |
| removed fraction | `34.32%` |

#### `exclude_low_quality` も有効だが、単独 rule では trade 削減が大きい。

| Metric | Value |
| --- | ---: |
| kept events | `47,085` |
| kept fraction | `71.55%` |
| kept severe loss | `7.10%` |
| kept mean | `+2.18%` |

#### 現時点の候補は `exclude_growth_or_q5_rar_low_quality`。削除量を抑えながら左尾の濃い部分を取れる。

| Metric | Value |
| --- | ---: |
| kept events | `57,156` |
| kept fraction | `86.86%` |
| kept severe loss | `7.18%` |
| removed severe loss | `15.77%` |
| removed median | `-0.26%` |
| removed fraction | `13.14%` |

#### Growth risk は low quality だけでは説明できない。

| Growth slice | Events | Mean | Median | Severe loss |
| --- | ---: | ---: | ---: | ---: |
| overall | `13,725` | `+1.49%` | `-0.24%` | `16.54%` |
| low quality | `5,980` | `+1.65%` | `-0.66%` | `17.81%` |
| high quality | `6,972` | `+1.27%` | `0.00%` | `15.13%` |

#### Profit non-positive は単一 fundamental sign として強いが、Growth の左尾全体は説明しきれない。

| Slice | Events | Severe loss |
| --- | ---: | ---: |
| Profit non-positive | `12,773` | `12.49%` |

### Interpretation

fundamental quality は bad tail の一部を説明するが、Growth risk の代理変数ではない。Growth high quality の severe loss rate は 15.13% と Growth low quality の 17.81% より低いが、non-Growth baseline よりまだ重い。Growth market には quality score だけでは拾えない listing stage、liquidity、valuation support、期待剥落のような効果が残っている。したがって、現時点では「低 quality を全部落とす」よりも、「Growth または Daily RAR Q5 で、かつ low quality の時だけ避ける」方が、削除量と左尾削減のバランスが良い。

### Production Implication

production strategy へ直入れするなら、まずは hard exclusion ではなく risk gate 候補として扱うのが妥当。次の検証では `exclude_growth_or_q5_rar_low_quality` を portfolio lens、同時保有制限、market split、capacity diagnostics と組み合わせ、severe loss rate の低下が portfolio-level drawdown と turnover に効くかを確認する。low quality 全除外は coverage と機会損失が大きいため、現段階では production default にしない。

### Caveats

この readout は event-level の 20営業日 outcome であり、同時ポジション、資金配分、約定制約、手数料、capacity はまだ見ていない。quality score は粗い5条件の合計で、業種差や財務項目の季節性は未調整。Growth high quality の残存リスクは確認できたが、market cap、ADV、valuation、需給などの追加特徴でさらに分解する必要がある。statement coverage は 94.13% で、missing statement は tail が重いため、missing の扱いは production で別途保守的に決める。

### Source Artifacts

- Baseline note: `apps/bt/docs/experiments/market-behavior/falling-knife-fundamental-quality-pruning/baseline-2026-04-27.md`
- Output bundle: `/tmp/trading25-research/market-behavior/falling-knife-fundamental-quality-pruning/20260506_falling_knife_fundamental_quality_v3_pit_master`
- Summary markdown: `/tmp/trading25-research/market-behavior/falling-knife-fundamental-quality-pruning/20260506_falling_knife_fundamental_quality_v3_pit_master/summary.md`
- Legacy structured digest was removed from the publication surface; use this README and the bundle `results.duckdb` / `summary.md` instead.
- Input bundle: `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260506_falling_knife_reversal_v3_pit_master`

## Purpose

`falling-knife-bad-tail-pruning` で見えた Growth market の悪い左尾が、市場区分そのものなのか、Growth に多い fundamental quality の低さなのかを分解する。

この研究は `falling-knife-reversal-study` の `event_df` を入力にし、各 `signal_date` 時点で開示済みの最新 `statements` 行だけを join する。`latest per event` は `disclosed_date <= signal_date` の後に選ぶ。

## Quality Definition

初期の `quality_score` は以下5条件の合計。

- forecast EPS > 0
- Profit > 0
- OperatingCashFlow > 0
- simple FCF margin > 0
- equity ratio >= 30%

Default では `quality_score >= 3` を `high_quality`、それ未満を `low_quality` とする。開示済み statement が無い event は `missing_statement`。

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_fundamental_quality_pruning.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle
```

Useful options:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_fundamental_quality_pruning.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle \
  --horizon-days 20 \
  --severe-loss-threshold -0.10 \
  --min-quality-score 3
```

## Tables

- `enriched_event_df`: falling-knife event plus PIT-safe statement features and quality labels.
- `quality_segment_summary_df`: market / quality / forecast / profit / CFO / FCF / equity bucket summaries.
- `quality_rule_summary_df`: exclusion-rule kept / removed comparison.

## Baselines

- [baseline-2026-04-27.md](./baseline-2026-04-27.md)

## Interpretation

The key question is whether Growth can be kept when quality is high, and whether non-Growth low-quality names still carry bad-tail risk. A useful rule should reduce severe-loss rate without removing the rebound right tail mechanically.
