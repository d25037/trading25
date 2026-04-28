# Falling Knife Non-Rebound Fundamental Profile

## Published Readout

### Decision

この実験は除外 rule の即時採用ではなく、non-rebound を説明する feature shortlist として採用する。現時点で最も強い候補は `PBR >= 3x`、Growth market、Profit <= 0、`quality_score < 3`、FY actual EPS <= 0、forward PER >= 40x、forecast EPS <= 0。特に `PBR >= 3x` は sample 27,227、non-rebound rate 53.11%、baseline 47.15% に対する lift +5.95pt、severe loss rate 18.78% で、valuation 側の最有力 bad feature として次の rule 検証に進める。

### Why This Research Was Run

先行研究では落ちるナイフを拾った後の左尾が問題であり、単に「下落後は平均的に勝てるか」ではなく、「リバウンドしない銘柄にどんな fundamental profile が偏っているか」を見る必要があった。この研究は最適化ではなく記述分析として、20営業日後にプラスへ戻らない events に Growth、低 quality、赤字、negative EPS、高 valuation がどの程度偏るかを確認した。

### Data Scope / PIT Assumptions

入力は `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260427_110323_78e01df6` の event bundle。分析期間は 2016-04-21 から 2026-04-23、horizon は 20 sessions、rebound は `catch_return_20d > 0%`、non-rebound は `catch_return_20d <= 0%`、severe loss は `catch_return_20d <= -10%`。baseline events は 155,548、rebound は 82,205、non-rebound は 73,343、non-rebound rate は 47.15%、statement coverage は 99.56%。fundamental join は `disclosed_date <= signal_date` に限定してから latest row を選ぶ。PBR と trailing PER は signal date 以前に開示された latest FY BPS / actual EPS、forward PER は signal date 以前の latest non-null forecast EPS を使う。PER / forward PER の分母が非正の場合は missing に混ぜず、`non_positive_eps` / `non_positive_forecast_eps` として別 bucket にした。

### Main Findings

#### Growth market は non-rebound と severe loss が重い主要 segment。

| Metric | Value |
| --- | ---: |
| events | `20,944` |
| non-rebound rate | `53.75%` |
| median return | `-0.78%` |
| severe loss | `20.34%` |
| non-rebound prevalence | `15.35%` |
| rebound prevalence | `11.78%` |
| prevalence lift | `+3.56pt` |

#### `PBR >= 3x` は valuation 側で最も強い bad feature。

| Metric | Value |
| --- | ---: |
| events | `27,227` |
| non-rebound rate | `53.11%` |
| median return | `-0.46%` |
| severe loss | `18.78%` |
| prevalence lift | `+4.18pt` |

#### Profit <= 0 は単一の損益 sign として強い。

| Metric | Value |
| --- | ---: |
| events | `30,246` |
| non-rebound rate | `51.10%` |
| severe loss | `13.43%` |
| prevalence lift | `+3.08pt` |

#### low quality は広い bad-tail contributor だが、単独では lift が薄い。

| Slice | Events | Non-rebound rate | Severe loss | Prevalence lift |
| --- | ---: | ---: | ---: | ---: |
| `quality_score < 3` | `47,834` | `49.45%` | `12.70%` | `+2.83pt` |
| `missing statement or quality_score < 3` | `48,516` | `49.58%` | n/a | n/a |

#### EPS/forecast EPS の非正分母は missing ではなく bad feature として扱う。

| Slice | Events | Non-rebound rate | Severe loss |
| --- | ---: | ---: | ---: |
| FY actual EPS <= 0 | `24,019` | `51.70%` | `14.51%` |
| forecast EPS <= 0 | `10,242` | `52.70%` | `15.15%` |

#### 高 forward PER は悪く、低 forward PER は defensive bucket として残る。

| Bucket | Events | Non-rebound rate | Severe loss | Readout |
| --- | ---: | ---: | ---: | --- |
| forward PER >= 40x | `20,246` | `51.53%` | `14.73%` | bad-tail bucket |
| forward PER < 15x | n/a | n/a | n/a | non-rebound 側で under-represented |

#### 低PBRは defensive bucket として機能している。

| Bucket | Non-rebound rate | Severe loss |
| --- | ---: | ---: |
| PBR 0.5-1x | `44.04%` | `6.90%` |
| PBR < 0.5x | `42.98%` | `6.69%` |

#### Growth risk は quality score だけでは消えない。

| Growth quality slice | Events | Non-rebound rate | Severe loss |
| --- | ---: | ---: | ---: |
| low quality | `10,228` | `54.50%` | `20.77%` |
| high quality | `10,478` | `52.85%` | `19.44%` |

### Interpretation

non-rebound は Growth、赤字、低 quality、高 Daily RAR、negative forecast EPS、高 PBR、高 forward PER に偏っている。ただし、fundamental quality だけでは説明が足りない。Growth high quality でも non-rebound rate 52.85%、severe loss rate 19.44% であり、Growth の市場特性、sentiment regime、valuation unwind が quality score の外側に残っている。valuation では PBR >= 3x が forward PER >= 40x より強く、FY actual EPS <= 0 や forecast EPS <= 0 は「PER missing」ではなく、それ自体が bad feature として扱うべき。

### Production Implication

次の production 候補は、Growth や high Daily RAR に quality/valuation 条件を重ねた risk filter として検証する。具体的には `PBR >= 3x`、Profit <= 0、FY actual EPS <= 0、forecast EPS <= 0、forward PER >= 40x を feature candidate にし、PBR < 1x や forward PER < 15x は defensive bucket として分けて見る。単独 feature の hard exclusion はまだ早く、portfolio-level drawdown、turnover、market split、capacity を確認してから production YAML へ反映する。

### Caveats

この分析は descriptive profile であり、因果推定や最適化済み rule ではない。event-level の 20営業日 outcome なので、同時保有、資金配分、約定、手数料、売買容量は未反映。valuation は PIT-safe に FY row を使うよう修正済みだが、PBR/PER は業種差や資本構成差を調整していない。actual EPS missing は 2.59%、actual EPS <= 0 は 15.44%、numeric PER null は 18.03%。forecast EPS missing は 2.66%、forecast EPS <= 0 は 9.05%、numeric forward PER null は 11.71% であり、coverage gap と非正 EPS を混同しない前提で読む必要がある。

### Source Artifacts

- Baseline note: `apps/bt/docs/experiments/market-behavior/falling-knife-non-rebound-fundamental-profile/baseline-2026-04-27.md`
- Output bundle: `/tmp/trading25-research/market-behavior/falling-knife-non-rebound-fundamental-profile/20260427_133647_9bc2b39c`
- Summary markdown: `/tmp/trading25-research/market-behavior/falling-knife-non-rebound-fundamental-profile/20260427_133647_9bc2b39c/summary.md`
- Summary JSON: `/tmp/trading25-research/market-behavior/falling-knife-non-rebound-fundamental-profile/20260427_133647_9bc2b39c/summary.json`
- Input bundle: `/tmp/trading25-research/market-behavior/falling-knife-reversal-study/20260427_110323_78e01df6`

## Purpose

`falling-knife-reversal-study` の次の問いを、除外ルール最適化ではなく特徴分析として扱う。

> 落ちるナイフの中で、リバウンドしない銘柄にはどんなファンダ的特徴があるか？

Primary label は `catch_return_{horizon}d <= rebound_threshold`。Default では 20営業日後 return が 0% 以下なら `non_rebound` とする。`severe_loss` は副次的な tail diagnostic として残す。

## Feature Construction

入力は `falling-knife-reversal-study` の `event_df`。各 `signal_date` 時点で開示済みの最新 `statements` 行だけを join する。

- `disclosed_date <= signal_date` で切ってから latest row を選ぶ
- forecast EPS / Profit / OperatingCashFlow / simple FCF margin / equity ratio から `quality_score` を作る
- signal date の `close` と PIT-safe valuation inputs から PER / forward PER / PBR を作る
  - PBR: latest FY BPS disclosed on or before `signal_date`
  - PER: latest FY actual EPS disclosed on or before `signal_date`
  - forward PER: latest non-null forecast EPS disclosed on or before `signal_date`
  - PER / forward PER with non-positive denominator are kept as separate
    `non_positive_eps` / `non_positive_forecast_eps` buckets instead of being
    mixed into `missing`.
- `quality_score >= 3` を default の `high_quality` とする

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_non_rebound_fundamental_profile.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle
```

Useful options:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_falling_knife_non_rebound_fundamental_profile.py \
  --input-bundle /path/to/falling-knife-reversal-study-bundle \
  --horizon-days 20 \
  --rebound-threshold 0.0 \
  --severe-loss-threshold -0.10 \
  --min-quality-score 3
```

## Tables

- `enriched_event_df`: falling-knife event plus PIT-safe statement features and `non_rebound` / `rebound` labels.
- `fundamental_profile_summary_df`: segment-level non-rebound rate by market, quality, forecast/profit/CFO/FCF/equity/valuation buckets.
- `feature_lift_summary_df`: binary feature prevalence in non-rebound vs rebound groups, relative risk, odds ratio, and return diagnostics.

## Baselines

- [baseline-2026-04-27.md](./baseline-2026-04-27.md)

## Interpretation

This experiment is designed to answer the descriptive question first. A good next strategy rule should come after identifying which fundamental features are over-represented among non-rebound events, not before.
