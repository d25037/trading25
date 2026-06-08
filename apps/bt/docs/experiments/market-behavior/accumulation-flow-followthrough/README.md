# Accumulation Flow Followthrough

## Published Readout

### Decision
- PIT-safe rerun completed. 旧 current-market / scale proxy headline は撤回し、`20260608_pit_safe_topix500_v2` の結果で置き換える。

### Why This Research Was Run
- CMF / Chaikin oscillator / OBV flow score による accumulation proxy が、next-open to future-close return と TOPIX excess return に残るかを PIT-safe universe で再検証する。
- 旧 readout は current market / scale proxy を使っていたため、TOPIX500 / Prime ex TOPIX500 を signal-date `stock_master_daily,index_membership_daily` に直して rerun した。

### Data Scope / PIT Assumptions
- Run ID: `20260608_pit_safe_topix500_v2`
- Analysis range: `2016-06-06 -> 2026-06-05`
- Universe source: `stock_master_daily,index_membership_daily`
- Membership mode: `signal_date_stock_master_index_membership`
- As-of policy: signal date の `stock_master_daily` と `index_membership_daily.index_code = TOPIX500` を使う。latest stock master fallback は使わない。
- Feature policy: CMF / Chaikin / OBV / SMA / recent high / lower wick は同日以前の OHLCV だけで計算する。

### Main Findings
#### 結論: full-period 20d excess は Standard が最も残る

| Universe | Filter | Dates | Signals | Mean cohort | Excess | Excess win |
| --- | --- | ---:| ---:| ---:| ---:| ---:|
| Standard | Accumulation + not extended | `2377` | `505,914` | `+1.09%` | `+0.075%` | `50.86%` |
| Standard | Accumulation + not extended + lower wick | `2377` | `150,161` | `+1.06%` | `+0.073%` | `49.18%` |
| Standard | Accumulation pressure | `2417` | `1,173,860` | `+1.05%` | `+0.020%` | `51.18%` |
| PRIME ex TOPIX500 | Accumulation pressure | `2417` | `1,545,086` | `+0.96%` | `-0.048%` | `51.72%` |
| TOPIX500 | Accumulation pressure | `2417` | `549,744` | `+0.90%` | `-0.107%` | `47.87%` |

#### 結論: 2024-forward OOS は cap 付き TOPIX500 / Prime ex TOPIX500 が強い

| Variant | Universe | Filter | Cap | Dates | Mean cohort | Excess | Excess win |
| --- | --- | --- | ---:| ---:| ---:| ---:| ---:|
| cap_10 | TOPIX500 | Accumulation pressure | `10` | `571` | `+2.23%` | `+0.61%` | `54.99%` |
| cap_50 | Standard | Accumulation + not extended + lower wick | `50` | `571` | `+1.98%` | `+0.38%` | `40.63%` |
| cap_10 | PRIME ex TOPIX500 | Accumulation pressure | `10` | `571` | `+1.98%` | `+0.37%` | `51.14%` |
| cap_25 | PRIME ex TOPIX500 | Accumulation pressure | `25` | `571` | `+1.94%` | `+0.32%` | `55.17%` |
| cap_25 | TOPIX500 | Accumulation pressure | `25` | `571` | `+1.91%` | `+0.29%` | `53.06%` |

### Interpretation
- full-period では Standard の `not_extended` branch が最も素直に excess を残すが、signal 数が非常に多く、edge は薄い。
- OOS 2024-forward では、concentration cap を入れた TOPIX500 / Prime ex TOPIX500 の accumulation pressure が強い。これは daily Ranking / signal family diagnostic として残す価値がある。
- lower-wick branch は Standard で候補に残るが、excess win が低く、単独採用より cap / liquidity / market-state 条件との組み合わせが必要。

### Production Implication
- production 候補は `cap_10/25 × TOPIX500 or PRIME ex TOPIX500 × accumulation_pressure` を優先する。
- Standard は universe-wide diagnostic としては有用だが、signal 数が多すぎるため、liquidity / crowding / market-state で further pruning してから使う。

### Caveats
- accumulation proxy は日足 OHLCV から推定した買い集め proxy で、実注文主体は観測していない。
- 旧 baseline の数値は下の既存セクションに残るが、この `Published Readout` より優先しない。

### Source Artifacts
- Experiment: `market-behavior/accumulation-flow-followthrough`
- Runner: `apps/bt/scripts/research/run_accumulation_flow_followthrough.py`
- Domain logic: `apps/bt/src/domains/analytics/accumulation_flow_followthrough.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/accumulation-flow-followthrough/20260608_pit_safe_topix500_v2/`
- Bundle tables: `universe_summary_df`, `event_summary_df`, `cohort_portfolio_summary_df`, `capped_cohort_portfolio_summary_df`, `oos_portfolio_summary_df`
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

## Question

CMF / Chaikin Oscillator / OBV で「価格上昇前の買い集め proxy」を作り、翌営業日寄りからその後の終値までの return に再現性があるかを調べる。

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_accumulation_flow_followthrough.py
```

主な option:

- `--start-date` / `--end-date`: signal date の分析範囲。
- `--horizons`: `5,10,20,60` のような forward close horizon。
- `--chaikin-fast-period` / `--chaikin-slow-period`: Chaikin oscillator の ADL EMA 期間。
- `--cmf-threshold` / `--chaikin-oscillator-threshold` / `--obv-score-threshold`: 各 flow proxy の成立閾値。
- `--min-votes`: CMF / Chaikin / OBV のうち必要な成立数。default は `2`。
- `--max-close-to-sma` / `--max-close-to-high`: まだ伸び切っていない価格状態の制約。
- `--lower-wick-threshold`: 下ヒゲ吸収 branch の閾値。
- `--concentration-caps`: entry date ごとの上位N銘柄 cap。default は `10,25,50`。

## Indicator Definitions

- `CMF`: `sum(money_flow_multiplier * volume) / sum(volume)`。
- `Chaikin oscillator`: `EMA(ADL, fast) - EMA(ADL, slow)`。CMF と同じ money flow multiplier を使うが、rolling 比率ではなく ADL の短期加速を測る。
- `OBV flow score`: `OBV` の lookback 差分 / 同期間の出来高合計。

## Output

Bundle は `manifest.json` / `results.duckdb` / `summary.md` を出力する。

主要 table:

- `universe_summary_df`: universe 別の銘柄数、stock-days、branch 別 event count。
- `event_df`: signal date 単位のイベント明細。filter branch は重複行として保持する。
- `event_summary_df`: universe / branch / horizon の trade-level 集計。
- `yearly_summary_df`: 年別の trade-level 集計。
- `entry_cohort_df`: 同一 entry date の signal を等ウェイト化した日次 cohort。
- `cohort_portfolio_summary_df`: entry cohort の portfolio lens 集計。
- `yearly_cohort_summary_df`: entry cohort の年別 portfolio lens 集計。
- `capped_entry_cohort_df`: signal strength 順に entry date ごとの銘柄数を cap した cohort。
- `capped_cohort_portfolio_summary_df`: concentration cap ごとの portfolio lens 集計。
- `oos_portfolio_summary_df`: `2016-2020` discovery / `2021-2023` validation / `2024-forward` OOS の固定期間別 portfolio lens。

Concentration cap の順位付けは、同日までに観測できる `accumulation_vote_count`、`CMF`、`OBV flow score`、`Chaikin oscillator`、`lower_wick_ratio` の順に使う。同一条件の最後は `code` で安定ソートする。

## Interpretation

この研究の CMF / Chaikin / OBV は機関投資家の実際の注文主体を直接観測するものではなく、日足 OHLCV から見た accumulation proxy として扱う。採用判断では `event_summary_df` の trade-level 平均だけでなく、同日に signal が集中したときの影響を見る `cohort_portfolio_summary_df`、TOPIX 同期間 return を差し引く `mean_excess_return`、および concentration cap / OOS の robustness を優先して確認する。

PIT 面では、signal date の特徴量は同日以前の OHLCV だけで計算し、future return は評価列として後から付与する。universe membership は signal-date `stock_master_daily` と `index_membership_daily` を使い、latest stock master fallback は使わない。
